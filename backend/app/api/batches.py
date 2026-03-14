from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.services.leadspicker_normalize import _normalize_url_for_dedup

router = APIRouter(prefix="/batches", tags=["batches"])

# ---------------------------------------------------------------------------
# Select clause for entries-full — everything the analysis view needs
# ---------------------------------------------------------------------------
_ENTRIES_SELECT = (
    "id,status,relevant,learning_data,ai_pre_score,ai_chat_state,"
    "batch_id,pipeline_type,signal_id,contact_id,"
    "analyzed_at,enriched_at,pushed_at,created_at,"
    "signals(id,content_url,content_title,content_text,content_summary,ai_classifier,"
    "author_name,published_at,"
    "source_robot,external_id,company_id,source_metadata,"
    "companies(id,name_raw,name_normalized,domain_normalized,"
    "linkedin_url,linkedin_url_cleaned,website,country,"
    "employee_count,industry,hq_location,fingerprint)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company)"
)


def _normalize_crunchbase_status(entry: dict) -> dict:
    """
    Compatibility shim:
    if DB enum doesn't yet include 'pushed-ready', we persist that marker
    in signals.source_metadata.entry_workflow_status and expose it as status
    in API responses for workflow correctness.
    """
    if entry.get("pipeline_type") != "crunchbase":
        return entry
    sig = entry.get("signals") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        return entry

    status = str(entry.get("status") or "").strip().lower()
    marker = str(meta.get("entry_workflow_status") or "").strip().lower()
    if marker == "pushed-ready" and status in {"new", "analyzed", "drafted"}:
        entry["status"] = "pushed-ready"
    return entry


# ---------------------------------------------------------------------------
# GET /api/batches  — list with progress stats
# ---------------------------------------------------------------------------
@router.get("")
async def list_batches(db: AsyncClient = Depends(get_supabase)):
    """All import batches newest-first, with label-progress stats."""
    batches_res = (
        await db.table("import_batches")
        .select("*")
        .order("imported_at", desc=True)
        .execute()
    )
    batches = batches_res.data
    if not batches:
        return []

    # Fetch (batch_id, relevant) for ALL entries — aggregate in Python.
    # Paginate to bypass PostgREST 1000-row default limit.
    from collections import defaultdict

    all_entries: list[dict] = []
    pg_size = 1000
    pg_offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select("batch_id,relevant")
            .neq("status", "eliminated")
            .range(pg_offset, pg_offset + pg_size - 1)
            .execute()
        )
        chunk = res.data or []
        all_entries.extend(chunk)
        if len(chunk) < pg_size:
            break
        pg_offset += pg_size

    stats: dict = defaultdict(lambda: {"yes": 0, "no": 0, "cc": 0, "unlabeled": 0, "total": 0})
    for e in all_entries:
        bid = e["batch_id"]
        rel = e.get("relevant")
        stats[bid]["total"] += 1
        if rel in ("yes", "no", "cc"):
            stats[bid][rel] += 1
        else:
            stats[bid]["unlabeled"] += 1

    return [
        {**b, "progress": stats.get(b["id"], {"yes": 0, "no": 0, "cc": 0, "unlabeled": 0, "total": 0})}
        for b in batches
    ]


# ---------------------------------------------------------------------------
# GET /api/batches/{batch_id}/entries-full
# ---------------------------------------------------------------------------
@router.get("/{batch_id}/entries-full")
async def get_entries_full(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    Return ALL non-eliminated entries for a batch with full signal/company/contact join.
    Ordered by ai_pre_score DESC NULLS LAST. Paginated transparently (PostgREST 1000-row limit).
    """
    entries = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select(_ENTRIES_SELECT)
            .eq("batch_id", batch_id)
            .neq("status", "eliminated")
            .order("ai_pre_score", desc=True, nullsfirst=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = res.data or []
        entries.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    return [_normalize_crunchbase_status(e) for e in entries]


# ---------------------------------------------------------------------------
# GET /api/batches/{batch_id}/context
# ---------------------------------------------------------------------------
@router.get("/{batch_id}/context")
async def get_batch_context(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    Returns two datasets the frontend uses for local matching:
      - blacklist: all blacklisted company names
      - contacted_fingerprints: company fingerprints already pushed in this pipeline_type
    """
    batch_res = (
        await db.table("import_batches")
        .select("id,pipeline_type")
        .eq("id", batch_id)
        .single()
        .execute()
    )
    if not batch_res.data:
        raise HTTPException(status_code=404, detail="Batch not found")
    pipeline_type = batch_res.data["pipeline_type"]

    # Blacklist
    blacklist_res = (
        await db.table("blacklisted_companies")
        .select("company_name,company_name_normalized")
        .execute()
    )

    # Contacted fingerprints: pushed entries for this pipeline_type (paginated)
    contacted: list[dict] = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select("pushed_at,signals(companies(fingerprint,name_raw))")
            .eq("pipeline_type", pipeline_type)
            .eq("status", "pushed")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = res.data or []
        for row in chunk:
            sig = row.get("signals") or {}
            co = sig.get("companies") or {}
            fp = co.get("fingerprint")
            if fp:
                contacted.append(
                    {
                        "fingerprint": fp,
                        "company_name": co.get("name_raw"),
                        "pushed_at": row.get("pushed_at"),
                    }
                )
        if len(chunk) < page_size:
            break
        offset += page_size

    return {
        "blacklist": blacklist_res.data,
        "contacted_fingerprints": contacted,
    }


# ---------------------------------------------------------------------------
# POST /api/batches/{batch_id}/finish-labeling
# ---------------------------------------------------------------------------
@router.post("/{batch_id}/finish-labeling")
async def finish_labeling(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    Archive all labeled entries from this batch to labeling_memory.
    Upsert on (pipeline_type, dedup_key) — overwrites label if the same URL
    was labeled again. Returns {total_labeled, written}.
    """
    batch_res = (
        await db.table("import_batches")
        .select("id,pipeline_type")
        .eq("id", batch_id)
        .single()
        .execute()
    )
    if not batch_res.data:
        raise HTTPException(status_code=404, detail="Batch not found")
    pipeline_type = batch_res.data["pipeline_type"]

    # Fetch labeled entries with their signal's content_url
    labeled_res = (
        await db.table("pipeline_entries")
        .select("relevant,signals(content_url)")
        .eq("batch_id", batch_id)
        .filter("relevant", "not.is", "null")
        .execute()
    )
    labeled = labeled_res.data

    if not labeled:
        return {"total_labeled": 0, "written": 0}

    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for e in labeled:
        sig = e.get("signals") or {}
        content_url = sig.get("content_url")
        dedup_key = _normalize_url_for_dedup(content_url)
        if not dedup_key:
            continue
        rows.append(
            {
                "pipeline_type": pipeline_type,
                "dedup_key": dedup_key,
                "label_value": e["relevant"],
                "labeled_at": now,
            }
        )

    if rows:
        await (
            db.table("labeling_memory")
            .upsert(rows, on_conflict="pipeline_type,dedup_key")
            .execute()
        )

    return {"total_labeled": len(labeled), "written": len(rows)}


# ---------------------------------------------------------------------------
# Select clause for draft-entries-full (extends _ENTRIES_SELECT with messages
# and contact.company_id for is_from_company computation)
# ---------------------------------------------------------------------------
_DRAFT_ENTRIES_SELECT = (
    "id,status,relevant,learning_data,ai_pre_score,ai_chat_state,"
    "batch_id,pipeline_type,signal_id,contact_id,"
    "analyzed_at,enriched_at,pushed_at,drafted_at,created_at,"
    "signals(id,content_url,content_title,content_text,content_summary,ai_classifier,"
    "author_name,published_at,"
    "source_robot,external_id,company_id,source_metadata,"
    "companies(id,name_raw,name_normalized,domain_normalized,"
    "linkedin_url,linkedin_url_cleaned,website,country,"
    "employee_count,industry,hq_location,fingerprint)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company,company_id),"
    "messages(id,draft_text,final_text,email_subject,ai_generated,version,created_at,updated_at)"
)


def _is_post_author(contact: dict, signal: dict) -> bool:
    """
    True when the contact's LinkedIn profile slug appears in the signal content_url.
    LP post URLs embed the author's profile ID in the path segment after /posts/.
    e.g. content_url = '.../posts/ezraalexandercohen-lucidcircus_...'
         linkedin_url = '.../in/ezraalexandercohen-lucidcircus'
    """
    li = (contact.get("linkedin_url") or "").rstrip("/")
    url = (signal.get("content_url") or "")
    if not li or not url:
        return False
    slug = li.split("/")[-1].lower()
    return bool(slug and slug in url.lower())


def _is_from_company(contact: dict, signal: dict) -> bool:
    """True when the contact's company_id matches the signal's company_id."""
    return bool(
        contact.get("company_id")
        and contact["company_id"] == signal.get("company_id")
    )


# ---------------------------------------------------------------------------
# POST /api/batches/{batch_id}/start-drafting
# ---------------------------------------------------------------------------
@router.post("/{batch_id}/start-drafting")
async def start_drafting(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    For all relevant (yes/cc) entries in the batch:
      - Create a messages record if one doesn't exist yet
      - Set status = 'drafted'
    Returns {created, total_relevant}.
    """
    # 1. Get all relevant entries
    entries_res = (
        await db.table("pipeline_entries")
        .select("id")
        .eq("batch_id", batch_id)
        .in_("relevant", ["yes", "cc"])
        .execute()
    )
    relevant_ids = [r["id"] for r in (entries_res.data or [])]
    if not relevant_ids:
        return {"created": 0, "total_relevant": 0}

    # 2. Find which ones already have a message
    existing_res = (
        await db.table("messages")
        .select("pipeline_entry_id")
        .in_("pipeline_entry_id", relevant_ids)
        .execute()
    )
    already_have = {r["pipeline_entry_id"] for r in (existing_res.data or [])}
    new_ids = [eid for eid in relevant_ids if eid not in already_have]

    # 3. Bulk INSERT message stubs for new entries
    if new_ids:
        msg_rows = [
            {"pipeline_entry_id": eid, "ai_generated": False, "version": 1}
            for eid in new_ids
        ]
        await db.table("messages").insert(msg_rows).execute()

    # 4. Bulk UPDATE entry status to 'drafted'
    now = datetime.now(timezone.utc).isoformat()
    # Chunk to stay within PostgREST IN limits
    from app.services.leadspicker_normalize import _chunks
    for chunk in _chunks(relevant_ids, 500):
        await (
            db.table("pipeline_entries")
            .update({"status": "drafted", "drafted_at": now})
            .in_("id", chunk)
            .execute()
        )

    return {"created": len(new_ids), "total_relevant": len(relevant_ids)}


# ---------------------------------------------------------------------------
# GET /api/batches/{batch_id}/draft-entries-full
# ---------------------------------------------------------------------------
@router.get("/{batch_id}/draft-entries-full")
async def get_draft_entries_full(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    All relevant entries for drafting with full join + messages.
    Includes computed contextual fields:
      is_post_author  — contact's LinkedIn slug appears in signal content_url
      is_from_company — contact.company_id matches signal.company_id
    """
    entries = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select(_DRAFT_ENTRIES_SELECT)
            .eq("batch_id", batch_id)
            .in_("relevant", ["yes", "cc"])
            .neq("status", "eliminated")
            .order("ai_pre_score", desc=True, nullsfirst=False)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = res.data or []
        entries.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    # Compute contextual booleans server-side
    for e in entries:
        _normalize_crunchbase_status(e)
        sig = e.get("signals") or {}
        ct = e.get("contacts") or {}
        e["is_post_author"] = _is_post_author(ct, sig)
        e["is_from_company"] = _is_from_company(ct, sig)
        # Flatten messages list → single latest message (highest version)
        msgs = e.get("messages") or []
        if msgs:
            e["message"] = max(msgs, key=lambda m: m.get("version", 0))
        else:
            e["message"] = None

    return entries


# ---------------------------------------------------------------------------
# POST /api/batches/{batch_id}/finish-drafting
# ---------------------------------------------------------------------------
@router.post("/{batch_id}/finish-drafting")
async def finish_drafting(batch_id: str, db: AsyncClient = Depends(get_supabase)):
    """
    For all drafted entries that have a non-empty draft_text:
      - Copy draft_text → final_text in messages
      - Set pipeline_entries.drafted_at (refresh timestamp)
    Returns {total_drafted, finalized, skipped_empty}.
    """
    # Fetch all drafted entries with their message
    entries_res = (
        await db.table("pipeline_entries")
        .select("id,messages(id,draft_text)")
        .eq("batch_id", batch_id)
        .in_("relevant", ["yes", "cc"])
        .execute()
    )

    now = datetime.now(timezone.utc).isoformat()
    finalized = 0
    skipped = 0
    for e in entries_res.data or []:
        msgs = e.get("messages") or []
        if not msgs:
            skipped += 1
            continue
        msg = max(msgs, key=lambda m: m.get("version", 0))
        draft = (msg.get("draft_text") or "").strip()
        if not draft:
            skipped += 1
            continue
        await (
            db.table("messages")
            .update({"final_text": draft})
            .eq("id", msg["id"])
            .execute()
        )
        finalized += 1

    total = len(entries_res.data or [])
    return {"total_drafted": total, "finalized": finalized, "skipped_empty": skipped}
