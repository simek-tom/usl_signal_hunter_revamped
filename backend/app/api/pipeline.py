"""
Pipeline-level endpoints (reservoir architecture).

Operates across all batches for a given pipeline_key, bypassing
the per-batch paradigm. Batches are retained as import metadata only.
"""

import asyncio
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase

# Reuse select clauses and helpers from batches.py
from app.api.batches import (
    _DRAFT_ENTRIES_SELECT,
    _normalize_crunchbase_status,
    _is_post_author,
    _is_from_company,
)

router = APIRouter(prefix="/pipeline", tags=["pipeline"])

# Staging table map (mirrors staging.py)
_SOURCE_TABLE_MAP = {
    "leadspicker": "staging_leadspicker",
    "crunchbase": "staging_crunchbase",
    "news": "staging_news",
}


async def _get_source_type(db: AsyncClient, pipeline_key: str) -> str:
    res = (
        await db.table("pipeline_configs")
        .select("source_type")
        .eq("pipeline_key", pipeline_key)
        .limit(1)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail=f"Pipeline not found: {pipeline_key}")
    return res.data[0]["source_type"]


# ---------------------------------------------------------------------------
# GET /api/pipeline/{pipeline_key}/stats
# ---------------------------------------------------------------------------
@router.get("/{pipeline_key}/stats")
async def get_pipeline_stats(
    pipeline_key: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Aggregate stats for the reservoir sections:
      staging  — total, yes, no, cc, unlabeled
      entries  — total, drafted, pushed
    """
    source_type = await _get_source_type(db, pipeline_key)
    table = _SOURCE_TABLE_MAP.get(source_type)
    if not table:
        raise HTTPException(status_code=422, detail=f"Unknown source_type: {source_type}")

    # Staging counts — parallel count queries (avoids 1000-row fetch limit)
    def _staging_count(label_filter=None):
        q = db.table(table).select("*", count="exact").eq("pipeline_key", pipeline_key).limit(0)
        if label_filter == "unlabeled":
            q = q.is_("label", "null")
        elif label_filter:
            q = q.eq("label", label_filter)
        return q.execute()

    s_total, s_yes, s_no, s_cc, s_unlabeled = await asyncio.gather(
        _staging_count(),
        _staging_count("yes"),
        _staging_count("no"),
        _staging_count("cc"),
        _staging_count("unlabeled"),
    )
    staging_stats = {
        "total":     s_total.count or 0,
        "yes":       s_yes.count or 0,
        "no":        s_no.count or 0,
        "cc":        s_cc.count or 0,
        "unlabeled": s_unlabeled.count or 0,
    }

    # Pipeline entries counts — parallel count queries
    def _entry_count(status_filter=None):
        q = (
            db.table("pipeline_entries")
            .select("*", count="exact")
            .eq("pipeline_type", pipeline_key)
            .neq("status", "eliminated")
            .limit(0)
        )
        if status_filter:
            q = q.eq("status", status_filter)
        return q.execute()

    e_total, e_drafted, e_pushed = await asyncio.gather(
        _entry_count(),
        _entry_count("drafted"),
        _entry_count("pushed"),
    )
    entries_stats = {
        "total":   e_total.count or 0,
        "drafted": e_drafted.count or 0,
        "pushed":  e_pushed.count or 0,
    }

    return {"staging": staging_stats, "entries": entries_stats}


# ---------------------------------------------------------------------------
# GET /api/pipeline/{pipeline_key}/draft-entries-full
# ---------------------------------------------------------------------------
@router.get("/{pipeline_key}/draft-entries-full")
async def get_pipeline_draft_entries(
    pipeline_key: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    All relevant (yes/cc) pipeline_entries for this pipeline, with full
    join + messages. Mirrors batches.get_draft_entries_full but pipeline-scoped.
    """
    from app.services.leadspicker_normalize import _chunks

    entries = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select(_DRAFT_ENTRIES_SELECT)
            .eq("pipeline_type", pipeline_key)
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

    for e in entries:
        _normalize_crunchbase_status(e)
        sig = e.get("signals") or {}
        ct = e.get("contacts") or {}
        e["is_post_author"] = _is_post_author(ct, sig)
        e["is_from_company"] = _is_from_company(ct, sig)
        msgs = e.get("messages") or []
        e["message"] = max(msgs, key=lambda m: m.get("version", 0)) if msgs else None

    return entries


# ---------------------------------------------------------------------------
# POST /api/pipeline/{pipeline_key}/start-drafting
# ---------------------------------------------------------------------------
@router.post("/{pipeline_key}/start-drafting")
async def start_pipeline_drafting(
    pipeline_key: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Create message stubs for all relevant (yes/cc) pipeline_entries in this
    pipeline that don't have a message yet. Sets status='drafted'.
    """
    from app.services.leadspicker_normalize import _chunks

    entries_res = (
        await db.table("pipeline_entries")
        .select("id")
        .eq("pipeline_type", pipeline_key)
        .in_("relevant", ["yes", "cc"])
        .neq("status", "eliminated")
        .execute()
    )
    relevant_ids = [r["id"] for r in (entries_res.data or [])]
    if not relevant_ids:
        return {"created": 0, "total_relevant": 0}

    existing_res = (
        await db.table("messages")
        .select("pipeline_entry_id")
        .in_("pipeline_entry_id", relevant_ids)
        .execute()
    )
    already_have = {r["pipeline_entry_id"] for r in (existing_res.data or [])}
    new_ids = [eid for eid in relevant_ids if eid not in already_have]

    if new_ids:
        msg_rows = [
            {"pipeline_entry_id": eid, "ai_generated": False, "version": 1}
            for eid in new_ids
        ]
        await db.table("messages").insert(msg_rows).execute()

    now = datetime.now(timezone.utc).isoformat()
    for chunk in _chunks(relevant_ids, 500):
        await (
            db.table("pipeline_entries")
            .update({"status": "drafted", "drafted_at": now})
            .in_("id", chunk)
            .execute()
        )

    return {"created": len(new_ids), "total_relevant": len(relevant_ids)}


# ---------------------------------------------------------------------------
# POST /api/pipeline/{pipeline_key}/finish-drafting
# ---------------------------------------------------------------------------
@router.post("/{pipeline_key}/finish-drafting")
async def finish_pipeline_drafting(
    pipeline_key: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Copy draft_text → final_text for all relevant entries in this pipeline
    that have a non-empty draft. Returns {finalized, skipped_empty}.
    """
    entries_res = (
        await db.table("pipeline_entries")
        .select("id,messages(id,draft_text)")
        .eq("pipeline_type", pipeline_key)
        .in_("relevant", ["yes", "cc"])
        .execute()
    )

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


# ---------------------------------------------------------------------------
# POST /api/pipeline/{pipeline_key}/label-ai-classifier-no
# ---------------------------------------------------------------------------
@router.post("/{pipeline_key}/label-ai-classifier-no")
async def label_ai_classifier_no(
    pipeline_key: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Bulk-label all unlabeled staging entries whose ai_classifier = 'no' (case-insensitive)
    as label='no'. Records the bulk operation in source_metadata.
    """
    from app.services.leadspicker_normalize import _chunks

    source_type = await _get_source_type(db, pipeline_key)
    table = _SOURCE_TABLE_MAP.get(source_type)
    if not table:
        raise HTTPException(status_code=422, detail=f"Unknown source_type: {source_type}")

    now = datetime.now(timezone.utc).isoformat()

    # Fetch all unlabeled entries where ai_classifier ilike 'no'
    matching = []
    page_size = 1000
    offset = 0
    while True:
        res = (
            await db.table(table)
            .select("id, source_metadata")
            .eq("pipeline_key", pipeline_key)
            .is_("label", "null")
            .ilike("ai_classifier", "no")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = res.data or []
        matching.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    if not matching:
        return {"labeled": 0}

    # Update each record individually (preserves existing source_metadata via merge),
    # run concurrently in chunks of 100 to stay fast
    async def _update_one(entry: dict) -> None:
        meta = dict(entry.get("source_metadata") or {})
        meta["bulk_labeled_by_ai_no"] = True
        meta["bulk_labeled_at"] = now
        await (
            db.table(table)
            .update({"label": "no", "labeled_at": now, "source_metadata": meta})
            .eq("id", entry["id"])
            .execute()
        )

    for chunk in _chunks(matching, 100):
        await asyncio.gather(*[_update_one(e) for e in chunk])

    return {"labeled": len(matching)}
