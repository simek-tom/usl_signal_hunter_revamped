"""
Airtable push service.

LP pipelines:
  - create Airtable rows

Crunchbase pipeline:
  - batch update existing Airtable rows by preserved Airtable record id
  - excludes computed/formula columns that are not writable
"""

from typing import Optional

from supabase import AsyncClient

from app.core.config import settings
from app.core.runtime_config import get_runtime_value
from app.services.airtable_client import batch_create, batch_update
from app.services.leadspicker_normalize import _chunks

_PUSH_SELECT = (
    "id,pipeline_type,status,signal_id,contact_id,"
    "signals(id,external_id,content_url,content_text,content_summary,ai_classifier,source_metadata,company_id,"
    "companies(id,name_raw,domain_normalized,website,linkedin_url,country,employee_count,industry)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company),"
    "messages(id,final_text,draft_text,version)"
)

_CB_DROP_COLUMNS = {
    "Half year reminder (suggested)",
    "Message draft",
    "CB financials link",
    "CB people link",
    "Contact enriched",
    "CB news link",
    "Tag",
    "Tags",
    "Reviewed by Roman",
    "Number of Investors",
    "relevant",
    "learning_data",
    "cb url for updating",
}

_CB_DROP_COLUMNS_NORMALIZED = {
    " ".join(str(name).strip().lower().split())
    for name in _CB_DROP_COLUMNS
}


def _norm_col(name: str) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _best_message_text(msgs: list[dict]) -> str:
    if not msgs:
        return ""
    msg = max(msgs, key=lambda m: m.get("version", 0))
    return (msg.get("final_text") or msg.get("draft_text") or "").strip()


def _build_airtable_fields(entry: dict) -> dict:
    sig = entry.get("signals") or {}
    co = sig.get("companies") or {}
    ct = entry.get("contacts") or {}
    msgs = entry.get("messages") or []

    return {
        "First Name": ct.get("first_name") or "",
        "Last Name": ct.get("last_name") or "",
        "Full Name": ct.get("full_name") or "",
        "E-mail": ct.get("email") or "",
        "Contact LinkedIn profile": ct.get("linkedin_url") or "",
        "Relation to the company": ct.get("relation_to_company") or "",
        "Company Name": co.get("name_raw") or "",
        "Company website": co.get("website") or co.get("domain_normalized") or "",
        "Company LinkedIn URL": co.get("linkedin_url") or "",
        "Base post URL": sig.get("content_url") or "",
        "General message": _best_message_text(msgs),
    }


def _cb_message_text(entry: dict) -> str:
    sig = entry.get("signals") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    if meta.get("message_fin"):
        return str(meta.get("message_fin") or "").strip()
    if meta.get("message_draft"):
        return str(meta.get("message_draft") or "").strip()
    return _best_message_text(entry.get("messages") or [])


def _cb_record_id(entry: dict) -> Optional[str]:
    sig = entry.get("signals") or {}
    ext = str(sig.get("external_id") or "").strip()
    if ext:
        return ext
    meta = sig.get("source_metadata") or {}
    if isinstance(meta, dict):
        fallback = str(meta.get("airtable_record_id") or "").strip()
        if fallback:
            return fallback
    return None


def _sanitize_cb_fields(raw_fields: dict) -> dict:
    out: dict = {}
    for k, v in (raw_fields or {}).items():
        key = str(k or "").strip()
        normalized = _norm_col(key)
        if not key or normalized in _CB_DROP_COLUMNS_NORMALIZED:
            continue
        out[key] = v
    return out


def _resolve_existing_field(raw_fields: dict, candidates: list[str]) -> Optional[str]:
    """
    Resolve a writable Airtable field name from candidates using raw field names
    present on the source record (case/spacing-insensitive).
    """
    by_norm = {_norm_col(k): k for k in (raw_fields or {}).keys()}
    for cand in candidates:
        hit = by_norm.get(_norm_col(cand))
        if hit:
            return hit
    return None


def _build_cb_update_fields(entry: dict) -> dict:
    sig = entry.get("signals") or {}
    co = sig.get("companies") or {}
    ct = entry.get("contacts") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}

    raw_fields = meta.get("raw_fields") or {}
    if not isinstance(raw_fields, dict):
        raw_fields = {}
    fields = _sanitize_cb_fields(raw_fields)

    message_fin = _cb_message_text(entry)
    main_contact = str(meta.get("main_contact") or ct.get("linkedin_url") or "").strip()
    sec1 = str(meta.get("secondary_contact_1") or "").strip()
    sec2 = str(meta.get("secondary_contact_2") or "").strip()
    sec3 = str(meta.get("secondary_contact_3") or "").strip()

    overlays = [
        (
            ["Name", "Company Name"],
            co.get("name_raw") or fields.get("Name") or fields.get("Company Name"),
        ),
        (
            ["Website", "Company website", "Company Website"],
            co.get("website")
            or co.get("domain_normalized")
            or fields.get("Website")
            or fields.get("Company website")
            or fields.get("Company Website"),
        ),
        (
            ["Company LinkedIn", "Company LinkedIn URL"],
            co.get("linkedin_url") or fields.get("Company LinkedIn") or fields.get("Company LinkedIn URL"),
        ),
        (
            ["Status"],
            meta.get("status") or fields.get("Status"),
        ),
        (
            ["Message fin", "Final Message"],
            message_fin,
        ),
        (
            ["Main Contact", "Main Contact LinkedIn"],
            main_contact,
        ),
        (
            ["Secondary Contact #1", "Secondary Contacts #1", "Secondary Contact 1"],
            sec1,
        ),
        (
            ["Secondary Contact #2", "Secondary Contacts #2", "Secondary Contact 2"],
            sec2,
        ),
        (
            ["Secondary Contact #3", "Secondary Contacts #3", "Secondary Contact 3"],
            sec3,
        ),
    ]

    for candidates, value in overlays:
        if value is None:
            continue
        field_name = _resolve_existing_field(raw_fields, candidates)
        if not field_name:
            continue
        fields[field_name] = value

    return _sanitize_cb_fields(fields)


async def _fetch_entries(
    db: AsyncClient,
    entry_ids: list[str],
) -> tuple[list[dict], int]:
    entries: list[dict] = []
    for chunk in _chunks(entry_ids, 200):
        res = (
            await db.table("pipeline_entries")
            .select(_PUSH_SELECT)
            .in_("id", chunk)
            .execute()
        )
        entries.extend(res.data or [])

    entry_map = {e["id"]: e for e in entries}
    ordered = [entry_map[eid] for eid in entry_ids if eid in entry_map]
    skipped = len(entry_ids) - len(ordered)
    return ordered, skipped


async def _log_push(
    db: AsyncClient,
    *,
    entry_id: str,
    table_name: str,
    external_id: Optional[str],
    push_status: str,
    response_data: Optional[dict],
):
    airtable_base_id = str(get_runtime_value("airtable_base_id") or settings.airtable_base_id).strip()
    await db.rpc(
        "complete_push",
        {
            "p_entry_id": entry_id,
            "p_target_system": "airtable",
            "p_target_project_id": f"{airtable_base_id}/{table_name}",
            "p_external_id": external_id,
            "p_push_status": push_status,
            "p_response_data": response_data,
        },
    ).execute()


async def create_lp_airtable_records(
    db: AsyncClient,
    entry_ids: list[str],
    table_name: str,
) -> dict:
    """
    Create Airtable records for the given entry IDs.

    Returns {created, failed, skipped}.
    Logs each batch attempt to push_log via complete_push RPC.
    """
    if not entry_ids:
        return {"created": 0, "failed": 0, "skipped": 0}

    ordered, skipped = await _fetch_entries(db, entry_ids)
    created = 0
    failed = 0

    for chunk in _chunks(ordered, 10):
        fields_list = [_build_airtable_fields(e) for e in chunk]
        chunk_ids = [e["id"] for e in chunk]
        push_status = "success"
        response_data: Optional[dict] = None
        external_ids: list[Optional[str]] = [None] * len(chunk)

        try:
            result = await batch_create(table_name, fields_list)
            for i, rec in enumerate(result):
                external_ids[i] = rec.get("id")
            created += len(chunk)
            response_data = {"record_count": len(result)}
        except Exception as exc:
            push_status = "fail"
            response_data = {"error": str(exc)[:500]}
            failed += len(chunk)

        # Log each entry in the chunk
        for eid, ext_id in zip(chunk_ids, external_ids):
            await _log_push(
                db,
                entry_id=eid,
                table_name=table_name,
                external_id=ext_id,
                push_status=push_status,
                response_data=response_data,
            )

        # Record contacted companies on success
        if push_status == "success":
            from app.services.leadspicker_normalize import normalize_domain, make_fingerprint
            from app.core.utils import normalize_company_name
            for entry_obj in chunk:
                sig = entry_obj.get("signals") or {}
                co = sig.get("companies") or {}
                co_name = co.get("name_raw") or ""
                co_domain = normalize_domain(co.get("website") or co.get("domain_normalized") or "")
                try:
                    await db.table("contacted_companies").insert({
                        "company_name_normalized": normalize_company_name(co_name) if co_name else None,
                        "domain_normalized": co_domain or None,
                        "linkedin_url": co.get("linkedin_url") or None,
                        "fingerprint": make_fingerprint(co_name, co_domain),
                        "contacted_via": entry_obj.get("pipeline_type"),
                        "pipeline_entry_id": entry_obj["id"],
                    }).execute()
                except Exception:
                    pass  # non-blocking

    return {"created": created, "failed": failed, "skipped": skipped}


async def update_crunchbase_airtable_records(
    db: AsyncClient,
    entry_ids: list[str],
    table_name: str,
) -> dict:
    """
    Crunchbase path:
      - update existing Airtable rows by record id (signals.external_id)
      - exclude computed/formula columns from payload
    """
    if not entry_ids:
        return {"created": 0, "failed": 0, "skipped": 0}

    ordered, skipped = await _fetch_entries(db, entry_ids)
    updates: list[tuple[str, str, dict]] = []  # (entry_id, record_id, fields)
    for entry in ordered:
        rec_id = _cb_record_id(entry)
        if not rec_id:
            skipped += 1
            continue
        fields = _build_cb_update_fields(entry)
        updates.append((entry["id"], rec_id, fields))

    created = 0
    failed = 0
    for chunk in _chunks(updates, 10):
        payload = [
            {"id": rec_id, "fields": fields}
            for _, rec_id, fields in chunk
        ]
        push_status = "success"
        response_data: Optional[dict] = None
        try:
            result = await batch_update(table_name, payload)
            created += len(chunk)
            response_data = {"record_count": len(result)}
        except Exception as exc:
            push_status = "fail"
            response_data = {"error": str(exc)[:500]}
            failed += len(chunk)

        for entry_id, rec_id, _ in chunk:
            await _log_push(
                db,
                entry_id=entry_id,
                table_name=table_name,
                external_id=rec_id,
                push_status=push_status,
                response_data=response_data,
            )

        # Record contacted companies on success
        if push_status == "success":
            from app.services.leadspicker_normalize import normalize_domain, make_fingerprint
            from app.core.utils import normalize_company_name
            for entry_id_c, _, _ in chunk:
                entry_obj = next((e for e in ordered if e["id"] == entry_id_c), None)
                if not entry_obj:
                    continue
                sig = entry_obj.get("signals") or {}
                co = sig.get("companies") or {}
                co_name = co.get("name_raw") or ""
                co_domain = normalize_domain(co.get("website") or co.get("domain_normalized") or "")
                try:
                    await db.table("contacted_companies").insert({
                        "company_name_normalized": normalize_company_name(co_name) if co_name else None,
                        "domain_normalized": co_domain or None,
                        "linkedin_url": co.get("linkedin_url") or None,
                        "fingerprint": make_fingerprint(co_name, co_domain),
                        "contacted_via": "crunchbase",
                        "pipeline_entry_id": entry_id_c,
                    }).execute()
                except Exception:
                    pass  # non-blocking

    return {"created": created, "failed": failed, "skipped": skipped}


async def push_entries_to_airtable(
    db: AsyncClient,
    entry_ids: list[str],
    table_name: str,
) -> dict:
    """
    Unified Airtable push dispatcher by pipeline_type.
    """
    if not entry_ids:
        return {"created": 0, "failed": 0, "skipped": 0}

    res = (
        await db.table("pipeline_entries")
        .select("id,pipeline_type")
        .in_("id", entry_ids)
        .execute()
    )
    rows = res.data or []
    if not rows:
        return {"created": 0, "failed": 0, "skipped": len(entry_ids)}

    types = {r.get("pipeline_type") for r in rows if r.get("pipeline_type")}
    if len(types) > 1:
        raise ValueError("All pushed entries must belong to the same pipeline_type")

    pipeline_type = next(iter(types), "")
    if pipeline_type == "crunchbase":
        return await update_crunchbase_airtable_records(db, entry_ids, table_name)
    return await create_lp_airtable_records(db, entry_ids, table_name)
