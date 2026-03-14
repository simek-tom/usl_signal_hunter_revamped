"""
LeadsPicker push service.

Pushes pipeline entries to a LeadsPicker project.
For each entry: builds person payload, POSTs to LP API,
then calls complete_push RPC to atomically log + update status.

push_row_limit is read from the `settings` table (key='push_row_limit').
Default: 100 if not set.
"""

from typing import Optional

import httpx
from supabase import AsyncClient

from app.services.leadspicker import lp_session

_DEFAULT_PUSH_LIMIT = 100

_PUSH_SELECT = (
    "id,pipeline_type,status,signal_id,contact_id,"
    "signals(id,external_id,content_url,content_text,ai_classifier,source_metadata,company_id,"
    "companies(id,name_raw,domain_normalized,website,linkedin_url,country)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company),"
    "messages(id,final_text,draft_text,version)"
)


async def _get_push_row_limit(db: AsyncClient) -> int:
    res = (
        await db.table("settings")
        .select("value")
        .eq("key", "push_row_limit")
        .execute()
    )
    if res.data:
        try:
            return int(res.data[0]["value"])
        except (ValueError, TypeError, KeyError):
            pass
    return _DEFAULT_PUSH_LIMIT


def _best_message_text(msgs: list[dict]) -> str:
    if not msgs:
        return ""
    msg = max(msgs, key=lambda m: m.get("version", 0))
    return (msg.get("final_text") or msg.get("draft_text") or "").strip()


def _cb_message_text(entry: dict) -> str:
    sig = entry.get("signals") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    return (
        str(meta.get("message_fin") or "").strip()
        or str(meta.get("message_draft") or "").strip()
        or _best_message_text(entry.get("messages") or [])
    )


def _build_lp_payload(entry: dict, *, message_text: str = "") -> dict:
    sig = entry.get("signals") or {}
    co = sig.get("companies") or {}
    ct = entry.get("contacts") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    linkedin_url = ct.get("linkedin_url") or meta.get("main_contact") or ""

    custom_fields = {
        "base_post_url": sig.get("content_url") or "",
    }
    if message_text:
        custom_fields["message_text"] = message_text
        custom_fields["general_message"] = message_text

    return {
        "first_name": ct.get("first_name") or "",
        "last_name": ct.get("last_name") or "",
        "email": ct.get("email") or "",
        "linkedin": linkedin_url,
        "position": ct.get("relation_to_company") or "",
        "company_name": co.get("name_raw") or "",
        "company_website": co.get("website") or co.get("domain_normalized") or "",
        "company_linkedin": co.get("linkedin_url") or "",
        "country": co.get("country") or "",
        # LP accepts arbitrary metadata via custom_fields.
        "custom_fields": custom_fields,
    }


async def push_to_leadspicker(
    db: AsyncClient,
    entry_ids: list[str],
    project_id: int,
) -> dict:
    """
    Push entries to a LeadsPicker project.

    Returns {pushed, failed, skipped} where:
      pushed  — successfully sent to LP + logged
      failed  — LP API returned an error (logged with push_status='fail')
      skipped — entries that had no usable data or exceeded push_row_limit
    """
    if not entry_ids:
        return {"pushed": 0, "failed": 0, "skipped": 0}

    limit = await _get_push_row_limit(db)
    if len(entry_ids) > limit:
        # Truncate to limit — caller should be aware
        entry_ids = entry_ids[:limit]

    # Fetch entries with joins
    from app.services.leadspicker_normalize import _chunks
    entries: list[dict] = []
    for chunk in _chunks(entry_ids, 200):
        res = (
            await db.table("pipeline_entries")
            .select(_PUSH_SELECT)
            .in_("id", chunk)
            .execute()
        )
        entries.extend(res.data or [])

    # Index by id to preserve order
    entry_map = {e["id"]: e for e in entries}

    pushed = 0
    failed = 0
    skipped = len(entry_ids) - len(entries)  # IDs that didn't resolve

    async with lp_session() as (client, headers):
        for eid in entry_ids:
            entry = entry_map.get(eid)
            if not entry:
                skipped += 1
                continue

            is_crunchbase = entry.get("pipeline_type") == "crunchbase"
            if is_crunchbase:
                sig = entry.get("signals") or {}
                meta = sig.get("source_metadata") or {}
                if not isinstance(meta, dict):
                    meta = {}
                status = str(entry.get("status") or "").strip().lower()
                marker = str(meta.get("entry_workflow_status") or "").strip().lower()
                is_ready = status == "pushed-ready" or marker == "pushed-ready"
                if not is_ready:
                    skipped += 1
                    continue
                msg = _cb_message_text(entry)
                if not msg:
                    skipped += 1
                    continue
                payload = _build_lp_payload(entry, message_text=msg)
            else:
                payload = _build_lp_payload(entry)
            external_id: Optional[str] = None
            push_status = "success"
            response_data: Optional[dict] = None

            try:
                payload["project_id"] = project_id
                resp = await client.post(
                    "/app/sb/api/persons",
                    headers=headers,
                    json=payload,
                )
                resp.raise_for_status()
                data = resp.json()
                response_data = data if isinstance(data, dict) else {"raw": str(data)}
                external_id = str(
                    data.get("id") or data.get("person_id") or ""
                ) or None
                push_status = "success"
                pushed += 1
            except httpx.HTTPStatusError as exc:
                push_status = "fail"
                response_data = {
                    "status_code": exc.response.status_code,
                    "body": exc.response.text[:500],
                }
                failed += 1
            except httpx.RequestError as exc:
                push_status = "fail"
                response_data = {"error": str(exc)[:500]}
                failed += 1

            # Always log the attempt (ok or error)
            await db.rpc(
                "complete_push",
                { ... },
            ).execute()

            # Record this company as contacted
            if push_status == "success":
                co = sig.get("companies") or {}
                from app.services.leadspicker_normalize import normalize_domain, make_fingerprint
                from app.core.utils import normalize_company_name
                co_name = co.get("name_raw") or ""
                co_domain = normalize_domain(co.get("website") or co.get("domain_normalized") or "")
                await db.table("contacted_companies").insert({
                    "company_name_normalized": normalize_company_name(co_name) if co_name else None,
                    "domain_normalized": co_domain or None,
                    "linkedin_url": co.get("linkedin_url") or None,
                    "fingerprint": make_fingerprint(co_name, co_domain),
                    "contacted_via": entry.get("pipeline_type"),
                    "pipeline_entry_id": eid,
                }).execute()

    return {"pushed": pushed, "failed": failed, "skipped": skipped}
