"""
LeadsPicker push service.

Pushes pipeline entries to a LeadsPicker project.
For each entry: builds person payload, POSTs to LP API,
then calls complete_push RPC to atomically log + update status.

push_row_limit is read from the `settings` table (key='push_row_limit').
Default: 100 if not set.

push_map (from pipeline config's push_column_map) controls the
custom_fields object sent to LP. Format: {lp_key: internal_field}.
Available internal_field values: content_url, content_text,
content_summary, ai_classifier, first_name, last_name, email,
contact_linkedin, position, company_name, company_website,
company_linkedin, country, message.
"""

from typing import Optional

import httpx
from supabase import AsyncClient

from app.services.leadspicker import lp_session

_DEFAULT_PUSH_LIMIT = 100

_PUSH_SELECT = (
    "id,pipeline_type,status,company_id,source_type,"
    "external_id,content_url,content_text,content_summary,ai_classifier,source_metadata,"
    "lead_full_name,lead_first_name,lead_last_name,lead_linkedin,lead_email,lead_position,lead_company_name,lead_company_linkedin,"
    "companies(id,name_raw,domain_normalized,website,linkedin_url,country),"
    "messages(id,final_text,draft_text,version)"
)


def _get_push_row_limit() -> int:
    from app.core.local_settings import get as local_get
    val = local_get("push_row_limit")
    if val is not None:
        try:
            return int(val)
        except (ValueError, TypeError):
            pass
    return _DEFAULT_PUSH_LIMIT


def _best_message_text(msgs: list[dict]) -> str:
    if not msgs:
        return ""
    msg = max(msgs, key=lambda m: m.get("version", 0))
    return (msg.get("final_text") or msg.get("draft_text") or "").strip()


def _cb_message_text(entry: dict) -> str:
    meta = entry.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    return (
        str(meta.get("message_fin") or "").strip()
        or str(meta.get("message_draft") or "").strip()
        or _best_message_text(entry.get("messages") or [])
    )


def _build_lp_payload(
    entry: dict,
    *,
    message_text: str = "",
    push_map: dict | None = None,
) -> dict:
    co = entry.get("companies") or {}
    meta = entry.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}
    linkedin_url = entry.get("lead_linkedin") or meta.get("main_contact") or ""

    # Resolve internal field values for custom_fields mapping
    _field_values = {
        "content_url": entry.get("content_url") or "",
        "content_text": entry.get("content_text") or "",
        "content_summary": entry.get("content_summary") or "",
        "ai_classifier": str(entry.get("ai_classifier") or ""),
        "first_name": entry.get("lead_first_name") or "",
        "last_name": entry.get("lead_last_name") or "",
        "email": entry.get("lead_email") or "",
        "contact_linkedin": entry.get("lead_linkedin") or "",
        "position": entry.get("lead_position") or "",
        "company_name": co.get("name_raw") or "",
        "company_website": co.get("website") or co.get("domain_normalized") or "",
        "company_linkedin": co.get("linkedin_url") or "",
        "country": co.get("country") or "",
        "message": message_text,
    }

    if push_map:
        # Custom mapping: {lp_custom_field_key: internal_field_name}
        custom_fields = {
            lp_key: _field_values[internal]
            for lp_key, internal in push_map.items()
            if internal in _field_values and _field_values[internal]
        }
    else:
        # Default behaviour
        custom_fields = {
            "base_post_url": entry.get("content_url") or "",
        }
        if message_text:
            custom_fields["message_text"] = message_text
            custom_fields["general_message"] = message_text

    return {
        "first_name": entry.get("lead_first_name") or "",
        "last_name": entry.get("lead_last_name") or "",
        "email": entry.get("lead_email") or "",
        "linkedin": linkedin_url,
        "position": entry.get("lead_position") or "",
        "company_name": co.get("name_raw") or "",
        "company_website": co.get("website") or co.get("domain_normalized") or "",
        "company_linkedin": co.get("linkedin_url") or "",
        "country": co.get("country") or "",
        "custom_fields": custom_fields,
    }


async def push_to_leadspicker(
    db: AsyncClient,
    entry_ids: list[str],
    project_id: int,
    push_map: dict | None = None,
    pipeline_key: str | None = None,
) -> dict:
    """
    Push entries to a LeadsPicker project.

    push_map overrides the default custom_fields mapping.
    Format: {lp_custom_field_key: internal_field_name}

    Returns {pushed, failed, skipped} where:
      pushed  — successfully sent to LP + logged
      failed  — LP API returned an error (logged with push_status='fail')
      skipped — entries that had no usable data or exceeded push_row_limit
    """
    if not entry_ids:
        return {"pushed": 0, "failed": 0, "skipped": 0}

    limit = _get_push_row_limit()
    if len(entry_ids) > limit:
        entry_ids = entry_ids[:limit]

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

    entry_map = {e["id"]: e for e in entries}

    pushed = 0
    failed = 0
    skipped = len(entry_ids) - len(entries)

    async with lp_session() as (client, headers):
        for eid in entry_ids:
            entry = entry_map.get(eid)
            if not entry:
                skipped += 1
                continue

            is_crunchbase = entry.get("pipeline_type") == "crunchbase"
            if is_crunchbase:
                meta = entry.get("source_metadata") or {}
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
                payload = _build_lp_payload(entry, message_text=msg, push_map=push_map)
            else:
                payload = _build_lp_payload(entry, push_map=push_map)

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

            await db.rpc(
                "complete_push",
                {
                    "p_entry_id": eid,
                    "p_target_system": "leadspicker",
                    "p_target_project_id": str(project_id),
                    "p_external_id": external_id,
                    "p_push_status": push_status,
                    "p_response_data": response_data,
                },
            ).execute()

            if push_status == "success":
                co = entry.get("companies") or {}
                try:
                    from app.api._helpers import build_blacklist_row
                    row = build_blacklist_row(
                        company_name=co.get("name_raw") or "",
                        company_linkedin=co.get("linkedin_url"),
                        company_website=co.get("website") or co.get("domain_normalized"),
                        reason="contacted",
                        added_by="auto:push_lp",
                        origin=pipeline_key or entry.get("pipeline_type"),
                        pipeline_entry_id=eid,
                    )
                    await db.table("blacklisted_companies").insert(row).execute()
                except Exception:
                    pass  # non-blocking

    return {"pushed": pushed, "failed": failed, "skipped": skipped}
