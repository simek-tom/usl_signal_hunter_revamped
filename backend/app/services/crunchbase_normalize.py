"""
Crunchbase (Airtable-backed) normalization + Supabase persistence.

Source inputs:
  - Airtable records (pyairtable format: {"id", "fields", ...})
  - CSV exports with Airtable-like column headers

Storage strategy:
  - Core company fields -> companies
  - Main contact URL -> contacts.linkedin_url
  - Airtable record ID (_id) -> signals.external_id
  - CB-specific fields -> signals.source_metadata JSONB
"""

import csv
import io
import json
from dataclasses import dataclass
from typing import Any, Optional

from supabase import AsyncClient

from app.services.leadspicker_normalize import (
    _chunks,
    _fp_or_filter,
    clean_linkedin_url,
    make_fingerprint,
    normalize_domain,
)


def _clean_key(key: str) -> str:
    return (
        str(key or "")
        .strip()
        .lower()
        .replace("_", " ")
    )


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, list):
        return ", ".join(_to_text(v) for v in value if _to_text(v))
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)
    return str(value).strip()


def _to_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = _to_text(value).strip().lower()
    if text in {"true", "1", "yes", "y", "checked"}:
        return True
    if text in {"false", "0", "no", "n", "unchecked"}:
        return False
    return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, list):
        return [_jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return _to_text(value)


def _pick(fields: dict[str, Any], aliases: list[str]) -> Any:
    if not fields:
        return None
    norm_map = {_clean_key(k): v for k, v in fields.items()}
    for alias in aliases:
        key = _clean_key(alias)
        if key in norm_map:
            return norm_map[key]
    return None


def _pick_text(fields: dict[str, Any], aliases: list[str]) -> str:
    return _to_text(_pick(fields, aliases)).strip()


@dataclass
class CrunchbaseRow:
    external_id: str
    company_name: str = ""
    company_website: str = ""
    company_linkedin: str = ""
    company_country: str = ""
    company_industry: str = ""
    company_hq_location: str = ""
    company_description: str = ""
    company_founded_on: str = ""
    company_employee_count: str = ""
    crunchbase_profile_url: str = ""
    content_summary: str = ""
    ai_classifier: str = ""
    main_contact: str = ""
    secondary_contact_1: str = ""
    secondary_contact_2: str = ""
    secondary_contact_3: str = ""
    message_fin: str = ""
    source_metadata: dict[str, Any] = None


def _metadata_from_fields(
    external_id: str,
    fields: dict[str, Any],
    *,
    message_fin: str,
    main_contact: str,
    secondary_contact_1: str,
    secondary_contact_2: str,
    secondary_contact_3: str,
) -> dict[str, Any]:
    status = _pick_text(fields, ["Status"])
    contact_enriched = _to_bool(_pick(fields, ["Contact enriched"]))
    series = _pick_text(fields, ["Series", "Funding Stage"])
    funding_amount = _pick(fields, ["Last Funding Amount", "Funding", "Total Funding Amount"])
    funding_rounds = _pick(fields, ["Funding Rounds", "Number of Funding Rounds"])
    investors = _pick(fields, ["Investors", "Number of Investors"])
    last_funding_date = _pick(fields, ["Last Funding Date"])
    revenue_range = _pick(fields, ["Revenue Range"])
    company_email = _pick_text(fields, ["Company Email", "Email", "Contact Email"])
    industries = _pick(fields, ["Industries", "Industry"])
    hq = _pick(fields, ["Headquarters Location", "HQ", "Headquarters"])
    description = _pick_text(fields, ["Description", "Company Description"])

    return {
        "airtable_record_id": external_id or None,
        "status": status or None,
        "contact_enriched": contact_enriched,
        "series": _to_text(series) or None,
        "last_funding_date": _to_text(last_funding_date) or None,
        "last_funding_amount_usd": _jsonable(funding_amount),
        "funding_rounds": _jsonable(funding_rounds),
        "investors": _jsonable(investors),
        "revenue_range": _to_text(revenue_range) or None,
        "company_email": company_email or None,
        "industries": _jsonable(industries),
        "hq_location": _to_text(hq) or None,
        "description": description or None,
        "message_fin": message_fin or "",
        "message_draft": message_fin or "",
        "main_contact": main_contact or "",
        "secondary_contact_1": secondary_contact_1 or "",
        "secondary_contact_2": secondary_contact_2 or "",
        "secondary_contact_3": secondary_contact_3 or "",
        "raw_fields": _jsonable(fields),
    }


def from_field_map(raw_fields: dict[str, Any], external_id_hint: str = "") -> CrunchbaseRow:
    fields = {str(k): v for k, v in (raw_fields or {}).items()}

    external_id = (
        _to_text(_pick(fields, ["_id", "record id", "airtable_record_id", "id"]))
        or external_id_hint
    ).strip()

    company_name = _pick_text(fields, ["Name", "Company Name"])
    company_website = _pick_text(fields, ["Website", "Company website", "Company Website"])
    company_linkedin = clean_linkedin_url(
        _pick_text(fields, ["Company LinkedIn", "Company LinkedIn URL", "LinkedIn"])
    )
    company_country = _pick_text(fields, ["Country", "Headquarters Region", "Headquarters Regions"])
    company_industry = _pick_text(fields, ["Industries", "Industry"])
    company_hq_location = _pick_text(fields, ["Headquarters Location", "HQ", "Headquarters"])
    company_description = _pick_text(fields, ["Description", "Company Description"])
    company_founded_on = _pick_text(fields, ["Founded on", "Founded Date", "Founded"])
    company_employee_count = _pick_text(
        fields, ["Employees", "Employee Count", "Number of Employees"]
    )
    crunchbase_profile_url = _pick_text(
        fields,
        ["Crunchbase URL", "Crunchbase profile URL", "Crunchbase profile url", "CB link"],
    )

    content_summary = _pick_text(fields, ["Summary", "Content summary", "Company summary"])
    ai_classifier = _pick_text(fields, ["AI classifier", "AI clasifier"])
    message_fin = _pick_text(fields, ["Message fin", "Final Message", "Message final"])
    main_contact = clean_linkedin_url(_pick_text(fields, ["Main Contact", "Main Contact LinkedIn"]))
    secondary_contact_1 = clean_linkedin_url(
        _pick_text(fields, ["Secondary Contact #1", "Secondary Contacts #1", "Secondary Contact 1"])
    )
    secondary_contact_2 = clean_linkedin_url(
        _pick_text(fields, ["Secondary Contact #2", "Secondary Contacts #2", "Secondary Contact 2"])
    )
    secondary_contact_3 = clean_linkedin_url(
        _pick_text(fields, ["Secondary Contact #3", "Secondary Contacts #3", "Secondary Contact 3"])
    )

    metadata = _metadata_from_fields(
        external_id=external_id,
        fields=fields,
        message_fin=message_fin,
        main_contact=main_contact,
        secondary_contact_1=secondary_contact_1,
        secondary_contact_2=secondary_contact_2,
        secondary_contact_3=secondary_contact_3,
    )

    return CrunchbaseRow(
        external_id=external_id,
        company_name=company_name,
        company_website=company_website,
        company_linkedin=company_linkedin,
        company_country=company_country,
        company_industry=company_industry,
        company_hq_location=company_hq_location,
        company_description=company_description,
        company_founded_on=company_founded_on,
        company_employee_count=company_employee_count,
        crunchbase_profile_url=crunchbase_profile_url,
        content_summary=content_summary,
        ai_classifier=ai_classifier,
        main_contact=main_contact,
        secondary_contact_1=secondary_contact_1,
        secondary_contact_2=secondary_contact_2,
        secondary_contact_3=secondary_contact_3,
        message_fin=message_fin,
        source_metadata=metadata,
    )


def from_airtable_record(record: dict[str, Any]) -> CrunchbaseRow:
    fields = dict(record.get("fields") or {})
    return from_field_map(fields, external_id_hint=str(record.get("id") or "").strip())


def parse_csv(raw_bytes: bytes) -> list[CrunchbaseRow]:
    text = raw_bytes.decode("utf-8-sig")
    sample = text[:4096]
    delimiter = ";"
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        delimiter = dialect.delimiter
    except Exception:
        pass

    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
    return [from_field_map(row) for row in reader]


async def process_crunchbase_import(
    db: AsyncClient,
    rows: list[CrunchbaseRow],
    pipeline_key: str,
    batch_id: str,
) -> int:
    """
    Write CB records to staging_crunchbase.
    Dedup via UNIQUE(dedup_key) + ON CONFLICT DO NOTHING.
    """
    if not rows:
        return 0

    staging_rows = []
    for r in rows:
        domain = normalize_domain(r.company_website)
        fp = make_fingerprint(r.company_name, domain)
        dedup = (r.external_id or fp or "").strip()
        if not dedup:
            continue

        staging_rows.append({
            "pipeline_key": pipeline_key,
            "batch_id": batch_id,
            "dedup_key": dedup,
            "company_name": r.company_name or None,
            "company_website": r.company_website or None,
            "company_linkedin": r.company_linkedin or None,
            "company_country": r.company_country or None,
            "company_industry": r.company_industry or None,
            "company_hq_location": r.company_hq_location or None,
            "company_description": r.company_description or None,
            "company_founded_on": r.company_founded_on or None,
            "company_employee_count": r.company_employee_count or None,
            "crunchbase_profile_url": r.crunchbase_profile_url or None,
            "funding_series": (r.source_metadata or {}).get("series") or None,
            "last_funding_amount": str((r.source_metadata or {}).get("last_funding_amount_usd") or "") or None,
            "last_funding_date": (r.source_metadata or {}).get("last_funding_date") or None,
            "funding_rounds": str((r.source_metadata or {}).get("funding_rounds") or "") or None,
            "investors": str((r.source_metadata or {}).get("investors") or "") or None,
            "revenue_range": (r.source_metadata or {}).get("revenue_range") or None,
            "content_url": r.crunchbase_profile_url or r.company_linkedin or r.company_website or None,
            "content_summary": r.content_summary or r.company_description or None,
            "ai_classifier": r.ai_classifier or None,
            "main_contact_linkedin": r.main_contact or None,
            "secondary_contact_1": r.secondary_contact_1 or None,
            "secondary_contact_2": r.secondary_contact_2 or None,
            "secondary_contact_3": r.secondary_contact_3 or None,
            "message_fin": r.message_fin or None,
            "external_id": r.external_id or None,
            "airtable_status": (r.source_metadata or {}).get("status") or None,
            "contact_enriched": (r.source_metadata or {}).get("contact_enriched"),
            "source_metadata": r.source_metadata or {},
        })

    inserted = 0
    for chunk in _chunks(staging_rows, 500):
        res = (
            await db.table("staging_crunchbase")
            .upsert(chunk, on_conflict="dedup_key", ignore_duplicates=True)
            .execute()
        )
        inserted += len(res.data or [])

    return inserted
