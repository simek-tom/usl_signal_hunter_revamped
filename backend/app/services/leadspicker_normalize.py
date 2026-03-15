"""
LP data normalization + Supabase persistence.

Confirmed LP item shape (contact_data.{key}.value pattern):
  contact_data.first_name.value, last_name, email, linkedin,
  company_name, company_website, company_linkedin,
  linkedin_company_country, company_employees, linkedin_company_size,
  source_robot, position
  + optional: post_content / content_text, post_url / content_url,
              content_summary, ai_classifier

Company upsert strategy:
  fingerprint = lower(name)|normalized_domain
  → new: INSERT; existing: call enrich_company RPC (don't clobber).

Signals and pipeline_entries are always freshly inserted per import.
"""

import csv
import io
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

from supabase import AsyncClient


# ---------------------------------------------------------------------------
# URL / fingerprint helpers
# ---------------------------------------------------------------------------

def normalize_domain(url: str) -> str:
    """Strip protocol, www, path, and trailing slash. Returns bare hostname."""
    if not url:
        return ""
    url = url.lower().strip()
    url = re.sub(r"^https?://", "", url)
    url = re.sub(r"^www\.", "", url)
    url = url.split("/")[0].split("?")[0].split("#")[0]
    return url.strip(".")


def clean_linkedin_url(url: str) -> str:
    """Keep only the first 5 slash-segments (removes trailing path noise)."""
    if not url:
        return ""
    parts = url.rstrip("/").split("/")
    return "/".join(parts[:5])


def make_fingerprint(name: str, domain: str) -> Optional[str]:
    """lowercase_name|normalized_domain. Returns None if both are empty."""
    name = name.strip().lower()
    domain = domain.strip().lower()
    if not name and not domain:
        return None
    return f"{name}|{domain}"


# ---------------------------------------------------------------------------
# Internal transfer object
# ---------------------------------------------------------------------------

@dataclass
class PersonData:
    # company
    company_name: str = ""
    company_website: str = ""
    company_linkedin: str = ""
    company_country: str = ""
    company_employee_count: str = ""
    # contact
    first_name: str = ""
    last_name: str = ""
    email: str = ""
    contact_linkedin: str = ""
    position: str = ""
    # signal
    external_id: str = ""
    content_text: str = ""
    content_url: str = ""
    content_summary: str = ""
    ai_classifier: str = ""
    source_robot: str = ""
    # LP metadata (goes into source_metadata)
    lp_left_out: bool = False
    lp_replied: bool = False
    lp_project_id: Optional[int] = None


# ---------------------------------------------------------------------------
# Default LP contact_data key fallbacks per internal field
# ---------------------------------------------------------------------------

# Each list is tried in order until a non-empty value is found.
_LP_API_DEFAULTS: dict[str, list[str]] = {
    "company_name":           ["company_name"],
    "company_website":        ["company_website"],
    "company_linkedin":       ["company_linkedin"],
    "company_country":        ["country", "linkedin_company_country"],
    "company_employee_count": ["company_employees", "linkedin_company_size"],
    "first_name":             ["first_name"],
    "last_name":              ["last_name"],
    "email":                  ["email"],
    "contact_linkedin":       ["linkedin"],
    "position":               ["position"],
    "content_text":           ["post_content", "linkedin_post", "content_text"],
    "content_url":            ["linkedin_post_url", "post_url", "content_url"],
    "content_summary":        ["summary", "content_summary", "post_summary"],
    "ai_classifier":          ["ai_classifier", "ai_clasifier"],
    "source_robot":           ["source_robot"],
}


# ---------------------------------------------------------------------------
# Mapping from raw LP API item → PersonData
# ---------------------------------------------------------------------------

def _cd(item: dict, key: str) -> str:
    """Safely extract contact_data.{key}.value as a stripped string."""
    cd = item.get("contact_data") or {}
    node = cd.get(key) or {}
    if isinstance(node, dict):
        return str(node.get("value") or "").strip()
    return str(node or "").strip()


def from_lp_api(item: dict, api_field_map: dict | None = None) -> PersonData:
    """
    Convert a raw LP API item to PersonData.

    api_field_map (from pipeline config's api_field_map) maps:
        internal_field_name → lp_contact_data_key

    When a field is in api_field_map, that specific LP key is used directly
    instead of the default multi-key fallback chain.
    """
    def get(internal_field: str) -> str:
        if api_field_map and internal_field in api_field_map:
            return _cd(item, api_field_map[internal_field])
        for key in _LP_API_DEFAULTS.get(internal_field, []):
            val = _cd(item, key)
            if val:
                return val
        return ""

    return PersonData(
        company_name=get("company_name"),
        company_website=get("company_website"),
        company_linkedin=clean_linkedin_url(get("company_linkedin")),
        company_country=get("company_country"),
        company_employee_count=get("company_employee_count"),
        first_name=get("first_name"),
        last_name=get("last_name"),
        email=get("email"),
        contact_linkedin=clean_linkedin_url(get("contact_linkedin")),
        position=get("position"),
        external_id=str(item.get("id") or ""),
        content_text=get("content_text"),
        content_url=get("content_url"),
        content_summary=get("content_summary"),
        ai_classifier=get("ai_classifier"),
        source_robot=get("source_robot"),
        lp_left_out=bool(item.get("is_left_out")),
        lp_replied=bool(item.get("has_reply_to_linkedin")),
        lp_project_id=item.get("_lp_project_id"),
    )


# ---------------------------------------------------------------------------
# Mapping from CSV row → PersonData
# ---------------------------------------------------------------------------

# Case-insensitive aliases for CSV column headers
_CSV_MAP: dict[str, str] = {
    # company
    "company name": "company_name", "company": "company_name",
    "firma": "company_name",
    "company website": "company_website", "website": "company_website",
    "company linkedin": "company_linkedin",
    "country": "company_country", "company country": "company_country",
    "employees": "company_employee_count", "employee count": "company_employee_count",
    "company employees": "company_employee_count",
    # contact
    "first name": "first_name", "firstname": "first_name",
    "last name": "last_name", "lastname": "last_name",
    "email": "email", "e-mail": "email",
    "linkedin": "contact_linkedin", "personal linkedin": "contact_linkedin",
    "position": "position", "title": "position", "job title": "position",
    # signal
    "post content": "content_text", "content text": "content_text",
    "post text": "content_text",
    "post url": "content_url", "content url": "content_url",
    "summary": "content_summary", "content summary": "content_summary",
    "ai classifier": "ai_classifier",
    "robot": "source_robot", "source robot": "source_robot",
}


def _csv_key(header: str, extra_map: dict[str, str] | None = None) -> Optional[str]:
    normalized = header.strip().lower().replace("_", " ")
    if extra_map:
        for k, v in extra_map.items():
            if k.strip().lower() == normalized:
                return v
    return _CSV_MAP.get(normalized)


def from_csv_row(row: dict, extra_map: dict[str, str] | None = None) -> PersonData:
    mapped: dict[str, str] = {}
    for header, value in row.items():
        key = _csv_key(header, extra_map)
        if key:
            mapped[key] = (value or "").strip()

    return PersonData(
        company_name=mapped.get("company_name", ""),
        company_website=mapped.get("company_website", ""),
        company_linkedin=clean_linkedin_url(mapped.get("company_linkedin", "")),
        company_country=mapped.get("company_country", ""),
        company_employee_count=mapped.get("company_employee_count", ""),
        first_name=mapped.get("first_name", ""),
        last_name=mapped.get("last_name", ""),
        email=mapped.get("email", ""),
        contact_linkedin=clean_linkedin_url(mapped.get("contact_linkedin", "")),
        position=mapped.get("position", ""),
        content_text=mapped.get("content_text", ""),
        content_url=mapped.get("content_url", ""),
        content_summary=mapped.get("content_summary", ""),
        ai_classifier=mapped.get("ai_classifier", ""),
        source_robot=mapped.get("source_robot", ""),
    )


def parse_csv(raw_bytes: bytes, column_map: dict[str, str] | None = None) -> list[PersonData]:
    text = raw_bytes.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    return [from_csv_row(row, extra_map=column_map) for row in reader]


# ---------------------------------------------------------------------------
# DB persistence — bulk strategy
# ---------------------------------------------------------------------------

def _chunks(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def _fp_or_filter(fps: list[str]) -> str:
    parts = []
    for fp in fps:
        escaped = fp.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'fingerprint.eq."{escaped}"')
    return ",".join(parts)


async def process_lp_import(db, people: list[PersonData], pipeline_key: str, batch_id: str) -> int:
    if not people:
        return 0

    rows = []
    for p in people:
        url = (p.content_url or "").strip()
        dedup = _normalize_url_for_dedup(url)
        if not dedup:
            continue

        rows.append({
            "pipeline_key": pipeline_key,
            "batch_id": batch_id,
            "dedup_key": dedup,
            "content_url": url or None,
            "content_text": p.content_text or None,
            "content_summary": p.content_summary or None,
            "ai_classifier": p.ai_classifier or None,
            "author_first_name": p.first_name or None,
            "author_last_name": p.last_name or None,
            "author_full_name": f"{p.first_name} {p.last_name}".strip() or None,
            "author_linkedin": p.contact_linkedin or None,
            "author_position": p.position or None,
            "company_name": p.company_name or None,
            "company_website": p.company_website or None,
            "company_linkedin": p.company_linkedin or None,
            "company_country": p.company_country or None,
            "company_employee_count": p.company_employee_count or None,
            "external_id": p.external_id or None,
            "source_robot": p.source_robot or None,
            "source_metadata": {
                "lp_left_out": p.lp_left_out,
                "lp_replied": p.lp_replied,
                "lp_project_id": p.lp_project_id,
            },
        })

    inserted = 0
    for chunk in _chunks(rows, 500):
        res = (
            await db.table("staging_leadspicker")
            .upsert(chunk, on_conflict="dedup_key", ignore_duplicates=True)
            .execute()
        )
        inserted += len(res.data or [])

    return inserted


def _normalize_url_for_dedup(url: str) -> str | None:
    if not url:
        return None
    import re
    u = url.lower().strip()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.rstrip("/") or None
