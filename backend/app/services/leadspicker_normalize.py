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
# Mapping from raw LP API item → PersonData
# ---------------------------------------------------------------------------

def _cd(item: dict, key: str) -> str:
    """Safely extract contact_data.{key}.value as a stripped string."""
    cd = item.get("contact_data") or {}
    node = cd.get(key) or {}
    if isinstance(node, dict):
        return str(node.get("value") or "").strip()
    return str(node or "").strip()


def from_lp_api(item: dict) -> PersonData:
    return PersonData(
        company_name=_cd(item, "company_name"),
        company_website=_cd(item, "company_website"),
        company_linkedin=clean_linkedin_url(_cd(item, "company_linkedin")),
        company_country=(
            _cd(item, "country")
            or _cd(item, "linkedin_company_country")
        ),
        company_employee_count=(
            _cd(item, "company_employees") or _cd(item, "linkedin_company_size")
        ),
        first_name=_cd(item, "first_name"),
        last_name=_cd(item, "last_name"),
        email=_cd(item, "email"),
        contact_linkedin=clean_linkedin_url(_cd(item, "linkedin")),
        position=_cd(item, "position"),
        external_id=str(item.get("id") or ""),
        content_text=(
            _cd(item, "post_content")
            or _cd(item, "linkedin_post")
            or _cd(item, "content_text")
        ),
        content_url=(
            _cd(item, "linkedin_post_url")
            or _cd(item, "post_url")
            or _cd(item, "content_url")
        ),
        content_summary=(
            _cd(item, "summary")
            or _cd(item, "content_summary")
            or _cd(item, "post_summary")
        ),
        ai_classifier=(
            _cd(item, "ai_classifier")
            or _cd(item, "ai_clasifier")   # LP typo variant
        ),
        source_robot=_cd(item, "source_robot"),
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


def _csv_key(header: str) -> Optional[str]:
    return _CSV_MAP.get(header.strip().lower().replace("_", " "))


def from_csv_row(row: dict) -> PersonData:
    mapped: dict[str, str] = {}
    for header, value in row.items():
        key = _csv_key(header)
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


def parse_csv(raw_bytes: bytes) -> list[PersonData]:
    text = raw_bytes.decode("utf-8-sig")  # strip BOM if present
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    return [from_csv_row(row) for row in reader]


# ---------------------------------------------------------------------------
# DB persistence — bulk strategy
# ---------------------------------------------------------------------------

def _chunks(lst: list, size: int):
    for i in range(0, len(lst), size):
        yield lst[i : i + size]


def _fp_or_filter(fps: list[str]) -> str:
    """
    Build a PostgREST OR filter string for fingerprint equality lookups.

    PostgREST in.() uses comma as list delimiter so fingerprints that
    contain commas (e.g. 'acme, inc.|acme.com') silently fail to match.
    Using or=() with per-value double-quoting avoids that entirely.

    Backslashes and double-quotes inside a value are escaped per PostgREST
    quoting rules; all values are quoted so no edge cases are missed.
    """
    parts = []
    for fp in fps:
        escaped = fp.replace("\\", "\\\\").replace('"', '\\"')
        parts.append(f'fingerprint.eq."{escaped}"')
    return ",".join(parts)


async def process_lp_import(
    db: AsyncClient,
    people: list[PersonData],
    pipeline_type: str,
    batch_id: str,
) -> int:
    """
    Persist all people to Supabase using bulk inserts.

    Request count:
      - 1 bulk SELECT (fingerprints, chunked per 100, via or= filter)
      - 1 bulk INSERT for new companies (chunked per 500)
      - 1 RPC per unique *existing* company fingerprint (deduplicated)
      - 1 bulk INSERT signals (chunked per 500)
      - 1 bulk INSERT contacts (chunked per 500)
      - 1 bulk INSERT pipeline_entries (chunked per 500)

    Returns count of inserted pipeline entries.
    """
    if not people:
        return 0

    # ------------------------------------------------------------------
    # Phase 1: resolve company_id for every person
    # ------------------------------------------------------------------

    # 1a. Compute fingerprint + domain per person (pure Python, no DB)
    person_fps: list[Optional[str]] = []
    fp_meta: dict[str, tuple[PersonData, str]] = {}  # fp -> (person, domain)

    for p in people:
        domain = normalize_domain(p.company_website)
        fp = make_fingerprint(p.company_name, domain)
        person_fps.append(fp)
        if fp and fp not in fp_meta:
            fp_meta[fp] = (p, domain)

    all_fps = list(fp_meta.keys())

    # 1b. Bulk SELECT existing companies.
    #     Use or=() with per-value quoting instead of in=() because
    #     PostgREST in.() treats comma as a delimiter, causing fingerprints
    #     that contain commas to silently fail to match.
    existing_map: dict[str, str] = {}  # fingerprint -> company_id
    for chunk in _chunks(all_fps, 100):
        rows = (
            await db.table("companies")
            .select("id, fingerprint")
            .or_(_fp_or_filter(chunk))
            .execute()
        )
        for row in rows.data:
            existing_map[row["fingerprint"]] = row["id"]

    # 1c. Bulk INSERT new companies
    new_fps_set = {fp for fp in all_fps if fp not in existing_map}
    if new_fps_set:
        new_rows = []
        for fp in new_fps_set:
            p, domain = fp_meta[fp]
            new_rows.append(
                {
                    "name_raw": p.company_name,
                    "domain_normalized": domain or None,
                    "linkedin_url": p.company_linkedin or None,
                    "linkedin_url_cleaned": p.company_linkedin or None,
                    "website": p.company_website or None,
                    "country": p.company_country or None,
                    "employee_count": p.company_employee_count or None,
                    "fingerprint": fp,
                }
            )
        for chunk in _chunks(new_rows, 500):
            result = await db.table("companies").insert(chunk).execute()
            for row in result.data:
                existing_map[row["fingerprint"]] = row["id"]

    # 1d. Enrich existing companies — one RPC per unique fingerprint (deduplicated)
    for fp, (p, domain) in fp_meta.items():
        if fp not in new_fps_set and fp in existing_map:
            await db.rpc(
                "enrich_company",
                {
                    "p_id": existing_map[fp],
                    "p_domain": domain or None,
                    "p_linkedin_url": p.company_linkedin or None,
                    "p_website": p.company_website or None,
                    "p_country": p.company_country or None,
                    "p_employee_count": p.company_employee_count or None,
                    "p_enriched_by": "leadspicker",
                },
            ).execute()

    # Resolve company_id per person (None if no fingerprint)
    company_ids = [existing_map.get(fp) if fp else None for fp in person_fps]

    # ------------------------------------------------------------------
    # Phase 2: Bulk INSERT signals
    # ------------------------------------------------------------------
    signal_rows = [
        {
            "company_id": cid,
            "source_type": "leadspicker",
            "external_id": p.external_id or None,
            "content_text": p.content_text or None,
            "content_url": p.content_url or None,
            "content_summary": p.content_summary or None,
            "ai_classifier": p.ai_classifier or None,
            "source_robot": p.source_robot or None,
            "source_metadata": {
                "lp_left_out": p.lp_left_out,
                "lp_replied": p.lp_replied,
                "lp_project_id": p.lp_project_id,
            },
        }
        for p, cid in zip(people, company_ids)
    ]
    signal_ids: list[str] = []
    for chunk in _chunks(signal_rows, 500):
        res = await db.table("signals").insert(chunk).execute()
        signal_ids.extend(r["id"] for r in res.data)

    # ------------------------------------------------------------------
    # Phase 3: Bulk INSERT contacts
    # ------------------------------------------------------------------
    contact_rows = [
        {
            "company_id": cid,
            "first_name": p.first_name or None,
            "last_name": p.last_name or None,
            "full_name": f"{p.first_name} {p.last_name}".strip() or None,
            "linkedin_url": p.contact_linkedin or None,
            "email": p.email or None,
            "relation_to_company": p.position or None,
            "source": "leadspicker",
        }
        for p, cid in zip(people, company_ids)
    ]
    contact_ids: list[str] = []
    for chunk in _chunks(contact_rows, 500):
        res = await db.table("contacts").insert(chunk).execute()
        contact_ids.extend(r["id"] for r in res.data)

    # ------------------------------------------------------------------
    # Phase 4: Bulk INSERT pipeline_entries
    # ------------------------------------------------------------------
    entry_rows = [
        {
            "signal_id": sid,
            "contact_id": cid,
            "pipeline_type": pipeline_type,
            "batch_id": batch_id,
            "status": "new",
        }
        for sid, cid in zip(signal_ids, contact_ids)
    ]
    for chunk in _chunks(entry_rows, 500):
        await db.table("pipeline_entries").insert(chunk).execute()

    return len(people)
