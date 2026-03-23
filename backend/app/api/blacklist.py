import csv
import io
import json
from uuid import UUID

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.core.utils import (
    clean_company_name,
    extract_linkedin_slug,
    extract_root_domain,
)
from app.api._helpers import build_blacklist_row
from app.schemas.schemas import BlacklistCreate, BlacklistRead

router = APIRouter(prefix="/blacklist", tags=["blacklist"])

_TARGET_FIELDS = {"company_name", "company_linkedin", "company_website", "reason", "added_by", "origin"}


def _score_entry(entry: dict, *, query_slug: str | None, query_domain: str | None,
                 query_clean: str | None) -> tuple[int, str]:
    """Score a single blacklist entry against query data. Returns (score, match_type)."""
    best_score = 0
    best_type = "none"

    # LinkedIn slug exact match → 100
    if query_slug and entry.get("linkedin_slug"):
        if query_slug == entry["linkedin_slug"]:
            return 100, "linkedin_slug"

    # Root domain exact match → 100
    if query_domain and entry.get("root_domain"):
        if query_domain == entry["root_domain"]:
            return 100, "root_domain"

    # Clean name matching
    if query_clean and entry.get("company_name_normalized"):
        entry_clean = entry["company_name_normalized"].replace(" ", "")
        if not entry_clean:
            pass
        elif query_clean == entry_clean:
            best_score, best_type = 60, "name_exact"
        elif len(query_clean) >= 4 and len(entry_clean) >= 4:
            if query_clean in entry_clean or entry_clean in query_clean:
                if best_score < 30:
                    best_score, best_type = 30, "name_substring"

    return best_score, best_type


def _severity(score: int) -> str:
    if score >= 100:
        return "high"
    if score >= 30:
        return "medium"
    return "low"


# ── CRUD Endpoints ───────────────────────────────────────────────────────────


@router.get("", response_model=list[BlacklistRead])
async def list_blacklist(db: AsyncClient = Depends(get_supabase)):
    result = (
        await db.table("blacklisted_companies")
        .select("*")
        .order("created_at", desc=True)
        .execute()
    )
    return result.data


@router.post("", response_model=BlacklistRead, status_code=201)
async def add_to_blacklist(
    body: BlacklistCreate,
    db: AsyncClient = Depends(get_supabase),
):
    row = build_blacklist_row(
        body.company_name,
        body.company_linkedin,
        body.company_website,
        body.reason,
        body.added_by,
        origin=body.origin,
        pipeline_entry_id=body.pipeline_entry_id,
    )
    result = await db.table("blacklisted_companies").insert(row).execute()
    return result.data[0]


@router.delete("/{entry_id}", status_code=204)
async def remove_from_blacklist(
    entry_id: UUID,
    db: AsyncClient = Depends(get_supabase),
):
    result = (
        await db.table("blacklisted_companies")
        .delete()
        .eq("id", str(entry_id))
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail="Blacklist entry not found")


# ── Check Endpoints ──────────────────────────────────────────────────────────


class BlacklistCheckRequest(BaseModel):
    company_name: str | None = None
    company_linkedin: str | None = None
    company_website: str | None = None


class BlacklistCheckBatchItem(BaseModel):
    id: str
    company_name: str | None = None
    company_linkedin: str | None = None
    company_website: str | None = None


async def _fetch_blacklist(db: AsyncClient) -> list[dict]:
    result = await db.table("blacklisted_companies").select("*").execute()
    return result.data


def _check_one(entries: list[dict], req: BlacklistCheckRequest) -> dict:
    query_slug = extract_linkedin_slug(req.company_linkedin)
    query_domain = extract_root_domain(req.company_website)
    query_clean = clean_company_name(req.company_name) if req.company_name else None

    best_score = 0
    best_type = "none"
    best_entry = None

    for entry in entries:
        score, match_type = _score_entry(
            entry, query_slug=query_slug, query_domain=query_domain, query_clean=query_clean
        )
        if score > best_score:
            best_score = score
            best_type = match_type
            best_entry = entry

    result = {"score": best_score, "severity": _severity(best_score), "match_type": best_type}
    if best_entry and best_score >= 30:
        result["matched_entry"] = {
            "id": best_entry.get("id"),
            "company_name": best_entry.get("company_name"),
            "company_linkedin": best_entry.get("company_linkedin"),
            "company_website": best_entry.get("company_website"),
        }
    return result


@router.post("/check")
async def check_blacklist(
    body: BlacklistCheckRequest,
    db: AsyncClient = Depends(get_supabase),
):
    entries = await _fetch_blacklist(db)
    return _check_one(entries, body)


@router.post("/check-batch")
async def check_blacklist_batch(
    items: list[BlacklistCheckBatchItem],
    db: AsyncClient = Depends(get_supabase),
):
    entries = await _fetch_blacklist(db)
    results = {}
    for item in items:
        req = BlacklistCheckRequest(
            company_name=item.company_name,
            company_linkedin=item.company_linkedin,
            company_website=item.company_website,
        )
        results[item.id] = _check_one(entries, req)
    return {"results": results}


# ── CSV Upload ───────────────────────────────────────────────────────────────


def _detect_delimiter(text: str) -> str:
    """Auto-detect CSV delimiter from first line."""
    first_line = text.split("\n", 1)[0]
    for delim in (";", "\t", ","):
        if delim in first_line:
            return delim
    return ","


@router.post("/upload-csv")
async def upload_blacklist_csv(
    file: UploadFile = File(...),
    column_map: str | None = Query(None, description="JSON object mapping CSV header → target field"),
    default_reason: str | None = Query(None, description="Fallback reason for all rows without a reason column"),
    default_added_by: str | None = Query(None, description="Fallback added_by for all rows without an added_by column"),
    default_origin: str | None = Query(None, description="Fallback origin for all rows without a origin column"),
    db: AsyncClient = Depends(get_supabase),
):
    raw = await file.read()
    text = raw.decode("utf-8-sig")
    if not text.strip():
        raise HTTPException(status_code=400, detail="Empty CSV file")

    extra_map: dict[str, str] | None = None
    if column_map:
        try:
            extra_map = json.loads(column_map)
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail="Invalid column_map JSON")

    delimiter = _detect_delimiter(text)
    reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)

    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV has no headers")

    if not extra_map:
        raise HTTPException(
            status_code=400,
            detail="column_map is required. Map CSV headers to target fields (company_name, company_linkedin, company_website, reason, added_by).",
        )

    # Build header map from explicit mapping only (exact header names, no normalization)
    header_map: dict[str, str] = {}
    for csv_header, target in extra_map.items():
        if target in _TARGET_FIELDS and csv_header in reader.fieldnames:
            header_map[csv_header] = target

    if "company_name" not in header_map.values():
        raise HTTPException(
            status_code=400,
            detail=f"No column maps to company_name. Headers found: {list(reader.fieldnames)}",
        )

    rows_to_insert: list[dict] = []
    total_in_file = 0
    for row in reader:
        total_in_file += 1
        mapped: dict[str, str] = {}
        for csv_col, target_field in header_map.items():
            val = (row.get(csv_col) or "").strip()
            if val:
                mapped[target_field] = val

        company_name = mapped.get("company_name")
        if not company_name:
            continue

        rows_to_insert.append(build_blacklist_row(
            company_name,
            mapped.get("company_linkedin"),
            mapped.get("company_website"),
            mapped.get("reason") or default_reason,
            mapped.get("added_by") or default_added_by,
            origin=mapped.get("origin") or default_origin,
            pipeline_entry_id="csv_import",
        ))

    if not rows_to_insert:
        return {"imported": 0, "skipped_duplicates": 0, "total_in_file": total_in_file}

    # Fetch existing for dedup
    existing = await db.table("blacklisted_companies").select("company_name_normalized").execute()
    existing_set = {r["company_name_normalized"] for r in existing.data if r.get("company_name_normalized")}

    new_rows: list[dict] = []
    skipped = 0
    seen: set[str] = set()
    for r in rows_to_insert:
        norm = r["company_name_normalized"]
        if norm in existing_set or norm in seen:
            skipped += 1
            continue
        seen.add(norm)
        new_rows.append(r)

    if new_rows:
        for i in range(0, len(new_rows), 500):
            await db.table("blacklisted_companies").insert(new_rows[i : i + 500]).execute()

    return {
        "imported": len(new_rows),
        "skipped_duplicates": skipped,
        "total_in_file": total_in_file,
    }
