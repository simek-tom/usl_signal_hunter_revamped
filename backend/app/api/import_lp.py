from datetime import datetime, timedelta, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Query
from pydantic import BaseModel, Field
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.models.models import PipelineType
from app.schemas.schemas import ImportLpRequest, ImportResult
from app.services.crunchbase_fetch import fetch_airtable_rows
from app.services.crunchbase_normalize import (
    parse_csv as parse_crunchbase_csv,
    process_crunchbase_import,
)
from app.services.dedup import deduplicate_batch
from app.services.leadspicker_fetch import fetch_project_people
from app.services.leadspicker_normalize import (
    from_lp_api,
    parse_csv as parse_lp_csv,
    process_lp_import,
)
from app.services.newsapi_fetch import fetch_news_articles
from app.services.news_normalize import from_newsapi_item, process_news_import

router = APIRouter(prefix="/import", tags=["import"])


class ImportCrunchbaseRequest(BaseModel):
    status: str | None = None
    contact_enriched: bool | None = None
    view: str | None = None
    max_records: int = Field(default=200, ge=1, le=5000)
    table_name: str | None = None


class ImportNewsRequest(BaseModel):
    query: str | None = None
    domains: str | None = None
    language: str | None = None
    from_date: str | None = None
    to_date: str | None = None
    sort_by: str | None = None
    page_size: int | None = Field(default=None, ge=1, le=100)
    max_pages: int | None = Field(default=None, ge=1, le=50)


async def _run_import(
    db: AsyncClient,
    people_raw,
    pipeline_type: PipelineType,
    source_details: dict,
    auto_dedup: bool = False,
) -> ImportResult:
    """Shared logic: create batch, persist people, return result."""
    # Create batch record up-front
    batch_result = (
        await db.table("import_batches")
        .insert(
            {
                "pipeline_type": pipeline_type.value,
                "source_details": source_details,
                "record_count": 0,
                "dedup_dropped_count": 0,
            }
        )
        .execute()
    )
    batch_id = batch_result.data[0]["id"]

    count = await process_lp_import(db, people_raw, pipeline_type.value, batch_id)
    # pipeline_type.value IS the pipeline_key (e.g. 'lp_general')

    # Update batch with final count
    await db.table("import_batches").update({"record_count": count}).eq(
        "id", batch_id
    ).execute()

    if auto_dedup:
        await deduplicate_batch(db, batch_id, pipeline_type.value)

    return ImportResult(
        batch_id=batch_id,
        pipeline_type=pipeline_type.value,
        record_count=count,
    )


async def _run_crunchbase_import(
    db: AsyncClient,
    rows,
    source_details: dict,
    auto_dedup: bool = False,
) -> ImportResult:
    batch_result = (
        await db.table("import_batches")
        .insert(
            {
                "pipeline_type": PipelineType.crunchbase.value,
                "source_details": source_details,
                "record_count": 0,
                "dedup_dropped_count": 0,
            }
        )
        .execute()
    )
    batch_id = batch_result.data[0]["id"]

    count = await process_crunchbase_import(db, rows, PipelineType.crunchbase.value, batch_id)

    await db.table("import_batches").update({"record_count": count}).eq(
        "id", batch_id
    ).execute()

    if auto_dedup:
        await deduplicate_batch(db, batch_id, PipelineType.crunchbase.value)

    return ImportResult(
        batch_id=batch_id,
        pipeline_type=PipelineType.crunchbase.value,
        record_count=count,
    )


async def _run_news_import(
    db: AsyncClient,
    rows,
    source_details: dict,
    auto_dedup: bool = False,
) -> ImportResult:
    batch_result = (
        await db.table("import_batches")
        .insert(
            {
                "pipeline_type": PipelineType.news.value,
                "source_details": source_details,
                "record_count": 0,
                "dedup_dropped_count": 0,
            }
        )
        .execute()
    )
    batch_id = batch_result.data[0]["id"]

    count = await process_news_import(db, rows, PipelineType.news.value, batch_id)

    await db.table("import_batches").update({"record_count": count}).eq(
        "id", batch_id
    ).execute()

    if auto_dedup:
        await deduplicate_batch(db, batch_id, PipelineType.news.value)

    return ImportResult(
        batch_id=batch_id,
        pipeline_type=PipelineType.news.value,
        record_count=count,
    )


async def _get_setting_value(db: AsyncClient, key: str):
    res = (
        await db.table("settings")
        .select("value")
        .eq("key", key)
        .limit(1)
        .execute()
    )
    if not (res.data or []):
        return None
    return res.data[0].get("value")


def _as_str(val, fallback: str) -> str:
    if val is None:
        return fallback
    out = str(val).strip()
    return out or fallback


def _as_int(val, fallback: int) -> int:
    try:
        return int(val)
    except Exception:
        return fallback


@router.post("/lp", response_model=ImportResult)
async def import_from_lp_api(
    body: ImportLpRequest,
    auto_dedup: bool = Query(False),
    db: AsyncClient = Depends(get_supabase),
):
    """Fetch people from LP API for given project IDs, import into pipeline."""
    try:
        raw_items = await fetch_project_people(body.project_ids)
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"LP API error {exc.response.status_code}: {exc.response.text[:200]}",
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"LP API unreachable: {exc}")

    if not raw_items:
        raise HTTPException(status_code=422, detail="No people returned from LP projects")

    people = [from_lp_api(item) for item in raw_items]

    return await _run_import(
        db,
        people,
        body.pipeline_type,
        source_details={
            "project_ids": body.project_ids,
            "fetched_count": len(raw_items),
        },
        auto_dedup=auto_dedup,
    )


@router.post("/lp/upload", response_model=ImportResult)
async def import_from_csv(
    pipeline_type: PipelineType = Query(...),
    auto_dedup: bool = Query(False),
    file: UploadFile = File(...),
    db: AsyncClient = Depends(get_supabase),
):
    """Parse a semicolon-delimited LP CSV export and import into pipeline."""
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="File must be a .csv")

    raw_bytes = await file.read()
    try:
        people = parse_lp_csv(raw_bytes)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"CSV parse error: {exc}")

    if not people:
        raise HTTPException(status_code=422, detail="CSV contains no data rows")

    return await _run_import(
        db,
        people,
        pipeline_type,
        source_details={
            "filename": file.filename,
            "row_count": len(people),
        },
        auto_dedup=auto_dedup,
    )


@router.post("/crunchbase", response_model=ImportResult)
async def import_from_crunchbase_airtable(
    body: ImportCrunchbaseRequest,
    auto_dedup: bool = Query(False),
    db: AsyncClient = Depends(get_supabase),
):
    """
    Fetch Crunchbase source rows from Airtable and import into crunchbase pipeline.
    """
    try:
        rows = await fetch_airtable_rows(
            table_name=body.table_name,
            status=body.status,
            contact_enriched=body.contact_enriched,
            view=body.view,
            max_records=body.max_records,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Airtable fetch failed: {exc}")

    if not rows:
        raise HTTPException(status_code=422, detail="No Crunchbase rows fetched from Airtable")

    return await _run_crunchbase_import(
        db,
        rows,
        source_details={
            "source": "airtable",
            "status": body.status,
            "contact_enriched": body.contact_enriched,
            "view": body.view,
            "max_records": body.max_records,
            "table_name": body.table_name,
            "fetched_count": len(rows),
        },
        auto_dedup=auto_dedup,
    )


@router.post("/crunchbase/upload", response_model=ImportResult)
async def import_crunchbase_from_csv(
    auto_dedup: bool = Query(False),
    file: UploadFile = File(...),
    db: AsyncClient = Depends(get_supabase),
):
    """
    Parse and import Crunchbase CSV exports.
    """
    if not file.filename or not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=422, detail="File must be a .csv")

    raw_bytes = await file.read()
    try:
        rows = parse_crunchbase_csv(raw_bytes)
    except Exception as exc:
        raise HTTPException(status_code=422, detail=f"CSV parse error: {exc}")

    if not rows:
        raise HTTPException(status_code=422, detail="CSV contains no data rows")

    return await _run_crunchbase_import(
        db,
        rows,
        source_details={
            "source": "csv",
            "filename": file.filename,
            "row_count": len(rows),
        },
        auto_dedup=auto_dedup,
    )


@router.post("/news", response_model=ImportResult)
async def import_from_news_api(
    body: ImportNewsRequest,
    auto_dedup: bool = Query(True),
    db: AsyncClient = Depends(get_supabase),
):
    """
    Fetch NewsAPI articles with multi-page pagination and import into news pipeline.
    Defaults come from settings table when request fields are omitted.
    """
    default_query = _as_str(
        await _get_setting_value(db, "news_default_query"),
        "(Series A OR Series B OR Series C) AND (expansion OR global expansion)",
    )
    default_domains = _as_str(
        await _get_setting_value(db, "news_default_domains"),
        "techcrunch.com,news.crunchbase.com,venturebeat.com,theinformation.com,sifted.eu",
    )
    default_language = _as_str(await _get_setting_value(db, "news_default_language"), "en")
    default_page_size = _as_int(await _get_setting_value(db, "news_default_page_size"), 100)
    default_max_pages = _as_int(await _get_setting_value(db, "news_default_max_pages"), 3)
    default_days_back = _as_int(await _get_setting_value(db, "news_default_days_back"), 7)

    now = datetime.now(timezone.utc)
    default_from = (now - timedelta(days=max(default_days_back, 1))).date().isoformat()
    default_to = now.date().isoformat()

    query = _as_str(body.query, default_query)
    domains = _as_str(body.domains, default_domains)
    language = _as_str(body.language, default_language)
    from_date = _as_str(body.from_date, default_from)
    to_date = _as_str(body.to_date, default_to)
    sort_by = _as_str(body.sort_by, "publishedAt")
    page_size = body.page_size or default_page_size
    max_pages = body.max_pages or default_max_pages

    try:
        fetched = await fetch_news_articles(
            query=query,
            domains=domains,
            language=language,
            from_date=from_date,
            to_date=to_date,
            sort_by=sort_by,
            page_size=page_size,
            max_pages=max_pages,
        )
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"NewsAPI error {exc.response.status_code}: {exc.response.text[:200]}",
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"NewsAPI unreachable: {exc}")
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    raw_articles = fetched.get("articles") or []
    if not raw_articles:
        raise HTTPException(status_code=422, detail="No articles returned from NewsAPI")

    rows = [from_newsapi_item(item) for item in raw_articles]
    rows = [r for r in rows if r.content_url]

    if not rows:
        raise HTTPException(status_code=422, detail="NewsAPI returned no articles with URLs")

    return await _run_news_import(
        db,
        rows,
        source_details={
            "source": "newsapi",
            "query": query,
            "domains": domains,
            "language": language,
            "from_date": from_date,
            "to_date": to_date,
            "sort_by": sort_by,
            "page_size": page_size,
            "max_pages": max_pages,
            "pages_fetched": fetched.get("pages_fetched"),
            "total_results": fetched.get("total_results"),
            "fetched_count": len(raw_articles),
        },
        auto_dedup=auto_dedup,
    )
