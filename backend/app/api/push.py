from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.config import settings
from app.core.runtime_config import get_runtime_value
from app.core.supabase import get_supabase
from app.services.push_lp import push_to_leadspicker
from app.services.push_airtable import push_entries_to_airtable

router = APIRouter(prefix="/push", tags=["push"])


class PushLpRequest(BaseModel):
    entry_ids: list[str]
    project_id: int
    pipeline_key: str | None = None


class PushAirtableRequest(BaseModel):
    entry_ids: list[str]
    table_name: str | None = None
    pipeline_key: str | None = None


async def _resolve_airtable_table(
    db: AsyncClient,
    entry_ids: list[str],
    requested_table_name: str | None,
) -> str:
    """
    Resolve Airtable table from unified pipeline configuration.

    - lp_czech  -> env-backed AIRTABLE_LP_CZECH_TABLE (always)
    - lp_general -> requested table if provided, else AIRTABLE_LP_GENERAL_TABLE
    - crunchbase -> requested table if provided, else AIRTABLE_CRUNCHBASE_TABLE
    - news -> requested table if provided, else AIRTABLE_NEWS_TABLE
    - other pipelines -> requested table must be provided
    """
    requested = (requested_table_name or "").strip()

    entries_res = (
        await db.table("pipeline_entries")
        .select("id,pipeline_type")
        .in_("id", entry_ids)
        .execute()
    )
    rows = entries_res.data or []
    if not rows:
        if requested:
            return requested
        raise HTTPException(status_code=422, detail="No matching pipeline entries found")

    pipeline_types = {r.get("pipeline_type") for r in rows if r.get("pipeline_type")}
    if len(pipeline_types) > 1:
        raise HTTPException(
            status_code=422,
            detail="All pushed entries must belong to the same pipeline_type",
        )

    pipeline_type = next(iter(pipeline_types), None)
    lp_czech_table = str(
        get_runtime_value("airtable_lp_czech_table") or settings.airtable_lp_czech_table
    ).strip()
    lp_general_table = str(
        get_runtime_value("airtable_lp_general_table") or settings.airtable_lp_general_table
    ).strip()
    crunchbase_table = str(
        get_runtime_value("airtable_crunchbase_table") or settings.airtable_crunchbase_table
    ).strip()
    news_table = str(
        get_runtime_value("airtable_news_table") or settings.airtable_news_table
    ).strip()

    if pipeline_type == "lp_czech":
        return lp_czech_table
    if pipeline_type == "lp_general":
        return requested or lp_general_table
    if pipeline_type == "crunchbase":
        return requested or crunchbase_table
    if pipeline_type == "news":
        return requested or news_table
    if requested:
        return requested

    raise HTTPException(
        status_code=422,
        detail="table_name is required for non-LP pipelines",
    )


# ---------------------------------------------------------------------------
# POST /api/push/leadspicker
# ---------------------------------------------------------------------------
@router.post("/leadspicker")
async def push_leadspicker(
    body: PushLpRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Push entries to a LeadsPicker project.
    Respects push_row_limit setting. Calls complete_push RPC per entry.
    Returns {pushed, failed, skipped}.
    """
    if not body.entry_ids:
        return {"pushed": 0, "failed": 0, "skipped": 0}

    # Load per-pipeline push column map from pipeline config
    push_map = None
    if body.pipeline_key:
        try:
            cfg_res = (
                await db.table("pipeline_configs")
                .select("default_import_params")
                .eq("pipeline_key", body.pipeline_key)
                .eq("is_active", True)
                .limit(1)
                .execute()
            )
            if cfg_res.data:
                params = cfg_res.data[0].get("default_import_params") or {}
                push_map = params.get("push_column_map") or None
        except Exception:
            pass  # mapping is optional; proceed without it

    return await push_to_leadspicker(db, body.entry_ids, body.project_id, push_map=push_map)


# ---------------------------------------------------------------------------
# POST /api/push/airtable
# ---------------------------------------------------------------------------
@router.post("/airtable")
async def push_airtable(
    body: PushAirtableRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Create Airtable records for the given entries.
    Batches in groups of 10 (Airtable limit). Calls complete_push RPC per entry.
    Returns {created, failed, skipped}.
    """
    if not body.entry_ids:
        return {"created": 0, "failed": 0, "skipped": 0}
    table_name = await _resolve_airtable_table(db, body.entry_ids, body.table_name)

    # Load airtable_push_column_map from pipeline config if pipeline_key provided
    push_map = None
    if body.pipeline_key:
        try:
            cfg_res = (
                await db.table("pipeline_configs")
                .select("default_import_params")
                .eq("pipeline_key", body.pipeline_key)
                .eq("is_active", True)
                .limit(1)
                .execute()
            )
            if cfg_res.data:
                params = cfg_res.data[0].get("default_import_params") or {}
                push_map = params.get("airtable_push_column_map") or None
        except Exception:
            pass  # optional; proceed without it

    try:
        return await push_entries_to_airtable(db, body.entry_ids, table_name, push_map=push_map)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc))


# ---------------------------------------------------------------------------
# GET /api/push/log/{batch_id}
# ---------------------------------------------------------------------------
@router.get("/log/{batch_id}")
async def get_push_log(
    batch_id: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Return all push_log rows for entries belonging to a batch,
    joined with pipeline_entry.id and contact full_name.
    """
    # Get all entry IDs for this batch
    entries_res = (
        await db.table("pipeline_entries")
        .select("id")
        .eq("batch_id", batch_id)
        .execute()
    )
    entry_ids = [r["id"] for r in (entries_res.data or [])]
    if not entry_ids:
        return []

    from app.services.leadspicker_normalize import _chunks
    logs: list[dict] = []
    for chunk in _chunks(entry_ids, 200):
        res = (
            await db.table("push_log")
            .select(
                "id,pipeline_entry_id,target_system,target_project_id,"
                "external_id,status,response_data,pushed_at"
            )
            .in_("pipeline_entry_id", chunk)
            .order("pushed_at", desc=True)
            .execute()
        )
        logs.extend(res.data or [])

    return logs
