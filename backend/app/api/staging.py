from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.supabase import get_supabase

router = APIRouter(prefix="/staging", tags=["staging"])

SOURCE_TABLE_MAP = {
    "leadspicker": "staging_leadspicker",
    "crunchbase": "staging_crunchbase",
    "news": "staging_news",
}


async def _resolve_source_type(db: AsyncClient, pipeline_key: str) -> str:
    res = (
        await db.table("pipeline_configs")
        .select("source_type")
        .eq("pipeline_key", pipeline_key)
        .limit(1)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail=f"Pipeline config not found: {pipeline_key}")
    return res.data[0]["source_type"]


async def _get_staging_table(db: AsyncClient, pipeline_key: str) -> str:
    source_type = await _resolve_source_type(db, pipeline_key)
    table = SOURCE_TABLE_MAP.get(source_type)
    if not table:
        raise HTTPException(status_code=422, detail=f"Unknown source_type: {source_type}")
    return table


# ---- List staging entries for analysis ----

@router.get("/{pipeline_key}/entries")
async def list_staging_entries(
    pipeline_key: str,
    batch_id: Optional[str] = Query(None),
    db: AsyncClient = Depends(get_supabase),
):
    table = await _get_staging_table(db, pipeline_key)
    query = db.table(table).select("*").eq("pipeline_key", pipeline_key)
    if batch_id:
        query = query.eq("batch_id", batch_id)
    query = query.order("ai_pre_score", desc=True, nullsfirst=False)

    entries = []
    page_size = 1000
    offset = 0
    while True:
        res = await query.range(offset, offset + page_size - 1).execute()
        chunk = res.data or []
        entries.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    return entries


# ---- Label a staging entry (instant save, NO promotion) ----

class LabelRequest(BaseModel):
    label: str   # 'yes' | 'no' | 'cc'
    learning_data: Optional[bool] = None


@router.post("/{pipeline_key}/{staging_id}/label")
async def label_staging_entry(
    pipeline_key: str,
    staging_id: str,
    body: LabelRequest,
    db: AsyncClient = Depends(get_supabase),
):
    if body.label not in ("yes", "no", "cc"):
        raise HTTPException(status_code=422, detail="label must be yes, no, or cc")

    table = await _get_staging_table(db, pipeline_key)
    now = datetime.now(timezone.utc).isoformat()

    update = {
        "label": body.label,
        "labeled_at": now,
    }
    if body.learning_data is not None:
        update["learning_data"] = body.learning_data

    # If relabeling a previously promoted entry to 'no', eliminate the pipeline_entry
    existing = (
        await db.table(table)
        .select("pipeline_entry_id, label")
        .eq("id", staging_id)
        .single()
        .execute()
    )
    if not existing.data:
        raise HTTPException(status_code=404, detail="Staging entry not found")

    old_entry_id = existing.data.get("pipeline_entry_id")
    if old_entry_id and body.label == "no":
        await (
            db.table("pipeline_entries")
            .update({"status": "eliminated"})
            .eq("id", old_entry_id)
            .execute()
        )

    res = (
        await db.table(table)
        .update(update)
        .eq("id", staging_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Staging entry not found")

    return {"success": True, "entry": res.data[0]}


# ---- Enrich a staging entry ----

class EnrichRequest(BaseModel):
    enriched_contact_name: Optional[str] = None
    enriched_contact_linkedin: Optional[str] = None
    enriched_contact_position: Optional[str] = None
    enriched_company_name: Optional[str] = None
    enriched_company_website: Optional[str] = None
    enriched_company_linkedin: Optional[str] = None


@router.put("/{pipeline_key}/{staging_id}/enrich")
async def enrich_staging_entry(
    pipeline_key: str,
    staging_id: str,
    body: EnrichRequest,
    db: AsyncClient = Depends(get_supabase),
):
    table = await _get_staging_table(db, pipeline_key)
    update = {k: v for k, v in body.dict(exclude_unset=True).items() if v is not None}
    if not update:
        raise HTTPException(status_code=422, detail="No enrichment fields provided")

    res = (
        await db.table(table)
        .update(update)
        .eq("id", staging_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Staging entry not found")

    return {"success": True, "entry": res.data[0]}


# ---- CB-specific action on staging ----

class CbStagingActionRequest(BaseModel):
    action: str  # 'yes' | 'eliminate' | 'uneliminate' | 'save_next' | 'save_stay'
    message_fin: Optional[str] = None
    main_contact: Optional[str] = None
    secondary_contact_1: Optional[str] = None
    secondary_contact_2: Optional[str] = None
    secondary_contact_3: Optional[str] = None


@router.post("/{pipeline_key}/{staging_id}/cb-action")
async def cb_staging_action(
    pipeline_key: str,
    staging_id: str,
    body: CbStagingActionRequest,
    db: AsyncClient = Depends(get_supabase),
):
    table = await _get_staging_table(db, pipeline_key)
    now = datetime.now(timezone.utc).isoformat()

    update = {}
    if body.message_fin is not None:
        update["message_fin"] = body.message_fin
    if body.main_contact is not None:
        update["main_contact_linkedin"] = body.main_contact
    if body.secondary_contact_1 is not None:
        update["secondary_contact_1"] = body.secondary_contact_1
    if body.secondary_contact_2 is not None:
        update["secondary_contact_2"] = body.secondary_contact_2
    if body.secondary_contact_3 is not None:
        update["secondary_contact_3"] = body.secondary_contact_3

    if body.action == "yes":
        update["label"] = "yes"
        update["labeled_at"] = now
        update["workflow_status"] = "analyzed"
    elif body.action == "eliminate":
        update["workflow_status"] = "eliminated"
    elif body.action == "uneliminate":
        update["workflow_status"] = "new"
    elif body.action == "save_next":
        update["workflow_status"] = "pushed-ready"

    res = (
        await db.table(table)
        .update(update)
        .eq("id", staging_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Staging entry not found")

    return {"success": True, "entry": res.data[0]}

@router.post("/{pipeline_key}/finish-analysis")
async def finish_analysis(
    pipeline_key: str,
    body: dict,  # expects { "batch_id": "..." }
    db: AsyncClient = Depends(get_supabase),
):
    """
    Promote all YES/CC labeled staging rows that haven't been promoted yet.
    Creates companies, contacts, signals, pipeline_entries.
    """
    from app.services.promote import promote_batch

    batch_id = body.get("batch_id")
    table = await _get_staging_table(db, pipeline_key)
    source_type = await _resolve_source_type(db, pipeline_key)

    result = await promote_batch(db, table, source_type, pipeline_key, batch_id)
    return result

from app.services.contacted_check import check_contacted

@router.post("/contacted-check")
async def contacted_check_endpoint(
    body: dict,
    db: AsyncClient = Depends(get_supabase),
):
    return await check_contacted(
        db,
        company_name=body.get("company_name"),
        company_website=body.get("company_website"),
        company_linkedin=body.get("company_linkedin"),
    )