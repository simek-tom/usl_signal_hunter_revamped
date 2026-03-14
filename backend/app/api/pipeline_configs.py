from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from typing import Optional
from supabase import AsyncClient
from app.core.supabase import get_supabase

router = APIRouter(prefix="/pipeline-configs", tags=["pipeline-configs"])


class PipelineConfigCreate(BaseModel):
    source_type: str  # 'leadspicker' | 'crunchbase' | 'news'
    pipeline_key: str
    label: str
    airtable_table_name: Optional[str] = None
    lp_project_ids: Optional[list[int]] = None
    default_import_params: Optional[dict] = None


class PipelineConfigUpdate(BaseModel):
    label: Optional[str] = None
    airtable_table_name: Optional[str] = None
    lp_project_ids: Optional[list[int]] = None
    default_import_params: Optional[dict] = None


@router.get("")
async def list_configs(db: AsyncClient = Depends(get_supabase)):
    res = (
        await db.table("pipeline_configs")
        .select("*")
        .eq("is_active", True)
        .order("source_type")
        .order("label")
        .execute()
    )
    return res.data


@router.post("", status_code=201)
async def create_config(
    body: PipelineConfigCreate,
    db: AsyncClient = Depends(get_supabase),
):
    if body.source_type not in ("leadspicker", "crunchbase", "news"):
        raise HTTPException(status_code=422, detail="source_type must be leadspicker, crunchbase, or news")
    row = {
        "source_type": body.source_type,
        "pipeline_key": body.pipeline_key,
        "label": body.label,
        "airtable_table_name": body.airtable_table_name,
        "lp_project_ids": body.lp_project_ids or [],
        "default_import_params": body.default_import_params or {},
    }
    res = await db.table("pipeline_configs").insert(row).execute()
    return res.data[0]


@router.put("/{config_id}")
async def update_config(
    config_id: str,
    body: PipelineConfigUpdate,
    db: AsyncClient = Depends(get_supabase),
):
    update = {k: v for k, v in body.dict(exclude_unset=True).items() if v is not None}
    if not update:
        raise HTTPException(status_code=422, detail="No fields to update")
    res = (
        await db.table("pipeline_configs")
        .update(update)
        .eq("id", config_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Config not found")
    return res.data[0]


@router.delete("/{config_id}", status_code=204)
async def delete_config(
    config_id: str,
    db: AsyncClient = Depends(get_supabase),
):
    res = (
        await db.table("pipeline_configs")
        .update({"is_active": False})
        .eq("id", config_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Config not found")