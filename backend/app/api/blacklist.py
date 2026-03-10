from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.core.utils import normalize_company_name
from app.schemas.schemas import BlacklistCreate, BlacklistRead

router = APIRouter(prefix="/blacklist", tags=["blacklist"])


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
    row = {
        "company_name": body.company_name,
        "company_name_normalized": normalize_company_name(body.company_name),
        "reason": body.reason,
        "added_by": body.added_by,
    }
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
