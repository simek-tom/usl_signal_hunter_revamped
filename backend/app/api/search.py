from fastapi import APIRouter, Depends, Query
from supabase import AsyncClient

from app.core.supabase import get_supabase

router = APIRouter(prefix="/search", tags=["search"])

_COMPANY_SELECT = (
    "id,name_raw,name_normalized,domain_normalized,"
    "linkedin_url,website,country,employee_count,industry,fingerprint"
)


@router.get("/master")
async def master_search(
    q: str = Query(..., min_length=1),
    db: AsyncClient = Depends(get_supabase),
):
    """
    Search companies by normalized name.
    Returns up to 10 exact matches + up to 10 additional partial matches.
    """
    term = q.strip().lower()

    # Exact: match name_normalized (when populated) OR name_raw case-insensitively
    exact_res = (
        await db.table("companies")
        .select(_COMPANY_SELECT)
        .or_(f"name_normalized.eq.{term},name_raw.ilike.{term}")
        .limit(10)
        .execute()
    )
    exact = exact_res.data or []
    exact_ids = {r["id"] for r in exact}

    # Partial: substring match in either field
    partial_res = (
        await db.table("companies")
        .select(_COMPANY_SELECT)
        .or_(f"name_normalized.ilike.%{term}%,name_raw.ilike.%{term}%")
        .limit(20)
        .execute()
    )
    partial = [r for r in (partial_res.data or []) if r["id"] not in exact_ids][:10]

    return {"exact": exact, "partial": partial}
