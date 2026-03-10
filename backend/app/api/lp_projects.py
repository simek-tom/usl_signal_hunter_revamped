from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.schemas.schemas import LpProjectRead
from app.services.leadspicker import fetch_lp_projects

router = APIRouter(prefix="/lp-projects", tags=["lp-projects"])


@router.get("", response_model=list[LpProjectRead])
async def list_lp_projects(db: AsyncClient = Depends(get_supabase)):
    """Return cached LP projects from the local table."""
    result = (
        await db.table("leadspicker_projects")
        .select("*")
        .order("name")
        .execute()
    )
    return result.data


@router.post("/refresh", response_model=list[LpProjectRead])
async def refresh_lp_projects(db: AsyncClient = Depends(get_supabase)):
    """Fetch projects live from the LP API, upsert into cache, return updated list."""
    try:
        projects = await fetch_lp_projects()
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"LP API returned {exc.response.status_code}: {exc.response.text[:200]}",
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"LP API unreachable: {exc}")

    if not projects:
        raise HTTPException(status_code=502, detail="LP API returned no projects")

    now = datetime.now(timezone.utc).isoformat()
    rows = [
        {
            "lp_project_id": p["lp_project_id"],
            "name": p["name"],
            "last_fetched_at": now,
        }
        for p in projects
    ]

    result = (
        await db.table("leadspicker_projects")
        .upsert(rows, on_conflict="lp_project_id")
        .execute()
    )
    return result.data
