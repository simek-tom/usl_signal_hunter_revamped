from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.schemas.schemas import LpProjectRead
from app.services.leadspicker import fetch_lp_projects, lp_session

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


@router.get("/{project_id}/preview")
async def preview_lp_project(project_id: int):
    """
    Fetch first 2 rows from an LP project and return all available contact_data
    column keys + their values. Used to help configure column mappings.
    """
    try:
        async with lp_session() as (client, headers):
            resp = await client.get(
                f"/app/sb/api/projects/{project_id}/people",
                headers=headers,
                params={"page": 1, "page_size": 2},
            )
            if resp.status_code == 400:
                raise HTTPException(status_code=404, detail="Project not found or has no people")
            resp.raise_for_status()
            data = resp.json()
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=502,
            detail=f"LP API error {exc.response.status_code}: {exc.response.text[:200]}",
        )
    except httpx.RequestError as exc:
        raise HTTPException(status_code=502, detail=f"LP API unreachable: {exc}")

    items = data.get("items") or []

    all_keys: set[str] = set()
    for item in items:
        cd = item.get("contact_data") or {}
        all_keys.update(cd.keys())

    columns = sorted(all_keys)

    rows = []
    for item in items:
        cd = item.get("contact_data") or {}
        row: dict = {"_id": str(item.get("id") or "")}
        for key in columns:
            node = cd.get(key) or {}
            row[key] = str(node.get("value") or "") if isinstance(node, dict) else str(node or "")
        rows.append(row)

    return {
        "columns": columns,
        "rows": rows,
        "total_count": data.get("count", 0),
    }
