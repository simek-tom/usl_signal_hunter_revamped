from fastapi import APIRouter, Depends
from supabase import AsyncClient

from app.core.supabase import get_supabase

router = APIRouter()


@router.get("/health")
async def health_check(db: AsyncClient = Depends(get_supabase)):
    result = await db.table("settings").select("key, value").execute()
    return {
        "status": "ok",
        "settings_count": len(result.data),
    }
