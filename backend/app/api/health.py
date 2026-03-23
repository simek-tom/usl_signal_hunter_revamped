from fastapi import APIRouter

from app.core.local_settings import read_all

router = APIRouter()


@router.get("/health")
async def health_check():
    return {
        "status": "ok",
        "settings_count": len(read_all()),
    }
