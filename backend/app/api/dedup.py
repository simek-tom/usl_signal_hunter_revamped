from fastapi import APIRouter, Depends, HTTPException
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.services.dedup import deduplicate_batch

router = APIRouter(prefix="/dedup", tags=["dedup"])


@router.post("/{batch_id}")
async def dedup_batch(
    batch_id: str,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Deduplicate a freshly imported batch.
    Marks entries whose content_url was already pushed as 'eliminated'.
    Returns {total, dropped, remaining}.
    """
    # Verify batch exists and get its pipeline_type
    batch_res = (
        await db.table("import_batches")
        .select("id, pipeline_type")
        .eq("id", batch_id)
        .single()
        .execute()
    )
    if not batch_res.data:
        raise HTTPException(status_code=404, detail="Batch not found")

    pipeline_type = batch_res.data["pipeline_type"]
    return await deduplicate_batch(db, batch_id, pipeline_type)
