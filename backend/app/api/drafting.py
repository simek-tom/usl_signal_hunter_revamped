from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.services.leadspicker_normalize import _chunks

router = APIRouter(prefix="/drafting", tags=["drafting"])


class SaveDraftRequest(BaseModel):
    draft_text: str


class RemoveEntriesRequest(BaseModel):
    entry_ids: list[str]


# ---------------------------------------------------------------------------
# PUT /api/drafting/{message_id}/save
# ---------------------------------------------------------------------------
@router.put("/{message_id}/save")
async def save_draft(
    message_id: str,
    body: SaveDraftRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Auto-save draft_text for a message. Called on every navigation.
    Single-field update — fast path, no joins.
    """
    res = (
        await db.table("messages")
        .update({"draft_text": body.draft_text})
        .eq("id", message_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Message not found")
    return {"success": True}


# ---------------------------------------------------------------------------
# POST /api/drafting/remove
# ---------------------------------------------------------------------------
@router.post("/remove")
async def remove_entries(
    body: RemoveEntriesRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Mark pipeline entries as 'eliminated' (exclude from drafting).
    """
    if not body.entry_ids:
        return {"removed": 0}

    count = 0
    for chunk in _chunks(body.entry_ids, 500):
        res = (
            await db.table("pipeline_entries")
            .update({"status": "eliminated"})
            .in_("id", chunk)
            .execute()
        )
        count += len(res.data or [])

    return {"removed": count}
