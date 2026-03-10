import json
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.services.gemini import chat as gemini_chat

router = APIRouter(prefix="/ai", tags=["ai"])

_ENTRY_CONTEXT_SELECT = (
    "id,pipeline_type,ai_chat_state,"
    "signals(content_url,content_text,content_summary,ai_classifier,"
    "companies(name_raw,website,linkedin_url,country,industry,employee_count)),"
    "contacts(first_name,last_name,full_name,linkedin_url,email,relation_to_company),"
    "messages(draft_text,final_text,version)"
)


class AiChatRequest(BaseModel):
    entry_id: str
    user_message: str


class AiClearRequest(BaseModel):
    entry_id: str


def _coerce_setting_str(value: Any, fallback: str) -> str:
    if value is None:
        return fallback
    val = str(value).strip()
    return val or fallback


async def _get_setting_value(db: AsyncClient, key: str) -> Any:
    res = (
        await db.table("settings")
        .select("value")
        .eq("key", key)
        .limit(1)
        .execute()
    )
    if not (res.data or []):
        return None
    return res.data[0].get("value")


def _latest_message_text(messages: list[dict]) -> str:
    if not messages:
        return ""
    msg = max(messages, key=lambda m: m.get("version", 0))
    return str(msg.get("draft_text") or msg.get("final_text") or "").strip()


def _normalize_chat_state(raw_state) -> list[dict]:
    if not isinstance(raw_state, list):
        return []
    out: list[dict] = []
    for item in raw_state:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        text = str(item.get("content") or "").strip()
        if role in {"user", "assistant"} and text:
            out.append({"role": role, "content": text})
    return out


def _template_to_text(template: Any) -> str:
    if isinstance(template, (dict, list)):
        return json.dumps(template, ensure_ascii=False, indent=2)
    return str(template or "")


def _build_system_prompt(template: Any, entry: dict) -> str:
    sig = entry.get("signals") or {}
    co = sig.get("companies") or {}
    ct = entry.get("contacts") or {}
    msgs = entry.get("messages") or []

    context_map = {
        "pipeline_type": entry.get("pipeline_type") or "",
        "summary": sig.get("content_summary") or "",
        "ai_classifier": sig.get("ai_classifier") or "",
        "post_url": sig.get("content_url") or "",
        "post_text": sig.get("content_text") or "",
        "company_name": co.get("name_raw") or "",
        "company_website": co.get("website") or "",
        "company_linkedin": co.get("linkedin_url") or "",
        "author_name": ct.get("full_name")
        or f"{ct.get('first_name') or ''} {ct.get('last_name') or ''}".strip(),
        "author_linkedin": ct.get("linkedin_url") or "",
        "position": ct.get("relation_to_company") or "",
        "current_draft": _latest_message_text(msgs),
    }

    class SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    template_text = _template_to_text(template).strip()
    if not template_text:
        template_text = (
            "You are a concise outreach drafting assistant. Be practical and specific."
        )

    # Allow either classic string templates or structured JSON prompts.
    # JSON braces can break str.format_map, so we fallback to the raw text.
    try:
        rendered = template_text.format_map(SafeDict(context_map))
    except Exception:
        rendered = template_text
    context_block = (
        "\n\nEntry Context:\n"
        f"- Pipeline type: {context_map['pipeline_type']}\n"
        f"- AI classifier: {context_map['ai_classifier']}\n"
        f"- Summary: {context_map['summary']}\n"
        f"- Company: {context_map['company_name']}\n"
        f"- Author: {context_map['author_name']}\n"
        f"- Position: {context_map['position']}\n"
        f"- Post URL: {context_map['post_url']}\n"
        f"- Post text: {context_map['post_text'][:4000]}\n"
        f"- Current draft: {context_map['current_draft']}\n"
    )
    return rendered + context_block


@router.post("/chat")
async def ai_chat(
    body: AiChatRequest,
    db: AsyncClient = Depends(get_supabase),
):
    user_text = body.user_message.strip()
    if not user_text:
        raise HTTPException(status_code=422, detail="user_message cannot be empty")

    entry_res = (
        await db.table("pipeline_entries")
        .select(_ENTRY_CONTEXT_SELECT)
        .eq("id", body.entry_id)
        .single()
        .execute()
    )
    if not entry_res.data:
        raise HTTPException(status_code=404, detail="Entry not found")
    entry = entry_res.data

    model_value = await _get_setting_value(db, "gemini_model")
    model_name = _coerce_setting_str(model_value, "gemini-2.0-flash")
    system_template = await _get_setting_value(db, "gemini_system_prompt")
    system_prompt = _build_system_prompt(system_template, entry)

    history = _normalize_chat_state(entry.get("ai_chat_state"))
    history_plus = history + [{"role": "user", "content": user_text}]

    assistant_text, error = await gemini_chat(
        messages=history_plus,
        system_prompt=system_prompt,
        model=model_name,
    )
    degraded = error is not None
    if not assistant_text:
        assistant_text = (
            "Gemini is currently unavailable. Continue drafting manually; no work is lost."
        )

    new_state = history_plus + [{"role": "assistant", "content": assistant_text}]

    await (
        db.table("pipeline_entries")
        .update(
            {
                "ai_chat_state": new_state,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        )
        .eq("id", body.entry_id)
        .execute()
    )

    return {
        "assistant_message": assistant_text,
        "ai_chat_state": new_state,
        "degraded": degraded,
        "error": error,
    }


@router.post("/clear")
async def clear_ai_chat(
    body: AiClearRequest,
    db: AsyncClient = Depends(get_supabase),
):
    res = (
        await db.table("pipeline_entries")
        .update({"ai_chat_state": None})
        .eq("id", body.entry_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"success": True}
