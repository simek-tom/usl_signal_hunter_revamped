"""
Gemini API client.

Model is provided by caller (typically from settings table),
API key comes from environment-backed app settings.
"""

from typing import Optional

import httpx

from app.core.config import settings
from app.core.runtime_config import get_runtime_value

_GEMINI_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
_DEFAULT_MODEL = "gemini-2.0-flash"


def _map_role(role: str) -> str:
    if role == "assistant":
        return "model"
    return "user"


def _to_contents(messages: list[dict]) -> list[dict]:
    contents: list[dict] = []
    for msg in messages:
        role = _map_role(str(msg.get("role") or "user").lower())
        text = str(msg.get("content") or "").strip()
        if not text:
            continue
        contents.append(
            {
                "role": role,
                "parts": [{"text": text}],
            }
        )
    return contents


async def chat(
    messages: list[dict],
    system_prompt: str,
    model: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (assistant_text, error).
    On failures, assistant_text is None and error is a user-readable string.
    """
    api_key = str(get_runtime_value("gemini_api_key") or settings.gemini_api_key).strip()
    if not api_key:
        return None, "Gemini API key is not configured."

    model_name = (model or _DEFAULT_MODEL).strip() or _DEFAULT_MODEL
    contents = _to_contents(messages)
    if not contents:
        return None, "No usable chat messages were provided."

    payload = {
        "system_instruction": {
            "parts": [{"text": system_prompt or "You are a helpful drafting assistant."}]
        },
        "contents": contents,
    }

    url = f"{_GEMINI_BASE_URL}/models/{model_name}:generateContent"
    params = {"key": api_key}

    try:
        async with httpx.AsyncClient(timeout=35.0) as client:
            resp = await client.post(url, params=params, json=payload)
            resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        snippet = (exc.response.text or "")[:300]
        return None, f"Gemini HTTP {exc.response.status_code}: {snippet}"
    except httpx.RequestError as exc:
        return None, f"Gemini request failed: {exc}"
    except Exception as exc:
        return None, f"Gemini unexpected error: {exc}"

    candidates = data.get("candidates") or []
    if not candidates:
        feedback = data.get("promptFeedback") or {}
        reason = feedback.get("blockReason") or "No response candidates."
        return None, f"Gemini returned no candidates ({reason})."

    parts = (((candidates[0] or {}).get("content") or {}).get("parts") or [])
    text = "".join(str(p.get("text") or "") for p in parts).strip()
    if not text:
        return None, "Gemini returned an empty response."

    return text, None
