from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from supabase import AsyncClient

from app.core.runtime_config import (
    RUNTIME_SECRET_KEYS,
    clear_runtime_override,
    get_runtime_value,
    has_runtime_override,
    is_runtime_env_key,
    load_runtime_env_overrides,
    set_runtime_override,
)
from app.core.supabase import get_supabase
from app.schemas.schemas import SettingRead, SettingUpdate

router = APIRouter(prefix="/settings", tags=["settings"])


RUNTIME_DB_DEFAULTS: dict[str, Any] = {
    "gemini_model": "gemini-2.0-flash",
    "gemini_system_prompt": "",
    "push_row_limit": 100,
    "news_default_query": "(Series A OR Series B OR Series C) AND (expansion OR global expansion)",
    "news_default_domains": "techcrunch.com,news.crunchbase.com,venturebeat.com,theinformation.com,sifted.eu",
    "news_default_language": "en",
    "news_default_page_size": 100,
    "news_default_max_pages": 3,
    "news_default_days_back": 7,
}

RUNTIME_FIELD_META: dict[str, dict[str, Any]] = {
    "leadspicker_api_key": {"label": "Leadspicker API Key", "group": "Leadspicker", "secret": True},
    "leadspicker_base_url": {"label": "Leadspicker Base URL", "group": "Leadspicker", "secret": False},
    "airtable_api_key": {"label": "Airtable API Key", "group": "Airtable", "secret": True},
    "airtable_base_id": {"label": "Airtable Base ID", "group": "Airtable", "secret": False},
    "airtable_lp_general_table": {"label": "LP General Table", "group": "Airtable", "secret": False},
    "airtable_lp_czech_table": {"label": "LP Czech Table", "group": "Airtable", "secret": False},
    "airtable_crunchbase_table": {"label": "Crunchbase Table", "group": "Airtable", "secret": False},
    "airtable_crunchbase_view": {"label": "Crunchbase View", "group": "Airtable", "secret": False},
    "airtable_news_table": {"label": "News Table", "group": "Airtable", "secret": False},
    "news_api_key": {"label": "NewsAPI Key", "group": "NewsAPI", "secret": True},
    "gemini_api_key": {"label": "Gemini API Key", "group": "Gemini", "secret": True},
    "gemini_model": {"label": "Gemini Model", "group": "Gemini", "secret": False},
    "gemini_system_prompt": {"label": "Gemini System Prompt", "group": "Gemini", "secret": False},
    "push_row_limit": {"label": "Push Row Limit", "group": "Behavior", "secret": False},
    "news_default_query": {"label": "Default News Query", "group": "NewsAPI", "secret": False},
    "news_default_domains": {"label": "Default News Domains", "group": "NewsAPI", "secret": False},
    "news_default_language": {"label": "Default News Language", "group": "NewsAPI", "secret": False},
    "news_default_page_size": {"label": "Default News Page Size", "group": "NewsAPI", "secret": False},
    "news_default_max_pages": {"label": "Default News Max Pages", "group": "NewsAPI", "secret": False},
    "news_default_days_back": {"label": "Default News Days Back", "group": "NewsAPI", "secret": False},
}

ALLOWED_RUNTIME_KEYS: set[str] = set(RUNTIME_FIELD_META.keys())
RUNTIME_DB_KEYS: set[str] = set(RUNTIME_DB_DEFAULTS.keys())


class RuntimeSettingsUpdate(BaseModel):
    values: dict[str, Any]


def _is_empty_value(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


async def _runtime_settings_payload(db: AsyncClient) -> dict[str, Any]:
    # Always reload env-backed overrides from DB to stay in sync.
    await load_runtime_env_overrides(db)

    db_rows = (
        await db.table("settings")
        .select("key,value")
        .in_("key", list(RUNTIME_DB_KEYS))
        .execute()
    )
    db_values = {str(r.get("key")): r.get("value") for r in (db_rows.data or [])}

    items: list[dict[str, Any]] = []
    for key, meta in RUNTIME_FIELD_META.items():
        if is_runtime_env_key(key):
            val = get_runtime_value(key)
            source = "settings_override" if has_runtime_override(key) else "env_file"
        else:
            if key in db_values:
                val = db_values[key]
                source = "settings_table"
            else:
                val = RUNTIME_DB_DEFAULTS.get(key)
                source = "default"

        items.append(
            {
                "key": key,
                "label": meta["label"],
                "group": meta["group"],
                "secret": bool(meta["secret"] or key in RUNTIME_SECRET_KEYS),
                "source": source,
                "value": val,
            }
        )

    return {"items": items}


@router.get("", response_model=list[SettingRead])
async def list_settings(db: AsyncClient = Depends(get_supabase)):
    result = await db.table("settings").select("key, value").execute()
    return result.data


@router.get("/runtime")
async def get_runtime_settings(db: AsyncClient = Depends(get_supabase)):
    return await _runtime_settings_payload(db)


@router.put("/runtime")
async def upsert_runtime_settings(
    body: RuntimeSettingsUpdate,
    db: AsyncClient = Depends(get_supabase),
):
    if not body.values:
        return await _runtime_settings_payload(db)

    unknown = sorted([k for k in body.values.keys() if k not in ALLOWED_RUNTIME_KEYS])
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported runtime settings keys: {', '.join(unknown)}",
        )

    for key, value in body.values.items():
        if _is_empty_value(value):
            await db.table("settings").delete().eq("key", key).execute()
            if is_runtime_env_key(key):
                clear_runtime_override(key)
            continue

        await db.table("settings").upsert({"key": key, "value": value}).execute()
        if is_runtime_env_key(key):
            set_runtime_override(key, value)

    return await _runtime_settings_payload(db)


@router.get("/{key}", response_model=SettingRead)
async def get_setting(key: str, db: AsyncClient = Depends(get_supabase)):
    result = await db.table("settings").select("key, value").eq("key", key).execute()
    if not result.data:
        raise HTTPException(status_code=404, detail=f"Setting '{key}' not found")
    return result.data[0]


@router.put("/{key}", response_model=SettingRead)
async def upsert_setting(
    key: str,
    body: SettingUpdate,
    db: AsyncClient = Depends(get_supabase),
):
    result = (
        await db.table("settings")
        .upsert({"key": key, "value": body.value})
        .execute()
    )
    return result.data[0]
