from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.core import local_settings
from app.core.runtime_config import (
    RUNTIME_SECRET_KEYS,
    clear_runtime_override,
    get_runtime_value,
    has_runtime_override,
    is_runtime_env_key,
    load_runtime_env_overrides,
    set_runtime_override,
)

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


def _runtime_settings_payload() -> dict[str, Any]:
    load_runtime_env_overrides()

    all_local = local_settings.read_all()

    items: list[dict[str, Any]] = []
    for key, meta in RUNTIME_FIELD_META.items():
        if is_runtime_env_key(key):
            val = get_runtime_value(key)
            source = "settings_override" if has_runtime_override(key) else "env_file"
        else:
            if key in all_local:
                val = all_local[key]
                source = "settings_file"
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


@router.get("/runtime")
async def get_runtime_settings():
    return _runtime_settings_payload()


@router.put("/runtime")
async def upsert_runtime_settings(body: RuntimeSettingsUpdate):
    if not body.values:
        return _runtime_settings_payload()

    unknown = sorted([k for k in body.values.keys() if k not in ALLOWED_RUNTIME_KEYS])
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=f"Unsupported runtime settings keys: {', '.join(unknown)}",
        )

    for key, value in body.values.items():
        if _is_empty_value(value):
            local_settings.delete(key)
            if is_runtime_env_key(key):
                clear_runtime_override(key)
            continue

        local_settings.set_value(key, value)
        if is_runtime_env_key(key):
            set_runtime_override(key, value)

    return _runtime_settings_payload()
