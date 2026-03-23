from __future__ import annotations

from typing import Any

from app.core.config import settings

RUNTIME_ENV_KEYS: tuple[str, ...] = (
    "leadspicker_api_key",
    "leadspicker_base_url",
    "airtable_api_key",
    "airtable_base_id",
    "airtable_lp_general_table",
    "airtable_lp_czech_table",
    "airtable_crunchbase_table",
    "airtable_crunchbase_view",
    "airtable_news_table",
    "news_api_key",
    "gemini_api_key",
)

RUNTIME_SECRET_KEYS: set[str] = {
    "leadspicker_api_key",
    "airtable_api_key",
    "news_api_key",
    "gemini_api_key",
}

_runtime_overrides: dict[str, Any] = {}


def _normalize_value(value: Any) -> Any:
    if isinstance(value, str):
        return value.strip()
    return value


def is_runtime_env_key(key: str) -> bool:
    return key in RUNTIME_ENV_KEYS


def has_runtime_override(key: str) -> bool:
    return key in _runtime_overrides


def get_runtime_value(key: str) -> Any:
    if key in _runtime_overrides:
        return _runtime_overrides[key]
    return getattr(settings, key, None)


def set_runtime_override(key: str, value: Any) -> None:
    if key not in RUNTIME_ENV_KEYS:
        return
    normalized = _normalize_value(value)
    if normalized is None:
        _runtime_overrides.pop(key, None)
        return
    if isinstance(normalized, str) and normalized == "":
        _runtime_overrides.pop(key, None)
        return
    _runtime_overrides[key] = normalized


def clear_runtime_override(key: str) -> None:
    _runtime_overrides.pop(key, None)


def load_runtime_env_overrides() -> None:
    """
    Loads env-backed overrides from the local settings file.
    Empty values clear overrides and fall back to .env-backed defaults.
    """
    from app.core.local_settings import read_all

    data = read_all()
    _runtime_overrides.clear()
    for key in RUNTIME_ENV_KEYS:
        if key in data:
            set_runtime_override(key, data[key])

