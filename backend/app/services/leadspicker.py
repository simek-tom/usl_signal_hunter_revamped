"""
Leadspicker API client — shared session helper + project fetcher.

Auth flow:
1. GET /app/sb/api/docs  → grab csrftoken cookie (or HTML-embedded token).
2. All subsequent calls carry X-API-Key + X-CSRFToken + Accept: application/json.
"""

import re
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

import httpx

from app.core.config import settings
from app.core.runtime_config import get_runtime_value

_CSRF_SEED_PATH = "/app/sb/api/docs"

_RE_DATA_ATTR = re.compile(r'<body[^>]+data-csrf-token=["\']([^"\']+)["\']', re.I)
_RE_META = re.compile(r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)["\']', re.I)
_RE_META_ALT = re.compile(r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']csrf-token["\']', re.I)
_RE_INPUT = re.compile(r'<input[^>]+name=["\']csrf(?:_token|Token|-token)["\'][^>]+value=["\']([^"\']+)["\']', re.I)
_RE_INPUT_ALT = re.compile(r'<input[^>]+value=["\']([^"\']+)["\'][^>]+name=["\']csrf(?:_token|Token|-token)["\']', re.I)


def _extract_csrf_from_html(html: str) -> Optional[str]:
    for pattern in (_RE_DATA_ATTR, _RE_META, _RE_META_ALT, _RE_INPUT, _RE_INPUT_ALT):
        m = pattern.search(html)
        if m:
            return m.group(1)
    return None


def _lp_base_url() -> str:
    return str(get_runtime_value("leadspicker_base_url") or settings.leadspicker_base_url).strip()


def _lp_api_key() -> str:
    return str(get_runtime_value("leadspicker_api_key") or settings.leadspicker_api_key).strip()


@asynccontextmanager
async def lp_session() -> AsyncGenerator[tuple[httpx.AsyncClient, dict], None]:
    """
    Async context manager that yields (client, auth_headers) with a live
    CSRF token already embedded. Use for all LP API calls.

    Usage:
        async with lp_session() as (client, headers):
            resp = await client.get("/app/sb/api/projects", headers=headers)
    """
    async with httpx.AsyncClient(
        base_url=_lp_base_url(),
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        api_key = _lp_api_key()
        seed = await client.get(
            _CSRF_SEED_PATH,
            headers={"X-API-Key": api_key},
        )
        seed.raise_for_status()

        csrf = (
            _extract_csrf_from_html(seed.text)
            or seed.cookies.get("csrftoken")
            or seed.cookies.get("csrf_token")
            or seed.cookies.get("XSRF-TOKEN")
        )

        headers = {
            "X-API-Key": api_key,
            "Accept": "application/json",
        }
        if csrf:
            headers["X-CSRFToken"] = csrf

        yield client, headers


async def fetch_lp_projects() -> list[dict]:
    """Returns [{lp_project_id, name}, ...] from the LP API."""
    async with lp_session() as (client, headers):
        resp = await client.get("/app/sb/api/projects", headers=headers)
        resp.raise_for_status()

        data = resp.json()
        projects = data if isinstance(data, list) else (
            data.get("results") or data.get("projects") or []
        )

        return [
            {
                "lp_project_id": int(p.get("id") or p.get("project_id")),
                "name": p.get("name") or p.get("title"),
            }
            for p in projects
            if p.get("id") or p.get("project_id")
        ]
