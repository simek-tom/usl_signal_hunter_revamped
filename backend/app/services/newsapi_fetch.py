"""
NewsAPI client with multi-page pagination support.
"""

from typing import Any

import httpx

from app.core.config import settings
from app.core.runtime_config import get_runtime_value

_BASE_URL = "https://newsapi.org/v2/everything"


async def fetch_news_articles(
    *,
    query: str,
    domains: str | None = None,
    language: str | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
    sort_by: str | None = None,
    page_size: int = 100,
    max_pages: int = 3,
) -> dict[str, Any]:
    """
    Fetch NewsAPI articles across multiple pages.

    Returns:
      {
        "articles": [...],
        "pages_fetched": int,
        "total_results": int,
      }
    """
    news_api_key = str(get_runtime_value("news_api_key") or settings.news_api_key).strip()
    if not news_api_key:
        raise ValueError("NEWS_API_KEY must be set")
    if not query or not query.strip():
        raise ValueError("query must be provided")

    page_size = max(1, min(int(page_size or 1), 100))
    max_pages = max(1, min(int(max_pages or 1), 50))

    params_base: dict[str, Any] = {
        "q": query.strip(),
        "apiKey": news_api_key,
        "pageSize": page_size,
    }
    if domains and domains.strip():
        params_base["domains"] = domains.strip()
    if language and language.strip():
        params_base["language"] = language.strip()
    if from_date and from_date.strip():
        params_base["from"] = from_date.strip()
    if to_date and to_date.strip():
        params_base["to"] = to_date.strip()
    if sort_by and sort_by.strip():
        params_base["sortBy"] = sort_by.strip()

    all_articles: list[dict[str, Any]] = []
    pages_fetched = 0
    total_results = 0

    async with httpx.AsyncClient(timeout=45.0) as client:
        for page in range(1, max_pages + 1):
            params = dict(params_base)
            params["page"] = page

            resp = await client.get(_BASE_URL, params=params)
            resp.raise_for_status()
            payload = resp.json() if resp.content else {}

            status = str(payload.get("status") or "").lower()
            if status != "ok":
                message = payload.get("message") or "NewsAPI returned non-ok status"
                raise ValueError(str(message))

            page_articles = payload.get("articles") or []
            total_results = int(payload.get("totalResults") or 0)

            pages_fetched += 1
            if not page_articles:
                break

            all_articles.extend(page_articles)

            if len(page_articles) < page_size:
                break

            if total_results and len(all_articles) >= total_results:
                break

    return {
        "articles": all_articles,
        "pages_fetched": pages_fetched,
        "total_results": total_results,
    }
