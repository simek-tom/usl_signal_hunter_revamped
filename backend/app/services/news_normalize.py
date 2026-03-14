"""
News data normalization + Supabase persistence.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from supabase import AsyncClient

from app.services.leadspicker_normalize import _chunks


def _combine_text(description: str, content: str) -> str:
    d = (description or "").strip()
    c = (content or "").strip()
    if d and c and d != c:
        return f"{d}\n\n{c}"
    return d or c


@dataclass
class NewsArticleData:
    content_title: str = ""
    content_text: str = ""
    content_url: str = ""
    source_name: str = ""
    source_id: str = ""
    author_name: str = ""
    published_at: str = ""
    url_to_image: str = ""
    raw: dict | None = None


def from_newsapi_item(item: dict) -> NewsArticleData:
    source = item.get("source") or {}
    title = str(item.get("title") or "").strip()
    description = str(item.get("description") or "").strip()
    content = str(item.get("content") or "").strip()
    url = str(item.get("url") or "").strip()
    image = str(item.get("urlToImage") or "").strip()
    author = str(item.get("author") or "").strip()
    published_at = str(item.get("publishedAt") or "").strip()

    return NewsArticleData(
        content_title=title,
        content_text=_combine_text(description, content),
        content_url=url,
        source_name=str(source.get("name") or "").strip(),
        source_id=str(source.get("id") or "").strip(),
        author_name=author,
        published_at=published_at,
        url_to_image=image,
        raw=item,
    )


def _clean_date(value: str) -> Optional[str]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        # Accept Zulu timestamps from NewsAPI
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return dt.isoformat()
    except Exception:
        return None


async def process_news_import(
    db: AsyncClient,
    articles: list[NewsArticleData],
    pipeline_key: str,
    batch_id: str,
) -> int:
    """
    Write news articles to staging_news.
    Dedup via UNIQUE(dedup_key) + ON CONFLICT DO NOTHING.
    """
    if not articles:
        return 0

    rows = []
    for a in articles:
        if not a.content_url:
            continue
        dedup = _normalize_url_for_dedup(a.content_url)
        if not dedup:
            continue

        rows.append({
            "pipeline_key": pipeline_key,
            "batch_id": batch_id,
            "dedup_key": dedup,
            "content_url": a.content_url,
            "content_title": a.content_title or None,
            "content_text": a.content_text or None,
            "content_summary": (a.raw or {}).get("description") or None,
            "article_author": a.author_name or None,
            "source_name": a.source_name or None,
            "source_id": a.source_id or None,
            "published_at": _clean_date(a.published_at),
            "url_to_image": a.url_to_image or None,
            "source_metadata": {
                "source_name": a.source_name or None,
                "source_id": a.source_id or None,
                "url_to_image": a.url_to_image or None,
                "raw": a.raw or {},
            },
        })

    inserted = 0
    for chunk in _chunks(rows, 500):
        res = (
            await db.table("staging_news")
            .upsert(chunk, on_conflict="dedup_key", ignore_duplicates=True)
            .execute()
        )
        inserted += len(res.data or [])

    return inserted


def _normalize_url_for_dedup(url):
    if not url:
        return None
    import re
    u = url.lower().strip()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.rstrip("/") or None
