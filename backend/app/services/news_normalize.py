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
    batch_id: str,
) -> int:
    """
    Persist normalized news articles to signals + pipeline_entries.
    company_id/contact_id remain NULL until operator enrichment.
    """
    if not articles:
        return 0

    now = datetime.now(timezone.utc).isoformat()
    signal_rows = []
    for a in articles:
        if not a.content_url:
            continue
        signal_rows.append(
            {
                "company_id": None,
                "source_type": "news",
                "external_id": a.content_url,
                "content_url": a.content_url,
                "content_title": a.content_title or None,
                "content_text": a.content_text or None,
                "content_summary": (a.raw or {}).get("description") or None,
                "source_robot": "newsapi",
                "author_name": a.author_name or None,
                "published_at": _clean_date(a.published_at),
                "fetched_at": now,
                "source_metadata": {
                    "source_name": a.source_name or None,
                    "source_id": a.source_id or None,
                    "url_to_image": a.url_to_image or None,
                    "raw": a.raw or {},
                },
            }
        )

    signal_ids: list[str] = []
    for chunk in _chunks(signal_rows, 500):
        res = await db.table("signals").insert(chunk).execute()
        signal_ids.extend(row["id"] for row in (res.data or []))

    entry_rows = [
        {
            "signal_id": sid,
            "contact_id": None,
            "pipeline_type": "news",
            "batch_id": batch_id,
            "status": "new",
        }
        for sid in signal_ids
    ]
    for chunk in _chunks(entry_rows, 500):
        await db.table("pipeline_entries").insert(chunk).execute()

    return len(signal_ids)
