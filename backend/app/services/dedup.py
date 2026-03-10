"""
Deduplication service.

Strategy (3 Supabase calls total):
  1. Fetch all pipeline_entries for the batch, joined with signals (content_url)
  2. Fetch all content_urls from signals that already have a 'pushed' entry
     for the same pipeline_type
  3. Batch UPDATE eliminated entries (if any)
  + 1 UPDATE on import_batches for dedup_dropped_count

Dedup key:
  LP / news  → normalized content_url  (strip protocol, www, trailing slash)
  CB         → company fingerprint via signal.company_id (not yet implemented)
"""

import re
from typing import Optional

from supabase import AsyncClient


def _normalize_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    u = url.lower().strip()
    u = re.sub(r"^https?://", "", u)
    u = re.sub(r"^www\.", "", u)
    return u.rstrip("/") or None


async def deduplicate_batch(
    db: AsyncClient,
    batch_id: str,
    pipeline_type: str,
) -> dict:
    """
    Mark duplicate pipeline_entries as 'eliminated'.

    Returns {"total": N, "dropped": K, "remaining": N-K}.
    """
    page_size = 1000

    # ------------------------------------------------------------------
    # 1. Fetch all entries in this batch
    # ------------------------------------------------------------------
    if pipeline_type == "crunchbase":
        select_expr = "id, signal_id, signals(external_id,companies(fingerprint))"
    else:
        select_expr = "id, signal_id, signals(content_url)"

    entries: list[dict] = []
    offset = 0
    while True:
        res = (
            await db.table("pipeline_entries")
            .select(select_expr)
            .eq("batch_id", batch_id)
            .range(offset, offset + page_size - 1)
            .execute()
        )
        chunk = res.data or []
        entries.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    if not entries:
        return {"total": 0, "dropped": 0, "remaining": 0}

    # ------------------------------------------------------------------
    # 2. Fetch all dedup keys already pushed for this pipeline_type
    #    PostgREST default limit is 1000 — paginate to get everything.
    # ------------------------------------------------------------------
    pushed_keys: set[str] = set()
    offset = 0
    while True:
        if pipeline_type == "crunchbase":
            res = (
                await db.table("pipeline_entries")
                .select("signals(external_id,companies(fingerprint))")
                .eq("pipeline_type", pipeline_type)
                .eq("status", "pushed")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        else:
            res = (
                await db.table("pipeline_entries")
                .select("signals(content_url)")
                .eq("pipeline_type", pipeline_type)
                .eq("status", "pushed")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        chunk = res.data or []
        for row in chunk:
            sig = row.get("signals") or {}
            if pipeline_type == "crunchbase":
                co = sig.get("companies") or {}
                key = str(sig.get("external_id") or "").strip() or str(co.get("fingerprint") or "").strip()
            else:
                key = _normalize_url(sig.get("content_url")) or ""
            if key:
                pushed_keys.add(key)
        if len(chunk) < page_size:
            break
        offset += page_size

    # ------------------------------------------------------------------
    # 3. Identify duplicates within this batch
    # ------------------------------------------------------------------
    dup_ids: list[str] = []
    seen_in_batch: set[str] = set()

    for entry in entries:
        sig = entry.get("signals") or {}
        if pipeline_type == "crunchbase":
            co = sig.get("companies") or {}
            key = str(sig.get("external_id") or "").strip() or str(co.get("fingerprint") or "").strip()
        else:
            key = _normalize_url(sig.get("content_url")) or ""

        if not key:
            continue  # no URL — can't dedup, keep it

        if key in pushed_keys or key in seen_in_batch:
            dup_ids.append(entry["id"])
        else:
            seen_in_batch.add(key)

    dropped = len(dup_ids)
    total = len(entries)

    # ------------------------------------------------------------------
    # 4. Batch update duplicates + update batch record
    # ------------------------------------------------------------------
    if dup_ids:
        await (
            db.table("pipeline_entries")
            .update({"status": "eliminated"})
            .in_("id", dup_ids)
            .execute()
        )

    await (
        db.table("import_batches")
        .update({"dedup_dropped_count": dropped})
        .eq("id", batch_id)
        .execute()
    )

    return {"total": total, "dropped": dropped, "remaining": total - dropped}
