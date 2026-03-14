"""
Promotion service: moves YES/CC staging rows into core relational tables.
"""

from datetime import datetime, timezone
from typing import Optional

from supabase import AsyncClient

from app.services.leadspicker_normalize import (
    make_fingerprint,
    normalize_domain,
    clean_linkedin_url,
    _chunks,
    _fp_or_filter,
)


async def promote_batch(
    db: AsyncClient,
    staging_table: str,
    source_type: str,
    pipeline_key: str,
    batch_id: Optional[str] = None,
) -> dict:
    """
    Find all YES/CC rows in the staging table that have not been promoted yet.
    For each, create company + contact + signal + pipeline_entry.
    Returns { promoted: N, skipped: N }.
    """
    query = (
        db.table(staging_table)
        .select("*")
        .in_("label", ["yes", "cc"])
        .is_("pipeline_entry_id", "null")
    )
    if batch_id:
        query = query.eq("batch_id", batch_id)

    rows = []
    offset = 0
    page_size = 500
    while True:
        res = await query.range(offset, offset + page_size - 1).execute()
        chunk = res.data or []
        rows.extend(chunk)
        if len(chunk) < page_size:
            break
        offset += page_size

    if not rows:
        return {"promoted": 0, "skipped": 0}

    now = datetime.now(timezone.utc).isoformat()
    promoted = 0
    skipped = 0

    for row in rows:
        try:
            entry_id = await _promote_single(db, source_type, pipeline_key, row, now)
            await (
                db.table(staging_table)
                .update({"pipeline_entry_id": entry_id, "promoted_at": now})
                .eq("id", row["id"])
                .execute()
            )
            promoted += 1
        except Exception:
            skipped += 1

    return {"promoted": promoted, "skipped": skipped}


async def _promote_single(
    db: AsyncClient,
    source_type: str,
    pipeline_key: str,
    row: dict,
    now: str,
) -> str:
    """Promote one staging row. Returns the created pipeline_entry ID."""

    if source_type == "leadspicker":
        return await _promote_lp(db, pipeline_key, row, now)
    elif source_type == "crunchbase":
        return await _promote_cb(db, pipeline_key, row, now)
    elif source_type == "news":
        return await _promote_news(db, pipeline_key, row, now)
    else:
        raise ValueError(f"Unknown source_type: {source_type}")


async def _resolve_company(db: AsyncClient, name: str, website: str, linkedin: str) -> str:
    """Find or create a company. Returns company ID."""
    domain = normalize_domain(website)
    fp = make_fingerprint(name, domain)

    if fp:
        existing = (
            await db.table("companies")
            .select("id")
            .or_(_fp_or_filter([fp]))
            .limit(1)
            .execute()
        )
        if existing.data:
            return existing.data[0]["id"]

    ins = await db.table("companies").insert({
        "name_raw": name or "Unknown company",
        "domain_normalized": domain or None,
        "linkedin_url": linkedin or None,
        "linkedin_url_cleaned": clean_linkedin_url(linkedin) if linkedin else None,
        "website": website or None,
        "fingerprint": fp,
    }).execute()
    return ins.data[0]["id"]


async def _promote_lp(db, pipeline_key, row, now) -> str:
    # Determine company and contact fields (enriched overrides raw)
    co_name = row.get("enriched_company_name") or row.get("company_name") or ""
    co_website = row.get("enriched_company_website") or row.get("company_website") or ""
    co_linkedin = row.get("enriched_company_linkedin") or row.get("company_linkedin") or ""
    ct_name = row.get("enriched_contact_name") or row.get("author_full_name") or ""
    ct_linkedin = row.get("enriched_contact_linkedin") or row.get("author_linkedin") or ""
    ct_position = row.get("enriched_contact_position") or row.get("author_position") or ""

    company_id = await _resolve_company(db, co_name, co_website, co_linkedin)

    # Create contact
    ct_res = await db.table("contacts").insert({
        "company_id": company_id,
        "full_name": ct_name or None,
        "first_name": row.get("author_first_name"),
        "last_name": row.get("author_last_name"),
        "linkedin_url": ct_linkedin or None,
        "relation_to_company": ct_position or None,
        "source": "leadspicker",
    }).execute()
    contact_id = ct_res.data[0]["id"]

    # Create signal
    sig_res = await db.table("signals").insert({
        "company_id": company_id,
        "source_type": "leadspicker",
        "external_id": row.get("external_id"),
        "content_url": row.get("content_url"),
        "content_text": row.get("content_text"),
        "content_summary": row.get("content_summary"),
        "ai_classifier": row.get("ai_classifier"),
        "source_robot": row.get("source_robot"),
        "source_metadata": row.get("source_metadata") or {},
    }).execute()
    signal_id = sig_res.data[0]["id"]

    # Create pipeline_entry
    entry_res = await db.table("pipeline_entries").insert({
        "signal_id": signal_id,
        "contact_id": contact_id,
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
    }).execute()
    return entry_res.data[0]["id"]


async def _promote_cb(db, pipeline_key, row, now) -> str:
    co_name = row.get("company_name") or ""
    co_website = row.get("company_website") or ""
    co_linkedin = row.get("company_linkedin") or ""

    company_id = await _resolve_company(db, co_name, co_website, co_linkedin)

    # Create contact (main contact)
    ct_res = await db.table("contacts").insert({
        "company_id": company_id,
        "linkedin_url": row.get("main_contact_linkedin"),
        "source": "crunchbase",
    }).execute()
    contact_id = ct_res.data[0]["id"]

    # Create signal
    sig_res = await db.table("signals").insert({
        "company_id": company_id,
        "source_type": "crunchbase",
        "external_id": row.get("external_id"),
        "content_url": row.get("content_url"),
        "content_text": row.get("company_description"),
        "content_title": row.get("company_name"),
        "content_summary": row.get("content_summary"),
        "ai_classifier": row.get("ai_classifier"),
        "source_robot": "airtable",
        "source_metadata": row.get("source_metadata") or {},
    }).execute()
    signal_id = sig_res.data[0]["id"]

    # Create pipeline_entry
    entry_res = await db.table("pipeline_entries").insert({
        "signal_id": signal_id,
        "contact_id": contact_id,
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
    }).execute()
    entry_id = entry_res.data[0]["id"]

    # Create message if message_fin exists
    msg = (row.get("message_fin") or "").strip()
    if msg:
        await db.table("messages").insert({
            "pipeline_entry_id": entry_id,
            "draft_text": msg,
            "final_text": msg,
            "ai_generated": False,
            "version": 1,
        }).execute()

    return entry_id


async def _promote_news(db, pipeline_key, row, now) -> str:
    co_name = row.get("enriched_company_name") or ""
    co_website = row.get("enriched_company_website") or ""
    co_linkedin = row.get("enriched_company_linkedin") or ""

    company_id = None
    if co_name or co_website or co_linkedin:
        company_id = await _resolve_company(db, co_name, co_website, co_linkedin)

    contact_id = None
    ct_name = row.get("enriched_contact_name") or ""
    ct_linkedin = row.get("enriched_contact_linkedin") or ""
    if ct_name or ct_linkedin:
        ct_res = await db.table("contacts").insert({
            "company_id": company_id,
            "full_name": ct_name or None,
            "linkedin_url": ct_linkedin or None,
            "relation_to_company": row.get("enriched_contact_position"),
            "source": "news",
        }).execute()
        contact_id = ct_res.data[0]["id"]

    sig_res = await db.table("signals").insert({
        "company_id": company_id,
        "source_type": "news",
        "external_id": row.get("content_url"),
        "content_url": row.get("content_url"),
        "content_title": row.get("content_title"),
        "content_text": row.get("content_text"),
        "content_summary": row.get("content_summary"),
        "source_robot": "newsapi",
        "author_name": row.get("article_author"),
        "published_at": row.get("published_at"),
        "source_metadata": row.get("source_metadata") or {},
    }).execute()
    signal_id = sig_res.data[0]["id"]

    entry_res = await db.table("pipeline_entries").insert({
        "signal_id": signal_id,
        "contact_id": contact_id,
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
    }).execute()
    return entry_res.data[0]["id"]