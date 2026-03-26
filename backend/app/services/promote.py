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
    For each, create company + pipeline_entry (denormalized).
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

    _ENRICHED_FIELDS = [
        "enriched_contact_name", "enriched_contact_first_name", "enriched_contact_last_name",
        "enriched_contact_linkedin", "enriched_contact_position",
        "enriched_company_name", "enriched_company_website", "enriched_company_linkedin",
    ]

    for row in rows:
        try:
            entry_id = await _promote_single(db, source_type, pipeline_key, row, now)
            # Persist auto-filled enriched values + promotion tracking
            staging_update: dict = {"pipeline_entry_id": entry_id, "promoted_at": now}
            for field in _ENRICHED_FIELDS:
                if row.get(field):
                    staging_update[field] = row[field]
            await (
                db.table(staging_table)
                .update(staging_update)
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
    # Auto-fill enriched fields from author/raw if empty.
    # Scenario 1 (author=lead): analyst labelled "yes" without enriching,
    # so we explicitly copy author → enriched as confirmation.
    _ENRICH_DEFAULTS = [
        ("enriched_contact_name", "author_full_name"),
        ("enriched_contact_linkedin", "author_linkedin"),
        ("enriched_contact_position", "author_position"),
        ("enriched_contact_first_name", "author_first_name"),
        ("enriched_contact_last_name", "author_last_name"),
        ("enriched_company_name", "company_name"),
        ("enriched_company_website", "company_website"),
        ("enriched_company_linkedin", "company_linkedin"),
    ]
    for enriched_key, raw_key in _ENRICH_DEFAULTS:
        if not row.get(enriched_key):
            row[enriched_key] = row.get(raw_key) or ""

    # If enriched first/last still empty, split from enriched full name
    if not row.get("enriched_contact_first_name"):
        parts = (row.get("enriched_contact_name") or "").split()
        row["enriched_contact_first_name"] = parts[0] if parts else ""
        row["enriched_contact_last_name"] = " ".join(parts[1:]) if len(parts) > 1 else ""

    # Resolve company from enriched fields ONLY (no fallback to raw)
    company_id = await _resolve_company(
        db,
        row["enriched_company_name"],
        row["enriched_company_website"],
        row["enriched_company_linkedin"],
    )

    # Enrich company record with extra staging fields
    co_updates = {}
    if row.get("company_country"):
        co_updates["country"] = row["company_country"]
    if row.get("company_employee_count"):
        co_updates["employee_count"] = row["company_employee_count"]
    if co_updates:
        await db.table("companies").update(co_updates).eq("id", company_id).execute()

    # Create pipeline_entry — NO fallbacks, clean author/lead separation
    entry_res = await db.table("pipeline_entries").insert({
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
        # Company (target/lead company)
        "company_id": company_id,
        # Signal data
        "source_type": "leadspicker",
        "external_id": row.get("external_id"),
        "content_url": row.get("content_url"),
        "content_text": row.get("content_text"),
        "content_summary": row.get("content_summary"),
        "content_title": "linkedin_post",
        "ai_classifier": row.get("ai_classifier"),
        "source_robot": row.get("source_robot"),
        "source_metadata": row.get("source_metadata") or {},
        # Author (post writer) — all three name columns, always raw
        "author_full_name":       row.get("author_full_name") or None,
        "author_first_name": row.get("author_first_name") or None,
        "author_last_name":  row.get("author_last_name") or None,
        "author_linkedin":   row.get("author_linkedin") or None,
        "author_position":   row.get("author_position") or None,
        "author_company_name": row.get("company_name") or None,
        "author_company_linkedin": row.get("company_linkedin") or None,
        # Lead (contact) — all three from enriched (auto-filled + split above)
        "lead_full_name":       row["enriched_contact_name"] or None,
        "lead_first_name": row["enriched_contact_first_name"] or None,
        "lead_last_name":  row["enriched_contact_last_name"] or None,
        "lead_linkedin":   row["enriched_contact_linkedin"] or None,
        "lead_position":   row["enriched_contact_position"] or None,
        "lead_company_name": row["enriched_company_name"] or None,
        "lead_company_linkedin": row["enriched_company_linkedin"] or None,
    }).execute()

    return entry_res.data[0]["id"]


async def _promote_cb(db, pipeline_key, row, now) -> str:
    co_name = row.get("company_name") or ""
    co_website = row.get("company_website") or ""
    co_linkedin = row.get("company_linkedin") or ""

    company_id = await _resolve_company(db, co_name, co_website, co_linkedin)

    # Enrich company with CB-specific fields
    co_updates = {}
    for staging_key, db_col in [
        ("company_country", "country"),
        ("company_employee_count", "employee_count"),
        ("company_industry", "industry"),
        ("company_hq_location", "hq_location"),
        ("company_description", "description"),
        ("company_founded_on", "founded_on"),
        ("crunchbase_profile_url", "crunchbase_profile_url"),
    ]:
        val = row.get(staging_key)
        if val:
            co_updates[db_col] = val
    if co_updates:
        await db.table("companies").update(co_updates).eq("id", company_id).execute()

    # Create pipeline_entry with all data inline
    entry_res = await db.table("pipeline_entries").insert({
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
        # Company
        "company_id": company_id,
        # Signal data
        "source_type": "crunchbase",
        "external_id": row.get("external_id"),
        "content_url": row.get("content_url"),
        "content_text": row.get("company_description"),
        "content_title": "crunchbase_profile",
        "content_summary": row.get("content_summary"),
        "ai_classifier": row.get("ai_classifier"),
        "source_robot": "airtable",
        "source_metadata": row.get("source_metadata") or {},
        # Lead (main contact)
        "lead_linkedin": row.get("main_contact_linkedin") or None,
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

    lead_name = row.get("enriched_contact_name") or ""
    lead_linkedin = row.get("enriched_contact_linkedin") or ""

    # Create pipeline_entry with all data inline
    entry_res = await db.table("pipeline_entries").insert({
        "pipeline_type": pipeline_key,
        "batch_id": row.get("batch_id"),
        "status": "analyzed",
        "relevant": row.get("label"),
        "learning_data": row.get("learning_data") or False,
        "ai_pre_score": row.get("ai_pre_score"),
        "analyzed_at": now,
        # Company
        "company_id": company_id,
        # Signal data
        "source_type": "news",
        "external_id": row.get("content_url"),
        "content_url": row.get("content_url"),
        "content_title": row.get("content_title"),
        "content_text": row.get("content_text"),
        "content_summary": row.get("content_summary"),
        "source_robot": "newsapi",
        "author_full_name": row.get("article_author") or None,
        "published_at": row.get("published_at"),
        "source_metadata": row.get("source_metadata") or {},
        # Lead (enriched contact)
        "lead_full_name": lead_name or None,
        "lead_linkedin": lead_linkedin or None,
        "lead_position": row.get("enriched_contact_position") or None,
    }).execute()
    return entry_res.data[0]["id"]