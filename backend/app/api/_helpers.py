"""
Shared select clauses and utility functions for pipeline entry queries.
"""

# ---------------------------------------------------------------------------
# Select clause for entries-full — everything the analysis view needs
# ---------------------------------------------------------------------------
ENTRIES_SELECT = (
    "id,status,relevant,learning_data,ai_pre_score,ai_chat_state,"
    "batch_id,pipeline_type,signal_id,contact_id,"
    "analyzed_at,enriched_at,pushed_at,created_at,"
    "signals(id,content_url,content_title,content_text,content_summary,ai_classifier,"
    "author_name,published_at,"
    "source_robot,external_id,company_id,source_metadata,"
    "companies(id,name_raw,name_normalized,domain_normalized,"
    "linkedin_url,linkedin_url_cleaned,website,country,"
    "employee_count,industry,hq_location,fingerprint)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company)"
)

# ---------------------------------------------------------------------------
# Select clause for draft-entries-full (extends ENTRIES_SELECT with messages
# and contact.company_id for is_from_company computation)
# ---------------------------------------------------------------------------
DRAFT_ENTRIES_SELECT = (
    "id,status,relevant,learning_data,ai_pre_score,ai_chat_state,"
    "batch_id,pipeline_type,signal_id,contact_id,"
    "analyzed_at,enriched_at,pushed_at,drafted_at,created_at,"
    "signals(id,content_url,content_title,content_text,content_summary,ai_classifier,"
    "author_name,published_at,"
    "source_robot,external_id,company_id,source_metadata,"
    "companies(id,name_raw,name_normalized,domain_normalized,"
    "linkedin_url,linkedin_url_cleaned,website,country,"
    "employee_count,industry,hq_location,fingerprint)),"
    "contacts(id,first_name,last_name,full_name,linkedin_url,email,relation_to_company,company_id),"
    "messages(id,draft_text,final_text,email_subject,ai_generated,version,created_at,updated_at)"
)


def normalize_crunchbase_status(entry: dict) -> dict:
    """
    Compatibility shim:
    if DB enum doesn't yet include 'pushed-ready', we persist that marker
    in signals.source_metadata.entry_workflow_status and expose it as status
    in API responses for workflow correctness.
    """
    if entry.get("pipeline_type") != "crunchbase":
        return entry
    sig = entry.get("signals") or {}
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        return entry

    status = str(entry.get("status") or "").strip().lower()
    marker = str(meta.get("entry_workflow_status") or "").strip().lower()
    if marker == "pushed-ready" and status in {"new", "analyzed", "drafted"}:
        entry["status"] = "pushed-ready"
    return entry


def is_post_author(contact: dict, signal: dict) -> bool:
    """
    True when the contact's LinkedIn profile slug appears in the signal content_url.
    """
    li = (contact.get("linkedin_url") or "").rstrip("/")
    url = (signal.get("content_url") or "")
    if not li or not url:
        return False
    slug = li.split("/")[-1].lower()
    return bool(slug and slug in url.lower())


def is_from_company(contact: dict, signal: dict) -> bool:
    """True when the contact's company_id matches the signal's company_id."""
    return bool(
        contact.get("company_id")
        and contact["company_id"] == signal.get("company_id")
    )


def build_blacklist_row(
    company_name: str,
    company_linkedin: str | None = None,
    company_website: str | None = None,
    reason: str | None = None,
    added_by: str | None = None,
    origin: str | None = None,
    pipeline_entry_id: str | None = None,
) -> dict:
    """Build a blacklisted_companies row dict with all derived fields."""
    from app.core.utils import normalize_company_name, extract_linkedin_slug, extract_root_domain

    row: dict = {
        "company_name": company_name,
        "company_name_normalized": normalize_company_name(company_name) if company_name else None,
        "company_linkedin": company_linkedin or None,
        "company_website": company_website or None,
        "linkedin_slug": extract_linkedin_slug(company_linkedin),
        "root_domain": extract_root_domain(company_website),
        "reason": reason or None,
        "added_by": added_by or None,
    }
    if origin is not None:
        row["origin"] = origin
    if pipeline_entry_id is not None:
        row["pipeline_entry_id"] = pipeline_entry_id
    return row
