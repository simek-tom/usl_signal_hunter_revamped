"""
Check if a company has been contacted before.
Runs at analysis, enrichment, and drafting stages.
"""

from typing import Optional
from supabase import AsyncClient
from app.services.leadspicker_normalize import normalize_domain, make_fingerprint
from app.core.utils import normalize_company_name


async def check_contacted(
    db: AsyncClient,
    company_name: Optional[str] = None,
    company_website: Optional[str] = None,
    company_linkedin: Optional[str] = None,
) -> dict:
    """
    Returns { is_contacted: bool, matches: [...] }
    """
    conditions = []
    name_norm = normalize_company_name(company_name) if company_name else None
    domain = normalize_domain(company_website) if company_website else None
    fp = make_fingerprint(company_name or "", domain or "") if (company_name or domain) else None

    if fp:
        conditions.append(f'fingerprint.eq."{fp}"')
    if domain:
        conditions.append(f'domain_normalized.eq."{domain}"')
    if company_linkedin:
        conditions.append(f'linkedin_url.eq."{company_linkedin}"')
    if name_norm:
        conditions.append(f'company_name_normalized.eq."{name_norm}"')

    if not conditions:
        return {"is_contacted": False, "matches": []}

    res = (
        await db.table("contacted_companies")
        .select("*")
        .or_(",".join(conditions))
        .limit(10)
        .execute()
    )

    matches = res.data or []
    return {"is_contacted": len(matches) > 0, "matches": matches}