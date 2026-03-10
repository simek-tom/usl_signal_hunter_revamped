"""
Crunchbase source fetchers.

Primary source is Airtable, filtered by optional:
  - Status
  - Contact enriched
  - View
  - Max records
"""

from typing import Optional

from app.core.config import settings
from app.core.runtime_config import get_runtime_value
from app.services.airtable_client import fetch_records
from app.services.crunchbase_normalize import CrunchbaseRow, from_airtable_record


async def fetch_airtable_rows(
    *,
    table_name: Optional[str] = None,
    status: Optional[str] = None,
    contact_enriched: Optional[bool] = None,
    view: Optional[str] = None,
    max_records: int = 200,
) -> list[CrunchbaseRow]:
    default_table = str(
        get_runtime_value("airtable_crunchbase_table") or settings.airtable_crunchbase_table
    ).strip()
    default_view = str(
        get_runtime_value("airtable_crunchbase_view") or settings.airtable_crunchbase_view
    ).strip()

    table = (table_name or "").strip() or default_table
    rows = await fetch_records(
        table_name=table,
        status=status,
        contact_enriched=contact_enriched,
        view=view or default_view or None,
        max_records=max_records,
    )
    return [from_airtable_record(r) for r in rows]
