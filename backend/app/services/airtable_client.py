"""
Shared Airtable client helpers.

Uses pyairtable for:
  - filtered fetch (Airtable formula + view + max records)
  - batch create (chunks of 10)
  - batch update (chunks of 10)
"""

import asyncio
from typing import Any, Optional

from app.core.config import settings
from app.core.runtime_config import get_runtime_value
from app.services.leadspicker_normalize import _chunks


def _get_table(table_name: str):
    api_key = str(get_runtime_value("airtable_api_key") or settings.airtable_api_key).strip()
    base_id = str(get_runtime_value("airtable_base_id") or settings.airtable_base_id).strip()

    if not api_key or not base_id:
        raise ValueError("AIRTABLE_API_KEY and AIRTABLE_BASE_ID must be set")
    if not table_name or not table_name.strip():
        raise ValueError("Airtable table name must be provided")

    from pyairtable import Api

    api = Api(api_key)
    return api.table(base_id, table_name.strip())


def _escape_formula_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_formula(
    status: Optional[str] = None,
    contact_enriched: Optional[bool] = None,
    extra_formula: Optional[str] = None,
) -> Optional[str]:
    clauses: list[str] = []

    if status and status.strip():
        val = _escape_formula_value(status.strip())
        clauses.append(f"{{Status}}='{val}'")

    if contact_enriched is not None:
        # Checkbox in Airtable evaluates to TRUE() / FALSE()
        clauses.append("{Contact enriched}=TRUE()" if contact_enriched else "{Contact enriched}=FALSE()")

    if extra_formula and extra_formula.strip():
        clauses.append(f"({extra_formula.strip()})")

    if not clauses:
        return None
    if len(clauses) == 1:
        return clauses[0]
    return "AND(" + ",".join(clauses) + ")"


async def fetch_records(
    table_name: str,
    *,
    status: Optional[str] = None,
    contact_enriched: Optional[bool] = None,
    view: Optional[str] = None,
    max_records: int = 200,
    extra_formula: Optional[str] = None,
) -> list[dict[str, Any]]:
    """
    Fetch Airtable records using optional filters.
    """
    table = _get_table(table_name)
    formula = _build_formula(
        status=status,
        contact_enriched=contact_enriched,
        extra_formula=extra_formula,
    )
    max_records = max(1, min(int(max_records or 1), 5000))

    def _run():
        return table.all(
            formula=formula,
            view=view or None,
            max_records=max_records,
        )

    return await asyncio.to_thread(_run)


async def batch_create(
    table_name: str,
    fields_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Batch-create Airtable rows in chunks of 10 (Airtable API limit).
    """
    if not fields_rows:
        return []

    table = _get_table(table_name)
    created: list[dict[str, Any]] = []

    for chunk in _chunks(fields_rows, 10):
        result = await asyncio.to_thread(
            table.batch_create,
            chunk,
            True,   # typecast
            None,   # use_field_ids
        )
        created.extend(result or [])

    return created


async def batch_update(
    table_name: str,
    records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """
    Batch-update Airtable rows in chunks of 10 (Airtable API limit).
    records item shape: {"id": "...", "fields": {...}}
    """
    if not records:
        return []

    table = _get_table(table_name)
    updated: list[dict[str, Any]] = []

    for chunk in _chunks(records, 10):
        result = await asyncio.to_thread(
            table.batch_update,
            chunk,
            False,  # replace
            True,   # typecast
            None,   # use_field_ids
        )
        updated.extend(result or [])

    return updated
