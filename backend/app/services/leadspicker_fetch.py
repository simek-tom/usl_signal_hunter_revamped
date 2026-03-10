"""
Paginated fetcher for LP project people.

LP people response shape (confirmed):
  { "items": [...], "count": N, "to_select_ids": [...] }

Each item has all person data nested under contact_data.{key}.value.
Pagination: ?page=N&page_size=50. Stop on HTTP 400 or empty items.
"""

from app.services.leadspicker import lp_session

_PAGE_SIZE = 50


async def fetch_project_people(project_ids: list[int]) -> list[dict]:
    """
    Fetch all people from one or more LP projects.
    Returns raw LP item dicts (contact_data nested).
    Tags each item with _lp_project_id for downstream use.
    """
    all_items: list[dict] = []

    async with lp_session() as (client, headers):
        for project_id in project_ids:
            page = 1
            while True:
                resp = await client.get(
                    f"/app/sb/api/projects/{project_id}/people",
                    headers=headers,
                    params={"page": page, "page_size": _PAGE_SIZE},
                )

                if resp.status_code == 400:
                    break

                resp.raise_for_status()
                data = resp.json()
                items = data.get("items", [])

                if not items:
                    break

                # Tag each item with its source project id
                for item in items:
                    item["_lp_project_id"] = project_id

                all_items.extend(items)

                if len(items) < _PAGE_SIZE:
                    break  # last page

                page += 1

    return all_items
