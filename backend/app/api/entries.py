from datetime import datetime, timezone
from typing import Optional, Literal

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from postgrest.exceptions import APIError
from supabase import AsyncClient

from app.core.supabase import get_supabase
from app.models.models import RelevanceLabel
from app.api.batches import _ENTRIES_SELECT
from app.services.leadspicker_normalize import make_fingerprint, normalize_domain

router = APIRouter(prefix="/entries", tags=["entries"])


class LabelRequest(BaseModel):
    label: RelevanceLabel
    learning_data: Optional[bool] = None


class EnrichRequest(BaseModel):
    # Contact
    full_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    relation_to_company: Optional[str] = None
    # Company
    company_name: Optional[str] = None
    company_website: Optional[str] = None
    company_linkedin: Optional[str] = None


class CbActionRequest(BaseModel):
    action: Literal["yes", "eliminate", "uneliminate", "save_next", "save_stay"]
    message_fin: Optional[str] = None
    main_contact: Optional[str] = None
    secondary_contact_1: Optional[str] = None
    secondary_contact_2: Optional[str] = None
    secondary_contact_3: Optional[str] = None


def _split_name(full: str) -> tuple[str, str]:
    """
    Split a full name into (first, last).
    Handles:
      'First Last'            → ('First', 'Last')
      'First Middle Last'     → ('First', 'Middle Last')
      'Last, First'           → ('First', 'Last')
      single token            → (token, '')
    """
    full = full.strip()
    if not full:
        return "", ""
    # Comma format: 'Last, First [Middle]'
    if "," in full:
        parts = [p.strip() for p in full.split(",", 1)]
        return parts[1], parts[0]
    # Space-separated
    parts = full.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


@router.post("/{entry_id}/label")
async def label_entry(
    entry_id: str,
    body: LabelRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Set relevance label on a pipeline entry.
    Also marks status='analyzed' and sets analyzed_at timestamp.
    """
    update: dict = {
        "relevant": body.label.value,
        "status": "analyzed",
        "analyzed_at": datetime.now(timezone.utc).isoformat(),
    }
    if body.learning_data is not None:
        update["learning_data"] = body.learning_data

    res = (
        await db.table("pipeline_entries")
        .update(update)
        .eq("id", entry_id)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Entry not found")

    return {"success": True}


@router.put("/{entry_id}/enrich")
async def enrich_entry(
    entry_id: str,
    body: EnrichRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Operator enrichment: update company (via enrich_company RPC) and/or contact,
    then mark entry as enriched.

    Returns the updated entry in entries-full shape so the frontend can update
    local state in-place without a separate fetch. Also returns contacted_history
    for the company's fingerprint so the frontend can flag already-contacted leads.
    """
    now = datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # 1. Fetch current entry to get company_id and contact_id
    # ------------------------------------------------------------------
    entry_res = (
        await db.table("pipeline_entries")
        .select(
            "id,contact_id,"
            "signals(id,company_id,"
            "companies(id,fingerprint,domain_normalized))"
        )
        .eq("id", entry_id)
        .single()
        .execute()
    )
    if not entry_res.data:
        raise HTTPException(status_code=404, detail="Entry not found")

    entry_raw = entry_res.data
    sig = entry_raw.get("signals") or {}
    signal_id: Optional[str] = sig.get("id")
    company_id: Optional[str] = sig.get("company_id")
    contact_id: Optional[str] = entry_raw.get("contact_id")

    # ------------------------------------------------------------------
    # 2. Enrich company (enrich-dont-clobber via RPC)
    # ------------------------------------------------------------------
    company_changed = any(
        [
            body.company_name is not None,
            body.company_website is not None,
            body.company_linkedin is not None,
        ]
    )
    if company_changed:
        new_domain = normalize_domain(body.company_website) if body.company_website else ""

        if company_id:
            await db.rpc(
                "enrich_company",
                {
                    "p_id": company_id,
                    "p_domain": new_domain or None,
                    "p_linkedin_url": body.company_linkedin or None,
                    "p_website": body.company_website or None,
                    "p_country": None,
                    "p_employee_count": None,
                    "p_enriched_by": "operator",
                },
            ).execute()
        else:
            fp = make_fingerprint(body.company_name or "", new_domain or "")
            if fp:
                existing_res = (
                    await db.table("companies")
                    .select("id")
                    .eq("fingerprint", fp)
                    .limit(1)
                    .execute()
                )
                if existing_res.data:
                    company_id = existing_res.data[0]["id"]

            if not company_id:
                name_raw = (
                    (body.company_name or "").strip()
                    or (new_domain or "").strip()
                    or "Unknown company"
                )
                ins_res = (
                    await db.table("companies")
                    .insert(
                        {
                            "name_raw": name_raw,
                            "domain_normalized": new_domain or None,
                            "linkedin_url": body.company_linkedin or None,
                            "linkedin_url_cleaned": body.company_linkedin or None,
                            "website": body.company_website or None,
                            "fingerprint": fp,
                        }
                    )
                    .execute()
                )
                company_id = ins_res.data[0]["id"]

            if signal_id and company_id:
                await (
                    db.table("signals")
                    .update({"company_id": company_id})
                    .eq("id", signal_id)
                    .execute()
                )

            if contact_id and company_id:
                await (
                    db.table("contacts")
                    .update({"company_id": company_id})
                    .eq("id", contact_id)
                    .execute()
                )

    # ------------------------------------------------------------------
    # 3. Already-contacted check — find pushed entries for this company
    # ------------------------------------------------------------------
    contacted_history: list[dict] = []
    if company_id:
        # Step 3a: get all signal IDs belonging to this company
        sigs_res = (
            await db.table("signals")
            .select("id")
            .eq("company_id", company_id)
            .execute()
        )
        signal_ids = [r["id"] for r in (sigs_res.data or [])]

        # Step 3b: find pushed pipeline entries referencing those signals
        if signal_ids:
            pushed_res = (
                await db.table("pipeline_entries")
                .select("id,pipeline_type,pushed_at,signals(content_url)")
                .eq("status", "pushed")
                .in_("signal_id", signal_ids)
                .execute()
            )
            for row in pushed_res.data or []:
                s = row.get("signals") or {}
                contacted_history.append(
                    {
                        "pipeline_entry_id": row["id"],
                        "pipeline_type": row["pipeline_type"],
                        "pushed_at": row.get("pushed_at"),
                        "content_url": s.get("content_url"),
                    }
                )

    # ------------------------------------------------------------------
    # 4. Upsert contact
    # ------------------------------------------------------------------
    contact_fields: dict = {}
    if body.full_name is not None:
        first, last = _split_name(body.full_name)
        contact_fields["full_name"] = body.full_name
        contact_fields["first_name"] = first
        contact_fields["last_name"] = last
    if body.linkedin_url is not None:
        contact_fields["linkedin_url"] = body.linkedin_url
    if body.relation_to_company is not None:
        contact_fields["relation_to_company"] = body.relation_to_company

    if contact_fields:
        if contact_id:
            if company_id:
                contact_fields["company_id"] = company_id
            await (
                db.table("contacts")
                .update(contact_fields)
                .eq("id", contact_id)
                .execute()
            )
        else:
            # Create new contact and link to entry
            contact_fields["company_id"] = company_id
            contact_fields["source"] = "operator"
            ct_res = await db.table("contacts").insert(contact_fields).execute()
            contact_id = ct_res.data[0]["id"]
            await (
                db.table("pipeline_entries")
                .update({"contact_id": contact_id})
                .eq("id", entry_id)
                .execute()
            )

    # ------------------------------------------------------------------
    # 5. Mark entry enriched
    # ------------------------------------------------------------------
    await (
        db.table("pipeline_entries")
        .update({"status": "enriched", "enriched_at": now})
        .eq("id", entry_id)
        .execute()
    )

    # ------------------------------------------------------------------
    # 6. Return updated entry in entries-full shape
    # ------------------------------------------------------------------
    updated_res = (
        await db.table("pipeline_entries")
        .select(_ENTRIES_SELECT)
        .eq("id", entry_id)
        .single()
        .execute()
    )

    return {
        "entry": updated_res.data,
        "contacted_history": contacted_history,
    }


@router.post("/{entry_id}/cb-action")
async def crunchbase_action(
    entry_id: str,
    body: CbActionRequest,
    db: AsyncClient = Depends(get_supabase),
):
    """
    Crunchbase analysis action handler.

    - Persists inline CB fields to signals.source_metadata
    - Mirrors main contact to contacts.linkedin_url
    - Mirrors message_fin to messages.final_text/draft_text
    - Applies action-driven status transitions
    """
    now = datetime.now(timezone.utc).isoformat()

    entry_res = (
        await db.table("pipeline_entries")
        .select("id,pipeline_type,signal_id,contact_id,status,relevant,signals(id,company_id,source_metadata)")
        .eq("id", entry_id)
        .single()
        .execute()
    )
    if not entry_res.data:
        raise HTTPException(status_code=404, detail="Entry not found")

    entry = entry_res.data
    if entry.get("pipeline_type") != "crunchbase":
        raise HTTPException(status_code=422, detail="CB actions are only valid for crunchbase pipeline entries")

    sig = entry.get("signals") or {}
    signal_id = sig.get("id") or entry.get("signal_id")
    company_id = sig.get("company_id")
    meta = sig.get("source_metadata") or {}
    if not isinstance(meta, dict):
        meta = {}

    # ------------------------------------------------------------------
    # 1) Merge CB inline fields into source_metadata
    # ------------------------------------------------------------------
    if body.message_fin is not None:
        meta["message_fin"] = body.message_fin
        meta["message_draft"] = body.message_fin
    if body.main_contact is not None:
        meta["main_contact"] = body.main_contact
    if body.secondary_contact_1 is not None:
        meta["secondary_contact_1"] = body.secondary_contact_1
    if body.secondary_contact_2 is not None:
        meta["secondary_contact_2"] = body.secondary_contact_2
    if body.secondary_contact_3 is not None:
        meta["secondary_contact_3"] = body.secondary_contact_3

    if body.action == "save_next":
        # Airtable-facing semantic status
        meta["status"] = "Quality B - Contacted"
        meta["entry_workflow_status"] = "pushed-ready"
    elif body.action in {"eliminate", "uneliminate"}:
        meta.pop("entry_workflow_status", None)

    if signal_id:
        await (
            db.table("signals")
            .update({"source_metadata": meta})
            .eq("id", signal_id)
            .execute()
        )

    # ------------------------------------------------------------------
    # 2) Update/Create main contact row
    # ------------------------------------------------------------------
    if body.main_contact is not None:
        linkedin = body.main_contact or None
        if entry.get("contact_id"):
            await (
                db.table("contacts")
                .update({"linkedin_url": linkedin})
                .eq("id", entry["contact_id"])
                .execute()
            )
        elif linkedin:
            ct_res = (
                await db.table("contacts")
                .insert(
                    {
                        "company_id": company_id,
                        "linkedin_url": linkedin,
                        "source": "crunchbase",
                    }
                )
                .execute()
            )
            if ct_res.data:
                await (
                    db.table("pipeline_entries")
                    .update({"contact_id": ct_res.data[0]["id"]})
                    .eq("id", entry_id)
                    .execute()
                )

    # ------------------------------------------------------------------
    # 3) Mirror Message fin into messages table for cross-push filtering
    # ------------------------------------------------------------------
    if body.message_fin is not None:
        msgs_res = (
            await db.table("messages")
            .select("id,version")
            .eq("pipeline_entry_id", entry_id)
            .execute()
        )
        msgs = msgs_res.data or []
        if msgs:
            target = max(msgs, key=lambda m: m.get("version", 0))
            await (
                db.table("messages")
                .update(
                    {
                        "draft_text": body.message_fin,
                        "final_text": body.message_fin,
                    }
                )
                .eq("id", target["id"])
                .execute()
            )
        else:
            await (
                db.table("messages")
                .insert(
                    {
                        "pipeline_entry_id": entry_id,
                        "draft_text": body.message_fin,
                        "final_text": body.message_fin,
                        "ai_generated": False,
                        "version": 1,
                    }
                )
                .execute()
            )

    # ------------------------------------------------------------------
    # 4) Apply action-driven entry state update
    # ------------------------------------------------------------------
    entry_update: dict = {}
    if body.action == "yes":
        entry_update = {
            "relevant": "yes",
            "status": "analyzed",
            "analyzed_at": now,
        }
    elif body.action == "eliminate":
        entry_update = {"status": "eliminated"}
    elif body.action == "uneliminate":
        entry_update = {"status": "new"}
    elif body.action == "save_next":
        entry_update = {"status": "pushed-ready"}
    else:
        entry_update = {}

    if entry_update:
        try:
            await (
                db.table("pipeline_entries")
                .update(entry_update)
                .eq("id", entry_id)
                .execute()
            )
        except APIError as exc:
            # Backward compatibility when DB enum doesn't include 'pushed-ready' yet.
            if (
                body.action == "save_next"
                and "invalid input value for enum entry_status" in str(exc)
            ):
                await (
                    db.table("signals")
                    .update({"source_metadata": meta})
                    .eq("id", signal_id)
                    .execute()
                )
                await (
                    db.table("pipeline_entries")
                    .update({"status": "drafted", "drafted_at": now})
                    .eq("id", entry_id)
                    .execute()
                )
            else:
                raise

    updated_res = (
        await db.table("pipeline_entries")
        .select(_ENTRIES_SELECT)
        .eq("id", entry_id)
        .single()
        .execute()
    )

    updated_entry = updated_res.data or {}
    updated_sig = updated_entry.get("signals") or {}
    updated_meta = updated_sig.get("source_metadata") or {}
    current_status = str(updated_entry.get("status") or "").strip().lower()
    if (
        isinstance(updated_meta, dict)
        and str(updated_meta.get("entry_workflow_status") or "").strip().lower() == "pushed-ready"
        and current_status in {"new", "analyzed", "drafted"}
    ):
        updated_entry["status"] = "pushed-ready"

    return {"success": True, "entry": updated_entry}
