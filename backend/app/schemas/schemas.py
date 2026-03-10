from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel

from app.models.models import (
    EntryStatus,
    PipelineType,
    RelevanceLabel,
    SourceType,
)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

class ImportLpRequest(BaseModel):
    project_ids: list[int]
    pipeline_type: PipelineType


class ImportResult(BaseModel):
    batch_id: UUID
    pipeline_type: str
    record_count: int


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

class SettingRead(BaseModel):
    key: str
    value: Any


class SettingUpdate(BaseModel):
    value: Any


# ---------------------------------------------------------------------------
# LpProject
# ---------------------------------------------------------------------------

class LpProjectRead(BaseModel):
    id: UUID
    lp_project_id: int
    name: Optional[str] = None
    last_fetched_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Blacklist
# ---------------------------------------------------------------------------

class BlacklistCreate(BaseModel):
    company_name: str
    reason: Optional[str] = None
    added_by: Optional[str] = None


class BlacklistRead(BaseModel):
    id: UUID
    company_name: str
    company_name_normalized: Optional[str] = None
    reason: Optional[str] = None
    added_by: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Company
# ---------------------------------------------------------------------------

class CompanyBase(BaseModel):
    name_raw: str
    name_normalized: Optional[str] = None
    domain_normalized: Optional[str] = None
    linkedin_url: Optional[str] = None
    linkedin_url_cleaned: Optional[str] = None
    website: Optional[str] = None
    country: Optional[str] = None
    description: Optional[str] = None
    crunchbase_profile_url: Optional[str] = None
    industry: Optional[str] = None
    employee_count: Optional[str] = None
    hq_location: Optional[str] = None
    founded_on: Optional[str] = None
    fingerprint: Optional[str] = None


class CompanyRead(CompanyBase):
    id: UUID
    last_enriched_at: Optional[datetime] = None
    last_enriched_by: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Contact
# ---------------------------------------------------------------------------

class ContactBase(BaseModel):
    company_id: Optional[UUID] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    full_name: Optional[str] = None
    linkedin_url: Optional[str] = None
    email: Optional[str] = None
    relation_to_company: Optional[str] = None
    source: Optional[str] = None


class ContactRead(ContactBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Signal
# ---------------------------------------------------------------------------

class SignalBase(BaseModel):
    company_id: Optional[UUID] = None
    source_type: SourceType
    external_id: Optional[str] = None
    content_url: Optional[str] = None
    content_text: Optional[str] = None
    content_title: Optional[str] = None
    content_summary: Optional[str] = None
    ai_classifier: Optional[str] = None
    source_robot: Optional[str] = None
    author_linkedin: Optional[str] = None
    author_name: Optional[str] = None
    published_at: Optional[datetime] = None
    fetched_at: Optional[datetime] = None
    source_metadata: Optional[dict[str, Any]] = None


class SignalRead(SignalBase):
    id: UUID
    created_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# PipelineEntry
# ---------------------------------------------------------------------------

class PipelineEntryBase(BaseModel):
    signal_id: UUID
    contact_id: Optional[UUID] = None
    pipeline_type: PipelineType
    batch_id: Optional[UUID] = None
    status: EntryStatus = EntryStatus.new
    relevant: Optional[RelevanceLabel] = None
    learning_data: bool = False
    ai_pre_score: Optional[float] = None
    ai_chat_state: Optional[list[Any]] = None


class PipelineEntryRead(PipelineEntryBase):
    id: UUID
    analyzed_at: Optional[datetime] = None
    enriched_at: Optional[datetime] = None
    drafted_at: Optional[datetime] = None
    pushed_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# Message
# ---------------------------------------------------------------------------

class MessageBase(BaseModel):
    pipeline_entry_id: UUID
    draft_text: Optional[str] = None
    final_text: Optional[str] = None
    email_subject: Optional[str] = None
    ai_generated: bool = False
    version: int = 1


class MessageRead(MessageBase):
    id: UUID
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------------------------------------------------------------------------
# ImportBatch
# ---------------------------------------------------------------------------

class ImportBatchBase(BaseModel):
    pipeline_type: PipelineType
    source_details: Optional[dict[str, Any]] = None
    record_count: int = 0
    dedup_dropped_count: int = 0


class ImportBatchRead(ImportBatchBase):
    id: UUID
    imported_at: datetime

    model_config = {"from_attributes": True}
