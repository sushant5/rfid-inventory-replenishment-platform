import uuid
from datetime import datetime

from pydantic import Field, field_validator

from abacus.models.architecture import BusinessEventStatus, BusinessEventType
from abacus.schemas.catalog import normalize_epc
from abacus.schemas.common import ApiModel


class BusinessEventCreate(ApiModel):
    source_system: str = Field(min_length=1, max_length=64, examples=["ORANGE_POS"])
    external_event_id: str = Field(min_length=1, max_length=128)
    event_type: BusinessEventType
    epc: str = Field(min_length=4, max_length=128)
    occurred_at: datetime
    note: str | None = Field(default=None, max_length=500)

    @field_validator("source_system")
    @classmethod
    def normalize_source_system(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("source_system must not be blank")
        return normalized

    @field_validator("external_event_id")
    @classmethod
    def normalize_external_event_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("external_event_id must not be blank")
        return normalized

    @field_validator("epc")
    @classmethod
    def normalize_tag(cls, value: str) -> str:
        return normalize_epc(value)

    @field_validator("occurred_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must include a timezone offset")
        return value

    @field_validator("note")
    @classmethod
    def normalize_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class BusinessEventRead(ApiModel):
    id: uuid.UUID
    store_id: uuid.UUID
    source_system: str
    external_event_id: str
    event_type: BusinessEventType
    epc: str
    occurred_at: datetime
    processing_status: BusinessEventStatus
    transition_id: uuid.UUID
    state_version: int
    note: str | None
    created_at: datetime
    idempotent_replay: bool = False
