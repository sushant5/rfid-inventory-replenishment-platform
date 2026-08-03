import uuid
from datetime import datetime

from pydantic import Field, field_validator, model_validator

from abacus.models.architecture import (
    FreshnessStatus,
    ObservationBatchStatus,
)
from abacus.schemas.catalog import normalize_epc
from abacus.schemas.common import ApiModel


class CanonicalObservationInput(ApiModel):
    event_id: str = Field(min_length=1, max_length=128)
    epc: str = Field(min_length=4, max_length=128)
    observed_at: datetime
    rssi: float = Field(ge=-120, le=0)
    antenna_id: str | None = Field(default=None, max_length=128)

    @field_validator("event_id")
    @classmethod
    def validate_event_id(cls, value: str) -> str:
        return str(uuid.UUID(value.strip()))

    @field_validator("epc")
    @classmethod
    def normalize_tag(cls, value: str) -> str:
        return normalize_epc(value)

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone offset")
        return value


class CanonicalObservationBatchCreate(ApiModel):
    device_id: uuid.UUID
    observations: list[CanonicalObservationInput] = Field(min_length=1, max_length=1000)
    backlog_drained: bool = True
    reader_coverage_ok: bool = True

    @model_validator(mode="after")
    def unique_events_within_batch(self) -> "CanonicalObservationBatchCreate":
        event_ids = [item.event_id for item in self.observations]
        if len(event_ids) != len(set(event_ids)):
            raise ValueError("event_id values must be unique within a batch")
        return self


class ObservationBatchAccepted(ApiModel):
    batch_id: uuid.UUID
    status: ObservationBatchStatus
    accepted: int


class ObservationBatchRead(ApiModel):
    batch_id: uuid.UUID
    status: ObservationBatchStatus
    accepted: int
    processed: int
    rejected: int
    pending: int


class InventoryProjectionRead(ApiModel):
    sku_id: uuid.UUID
    sku: str
    zone_id: uuid.UUID
    zone: str
    quantity: int
    as_of: datetime
    confidence: float
    freshness_status: FreshnessStatus


class ItemStateRead(ApiModel):
    epc: str
    sku_id: uuid.UUID
    sku: str
    store_id: uuid.UUID | None
    zone_id: uuid.UUID | None
    last_observed_at: datetime
    last_received_at: datetime
    confidence: float
    state_version: int
    freshness_status: FreshnessStatus
