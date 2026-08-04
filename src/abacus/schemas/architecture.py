import uuid
from datetime import datetime

from pydantic import Field, field_validator, model_validator

from abacus.models.architecture import (
    FreshnessStatus,
    ItemPresenceStatus,
    ObservationBatchStatus,
    RfidEventProcessingStatus,
)
from abacus.schemas.catalog import normalize_epc
from abacus.schemas.common import ApiModel


class ObservationInput(ApiModel):
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


class ObservationBatchCreate(ApiModel):
    device_id: uuid.UUID
    observations: list[ObservationInput] = Field(min_length=1, max_length=1000)
    backlog_drained: bool = True
    reader_coverage_ok: bool = True

    @model_validator(mode="after")
    def unique_events_within_batch(self) -> "ObservationBatchCreate":
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


class RfidQuarantineRead(ApiModel):
    id: uuid.UUID
    batch_id: uuid.UUID
    event_id: str | None
    reason: str = Field(description="Original reason the event entered quarantine.")
    current_rejection_reason: str | None = Field(
        description=(
            "Latest processing failure from the event ledger; null while replay is pending "
            "or after recovery succeeds."
        )
    )
    payload: dict[str, object]
    quarantined_at: datetime
    processing_status: RfidEventProcessingStatus | None
    resolved_at: datetime | None


class RfidQuarantinePage(ApiModel):
    items: list[RfidQuarantineRead]
    total: int
    limit: int
    offset: int


class RfidQuarantineReplayRead(ApiModel):
    quarantine_id: uuid.UUID
    batch_id: uuid.UUID
    event_id: str
    processing_status: RfidEventProcessingStatus
    queued: bool


class InventoryProjectionRead(ApiModel):
    sku_id: uuid.UUID
    sku: str
    zone_id: uuid.UUID
    zone: str
    quantity: int
    as_of: datetime = Field(
        description=(
            "Newest item observation contributing to this inventory bucket; use "
            "oldest_item_observed_at for the conservative age boundary."
        )
    )
    oldest_item_observed_at: datetime = Field(
        description="Oldest observation contributing to this inventory bucket."
    )
    confidence: float = Field(
        description="Effective item-location confidence after read-time recency decay."
    )
    freshness_status: FreshnessStatus


class InventoryProjectionPage(ApiModel):
    items: list[InventoryProjectionRead]
    total: int
    limit: int
    offset: int


class ItemStateRead(ApiModel):
    epc: str
    sku_id: uuid.UUID
    sku: str
    store_id: uuid.UUID | None
    zone_id: uuid.UUID | None
    last_observed_at: datetime
    last_received_at: datetime
    confidence: float = Field(
        description="Effective item-location confidence after read-time recency decay."
    )
    presence_status: ItemPresenceStatus = Field(
        description=(
            "OBSERVED while recent, UNOBSERVED after the configured age threshold, "
            "LOCATION_UNKNOWN for a recoverable pre-upgrade timeout tombstone, or REMOVED "
            "after an authoritative business event. Located UNOBSERVED items remain inventory."
        )
    )
    authoritative_removal_event_id: uuid.UUID | None = Field(
        description="Business event that authoritatively removed the item, when applicable."
    )
    authoritative_removed_at: datetime | None = Field(
        description="Business-event time of the authoritative removal, when applicable."
    )
    state_version: int
    freshness_status: FreshnessStatus
