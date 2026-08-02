import uuid
from datetime import datetime
from typing import Literal

from pydantic import Field, field_validator, model_validator

from abacus.enums import ObservationStatus, ZoneKind
from abacus.schemas.catalog import normalize_epc as normalize_catalog_epc
from abacus.schemas.common import ApiModel


class RfidObservationInput(ApiModel):
    event_id: str = Field(min_length=1, max_length=128)
    epc: str = Field(min_length=4, max_length=128)
    observed_at: datetime
    reader_sequence: int | None = Field(default=None, ge=0)
    antenna_port: int | None = Field(default=None, ge=0, le=10_000)
    rssi_dbm: float | None = Field(default=None, ge=-120, le=0)

    @field_validator("event_id")
    @classmethod
    def strip_event_id(cls, value: str) -> str:
        try:
            return str(uuid.UUID(value.strip()))
        except ValueError as exc:
            raise ValueError("event_id must be a UUID") from exc

    @field_validator("epc")
    @classmethod
    def normalize_epc(cls, value: str) -> str:
        return normalize_catalog_epc(value)

    @field_validator("observed_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("observed_at must include a timezone offset")
        return value


class RfidBatchInput(ApiModel):
    batch_id: str = Field(min_length=1, max_length=128)
    observations: list[RfidObservationInput] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def unique_event_ids(self) -> "RfidBatchInput":
        ids = [item.event_id for item in self.observations]
        if len(ids) != len(set(ids)):
            raise ValueError("event_id values must be unique within a batch")
        return self


class RfidEventIngressResult(ApiModel):
    event_id: str
    disposition: Literal["ACCEPTED", "DUPLICATE", "CONFLICT"]
    observation_id: uuid.UUID | None = None
    detail: str | None = None


class RfidBatchReceipt(ApiModel):
    batch_id: str
    accepted_count: int
    duplicate_count: int
    conflict_count: int
    results: list[RfidEventIngressResult]


class RfidObservationRead(ApiModel):
    id: uuid.UUID
    event_id: str
    epc: str
    observed_at: datetime
    status: ObservationStatus
    quarantine_reason: str | None
    resolved_epc_binding_id: uuid.UUID | None
    resolution_strategy: str | None


class RfidObservationListRead(ApiModel):
    items: list[RfidObservationRead]
    total: int
    limit: int
    offset: int


class InventoryBalanceRead(ApiModel):
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    zone_id: uuid.UUID
    zone_kind: ZoneKind
    sku_id: uuid.UUID
    sku_code: str
    quantity: int
    as_of: datetime


class InventoryBalanceListRead(ApiModel):
    items: list[InventoryBalanceRead]
    total: int
    limit: int
    offset: int
