import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from abacus.schemas.catalog import normalize_epc


class RfidObservationEvent(BaseModel):
    """Stable internal contract for one accepted device observation."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1)
    tenant_id: uuid.UUID
    batch_id: uuid.UUID
    event_id: str = Field(min_length=1, max_length=128)
    device_id: uuid.UUID
    store_id: uuid.UUID
    zone_id: uuid.UUID
    epc: str = Field(min_length=4, max_length=128)
    observed_at: datetime
    received_at: datetime
    rssi: float
    antenna_id: str | None = Field(default=None, max_length=128)
    reader_health: float = Field(default=1.0, ge=0.0, le=1.0)
    is_buffered: bool = False
    backlog_drained: bool = True
    reader_coverage_ok: bool = True

    @field_validator("epc")
    @classmethod
    def normalize_tag(cls, value: str) -> str:
        return normalize_epc(value)

    @property
    def partition_key(self) -> str:
        return f"{self.tenant_id}:{self.epc}"
