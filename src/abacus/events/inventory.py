import uuid
from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class InventoryDeltaEvent(BaseModel):
    """Signed, replay-safe change to one inventory projection bucket."""

    model_config = ConfigDict(extra="forbid")

    schema_version: int = Field(default=1, ge=1)
    delta_id: str = Field(min_length=1, max_length=255)
    transition_id: uuid.UUID
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    sku_id: uuid.UUID
    zone_id: uuid.UUID
    epc: str = Field(min_length=4, max_length=128)
    quantity_delta: int = Field(ge=-1, le=1)
    confidence: float = Field(ge=0.0, le=1.0)
    observed_at: datetime

    @property
    def partition_key(self) -> str:
        return f"{self.tenant_id}:{self.store_id}:{self.sku_id}:{self.zone_id}"
