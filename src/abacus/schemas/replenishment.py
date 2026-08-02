import uuid
from datetime import datetime
from typing import Self

from pydantic import Field, field_validator, model_validator

from abacus.models.replenishment import (
    PolicyImportStatus,
    PolicySelectorType,
    ReplenishmentReason,
    ReplenishmentRunStatus,
    ReplenishmentTaskStatus,
    ReplenishmentTrigger,
)
from abacus.schemas.common import ApiModel


def _require_timezone(value: datetime | None, field_name: str) -> datetime | None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise ValueError(f"{field_name} must include a timezone offset")
    return value


class PolicyDefinition(ApiModel):
    external_key: str = Field(min_length=1, max_length=128)
    store_id: uuid.UUID | None = None
    selector_type: PolicySelectorType
    selector_value: str = Field(min_length=1, max_length=128)
    minimum_floor_quantity: int = Field(ge=0)
    target_floor_quantity: int = Field(ge=0)
    maximum_floor_quantity: int | None = Field(default=None, ge=0)
    priority: int = Field(default=0, ge=-1_000_000, le=1_000_000)
    effective_from: datetime
    effective_to: datetime | None = None
    active: bool = True

    @field_validator("external_key")
    @classmethod
    def normalize_external_key(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("external_key cannot be blank")
        return normalized

    @field_validator("selector_value")
    @classmethod
    def normalize_selector_value(cls, value: str) -> str:
        normalized = " ".join(value.strip().split()).upper()
        if not normalized:
            raise ValueError("selector_value cannot be blank")
        return normalized

    @field_validator("effective_from", "effective_to")
    @classmethod
    def validate_effective_timestamp(
        cls,
        value: datetime | None,
        info: object,
    ) -> datetime | None:
        field_name = getattr(info, "field_name", "effective timestamp")
        return _require_timezone(value, str(field_name))

    @model_validator(mode="after")
    def validate_quantities_and_interval(self) -> Self:
        if self.target_floor_quantity < self.minimum_floor_quantity:
            raise ValueError(
                "target_floor_quantity must be greater than or equal to minimum_floor_quantity"
            )
        if (
            self.maximum_floor_quantity is not None
            and self.maximum_floor_quantity < self.target_floor_quantity
        ):
            raise ValueError(
                "maximum_floor_quantity must be greater than or equal to target_floor_quantity"
            )
        if self.effective_to is not None and self.effective_to <= self.effective_from:
            raise ValueError("effective_to must be later than effective_from")
        return self


class PolicyCreate(PolicyDefinition):
    pass


class PolicyPatch(ApiModel):
    store_id: uuid.UUID | None = None
    selector_type: PolicySelectorType | None = None
    selector_value: str | None = Field(default=None, min_length=1, max_length=128)
    minimum_floor_quantity: int | None = Field(default=None, ge=0)
    target_floor_quantity: int | None = Field(default=None, ge=0)
    maximum_floor_quantity: int | None = Field(default=None, ge=0)
    priority: int | None = Field(default=None, ge=-1_000_000, le=1_000_000)
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    active: bool | None = None

    @field_validator("selector_value")
    @classmethod
    def normalize_selector_value(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.strip().split()).upper()
        if not normalized:
            raise ValueError("selector_value cannot be blank")
        return normalized

    @field_validator("effective_from", "effective_to")
    @classmethod
    def validate_effective_timestamp(
        cls,
        value: datetime | None,
        info: object,
    ) -> datetime | None:
        field_name = getattr(info, "field_name", "effective timestamp")
        return _require_timezone(value, str(field_name))


class PolicyBulkUpsertRequest(ApiModel):
    policies: list[PolicyDefinition] = Field(min_length=1, max_length=1000)

    @model_validator(mode="after")
    def require_unique_external_keys(self) -> Self:
        keys = [policy.external_key for policy in self.policies]
        if len(keys) != len(set(keys)):
            raise ValueError("external_key values must be unique within an import")
        return self


class PolicyRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    external_key: str
    store_id: uuid.UUID | None
    selector_type: PolicySelectorType
    selector_value: str
    minimum_floor_quantity: int
    target_floor_quantity: int
    maximum_floor_quantity: int | None
    priority: int
    effective_from: datetime
    effective_to: datetime | None
    active: bool
    revision: int
    created_at: datetime
    updated_at: datetime


class PolicyListRead(ApiModel):
    items: list[PolicyRead]
    total: int
    limit: int
    offset: int


class PolicyImportRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    idempotency_key: str
    request_hash: str
    status: PolicyImportStatus
    total_count: int
    created_count: int
    updated_count: int
    unchanged_count: int
    rejected_count: int
    reconciliation: dict[str, object]
    errors: list[dict[str, object]]
    created_at: datetime
    updated_at: datetime


class ReplenishmentEvaluationRequest(ApiModel):
    store_id: uuid.UUID
    sku_ids: list[uuid.UUID] = Field(min_length=1, max_length=500)
    generate_tasks: bool = True

    @model_validator(mode="after")
    def require_unique_skus(self) -> Self:
        if len(self.sku_ids) != len(set(self.sku_ids)):
            raise ValueError("sku_ids must be unique")
        return self


class ReplenishmentRunLineRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    run_id: uuid.UUID
    store_id: uuid.UUID
    sku_id: uuid.UUID
    sku_code: str
    policy_id: uuid.UUID | None
    task_id: uuid.UUID | None
    selector_type: PolicySelectorType | None
    selector_value: str | None
    policy_priority: int | None
    minimum_floor_quantity: int | None
    target_floor_quantity: int | None
    maximum_floor_quantity: int | None
    floor_quantity: int
    backroom_quantity: int
    open_task_quantity: int
    recommended_quantity: int
    reason: ReplenishmentReason
    formula: str
    inventory_as_of: datetime | None
    created_at: datetime


class ReplenishmentRunRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    idempotency_key: str | None
    trigger: ReplenishmentTrigger
    status: ReplenishmentRunStatus
    evaluated_at: datetime
    requested_by_subject: str | None
    line_count: int
    tasks_created: int
    tasks_updated: int
    created_at: datetime
    lines: list[ReplenishmentRunLineRead]


class ReplenishmentTaskRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    sku_id: uuid.UUID
    sku_code: str
    source_policy_id: uuid.UUID
    status: ReplenishmentTaskStatus
    quantity: int
    moved_quantity: int
    remaining_quantity: int
    version: int
    claimed_by_subject: str | None
    claimed_at: datetime | None
    completed_at: datetime | None
    last_note: str | None
    created_at: datetime
    updated_at: datetime


class ReplenishmentTaskListRead(ApiModel):
    items: list[ReplenishmentTaskRead]
    total: int
    limit: int
    offset: int


class ReplenishmentTaskUpdate(ApiModel):
    status: ReplenishmentTaskStatus
    expected_version: int = Field(ge=1)
    moved_quantity: int | None = Field(default=None, ge=0)
    note: str | None = Field(default=None, max_length=1000)
