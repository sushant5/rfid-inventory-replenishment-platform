import uuid
from datetime import datetime

from pydantic import Field, field_validator, model_validator

from abacus.models.architecture import CanonicalTaskStatus, PolicyVersionStatus
from abacus.schemas.common import ApiModel


def _label(value: str, *, field: str) -> str:
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError(f"{field} cannot be blank")
    return normalized


class PolicyRuleWrite(ApiModel):
    store_id: uuid.UUID | None = None
    category: str | None = Field(default=None, max_length=128)
    style_code: str | None = Field(default=None, max_length=64)
    sku_id: uuid.UUID | None = None
    size: str | None = Field(default=None, max_length=64)
    min_floor_qty: int = Field(ge=0)
    target_floor_qty: int = Field(ge=0)
    priority: int = Field(default=0, ge=-1_000_000, le=1_000_000)

    @field_validator("category")
    @classmethod
    def normalize_category(cls, value: str | None) -> str | None:
        return _label(value, field="category").upper() if value is not None else None

    @field_validator("style_code")
    @classmethod
    def normalize_style_code(cls, value: str | None) -> str | None:
        return _label(value, field="style_code").upper() if value is not None else None

    @field_validator("size")
    @classmethod
    def normalize_size(cls, value: str | None) -> str | None:
        return _label(value, field="size") if value is not None else None

    @model_validator(mode="after")
    def validate_selector(self) -> "PolicyRuleWrite":
        selector_count = sum(
            value is not None for value in (self.category, self.style_code, self.sku_id)
        )
        if selector_count > 1:
            raise ValueError("a rule may select category, style_code, or sku_id, not multiple")
        if self.size is not None and self.sku_id is None:
            raise ValueError("size requires sku_id")
        if self.store_id is not None and selector_count == 0:
            raise ValueError("store-default rules are outside the defined precedence")
        if self.target_floor_qty < self.min_floor_qty:
            raise ValueError("target_floor_qty must be at least min_floor_qty")
        return self


class PolicyCreate(ApiModel):
    name: str = Field(min_length=1, max_length=255)
    description: str | None = Field(default=None, max_length=2000)
    rules: list[PolicyRuleWrite] = Field(min_length=1, max_length=500)

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        return _label(value, field="name")

    @field_validator("description")
    @classmethod
    def normalize_description(cls, value: str | None) -> str | None:
        return _label(value, field="description") if value is not None else None


class PolicyRulesPatch(ApiModel):
    rules: list[PolicyRuleWrite] = Field(min_length=1, max_length=500)


class PolicyDefinitionRead(ApiModel):
    id: uuid.UUID
    name: str
    description: str | None
    created_at: datetime
    updated_at: datetime


class PolicyVersionRead(ApiModel):
    id: uuid.UUID
    policy_id: uuid.UUID
    version_number: int
    status: PolicyVersionStatus
    activated_at: datetime | None
    activated_by_user_id: uuid.UUID | None
    created_at: datetime
    updated_at: datetime


class PolicyRuleRead(ApiModel):
    id: uuid.UUID
    version_id: uuid.UUID
    store_id: uuid.UUID | None
    category: str | None
    style_code: str | None
    sku_id: uuid.UUID | None
    size: str | None
    min_floor_qty: int
    target_floor_qty: int
    priority: int


class PolicyBundleRead(ApiModel):
    policy: PolicyDefinitionRead
    version: PolicyVersionRead
    rules: list[PolicyRuleRead]


class ReplenishmentEvaluationCreate(ApiModel):
    store_id: uuid.UUID
    sku_ids: list[uuid.UUID] = Field(default_factory=list, max_length=5000)

    @field_validator("sku_ids")
    @classmethod
    def reject_duplicate_skus(cls, value: list[uuid.UUID]) -> list[uuid.UUID]:
        if len(value) != len(set(value)):
            raise ValueError("sku_ids cannot contain duplicates")
        return value


class ReplenishmentTaskRead(ApiModel):
    id: uuid.UUID
    store_id: uuid.UUID
    sku_id: uuid.UUID
    policy_version_id: uuid.UUID
    policy_rule_id: uuid.UUID
    status: CanonicalTaskStatus
    quantity: int
    version: int
    claimed_by_user_id: uuid.UUID | None
    claimed_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    expires_at: datetime | None
    note: str | None
    created_at: datetime
    updated_at: datetime


class ReplenishmentEvaluationRead(ApiModel):
    store_id: uuid.UUID
    created_count: int
    suppressed_connectivity: bool
    suppressed_low_confidence: int
    tasks: list[ReplenishmentTaskRead]


class ReplenishmentTaskPatch(ApiModel):
    status: CanonicalTaskStatus
    version: int = Field(ge=1)
    note: str | None = Field(default=None, max_length=2000)

    @field_validator("note")
    @classmethod
    def normalize_note(cls, value: str | None) -> str | None:
        return _label(value, field="note") if value is not None else None
