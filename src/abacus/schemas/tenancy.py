import uuid
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import Field, field_validator, model_validator

from abacus.enums import BatchStatus, DeviceStatus, StoreStatus, TenantStatus, ZoneKind
from abacus.schemas.common import ApiModel

CODE_PATTERN = r"^[a-z0-9][a-z0-9_-]{1,63}$"


class TenantCreate(ApiModel):
    code: str = Field(min_length=2, max_length=64, pattern=CODE_PATTERN)
    name: str = Field(min_length=1, max_length=255)

    @field_validator("code")
    @classmethod
    def normalize_code(cls, value: str) -> str:
        return value.strip().lower()


class TenantRead(ApiModel):
    id: uuid.UUID
    code: str
    name: str
    status: TenantStatus
    created_at: datetime
    updated_at: datetime


class OrganizationUnitSegment(ApiModel):
    code: str = Field(min_length=2, max_length=64, pattern=CODE_PATTERN)
    name: str = Field(min_length=1, max_length=255)
    unit_type: str = Field(min_length=2, max_length=32)

    @field_validator("code")
    @classmethod
    def normalize_code(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("unit_type")
    @classmethod
    def normalize_type(cls, value: str) -> str:
        return value.strip().upper()


class ZoneCreate(ApiModel):
    code: str = Field(min_length=2, max_length=64, pattern=CODE_PATTERN)
    name: str = Field(min_length=1, max_length=255)
    kind: ZoneKind

    @field_validator("code")
    @classmethod
    def normalize_code(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("name cannot be blank")
        return normalized


class DeviceCreate(ApiModel):
    serial_number: str = Field(min_length=3, max_length=128)
    display_name: str = Field(min_length=1, max_length=255)
    zone_code: str = Field(min_length=2, max_length=64, pattern=CODE_PATTERN)

    @field_validator("serial_number")
    @classmethod
    def normalize_serial(cls, value: str) -> str:
        normalized = value.strip().upper()
        if len(normalized) < 3:
            raise ValueError("serial_number must contain at least 3 non-whitespace characters")
        return normalized

    @field_validator("zone_code")
    @classmethod
    def normalize_zone_code(cls, value: str) -> str:
        return value.strip().lower()


class StoreDeviceCreate(ApiModel):
    """Register one device directly against a server-resolved store."""

    serial_number: str = Field(min_length=3, max_length=128)
    display_name: str = Field(min_length=1, max_length=255)
    zone_id: uuid.UUID

    @field_validator("serial_number")
    @classmethod
    def normalize_serial(cls, value: str) -> str:
        normalized = value.strip().upper()
        if len(normalized) < 3:
            raise ValueError("serial_number must contain at least 3 non-whitespace characters")
        return normalized

    @field_validator("display_name")
    @classmethod
    def normalize_display_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("display_name cannot be blank")
        return normalized


class StoreCreate(ApiModel):
    code: str = Field(min_length=2, max_length=64, pattern=CODE_PATTERN)
    name: str = Field(min_length=1, max_length=255)
    timezone: str = Field(min_length=1, max_length=64)
    organization_path: list[OrganizationUnitSegment] = Field(default_factory=list, max_length=10)
    zones: list[ZoneCreate] = Field(min_length=2, max_length=100)
    devices: list[DeviceCreate] = Field(default_factory=list, max_length=1000)
    configuration: dict[str, Any] = Field(default_factory=dict)

    @field_validator("code")
    @classmethod
    def normalize_code(cls, value: str) -> str:
        return value.strip().lower()

    @field_validator("timezone")
    @classmethod
    def validate_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("timezone must be a valid IANA timezone") from exc
        return value

    @model_validator(mode="after")
    def validate_store_configuration(self) -> "StoreCreate":
        zone_codes = [zone.code for zone in self.zones]
        if len(zone_codes) != len(set(zone_codes)):
            raise ValueError("zone codes must be unique within a store")
        kinds = {zone.kind for zone in self.zones}
        required = {ZoneKind.SALES_FLOOR, ZoneKind.BACKROOM}
        if not required.issubset(kinds):
            raise ValueError("each store requires SALES_FLOOR and BACKROOM zones")
        unknown_zones = {device.zone_code for device in self.devices} - set(zone_codes)
        if unknown_zones:
            raise ValueError(f"devices reference unknown zones: {sorted(unknown_zones)}")
        path_codes = [unit.code for unit in self.organization_path]
        if len(path_codes) != len(set(path_codes)):
            raise ValueError("organization path cannot repeat a unit code")
        return self


class BulkStoreOnboardingRequest(ApiModel):
    stores: list[StoreCreate] = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def validate_batch_uniqueness(self) -> "BulkStoreOnboardingRequest":
        store_codes = [store.code for store in self.stores]
        if len(store_codes) != len(set(store_codes)):
            raise ValueError("store codes must be unique within the request")
        serials = [device.serial_number for store in self.stores for device in store.devices]
        if len(serials) != len(set(serials)):
            raise ValueError("device serial numbers must be unique within the request")
        return self


class OnboardingBatchRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    idempotency_key: str
    status: BatchStatus
    total_count: int
    succeeded_count: int
    failed_count: int
    errors: list[dict[str, Any]]
    created_at: datetime
    updated_at: datetime


class StoreRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    organization_unit_id: uuid.UUID | None
    code: str
    name: str
    timezone: str
    status: StoreStatus
    configuration: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class ZoneRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    code: str
    name: str
    kind: ZoneKind


class DeviceRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    serial_number: str
    display_name: str
    status: DeviceStatus


class DeviceCredentialRead(ApiModel):
    device_id: uuid.UUID
    api_key: str
    warning: str = "This credential is shown once. Store it securely."


class DeviceTokenRead(ApiModel):
    device_id: uuid.UUID
    device_token: str
    warning: str = "This credential is shown once. Store it securely."


class DeviceAssignmentCreate(ApiModel):
    store_id: uuid.UUID
    zone_id: uuid.UUID
    effective_from: datetime

    @field_validator("effective_from")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("effective_from must include a timezone offset")
        return value


class DeviceAssignmentRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    device_id: uuid.UUID
    store_id: uuid.UUID
    zone_id: uuid.UUID
    valid_from: datetime = Field(validation_alias="effective_from")
    valid_to: datetime | None = Field(validation_alias="effective_to")


class StoreDeviceMappingRead(ApiModel):
    device: DeviceRead
    assignment: DeviceAssignmentRead


class StoreDeviceRegistrationRead(ApiModel):
    device: DeviceRead
    assignment: DeviceAssignmentRead
    device_token: str
    warning: str = "This credential is shown once. Store it securely."
