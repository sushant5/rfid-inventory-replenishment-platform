import re
import uuid
from datetime import datetime
from typing import Any

from pydantic import Field, field_validator

from abacus.models.catalog import CatalogImportMode, CatalogImportStatus
from abacus.schemas.common import ApiModel

PRODUCT_CODE_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9._-]*$")
EPC_URI_PATTERN = re.compile(r"^urn:epc:(?:id|tag):[a-z0-9-]+:[a-z0-9.*%_-]+(?:\.[a-z0-9.*%_-]+)*$")
GTIN_LENGTHS = {8, 12, 13, 14}


def normalize_product_code(value: str, *, field_name: str, max_length: int) -> str:
    normalized = value.strip().upper()
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} must be at most {max_length} characters")
    if PRODUCT_CODE_PATTERN.fullmatch(normalized) is None:
        raise ValueError(
            f"{field_name} may contain only letters, digits, periods, underscores, and hyphens"
        )
    return normalized


def normalize_upc(value: str) -> str:
    normalized = re.sub(r"[\s-]", "", value)
    if not normalized.isdigit() or len(normalized) not in GTIN_LENGTHS:
        raise ValueError("upc must be a valid 8, 12, 13, or 14 digit GTIN")

    payload = normalized[:-1]
    expected_check_digit = (
        10
        - sum(
            int(digit) * (3 if position % 2 == 0 else 1)
            for position, digit in enumerate(reversed(payload))
        )
        % 10
    ) % 10
    if int(normalized[-1]) != expected_check_digit:
        raise ValueError("upc has an invalid GTIN check digit")
    return normalized


def normalize_epc(value: str) -> str:
    stripped = value.strip()
    if not stripped:
        raise ValueError("epc is required")

    if stripped.lower().startswith("urn:epc:"):
        normalized_uri = stripped.lower()
        if len(normalized_uri) > 128 or EPC_URI_PATTERN.fullmatch(normalized_uri) is None:
            raise ValueError("epc must be a valid EPC URI or hexadecimal EPC")
        return normalized_uri

    without_prefix = stripped[2:] if stripped.lower().startswith("0x") else stripped
    normalized_hex = re.sub(r"[\s:-]", "", without_prefix).upper()
    if (
        len(normalized_hex) < 16
        or len(normalized_hex) > 64
        or len(normalized_hex) % 2 != 0
        or re.fullmatch(r"[0-9A-F]+", normalized_hex) is None
    ):
        raise ValueError("epc must be 16 to 64 hexadecimal characters with an even length")
    return normalized_hex


def _normalize_label(value: str, *, field_name: str, max_length: int) -> str:
    normalized = " ".join(value.strip().split())
    if not normalized:
        raise ValueError(f"{field_name} is required")
    if len(normalized) > max_length:
        raise ValueError(f"{field_name} must be at most {max_length} characters")
    return normalized


class CatalogRowData(ApiModel):
    style_code: str
    style_name: str
    sku: str
    upc: str
    color: str
    size: str
    epc: str
    style_attributes: dict[str, Any] = Field(default_factory=dict)
    attributes: dict[str, Any] = Field(default_factory=dict)

    @field_validator("style_code")
    @classmethod
    def validate_style_code(cls, value: str) -> str:
        return normalize_product_code(value, field_name="style_code", max_length=64)

    @field_validator("sku")
    @classmethod
    def validate_sku(cls, value: str) -> str:
        return normalize_product_code(value, field_name="sku", max_length=128)

    @field_validator("style_name")
    @classmethod
    def validate_style_name(cls, value: str) -> str:
        return _normalize_label(value, field_name="style_name", max_length=255)

    @field_validator("color")
    @classmethod
    def validate_color(cls, value: str) -> str:
        return _normalize_label(value, field_name="color", max_length=128)

    @field_validator("size")
    @classmethod
    def validate_size(cls, value: str) -> str:
        return _normalize_label(value, field_name="size", max_length=64)

    @field_validator("upc")
    @classmethod
    def validate_upc(cls, value: str) -> str:
        return normalize_upc(value)

    @field_validator("epc")
    @classmethod
    def validate_epc(cls, value: str) -> str:
        return normalize_epc(value)


class CatalogImportRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    idempotency_key: str
    checksum: str
    mode: CatalogImportMode
    status: CatalogImportStatus
    filename: str
    content_type: str
    size_bytes: int
    total_rows: int
    valid_rows: int
    invalid_rows: int
    inserted_count: int
    updated_count: int
    unchanged_count: int
    deactivated_count: int
    reconciliation: dict[str, Any]
    failure_reason: str | None
    promoted_at: datetime | None
    created_at: datetime
    updated_at: datetime


class CatalogImportListRead(ApiModel):
    items: list[CatalogImportRead]
    total: int
    limit: int
    offset: int


class CatalogImportErrorRead(ApiModel):
    id: uuid.UUID
    import_id: uuid.UUID
    row_number: int | None
    field: str | None
    code: str
    message: str
    rejected_value: str | None
    evidence: dict[str, Any]
    created_at: datetime


class CatalogImportErrorListRead(ApiModel):
    items: list[CatalogImportErrorRead]
    total: int
    limit: int
    offset: int


class SkuRead(ApiModel):
    id: uuid.UUID
    tenant_id: uuid.UUID
    product_style_id: uuid.UUID
    style_code: str
    style_name: str
    code: str
    upc: str
    color: str
    size: str
    attributes: dict[str, Any]
    active: bool


class SkuListRead(ApiModel):
    items: list[SkuRead]
    total: int
    limit: int
    offset: int
