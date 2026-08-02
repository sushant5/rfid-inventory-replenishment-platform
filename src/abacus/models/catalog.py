import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class CatalogImportMode(StrEnum):
    """Whether an import patches or replaces a tenant's active catalog."""

    DELTA = "DELTA"
    FULL = "FULL"


class CatalogImportStatus(StrEnum):
    VALIDATING = "VALIDATING"
    READY = "READY"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"
    FAILED = "FAILED"


class CatalogRowStatus(StrEnum):
    VALID = "VALID"
    INVALID = "INVALID"


class CatalogRowAction(StrEnum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    UNCHANGED = "UNCHANGED"


class ProductStyle(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "product_styles"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_product_styles_tenant_code"),
        CheckConstraint("code = upper(code)", name="ck_product_styles_code_uppercase"),
        Index("ix_product_styles_tenant_active", "tenant_id", "active"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class Sku(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "skus"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_skus_tenant_code"),
        UniqueConstraint("tenant_id", "upc", name="uq_skus_tenant_upc"),
        CheckConstraint("code = upper(code)", name="ck_skus_code_uppercase"),
        Index("ix_skus_tenant_style_active", "tenant_id", "product_style_id", "active"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    product_style_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("product_styles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    code: Mapped[str] = mapped_column(String(128), nullable=False)
    upc: Mapped[str] = mapped_column(String(14), nullable=False)
    color: Mapped[str] = mapped_column(String(128), nullable=False)
    size: Mapped[str] = mapped_column(String(64), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class CatalogImport(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "catalog_imports"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uq_catalog_imports_tenant_idempotency",
        ),
        Index("ix_catalog_imports_tenant_created", "tenant_id", "created_at"),
        CheckConstraint("size_bytes >= 0", name="ck_catalog_imports_nonnegative_size"),
        CheckConstraint("total_rows >= 0", name="ck_catalog_imports_nonnegative_rows"),
        CheckConstraint(
            "valid_rows >= 0 AND invalid_rows >= 0 "
            "AND inserted_count >= 0 AND updated_count >= 0 "
            "AND unchanged_count >= 0 AND deactivated_count >= 0",
            name="ck_catalog_imports_nonnegative_counts",
        ),
        CheckConstraint(
            "valid_rows + invalid_rows = total_rows",
            name="ck_catalog_imports_row_reconciliation",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    checksum: Mapped[str] = mapped_column(String(64), nullable=False)
    mode: Mapped[CatalogImportMode] = mapped_column(
        Enum(
            CatalogImportMode,
            name="catalog_import_mode",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    status: Mapped[CatalogImportStatus] = mapped_column(
        Enum(
            CatalogImportStatus,
            name="catalog_import_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=CatalogImportStatus.VALIDATING,
    )
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(128), nullable=False)
    size_bytes: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    valid_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    invalid_rows: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    inserted_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unchanged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deactivated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reconciliation: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    failure_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    promoted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class CatalogImportRow(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "catalog_import_rows"
    __table_args__ = (
        UniqueConstraint("import_id", "row_number", name="uq_catalog_import_rows_number"),
        CheckConstraint("row_number >= 2", name="ck_catalog_import_rows_data_row_number"),
        Index("ix_catalog_import_rows_tenant_import", "tenant_id", "import_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    import_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog_imports.id", ondelete="CASCADE"),
        nullable=False,
    )
    row_number: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_data: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    normalized_data: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[CatalogRowStatus] = mapped_column(
        Enum(
            CatalogRowStatus,
            name="catalog_row_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    action: Mapped[CatalogRowAction | None] = mapped_column(
        Enum(
            CatalogRowAction,
            name="catalog_row_action",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )


class CatalogImportError(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "catalog_import_errors"
    __table_args__ = (Index("ix_catalog_import_errors_tenant_import", "tenant_id", "import_id"),)

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    import_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog_imports.id", ondelete="CASCADE"),
        nullable=False,
    )
    row_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    field: Mapped[str | None] = mapped_column(String(128), nullable=True)
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    message: Mapped[str] = mapped_column(String(500), nullable=False)
    rejected_value: Mapped[str | None] = mapped_column(String(500), nullable=True)
    evidence: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)


class EpcBinding(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "epc_bindings"
    __table_args__ = (
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_epc_bindings_valid_interval",
        ),
        Index(
            "uq_epc_bindings_active_tenant_epc",
            "tenant_id",
            "epc",
            unique=True,
            postgresql_where=text("effective_to IS NULL"),
        ),
        Index(
            "ix_epc_bindings_resolve",
            "tenant_id",
            "epc",
            "effective_from",
            "effective_to",
        ),
        Index("ix_epc_bindings_sku_active", "tenant_id", "sku_id", "effective_to"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
        nullable=False,
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    source_import_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog_imports.id", ondelete="RESTRICT"),
        nullable=False,
    )
