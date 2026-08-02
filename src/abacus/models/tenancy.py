import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from abacus.enums import (
    BatchStatus,
    DeviceStatus,
    StoreStatus,
    TenantStatus,
    ZoneKind,
)
from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Tenant(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "tenants"
    __table_args__ = (CheckConstraint("code = lower(code)", name="ck_tenants_code_lowercase"),)

    code: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[TenantStatus] = mapped_column(
        Enum(TenantStatus, name="tenant_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=TenantStatus.PROVISIONING,
    )


class OrganizationUnit(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "organization_units"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_org_units_tenant_code"),
        CheckConstraint("code = lower(code)", name="ck_org_units_code_lowercase"),
        Index("ix_org_units_tenant_parent", "tenant_id", "parent_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    parent_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organization_units.id", ondelete="RESTRICT"),
        nullable=True,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    unit_type: Mapped[str] = mapped_column(String(32), nullable=False)


class Store(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "stores"
    __table_args__ = (
        UniqueConstraint("tenant_id", "code", name="uq_stores_tenant_code"),
        CheckConstraint("code = lower(code)", name="ck_stores_code_lowercase"),
        Index("ix_stores_tenant_status", "tenant_id", "status"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    organization_unit_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("organization_units.id", ondelete="RESTRICT"),
        nullable=True,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[StoreStatus] = mapped_column(
        Enum(StoreStatus, name="store_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=StoreStatus.PROVISIONING,
    )
    configuration: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
    )


class Zone(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "zones"
    __table_args__ = (
        UniqueConstraint("store_id", "code", name="uq_zones_store_code"),
        CheckConstraint("code = lower(code)", name="ck_zones_code_lowercase"),
        Index("ix_zones_tenant_store_kind", "tenant_id", "store_id", "kind"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=False,
    )
    code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[ZoneKind] = mapped_column(
        Enum(ZoneKind, name="zone_kind", native_enum=False, create_constraint=True),
        nullable=False,
    )


class Device(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "devices"
    __table_args__ = (
        UniqueConstraint("serial_number", name="uq_devices_serial_number"),
        Index("ix_devices_tenant_status", "tenant_id", "status"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    serial_number: Mapped[str] = mapped_column(String(128), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    credential_hash: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[DeviceStatus] = mapped_column(
        Enum(DeviceStatus, name="device_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=DeviceStatus.ACTIVE,
    )


class DeviceAssignment(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "device_assignments"
    __table_args__ = (
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_device_assignments_valid_interval",
        ),
        Index(
            "ix_device_assignments_lookup",
            "tenant_id",
            "device_id",
            "effective_from",
            "effective_to",
        ),
        Index(
            "uq_device_assignments_active_device",
            "tenant_id",
            "device_id",
            unique=True,
            postgresql_where=text("effective_to IS NULL"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("devices.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"),
        nullable=False,
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"),
        nullable=False,
    )
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class OnboardingBatch(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "onboarding_batches"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uq_onboarding_batches_tenant_idempotency",
        ),
        CheckConstraint("total_count > 0", name="ck_onboarding_batches_positive_total"),
        CheckConstraint(
            "succeeded_count >= 0 AND failed_count >= 0",
            name="ck_onboarding_batches_nonnegative_results",
        ),
        CheckConstraint(
            "succeeded_count + failed_count <= total_count",
            name="ck_onboarding_batches_results_within_total",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[BatchStatus] = mapped_column(
        Enum(BatchStatus, name="batch_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=BatchStatus.RECEIVED,
    )
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    succeeded_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    errors: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)
