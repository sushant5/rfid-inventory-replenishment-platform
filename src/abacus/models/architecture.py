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
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class Product(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("tenant_id", "style_code", name="uq_products_tenant_style"),
        UniqueConstraint("tenant_id", "id", name="uq_products_tenant_id"),
        Index("ix_products_tenant_category", "tenant_id", "category"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    style_code: Mapped[str] = mapped_column(String(64), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    category: Mapped[str] = mapped_column(String(128), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ProductVariant(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "product_variants"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "product_id", "color", name="uq_product_variants_product_color"
        ),
        UniqueConstraint("tenant_id", "id", name="uq_product_variants_tenant_id"),
        ForeignKeyConstraint(
            ["tenant_id", "product_id"],
            ["products.tenant_id", "products.id"],
            name="fk_product_variants_tenant_product",
            ondelete="CASCADE",
        ),
        Index("ix_product_variants_tenant_product", "tenant_id", "product_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    product_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("products.id", ondelete="CASCADE"), nullable=False
    )
    color: Mapped[str] = mapped_column(String(128), nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class RfidTag(TimestampMixin, Base):
    __tablename__ = "rfid_tags"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "epc", name="pk_rfid_tags"),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_rfid_tags_tenant_sku",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "source_import_id"],
            ["catalog_imports.tenant_id", "catalog_imports.id"],
            name="fk_rfid_tags_tenant_source_import",
            ondelete="RESTRICT",
        ),
        Index("ix_rfid_tags_tenant_sku", "tenant_id", "sku_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"), nullable=False
    )
    source_import_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("catalog_imports.id", ondelete="RESTRICT"), nullable=False
    )
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class IdentityRole(StrEnum):
    STORE_ASSOCIATE = "STORE_ASSOCIATE"
    STORE_MANAGER = "STORE_MANAGER"
    CORPORATE_USER = "CORPORATE_USER"
    TENANT_ADMIN = "TENANT_ADMIN"


CanonicalIdentityRole = IdentityRole


class UserRole(Base):
    __tablename__ = "user_roles"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "user_id", "role", name="pk_user_roles"),
        ForeignKeyConstraint(
            ["tenant_id", "user_id"],
            ["users.tenant_id", "users.id"],
            name="fk_user_roles_tenant_user",
            ondelete="CASCADE",
        ),
        Index("ix_user_roles_tenant_role", "tenant_id", "role"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    role: Mapped[CanonicalIdentityRole] = mapped_column(
        Enum(
            CanonicalIdentityRole,
            name="canonical_identity_role",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class UserStoreAssignment(Base):
    __tablename__ = "user_store_assignments"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "user_id", "store_id", name="pk_user_store_assignments"),
        ForeignKeyConstraint(
            ["tenant_id", "user_id"],
            ["users.tenant_id", "users.id"],
            name="fk_user_store_assignments_tenant_user",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_user_store_assignments_tenant_store",
            ondelete="CASCADE",
        ),
        Index("ix_user_store_assignments_store", "tenant_id", "store_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ObservationBatchStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    COMPLETED_WITH_ERRORS = "COMPLETED_WITH_ERRORS"


class RfidObservationBatch(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "rfid_observation_batches"
    __table_args__ = (
        CheckConstraint(
            "accepted_count >= 0 AND processed_count >= 0 AND rejected_count >= 0",
            name="ck_rfid_batches_nonnegative_counts",
        ),
        CheckConstraint(
            "processed_count + rejected_count <= accepted_count",
            name="ck_rfid_batches_counts_reconcile",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_rfid_observation_batches_tenant_id"),
        ForeignKeyConstraint(
            ["tenant_id", "device_id"],
            ["devices.tenant_id", "devices.id"],
            name="fk_rfid_batches_tenant_device",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_rfid_batches_tenant_store",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id", "zone_id"],
            ["zones.tenant_id", "zones.store_id", "zones.id"],
            name="fk_rfid_batches_tenant_store_zone",
            ondelete="RESTRICT",
        ),
        Index("ix_rfid_batches_tenant_received", "tenant_id", "received_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("devices.id", ondelete="RESTRICT"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"), nullable=False
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[ObservationBatchStatus] = mapped_column(
        Enum(
            ObservationBatchStatus,
            name="observation_batch_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=ObservationBatchStatus.ACCEPTED,
    )
    accepted_count: Mapped[int] = mapped_column(Integer, nullable=False)
    processed_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    @property
    def pending_count(self) -> int:
        return self.accepted_count - self.processed_count - self.rejected_count


class RfidEventProcessingStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    REJECTED = "REJECTED"


class RfidObservationEventLedger(Base):
    """Immutable event identity and durable processing result.

    The composite primary key is the final replay-protection boundary. The worker may
    retry a durable event, and a device may submit the same event in a later batch,
    but only the first pending ledger row is allowed to influence item state.
    """

    __tablename__ = "rfid_observation_events"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "event_id", name="pk_rfid_observation_events"),
        ForeignKeyConstraint(
            ["tenant_id", "device_id"],
            ["devices.tenant_id", "devices.id"],
            name="fk_rfid_events_tenant_device",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_rfid_events_tenant_store",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id", "zone_id"],
            ["zones.tenant_id", "zones.store_id", "zones.id"],
            name="fk_rfid_events_tenant_store_zone",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_rfid_observation_events_pending",
            "tenant_id",
            "first_received_at",
            postgresql_where=text("processing_status = 'PENDING'"),
        ),
        Index(
            "ix_rfid_events_processed_epc_watermark",
            "tenant_id",
            "epc",
            text("observed_at DESC"),
            postgresql_where=text("processing_status = 'PROCESSED'"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("devices.id", ondelete="RESTRICT"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"), nullable=False
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"), nullable=False
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    rssi: Mapped[float] = mapped_column(Float, nullable=False)
    antenna_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reader_health: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    is_buffered: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    backlog_drained: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    reader_coverage_ok: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    processing_status: Mapped[RfidEventProcessingStatus] = mapped_column(
        Enum(
            RfidEventProcessingStatus,
            name="rfid_event_processing_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=RfidEventProcessingStatus.PENDING,
    )
    disposition: Mapped[str | None] = mapped_column(String(32), nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RfidObservationBatchEvent(Base):
    """Links every accepted request batch to the canonical event identity."""

    __tablename__ = "rfid_observation_batch_events"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id", "batch_id", "event_id", name="pk_rfid_observation_batch_events"
        ),
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["rfid_observation_events.tenant_id", "rfid_observation_events.event_id"],
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "batch_id"],
            ["rfid_observation_batches.tenant_id", "rfid_observation_batches.id"],
            name="fk_rfid_batch_events_tenant_batch",
            ondelete="CASCADE",
        ),
        Index("ix_rfid_batch_events_event", "tenant_id", "event_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    batch_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("rfid_observation_batches.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    processing_status: Mapped[RfidEventProcessingStatus] = mapped_column(
        Enum(
            RfidEventProcessingStatus,
            name="rfid_batch_event_processing_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=RfidEventProcessingStatus.PENDING,
    )
    disposition: Mapped[str | None] = mapped_column(String(32), nullable=True)
    rejection_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    finalized_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class RfidObservationOutbox(UUIDPrimaryKeyMixin, Base):
    """Transactional raw-event inbox drained by the hosted PostgreSQL worker."""

    __tablename__ = "rfid_observation_outbox"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["rfid_observation_events.tenant_id", "rfid_observation_events.event_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "event_id", name="uq_rfid_outbox_tenant_event"),
        Index(
            "uq_rfid_observation_outbox_acceptance_sequence",
            "acceptance_sequence",
            unique=True,
        ),
        Index(
            "ix_rfid_observation_outbox_unpublished",
            "acceptance_sequence",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index(
            "ix_rfid_observation_outbox_tenant_pending",
            "tenant_id",
            "acceptance_sequence",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    acceptance_sequence: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('rfid_observation_acceptance_seq'::regclass)"),
    )
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class RfidQuarantine(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "rfid_quarantine"
    __table_args__ = (
        UniqueConstraint("tenant_id", "event_id", name="uq_rfid_quarantine_tenant_event"),
        ForeignKeyConstraint(
            ["tenant_id", "batch_id"],
            ["rfid_observation_batches.tenant_id", "rfid_observation_batches.id"],
            name="fk_rfid_quarantine_tenant_batch",
            ondelete="CASCADE",
        ),
        Index("ix_rfid_quarantine_tenant_batch", "tenant_id", "batch_id"),
        Index(
            "ix_rfid_quarantine_tenant_created",
            "tenant_id",
            text("quarantined_at DESC"),
            text("id DESC"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    batch_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("rfid_observation_batches.id", ondelete="CASCADE"), nullable=False
    )
    event_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reason: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    quarantined_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class CurrentItemState(Base):
    __tablename__ = "current_item_state"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "epc", name="pk_current_item_state"),
        CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_item_state_confidence"),
        CheckConstraint("state_version >= 1", name="ck_item_state_positive_version"),
        CheckConstraint(
            "(store_id IS NULL) = (zone_id IS NULL)",
            name="ck_current_item_state_location_pair",
        ),
        CheckConstraint(
            "authoritative_removal_event_id IS NULL OR (store_id IS NULL AND zone_id IS NULL)",
            name="ck_current_item_state_authoritative_removal_location",
        ),
        CheckConstraint(
            "(authoritative_removal_event_id IS NULL) = (authoritative_removed_at IS NULL)",
            name="ck_current_item_state_authoritative_removal_pair",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_current_item_state_tenant_sku",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_current_item_state_tenant_store",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id", "zone_id"],
            ["zones.tenant_id", "zones.store_id", "zones.id"],
            name="fk_current_item_state_tenant_store_zone",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "authoritative_removal_event_id"],
            ["business_events.tenant_id", "business_events.id"],
            name="fk_current_item_state_tenant_authoritative_removal_event",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_current_item_state_reconcile",
            "tenant_id",
            "store_id",
            "sku_id",
            "zone_id",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"), nullable=False
    )
    store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"), nullable=True
    )
    zone_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"), nullable=True
    )
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    authoritative_removal_event_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), nullable=True
    )
    authoritative_removed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )


class InventoryTransitionOutbox(Base):
    __tablename__ = "inventory_transition_outbox"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "transition_id",
            name="uq_inventory_transition_outbox_tenant_transition",
        ),
        UniqueConstraint("tenant_id", "epc", "state_version", name="uq_outbox_item_state_version"),
        Index(
            "ix_inventory_outbox_unpublished",
            "tenant_id",
            "created_at",
            postgresql_where=text("published_at IS NULL AND quarantined_at IS NULL"),
        ),
        Index(
            "ix_inventory_outbox_unreconciled_quarantine",
            "tenant_id",
            "quarantined_at",
            postgresql_where=text("quarantined_at IS NOT NULL AND reconciled_at IS NULL"),
        ),
        CheckConstraint(
            "NOT (published_at IS NOT NULL AND quarantined_at IS NOT NULL)",
            name="ck_inventory_outbox_one_terminal_state",
        ),
        CheckConstraint(
            "(quarantined_at IS NULL AND quarantine_reason IS NULL "
            "AND reconciled_at IS NULL) OR "
            "(quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)",
            name="ck_inventory_outbox_quarantine_metadata",
        ),
    )

    transition_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    deltas: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    quarantined_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    quarantine_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    publish_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class BusinessEventType(StrEnum):
    """Authoritative events that remove one physical item from available inventory."""

    SALE = "SALE"
    TRANSFER_OUT = "TRANSFER_OUT"
    ADJUSTMENT_REMOVE = "ADJUSTMENT_REMOVE"


class BusinessEventStatus(StrEnum):
    PENDING_PROJECTION = "PENDING_PROJECTION"
    PROJECTED = "PROJECTED"
    FAILED = "FAILED"


class BusinessEvent(UUIDPrimaryKeyMixin, Base):
    """Idempotent POS/WMS removal accepted by the hosted vertical slice."""

    __tablename__ = "business_events"
    __table_args__ = (
        UniqueConstraint("tenant_id", "id", name="uq_business_events_tenant_id"),
        UniqueConstraint(
            "tenant_id",
            "source_system",
            "external_event_id",
            name="uq_business_events_source_event",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_business_events_tenant_store",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "transition_id"],
            ["inventory_transition_outbox.tenant_id", "inventory_transition_outbox.transition_id"],
            name="fk_business_events_tenant_transition",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_business_events_store_created",
            "tenant_id",
            "store_id",
            text("created_at DESC"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"), nullable=False
    )
    source_system: Mapped[str] = mapped_column(String(64), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    request_fingerprint: Mapped[str] = mapped_column(String(64), nullable=False)
    event_type: Mapped[BusinessEventType] = mapped_column(
        Enum(
            BusinessEventType,
            name="business_event_type",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    transition_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("inventory_transition_outbox.transition_id", ondelete="RESTRICT"),
        nullable=False,
    )
    state_version: Mapped[int] = mapped_column(Integer, nullable=False)
    note: Mapped[str | None] = mapped_column(String(500), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class FreshnessStatus(StrEnum):
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    STALE = "STALE"


class ItemPresenceStatus(StrEnum):
    OBSERVED = "OBSERVED"
    UNOBSERVED = "UNOBSERVED"
    LOCATION_UNKNOWN = "LOCATION_UNKNOWN"
    REMOVED = "REMOVED"


class InventoryProjection(Base):
    __tablename__ = "inventory_projection"
    __table_args__ = (
        PrimaryKeyConstraint(
            "tenant_id", "store_id", "sku_id", "zone_id", name="pk_inventory_projection"
        ),
        CheckConstraint("quantity >= 0", name="ck_inventory_projection_nonnegative"),
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1", name="ck_inventory_projection_confidence"
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_inventory_projection_tenant_store",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_inventory_projection_tenant_sku",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id", "zone_id"],
            ["zones.tenant_id", "zones.store_id", "zones.id"],
            name="fk_inventory_projection_tenant_store_zone",
            ondelete="CASCADE",
        ),
        Index("ix_inventory_projection_store_sku", "tenant_id", "store_id", "sku_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"), nullable=False
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="CASCADE"), nullable=False
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    as_of: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    freshness_status: Mapped[FreshnessStatus] = mapped_column(
        Enum(
            FreshnessStatus,
            name="freshness_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=FreshnessStatus.STALE,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class AppliedInventoryDelta(Base):
    __tablename__ = "applied_inventory_deltas"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_applied_inventory_deltas_tenant_store",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_applied_inventory_deltas_tenant_sku",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id", "zone_id"],
            ["zones.tenant_id", "zones.store_id", "zones.id"],
            name="fk_applied_inventory_deltas_tenant_store_zone",
            ondelete="CASCADE",
        ),
    )

    delta_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"), nullable=False
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="CASCADE"), nullable=False
    )
    quantity_delta: Mapped[int] = mapped_column(Integer, nullable=False)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class StoreConnectivity(Base):
    __tablename__ = "store_connectivity"
    __table_args__ = (
        PrimaryKeyConstraint("tenant_id", "store_id", name="pk_store_connectivity"),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_store_connectivity_tenant_store",
            ondelete="CASCADE",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
    )
    gateway_last_heartbeat: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status_received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_live_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_live_received_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    oldest_buffered_event_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    backlog_drained: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    reader_coverage_ok: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    freshness_status: Mapped[FreshnessStatus] = mapped_column(
        Enum(
            FreshnessStatus,
            name="store_freshness_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=FreshnessStatus.STALE,
    )
    inventory_reconciliation_required_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )


class PolicyVersionStatus(StrEnum):
    DRAFT = "DRAFT"
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"


class PolicyDefinition(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_policies"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_versioned_policy_tenant_name"),
        UniqueConstraint("tenant_id", "id", name="uq_replenishment_policies_tenant_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class PolicyVersion(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_policy_versions"
    __table_args__ = (
        UniqueConstraint("policy_id", "version_number", name="uq_policy_versions_number"),
        UniqueConstraint("tenant_id", "id", name="uq_replenishment_policy_versions_tenant_id"),
        ForeignKeyConstraint(
            ["tenant_id", "policy_id"],
            ["replenishment_policies.tenant_id", "replenishment_policies.id"],
            name="fk_policy_versions_tenant_policy",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "activated_by_user_id"],
            ["users.tenant_id", "users.id"],
            name="fk_policy_versions_tenant_activated_by",
        ),
        Index(
            "uq_policy_versions_one_active",
            "policy_id",
            unique=True,
            postgresql_where=text("status = 'ACTIVE'"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_policies.id", ondelete="CASCADE"), nullable=False
    )
    version_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[PolicyVersionStatus] = mapped_column(
        Enum(
            PolicyVersionStatus,
            name="policy_version_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=PolicyVersionStatus.DRAFT,
    )
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    activated_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )


class PolicyRule(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_policy_rules"
    __table_args__ = (
        CheckConstraint("min_floor_qty >= 0", name="ck_policy_rules_min_nonnegative"),
        UniqueConstraint(
            "tenant_id",
            "version_id",
            "id",
            name="uq_replenishment_policy_rules_tenant_version_id",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "version_id"],
            ["replenishment_policy_versions.tenant_id", "replenishment_policy_versions.id"],
            name="fk_policy_rules_tenant_version",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_policy_rules_tenant_store",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_policy_rules_tenant_sku",
            ondelete="CASCADE",
        ),
        CheckConstraint(
            "target_floor_qty >= min_floor_qty", name="ck_policy_rules_target_at_least_min"
        ),
        CheckConstraint(
            "num_nonnulls(category, style_code, sku_id) <= 1",
            name="ck_policy_rules_one_selector",
        ),
        CheckConstraint(
            "size IS NULL OR sku_id IS NOT NULL",
            name="ck_policy_rules_size_requires_sku",
        ),
        Index("ix_policy_rules_resolution", "tenant_id", "store_id", "sku_id", "style_code"),
        Index(
            "uq_policy_rules_scope_priority",
            "version_id",
            text("COALESCE(store_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            text("COALESCE(category, '')"),
            text("COALESCE(style_code, '')"),
            text("COALESCE(sku_id, '00000000-0000-0000-0000-000000000000'::uuid)"),
            text("COALESCE(size, '')"),
            "priority",
            unique=True,
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_policy_versions.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=True
    )
    category: Mapped[str | None] = mapped_column(String(128), nullable=True)
    style_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    sku_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("skus.id", ondelete="CASCADE"), nullable=True
    )
    size: Mapped[str | None] = mapped_column(String(64), nullable=True)
    min_floor_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    target_floor_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class ReplenishmentTaskStatus(StrEnum):
    OPEN = "OPEN"
    CLAIMED = "CLAIMED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    CANCELED = "CANCELED"
    EXPIRED = "EXPIRED"


class ReplenishmentVerificationStatus(StrEnum):
    NOT_APPLICABLE = "NOT_APPLICABLE"
    PENDING = "PENDING"
    VERIFIED = "VERIFIED"
    UNVERIFIED = "UNVERIFIED"


class ReplenishmentTask(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_tasks"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_replenishment_tasks_positive_quantity"),
        CheckConstraint("version >= 1", name="ck_replenishment_tasks_positive_version"),
        CheckConstraint(
            "verified_quantity >= 0 AND verified_quantity <= quantity",
            name="ck_replenishment_tasks_verified_quantity",
        ),
        UniqueConstraint("tenant_id", "id", name="uq_replenishment_tasks_tenant_id"),
        ForeignKeyConstraint(
            ["tenant_id", "store_id"],
            ["stores.tenant_id", "stores.id"],
            name="fk_replenishment_tasks_tenant_store",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "sku_id"],
            ["skus.tenant_id", "skus.id"],
            name="fk_replenishment_tasks_tenant_sku",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "policy_version_id"],
            ["replenishment_policy_versions.tenant_id", "replenishment_policy_versions.id"],
            name="fk_replenishment_tasks_tenant_policy_version",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "policy_version_id", "policy_rule_id"],
            [
                "replenishment_policy_rules.tenant_id",
                "replenishment_policy_rules.version_id",
                "replenishment_policy_rules.id",
            ],
            name="fk_replenishment_tasks_tenant_version_rule",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "claimed_by_user_id"],
            ["users.tenant_id", "users.id"],
            name="fk_replenishment_tasks_tenant_claimed_by",
        ),
        Index(
            "uq_replenishment_tasks_active_store_sku",
            "tenant_id",
            "store_id",
            "sku_id",
            unique=True,
            postgresql_where=text("status IN ('OPEN', 'CLAIMED', 'IN_PROGRESS')"),
        ),
        Index("ix_replenishment_tasks_store_status", "tenant_id", "store_id", "status"),
        Index(
            "ix_replenishment_tasks_verification_candidate",
            "tenant_id",
            "store_id",
            "sku_id",
            "status",
            "started_at",
            postgresql_where=text("status IN ('IN_PROGRESS', 'COMPLETED')"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"), nullable=False
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"), nullable=False
    )
    policy_version_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_policy_versions.id", ondelete="RESTRICT"), nullable=False
    )
    policy_rule_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_policy_rules.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[ReplenishmentTaskStatus] = mapped_column(
        Enum(
            ReplenishmentTaskStatus,
            name="replenishment_task_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=ReplenishmentTaskStatus.OPEN,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    verified_quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    verification_deadline: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    claimed_by_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)


class ReplenishmentTaskEvidence(UUIDPrimaryKeyMixin, Base):
    """One stable backroom-to-floor RFID transition attributed to a task."""

    __tablename__ = "replenishment_task_evidence"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "transition_id",
            name="uq_replenishment_task_evidence_transition",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "task_id"],
            ["replenishment_tasks.tenant_id", "replenishment_tasks.id"],
            name="fk_replenishment_task_evidence_tenant_task",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "transition_id"],
            ["inventory_transition_outbox.tenant_id", "inventory_transition_outbox.transition_id"],
            name="fk_replenishment_task_evidence_tenant_transition",
            ondelete="RESTRICT",
        ),
        Index(
            "ix_replenishment_task_evidence_task_observed",
            "tenant_id",
            "task_id",
            "observed_at",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    task_id: Mapped[uuid.UUID] = mapped_column(nullable=False)
    transition_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
