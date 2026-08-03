import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from abacus.enums import ObservationStatus
from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class RfidObservation(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "rfid_observations"
    __table_args__ = (
        UniqueConstraint("tenant_id", "event_id", name="uq_rfid_observations_tenant_event"),
        UniqueConstraint(
            "acceptance_sequence",
            name="uq_rfid_observations_acceptance_sequence",
        ),
        Index(
            "ix_rfid_observations_epc_time_acceptance",
            "tenant_id",
            "epc",
            "observed_at",
            "acceptance_sequence",
        ),
        Index(
            "ix_rfid_observations_tenant_acceptance",
            "tenant_id",
            "acceptance_sequence",
        ),
        Index("ix_rfid_observations_status_time", "status", "ingested_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    batch_id: Mapped[str] = mapped_column(String(128), nullable=False)
    device_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("devices.id", ondelete="RESTRICT"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"),
        nullable=True,
    )
    zone_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"),
        nullable=True,
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    resolved_epc_binding_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("epc_bindings.id", ondelete="RESTRICT"),
        nullable=True,
    )
    resolution_strategy: Mapped[str | None] = mapped_column(String(32), nullable=True)
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    acceptance_sequence: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        server_default=text("nextval('rfid_observation_acceptance_seq'::regclass)"),
    )
    ingested_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    reader_sequence: Mapped[int | None] = mapped_column(Integer, nullable=True)
    antenna_port: Mapped[int | None] = mapped_column(Integer, nullable=True)
    rssi_dbm: Mapped[float | None] = mapped_column(Float, nullable=True)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[ObservationStatus] = mapped_column(
        Enum(
            ObservationStatus,
            name="observation_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=ObservationStatus.RECEIVED,
    )
    quarantine_reason: Mapped[str | None] = mapped_column(String(128), nullable=True)
    raw_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)


class InventoryItemState(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "inventory_item_states"
    __table_args__ = (
        UniqueConstraint("tenant_id", "epc", name="uq_inventory_item_states_tenant_epc"),
        CheckConstraint("candidate_count >= 0", name="ck_inventory_item_states_candidate_count"),
        CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_inventory_item_states_confidence",
        ),
        CheckConstraint(
            "(candidate_store_id IS NULL) = (candidate_zone_id IS NULL)",
            name="ck_inventory_item_states_candidate_location",
        ),
        CheckConstraint(
            "(candidate_store_id IS NULL) = (candidate_started_at IS NULL)",
            name="ck_inventory_item_states_candidate_time",
        ),
        Index("ix_inventory_item_states_location", "tenant_id", "store_id", "zone_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
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
    candidate_store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"),
        nullable=True,
    )
    candidate_zone_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"),
        nullable=True,
    )
    candidate_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    candidate_started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    confidence: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)


class InventoryBalance(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "inventory_balances"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "store_id",
            "zone_id",
            "sku_id",
            name="uq_inventory_balances_location_sku",
        ),
        CheckConstraint("quantity >= 0", name="ck_inventory_balances_nonnegative_quantity"),
        Index("ix_inventory_balances_store_sku", "tenant_id", "store_id", "sku_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=False,
    )
    zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="CASCADE"),
        nullable=False,
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
        nullable=False,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Processing time of the last quantity transition. Replenishment uses this
    # instead of ``updated_at`` so a same-location freshness refresh cannot make
    # verified physical work appear in the RFID quantity projection.
    quantity_changed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )
    # Event time for the newest RFID observation that changed or reaffirmed this
    # confirmed projection. This is intentionally separate from ``updated_at``,
    # which is database/projection processing time.
    last_relevant_observation_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class InventoryChange(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "inventory_changes"
    __table_args__ = (
        UniqueConstraint("observation_id", name="uq_inventory_changes_observation"),
        Index("ix_inventory_changes_store_time", "tenant_id", "to_store_id", "observed_at"),
        Index(
            "ix_inventory_changes_replenishment_task",
            "replenishment_task_id",
            postgresql_where=text("replenishment_task_id IS NOT NULL"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    epc: Mapped[str] = mapped_column(String(128), nullable=False)
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
        nullable=False,
    )
    observation_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("rfid_observations.id", ondelete="RESTRICT"),
        nullable=False,
    )
    replenishment_task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("legacy_replenishment_tasks.id", ondelete="RESTRICT"),
        nullable=True,
    )
    from_store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"),
        nullable=True,
    )
    from_zone_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"),
        nullable=True,
    )
    to_store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="RESTRICT"),
        nullable=False,
    )
    to_zone_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("zones.id", ondelete="RESTRICT"),
        nullable=False,
    )
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
