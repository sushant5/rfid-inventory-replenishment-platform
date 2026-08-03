"""add durable inventory state, outbox, and projection tables

Revision ID: e4d9b2f8a315
Revises: d3c8a1e7f204
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e4d9b2f8a315"
down_revision: str | None = "d3c8a1e7f204"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    batch_status = sa.Enum(
        "ACCEPTED",
        "PROCESSING",
        "COMPLETED",
        "COMPLETED_WITH_ERRORS",
        name="observation_batch_status",
        native_enum=False,
        create_constraint=True,
    )
    freshness_status = sa.Enum(
        "LIVE",
        "DEGRADED",
        "STALE",
        name="freshness_status",
        native_enum=False,
        create_constraint=True,
    )
    store_freshness_status = sa.Enum(
        "LIVE",
        "DEGRADED",
        "STALE",
        name="store_freshness_status",
        native_enum=False,
        create_constraint=True,
    )
    event_status = sa.Enum(
        "PENDING",
        "PROCESSED",
        "REJECTED",
        name="rfid_event_processing_status",
        native_enum=False,
        create_constraint=True,
    )
    batch_event_status = sa.Enum(
        "PENDING",
        "PROCESSED",
        "REJECTED",
        name="rfid_batch_event_processing_status",
        native_enum=False,
        create_constraint=True,
    )

    op.create_table(
        "rfid_observation_batches",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("device_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", batch_status, nullable=False),
        sa.Column("accepted_count", sa.Integer(), nullable=False),
        sa.Column("processed_count", sa.Integer(), nullable=False),
        sa.Column("rejected_count", sa.Integer(), nullable=False),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "accepted_count >= 0 AND processed_count >= 0 AND rejected_count >= 0",
            name="ck_rfid_batches_nonnegative_counts",
        ),
        sa.CheckConstraint(
            "processed_count + rejected_count <= accepted_count",
            name="ck_rfid_batches_counts_reconcile",
        ),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["zone_id"], ["zones.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_rfid_batches_tenant_received",
        "rfid_observation_batches",
        ["tenant_id", "received_at"],
    )
    op.create_table(
        "rfid_observation_events",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("payload_fingerprint", sa.String(64), nullable=False),
        sa.Column("device_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("epc", sa.String(128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("first_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("rssi", sa.Float(), nullable=False),
        sa.Column("antenna_id", sa.String(128), nullable=True),
        sa.Column("reader_health", sa.Float(), nullable=False),
        sa.Column("is_buffered", sa.Boolean(), nullable=False),
        sa.Column("backlog_drained", sa.Boolean(), nullable=False),
        sa.Column("reader_coverage_ok", sa.Boolean(), nullable=False),
        sa.Column("processing_status", event_status, nullable=False),
        sa.Column("disposition", sa.String(32), nullable=True),
        sa.Column("rejection_reason", sa.String(128), nullable=True),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["device_id"], ["devices.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["zone_id"], ["zones.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "event_id", name="pk_rfid_observation_events"),
    )
    op.create_index(
        "ix_rfid_observation_events_pending",
        "rfid_observation_events",
        ["tenant_id", "first_received_at"],
        postgresql_where=sa.text("processing_status = 'PENDING'"),
    )
    op.create_table(
        "rfid_observation_batch_events",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column("processing_status", batch_event_status, nullable=False),
        sa.Column("disposition", sa.String(32), nullable=True),
        sa.Column("rejection_reason", sa.String(128), nullable=True),
        sa.Column("finalized_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["rfid_observation_events.tenant_id", "rfid_observation_events.event_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["rfid_observation_batches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint(
            "tenant_id", "batch_id", "event_id", name="pk_rfid_observation_batch_events"
        ),
    )
    op.create_index(
        "ix_rfid_batch_events_event",
        "rfid_observation_batch_events",
        ["tenant_id", "event_id"],
    )
    op.create_table(
        "rfid_observation_outbox",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=False),
        sa.Column(
            "acceptance_sequence",
            sa.BigInteger(),
            server_default=sa.text("nextval('rfid_observation_acceptance_seq'::regclass)"),
            nullable=False,
        ),
        sa.Column("partition_key", sa.String(255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id", "event_id"],
            ["rfid_observation_events.tenant_id", "rfid_observation_events.event_id"],
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "event_id", name="uq_rfid_outbox_tenant_event"),
    )
    op.create_index(
        "uq_rfid_observation_outbox_acceptance_sequence",
        "rfid_observation_outbox",
        ["acceptance_sequence"],
        unique=True,
    )
    op.create_index(
        "ix_rfid_observation_outbox_unpublished",
        "rfid_observation_outbox",
        ["acceptance_sequence"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "rfid_quarantine",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("batch_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_id", sa.String(128), nullable=True),
        sa.Column("reason", sa.String(128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "quarantined_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["batch_id"], ["rfid_observation_batches.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "event_id", name="uq_rfid_quarantine_tenant_event"),
    )
    op.create_index("ix_rfid_quarantine_tenant_batch", "rfid_quarantine", ["tenant_id", "batch_id"])
    op.create_table(
        "current_item_state",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("epc", sa.String(128), nullable=False),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("last_observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.CheckConstraint("confidence >= 0 AND confidence <= 1", name="ck_item_state_confidence"),
        sa.CheckConstraint("state_version >= 1", name="ck_item_state_positive_version"),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["zone_id"], ["zones.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("tenant_id", "epc", name="pk_current_item_state"),
    )
    op.create_index(
        "ix_current_item_state_reconcile",
        "current_item_state",
        ["tenant_id", "store_id", "sku_id", "zone_id"],
    )
    op.create_table(
        "inventory_transition_outbox",
        sa.Column("transition_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("epc", sa.String(128), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("deltas", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publish_attempts", sa.Integer(), nullable=False),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("transition_id"),
        sa.UniqueConstraint(
            "tenant_id", "epc", "state_version", name="uq_outbox_item_state_version"
        ),
    )
    op.create_index(
        "ix_inventory_outbox_unpublished",
        "inventory_transition_outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_table(
        "inventory_projection",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("as_of", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("freshness_status", freshness_status, nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("quantity >= 0", name="ck_inventory_projection_nonnegative"),
        sa.CheckConstraint(
            "confidence >= 0 AND confidence <= 1",
            name="ck_inventory_projection_confidence",
        ),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["zone_id"], ["zones.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint(
            "tenant_id", "store_id", "sku_id", "zone_id", name="pk_inventory_projection"
        ),
    )
    op.create_index(
        "ix_inventory_projection_store_sku",
        "inventory_projection",
        ["tenant_id", "store_id", "sku_id"],
    )
    op.create_table(
        "applied_inventory_deltas",
        sa.Column("delta_id", sa.String(255), nullable=False),
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("zone_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("quantity_delta", sa.Integer(), nullable=False),
        sa.Column(
            "applied_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["zone_id"], ["zones.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("delta_id"),
    )
    op.create_table(
        "store_connectivity",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("gateway_last_heartbeat", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_live_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("oldest_buffered_event_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("backlog_drained", sa.Boolean(), nullable=False),
        sa.Column("reader_coverage_ok", sa.Boolean(), nullable=False),
        sa.Column("freshness_status", store_freshness_status, nullable=False),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("tenant_id", "store_id", name="pk_store_connectivity"),
    )


def downgrade() -> None:
    op.drop_table("store_connectivity")
    op.drop_table("applied_inventory_deltas")
    op.drop_index("ix_inventory_projection_store_sku", table_name="inventory_projection")
    op.drop_table("inventory_projection")
    op.drop_index("ix_inventory_outbox_unpublished", table_name="inventory_transition_outbox")
    op.drop_table("inventory_transition_outbox")
    op.drop_index("ix_current_item_state_reconcile", table_name="current_item_state")
    op.drop_table("current_item_state")
    op.drop_index("ix_rfid_quarantine_tenant_batch", table_name="rfid_quarantine")
    op.drop_table("rfid_quarantine")
    op.drop_index("ix_rfid_observation_outbox_unpublished", table_name="rfid_observation_outbox")
    op.drop_table("rfid_observation_outbox")
    op.drop_index("ix_rfid_batch_events_event", table_name="rfid_observation_batch_events")
    op.drop_table("rfid_observation_batch_events")
    op.drop_index("ix_rfid_observation_events_pending", table_name="rfid_observation_events")
    op.drop_table("rfid_observation_events")
    op.drop_index("ix_rfid_batches_tenant_received", table_name="rfid_observation_batches")
    op.drop_table("rfid_observation_batches")
