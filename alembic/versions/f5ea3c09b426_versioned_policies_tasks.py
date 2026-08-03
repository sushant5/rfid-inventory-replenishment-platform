"""add immutable policy versions and canonical replenishment tasks

Revision ID: f5ea3c09b426
Revises: e4d9b2f8a315
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f5ea3c09b426"
down_revision: str | None = "e4d9b2f8a315"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.rename_table("replenishment_policies", "legacy_replenishment_rules")
    op.rename_table("replenishment_tasks", "legacy_replenishment_tasks")
    op.execute(
        "ALTER INDEX uq_replenishment_tasks_active_store_sku "
        "RENAME TO uq_legacy_replenishment_tasks_active_store_sku"
    )
    op.execute(
        "ALTER INDEX ix_replenishment_tasks_tenant_store_status "
        "RENAME TO ix_legacy_replenishment_tasks_tenant_store_status"
    )
    op.execute(
        "ALTER INDEX ix_replenishment_tasks_unreviewed_cutover "
        "RENAME TO ix_legacy_replenishment_tasks_unreviewed_cutover"
    )

    version_status = sa.Enum(
        "DRAFT",
        "ACTIVE",
        "RETIRED",
        name="policy_version_status",
        native_enum=False,
        create_constraint=True,
    )
    task_status = sa.Enum(
        "OPEN",
        "CLAIMED",
        "IN_PROGRESS",
        "COMPLETED",
        "CANCELED",
        "EXPIRED",
        name="canonical_task_status",
        native_enum=False,
        create_constraint=True,
    )
    op.create_table(
        "replenishment_policies",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_versioned_policy_tenant_name"),
    )
    op.create_table(
        "replenishment_policy_versions",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_number", sa.Integer(), nullable=False),
        sa.Column("status", version_status, nullable=False),
        sa.Column("activated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("activated_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["activated_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["policy_id"], ["replenishment_policies.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("policy_id", "version_number", name="uq_policy_versions_number"),
    )
    op.create_index(
        "uq_policy_versions_one_active",
        "replenishment_policy_versions",
        ["policy_id"],
        unique=True,
        postgresql_where=sa.text("status = 'ACTIVE'"),
    )
    op.create_table(
        "replenishment_policy_rules",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("category", sa.String(128), nullable=True),
        sa.Column("style_code", sa.String(64), nullable=True),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("size", sa.String(64), nullable=True),
        sa.Column("min_floor_qty", sa.Integer(), nullable=False),
        sa.Column("target_floor_qty", sa.Integer(), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("min_floor_qty >= 0", name="ck_policy_rules_min_nonnegative"),
        sa.CheckConstraint(
            "target_floor_qty >= min_floor_qty", name="ck_policy_rules_target_at_least_min"
        ),
        sa.CheckConstraint(
            "num_nonnulls(category, style_code, sku_id) <= 1",
            name="ck_policy_rules_one_selector",
        ),
        sa.CheckConstraint(
            "size IS NULL OR sku_id IS NOT NULL",
            name="ck_policy_rules_size_requires_sku",
        ),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["version_id"], ["replenishment_policy_versions.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_policy_rules_resolution",
        "replenishment_policy_rules",
        ["tenant_id", "store_id", "sku_id", "style_code"],
    )
    op.execute(
        """
        CREATE UNIQUE INDEX uq_policy_rules_scope_priority
        ON replenishment_policy_rules (
          version_id,
          COALESCE(store_id, '00000000-0000-0000-0000-000000000000'::uuid),
          COALESCE(category, ''), COALESCE(style_code, ''),
          COALESCE(sku_id, '00000000-0000-0000-0000-000000000000'::uuid),
          COALESCE(size, ''), priority
        )
        """
    )
    op.create_table(
        "replenishment_tasks",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_version_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("policy_rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("status", task_status, nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("claimed_by_user_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("quantity > 0", name="ck_canonical_tasks_positive_quantity"),
        sa.CheckConstraint("version >= 1", name="ck_canonical_tasks_positive_version"),
        sa.ForeignKeyConstraint(["claimed_by_user_id"], ["users.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(
            ["policy_rule_id"], ["replenishment_policy_rules.id"], ondelete="RESTRICT"
        ),
        sa.ForeignKeyConstraint(
            ["policy_version_id"],
            ["replenishment_policy_versions.id"],
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_canonical_tasks_store_status",
        "replenishment_tasks",
        ["tenant_id", "store_id", "status"],
    )
    op.create_index(
        "uq_replenishment_tasks_active_store_sku",
        "replenishment_tasks",
        ["tenant_id", "store_id", "sku_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('OPEN', 'CLAIMED', 'IN_PROGRESS')"),
    )

    op.execute(
        """
        CREATE FUNCTION prevent_activated_policy_rule_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE target_version uuid;
        DECLARE target_status text;
        BEGIN
          target_version := CASE WHEN TG_OP = 'DELETE' THEN OLD.version_id ELSE NEW.version_id END;
          SELECT status INTO target_status
          FROM public.replenishment_policy_versions
          WHERE id = target_version;
          IF target_status IS DISTINCT FROM 'DRAFT' THEN
            RAISE EXCEPTION 'activated replenishment policy versions are immutable';
          END IF;
          RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_policy_rules_draft_only
        BEFORE INSERT OR UPDATE OR DELETE ON replenishment_policy_rules
        FOR EACH ROW EXECUTE FUNCTION prevent_activated_policy_rule_mutation()
        """
    )


def downgrade() -> None:
    op.execute("DROP TRIGGER IF EXISTS trg_policy_rules_draft_only ON replenishment_policy_rules")
    op.execute("DROP FUNCTION IF EXISTS prevent_activated_policy_rule_mutation()")
    op.drop_index("uq_replenishment_tasks_active_store_sku", table_name="replenishment_tasks")
    op.drop_index("ix_canonical_tasks_store_status", table_name="replenishment_tasks")
    op.drop_table("replenishment_tasks")
    op.execute("DROP INDEX IF EXISTS uq_policy_rules_scope_priority")
    op.drop_index("ix_policy_rules_resolution", table_name="replenishment_policy_rules")
    op.drop_table("replenishment_policy_rules")
    op.drop_index("uq_policy_versions_one_active", table_name="replenishment_policy_versions")
    op.drop_table("replenishment_policy_versions")
    op.drop_table("replenishment_policies")

    op.execute(
        "ALTER INDEX uq_legacy_replenishment_tasks_active_store_sku "
        "RENAME TO uq_replenishment_tasks_active_store_sku"
    )
    op.execute(
        "ALTER INDEX ix_legacy_replenishment_tasks_tenant_store_status "
        "RENAME TO ix_replenishment_tasks_tenant_store_status"
    )
    op.execute(
        "ALTER INDEX ix_legacy_replenishment_tasks_unreviewed_cutover "
        "RENAME TO ix_replenishment_tasks_unreviewed_cutover"
    )
    op.rename_table("legacy_replenishment_tasks", "replenishment_tasks")
    op.rename_table("legacy_replenishment_rules", "replenishment_policies")
