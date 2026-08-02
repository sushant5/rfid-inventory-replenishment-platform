"""link RFID moves to replenishment reservations

Revision ID: a2c7e91f4b6d
Revises: f4d8b2a61c03
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a2c7e91f4b6d"
down_revision: str | None = "f4d8b2a61c03"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "replenishment_tasks",
        sa.Column(
            "reconciled_before_tracking_quantity",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
    )
    op.add_column(
        "replenishment_tasks",
        sa.Column(
            "reservation_cutover_reviewed",
            sa.Boolean(),
            server_default=sa.false(),
            nullable=False,
        ),
    )
    # The old release had no durable movement-to-task attribution, so the database
    # cannot know whether a legacy moved unit is already visible in RFID. Preserve the
    # conservative zero baseline and force an explicit operator decision for every
    # task that recorded movement before tracking existed. Readiness remains blocked
    # until each row is reconciled.
    op.execute(
        "UPDATE replenishment_tasks SET reservation_cutover_reviewed = true "
        "WHERE moved_quantity = 0"
    )
    op.add_column(
        "replenishment_tasks",
        sa.Column("reservation_cutover_reviewed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "replenishment_tasks",
        sa.Column("reservation_cutover_reviewed_by", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "replenishment_tasks",
        sa.Column("reservation_cutover_note", sa.String(length=1000), nullable=True),
    )
    op.create_check_constraint(
        "ck_replenishment_tasks_reconciled_baseline",
        "replenishment_tasks",
        "reconciled_before_tracking_quantity >= 0 "
        "AND reconciled_before_tracking_quantity <= moved_quantity",
    )
    op.create_check_constraint(
        "ck_replenishment_tasks_cutover_audit",
        "replenishment_tasks",
        "(reservation_cutover_reviewed_at IS NULL "
        "AND reservation_cutover_reviewed_by IS NULL "
        "AND reservation_cutover_note IS NULL) OR "
        "(reservation_cutover_reviewed = true "
        "AND reservation_cutover_reviewed_at IS NOT NULL "
        "AND reservation_cutover_reviewed_by IS NOT NULL "
        "AND reservation_cutover_note IS NOT NULL)",
    )
    op.create_index(
        "ix_replenishment_tasks_unreviewed_cutover",
        "replenishment_tasks",
        ["id"],
        unique=False,
        postgresql_where=sa.text("reservation_cutover_reviewed = false"),
    )
    op.add_column(
        "inventory_changes",
        sa.Column(
            "replenishment_task_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
    )
    op.create_foreign_key(
        "fk_inventory_changes_replenishment_task",
        "inventory_changes",
        "replenishment_tasks",
        ["replenishment_task_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_inventory_changes_replenishment_task",
        "inventory_changes",
        ["replenishment_task_id"],
        unique=False,
        postgresql_where=sa.text("replenishment_task_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(
        "ix_inventory_changes_replenishment_task",
        table_name="inventory_changes",
        postgresql_where=sa.text("replenishment_task_id IS NOT NULL"),
    )
    op.drop_constraint(
        "fk_inventory_changes_replenishment_task",
        "inventory_changes",
        type_="foreignkey",
    )
    op.drop_column("inventory_changes", "replenishment_task_id")
    op.drop_index(
        "ix_replenishment_tasks_unreviewed_cutover",
        table_name="replenishment_tasks",
        postgresql_where=sa.text("reservation_cutover_reviewed = false"),
    )
    op.drop_constraint(
        "ck_replenishment_tasks_cutover_audit",
        "replenishment_tasks",
        type_="check",
    )
    op.drop_constraint(
        "ck_replenishment_tasks_reconciled_baseline",
        "replenishment_tasks",
        type_="check",
    )
    op.drop_column("replenishment_tasks", "reservation_cutover_note")
    op.drop_column("replenishment_tasks", "reservation_cutover_reviewed_by")
    op.drop_column("replenishment_tasks", "reservation_cutover_reviewed_at")
    op.drop_column("replenishment_tasks", "reservation_cutover_reviewed")
    op.drop_column("replenishment_tasks", "reconciled_before_tracking_quantity")
