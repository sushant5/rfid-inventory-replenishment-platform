"""make replenishment exception outcomes terminal

Revision ID: e7a9c4d2b611
Revises: c421c8a25f4e
Create Date: 2026-08-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e7a9c4d2b611"
down_revision: str | None = "c421c8a25f4e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # EXCEPTION used to be recoverable and therefore had no completion timestamp.
    # Backfill every legacy terminal row defensively before enforcing integrity;
    # VERIFIED/CANCELLED should already be populated by the old service path.
    op.execute(
        "UPDATE replenishment_tasks "
        "SET completed_at = COALESCE(completed_at, updated_at, created_at, CURRENT_TIMESTAMP) "
        "WHERE status IN ('VERIFIED', 'CANCELLED', 'EXCEPTION') "
        "AND completed_at IS NULL"
    )
    op.create_check_constraint(
        "ck_replenishment_tasks_terminal_completion",
        "replenishment_tasks",
        "(status IN ('VERIFIED', 'CANCELLED', 'EXCEPTION') "
        "AND completed_at IS NOT NULL) OR "
        "(status IN ('OPEN', 'CLAIMED', 'IN_PROGRESS', 'AWAITING_VERIFICATION') "
        "AND completed_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_replenishment_tasks_terminal_completion",
        "replenishment_tasks",
        type_="check",
    )
    # Restore the pre-migration representation in which EXCEPTION was recoverable
    # and therefore did not carry a completion timestamp.
    op.execute("UPDATE replenishment_tasks SET completed_at = NULL WHERE status = 'EXCEPTION'")
