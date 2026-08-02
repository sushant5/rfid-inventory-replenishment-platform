"""bound RFID move confirmation window

Revision ID: 902164e9262d
Revises: 5adcde8aa247
Create Date: 2026-08-01 00:15:39.570537
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "902164e9262d"
down_revision: str | None = "5adcde8aa247"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "inventory_item_states",
        sa.Column("candidate_started_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "UPDATE inventory_item_states SET candidate_started_at = last_observed_at "
        "WHERE candidate_store_id IS NOT NULL"
    )
    op.create_check_constraint(
        "ck_inventory_item_states_candidate_time",
        "inventory_item_states",
        "(candidate_store_id IS NULL) = (candidate_started_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_inventory_item_states_candidate_time",
        "inventory_item_states",
        type_="check",
    )
    op.drop_column("inventory_item_states", "candidate_started_at")
