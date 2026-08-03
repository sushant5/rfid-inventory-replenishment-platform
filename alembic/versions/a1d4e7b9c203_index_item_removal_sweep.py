"""Index the timed-removal sweep.

Revision ID: a1d4e7b9c203
Revises: f0c1d2e3a4b5
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a1d4e7b9c203"
down_revision: str | None = "f0c1d2e3a4b5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX_NAME = "ix_current_item_state_removal_sweep"


def upgrade() -> None:
    op.create_index(
        INDEX_NAME,
        "current_item_state",
        ["tenant_id", "last_observed_at"],
        postgresql_where=sa.text("zone_id IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="current_item_state")
