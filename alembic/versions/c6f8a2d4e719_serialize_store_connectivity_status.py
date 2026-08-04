"""serialize store connectivity status receipts

Revision ID: c6f8a2d4e719
Revises: a9d4e6f2b713
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c6f8a2d4e719"
down_revision: str | None = "a9d4e6f2b713"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

STATUS_RECEIPT_BACKFILL_SQL = (
    "UPDATE store_connectivity "
    "SET status_received_at = GREATEST("
    "status_received_at, gateway_last_heartbeat, last_live_event_at, updated_at)"
)


def upgrade() -> None:
    op.add_column(
        "store_connectivity",
        sa.Column(
            "status_received_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.add_column(
        "store_connectivity",
        sa.Column("last_live_received_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(STATUS_RECEIPT_BACKFILL_SQL)


def downgrade() -> None:
    op.drop_column("store_connectivity", "last_live_received_at")
    op.drop_column("store_connectivity", "status_received_at")
