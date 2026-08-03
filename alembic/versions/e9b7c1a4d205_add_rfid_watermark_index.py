"""add processed RFID watermark index

Revision ID: e9b7c1a4d205
Revises: d8c3a4e1f902
Create Date: 2026-08-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "e9b7c1a4d205"
down_revision: str | None = "d8c3a4e1f902"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

INDEX_NAME = "ix_rfid_events_processed_epc_watermark"


def upgrade() -> None:
    op.execute(
        f"""
        CREATE INDEX {INDEX_NAME}
        ON rfid_observation_events (tenant_id, epc, observed_at DESC)
        WHERE processing_status = 'PROCESSED'
        """
    )


def downgrade() -> None:
    op.drop_index(INDEX_NAME, table_name="rfid_observation_events")
