"""add durable RFID acceptance sequence

Revision ID: b6e3f19a2d44
Revises: a2c7e91f4b6d
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b6e3f19a2d44"
down_revision: str | None = "a2c7e91f4b6d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "rfid_observations",
        sa.Column("acceptance_sequence", sa.BigInteger(), nullable=True),
    )
    # Preserve the deterministic order used by the previous release for historical
    # rows, then let PostgreSQL allocate every new value. Sequence gaps are expected
    # after transaction rollbacks and have no business meaning.
    op.execute(
        """
        WITH ordered AS (
            SELECT id,
                   row_number() OVER (ORDER BY ingested_at, event_id, id) AS sequence_value
            FROM rfid_observations
        )
        UPDATE rfid_observations AS observation
        SET acceptance_sequence = ordered.sequence_value
        FROM ordered
        WHERE observation.id = ordered.id
        """
    )
    # CACHE 1 matters: larger per-connection caches can hand a higher value to a
    # request that reaches nextval first on another pooled connection.
    op.execute("CREATE SEQUENCE rfid_observation_acceptance_seq AS BIGINT CACHE 1")
    op.execute(
        """
        SELECT setval(
            'rfid_observation_acceptance_seq',
            COALESCE((SELECT max(acceptance_sequence) FROM rfid_observations), 1),
            EXISTS (SELECT 1 FROM rfid_observations)
        )
        """
    )
    op.execute(
        "ALTER SEQUENCE rfid_observation_acceptance_seq "
        "OWNED BY rfid_observations.acceptance_sequence"
    )
    op.alter_column(
        "rfid_observations",
        "acceptance_sequence",
        existing_type=sa.BigInteger(),
        server_default=sa.text("nextval('rfid_observation_acceptance_seq'::regclass)"),
        nullable=False,
    )
    op.create_unique_constraint(
        "uq_rfid_observations_acceptance_sequence",
        "rfid_observations",
        ["acceptance_sequence"],
    )
    op.drop_index("ix_rfid_observations_epc_time", table_name="rfid_observations")
    op.create_index(
        "ix_rfid_observations_epc_time_acceptance",
        "rfid_observations",
        ["tenant_id", "epc", "observed_at", "acceptance_sequence"],
        unique=False,
    )
    op.create_index(
        "ix_rfid_observations_tenant_acceptance",
        "rfid_observations",
        ["tenant_id", "acceptance_sequence"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_rfid_observations_tenant_acceptance",
        table_name="rfid_observations",
    )
    op.drop_index(
        "ix_rfid_observations_epc_time_acceptance",
        table_name="rfid_observations",
    )
    op.create_index(
        "ix_rfid_observations_epc_time",
        "rfid_observations",
        ["tenant_id", "epc", "observed_at"],
        unique=False,
    )
    op.drop_constraint(
        "uq_rfid_observations_acceptance_sequence",
        "rfid_observations",
        type_="unique",
    )
    op.alter_column(
        "rfid_observations",
        "acceptance_sequence",
        existing_type=sa.BigInteger(),
        server_default=None,
    )
    op.execute("ALTER SEQUENCE rfid_observation_acceptance_seq OWNED BY NONE")
    op.drop_column("rfid_observations", "acceptance_sequence")
    op.execute("DROP SEQUENCE rfid_observation_acceptance_seq")
