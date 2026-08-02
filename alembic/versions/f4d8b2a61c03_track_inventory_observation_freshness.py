"""track inventory observation freshness

Revision ID: f4d8b2a61c03
Revises: e7a9c4d2b611
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f4d8b2a61c03"
down_revision: str | None = "e7a9c4d2b611"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "inventory_balances",
        sa.Column("quantity_changed_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Reconstruct transition processing time from the immutable movement ledger.
    # Same-location reaffirmations may also have refreshed balance.updated_at in the
    # old release, so updated_at is only a defensive fallback for legacy rows whose
    # originating change is unavailable.
    op.execute(
        """
        UPDATE inventory_balances AS balance
        SET quantity_changed_at = evidence.last_changed_at
        FROM (
            SELECT
                affected.tenant_id,
                affected.store_id,
                affected.zone_id,
                affected.sku_id,
                MAX(affected.created_at) AS last_changed_at
            FROM (
                SELECT
                    tenant_id,
                    to_store_id AS store_id,
                    to_zone_id AS zone_id,
                    sku_id,
                    created_at
                FROM inventory_changes
                UNION ALL
                SELECT
                    tenant_id,
                    from_store_id AS store_id,
                    from_zone_id AS zone_id,
                    sku_id,
                    created_at
                FROM inventory_changes
                WHERE from_store_id IS NOT NULL
                  AND from_zone_id IS NOT NULL
            ) AS affected
            GROUP BY affected.tenant_id, affected.store_id, affected.zone_id, affected.sku_id
        ) AS evidence
        WHERE balance.tenant_id = evidence.tenant_id
          AND balance.store_id = evidence.store_id
          AND balance.zone_id = evidence.zone_id
          AND balance.sku_id = evidence.sku_id
        """
    )
    op.execute(
        "UPDATE inventory_balances "
        "SET quantity_changed_at = updated_at "
        "WHERE quantity_changed_at IS NULL"
    )
    op.alter_column("inventory_balances", "quantity_changed_at", nullable=False)
    op.add_column(
        "inventory_balances",
        sa.Column("last_relevant_observation_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Backfill only from auditable transitions known to have changed each aggregate.
    # Historical same-location reaffirmations cannot be reconstructed reliably, so
    # this is deliberately a conservative lower bound rather than updated_at.
    op.execute(
        """
        UPDATE inventory_balances AS balance
        SET last_relevant_observation_at = evidence.last_observed_at
        FROM (
            SELECT
                affected.tenant_id,
                affected.store_id,
                affected.zone_id,
                affected.sku_id,
                MAX(affected.observed_at) AS last_observed_at
            FROM (
                SELECT
                    tenant_id,
                    to_store_id AS store_id,
                    to_zone_id AS zone_id,
                    sku_id,
                    observed_at
                FROM inventory_changes
                UNION ALL
                SELECT
                    tenant_id,
                    from_store_id AS store_id,
                    from_zone_id AS zone_id,
                    sku_id,
                    observed_at
                FROM inventory_changes
                WHERE from_store_id IS NOT NULL
                  AND from_zone_id IS NOT NULL
            ) AS affected
            GROUP BY affected.tenant_id, affected.store_id, affected.zone_id, affected.sku_id
        ) AS evidence
        WHERE balance.tenant_id = evidence.tenant_id
          AND balance.store_id = evidence.store_id
          AND balance.zone_id = evidence.zone_id
          AND balance.sku_id = evidence.sku_id
        """
    )


def downgrade() -> None:
    op.drop_column("inventory_balances", "last_relevant_observation_at")
    op.drop_column("inventory_balances", "quantity_changed_at")
