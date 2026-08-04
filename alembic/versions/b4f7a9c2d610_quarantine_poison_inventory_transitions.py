"""quarantine poison inventory transitions

Revision ID: b4f7a9c2d610
Revises: a1d4e7b9c203
"""

# The only interpolated fragment is selected from two module-owned SQL literals.
# ruff: noqa: S608

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b4f7a9c2d610"
down_revision: str | None = "a1d4e7b9c203"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

PENDING_OUTBOX_TENANTS_SIGNATURE = "public.app_pending_inventory_outbox_tenants()"


def _replace_pending_tenant_resolver(*, exclude_quarantined: bool) -> None:
    quarantine_predicate = "AND transition.quarantined_at IS NULL" if exclude_quarantined else ""
    op.execute(
        f"""
        CREATE OR REPLACE FUNCTION public.app_pending_inventory_outbox_tenants()
        RETURNS TABLE (tenant_id uuid)
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT DISTINCT transition.tenant_id
          FROM public.inventory_transition_outbox AS transition
          JOIN public.tenants AS tenant ON tenant.id = transition.tenant_id
          WHERE transition.published_at IS NULL
            {quarantine_predicate}
            AND tenant.status = 'ACTIVE'
          ORDER BY transition.tenant_id
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {PENDING_OUTBOX_TENANTS_SIGNATURE} FROM PUBLIC")


def upgrade() -> None:
    op.add_column(
        "inventory_transition_outbox",
        sa.Column("quarantined_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "inventory_transition_outbox",
        sa.Column("quarantine_reason", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "inventory_transition_outbox",
        sa.Column("reconciled_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "store_connectivity",
        sa.Column(
            "inventory_reconciliation_required_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_inventory_outbox_one_terminal_state",
        "inventory_transition_outbox",
        "NOT (published_at IS NOT NULL AND quarantined_at IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_inventory_outbox_quarantine_metadata",
        "inventory_transition_outbox",
        "(quarantined_at IS NULL AND quarantine_reason IS NULL "
        "AND reconciled_at IS NULL) OR "
        "(quarantined_at IS NOT NULL AND quarantine_reason IS NOT NULL)",
    )
    op.drop_index(
        "ix_inventory_outbox_unpublished",
        table_name="inventory_transition_outbox",
    )
    op.create_index(
        "ix_inventory_outbox_unpublished",
        "inventory_transition_outbox",
        ["tenant_id", "created_at"],
        postgresql_where=sa.text("published_at IS NULL AND quarantined_at IS NULL"),
    )
    op.create_index(
        "ix_inventory_outbox_unreconciled_quarantine",
        "inventory_transition_outbox",
        ["tenant_id", "quarantined_at"],
        postgresql_where=sa.text("quarantined_at IS NOT NULL AND reconciled_at IS NULL"),
    )
    _replace_pending_tenant_resolver(exclude_quarantined=True)


def downgrade() -> None:
    # Older workers only understand published/pending. Treat terminal quarantines as
    # consumed before removing the explicit quarantine state so a poison row cannot
    # be resurrected by a rollback deployment.
    op.execute(
        """
        DO $block$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM public.inventory_transition_outbox
            WHERE quarantined_at IS NOT NULL
              AND reconciled_at IS NULL
          ) THEN
            RAISE EXCEPTION
              'reconcile quarantined inventory transitions before downgrading';
          END IF;
        END
        $block$
        """
    )
    op.drop_constraint(
        "ck_inventory_outbox_one_terminal_state",
        "inventory_transition_outbox",
        type_="check",
    )
    op.execute(
        """
        UPDATE public.inventory_transition_outbox
        SET published_at = quarantined_at
        WHERE quarantined_at IS NOT NULL
          AND published_at IS NULL
        """
    )
    _replace_pending_tenant_resolver(exclude_quarantined=False)
    op.drop_index(
        "ix_inventory_outbox_unreconciled_quarantine",
        table_name="inventory_transition_outbox",
    )
    op.drop_index(
        "ix_inventory_outbox_unpublished",
        table_name="inventory_transition_outbox",
    )
    op.create_index(
        "ix_inventory_outbox_unpublished",
        "inventory_transition_outbox",
        ["created_at"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.drop_constraint(
        "ck_inventory_outbox_quarantine_metadata",
        "inventory_transition_outbox",
        type_="check",
    )
    op.drop_column("store_connectivity", "inventory_reconciliation_required_at")
    op.drop_column("inventory_transition_outbox", "reconciled_at")
    op.drop_column("inventory_transition_outbox", "quarantine_reason")
    op.drop_column("inventory_transition_outbox", "quarantined_at")
