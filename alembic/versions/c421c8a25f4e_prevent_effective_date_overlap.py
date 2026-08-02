"""prevent effective-dated mapping overlap

Revision ID: c421c8a25f4e
Revises: 902164e9262d
Create Date: 2026-08-01
"""

from collections.abc import Sequence

from alembic import op

revision: str = "c421c8a25f4e"
down_revision: str | None = "902164e9262d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # UUID/text equality in a GiST exclusion constraint is provided by btree_gist.
    op.execute("CREATE EXTENSION IF NOT EXISTS btree_gist")
    op.execute(
        "ALTER TABLE device_assignments "
        "ADD CONSTRAINT ex_device_assignments_no_overlap "
        "EXCLUDE USING gist ("
        "tenant_id WITH =, device_id WITH =, "
        "tstzrange(effective_from, effective_to, '[)') WITH &&)"
    )
    op.execute(
        "ALTER TABLE epc_bindings "
        "ADD CONSTRAINT ex_epc_bindings_no_overlap "
        "EXCLUDE USING gist ("
        "tenant_id WITH =, epc WITH =, "
        "tstzrange(effective_from, effective_to, '[)') WITH &&)"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE epc_bindings DROP CONSTRAINT IF EXISTS ex_epc_bindings_no_overlap")
    op.execute(
        "ALTER TABLE device_assignments DROP CONSTRAINT IF EXISTS ex_device_assignments_no_overlap"
    )
