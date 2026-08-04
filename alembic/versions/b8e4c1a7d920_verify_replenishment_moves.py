"""record RFID evidence for completed replenishment tasks

Revision ID: b8e4c1a7d920
Revises: d7a9b3e5f820
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b8e4c1a7d920"
down_revision: str | None = "d7a9b3e5f820"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ADDED_TENANT_TABLES: tuple[str, ...] = ("replenishment_task_evidence",)
ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.tenant_id', true), '')::uuid"


def _application_role() -> str:
    role = os.environ.get("APPLICATION_DATABASE_ROLE", "abacus_app").strip()
    if not ROLE_NAME_PATTERN.fullmatch(role):
        raise RuntimeError("APPLICATION_DATABASE_ROLE must be a simple PostgreSQL role name")
    return role


def _quoted_identifier(name: str) -> str:
    return op.get_bind().dialect.identifier_preparer.quote(name)


def _role_exists(role: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :role)"),
            {"role": role},
        )
    )


def upgrade() -> None:
    application_role = _application_role()
    migration_owner = str(op.get_bind().scalar(sa.text("SELECT current_user")))
    application_role_exists = _role_exists(application_role)
    if application_role_exists and application_role == migration_owner:
        raise RuntimeError("Migration owner and application database role must be different roles")

    op.add_column(
        "replenishment_tasks",
        sa.Column("verified_quantity", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "replenishment_tasks",
        sa.Column("verification_deadline", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_replenishment_tasks_verified_quantity",
        "replenishment_tasks",
        "verified_quantity >= 0 AND verified_quantity <= quantity",
    )
    op.create_unique_constraint(
        "uq_replenishment_tasks_tenant_id",
        "replenishment_tasks",
        ["tenant_id", "id"],
    )

    op.create_table(
        "replenishment_task_evidence",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("epc", sa.String(length=128), nullable=False),
        sa.Column("observed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_replenishment_task_evidence_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "task_id"],
            ["replenishment_tasks.tenant_id", "replenishment_tasks.id"],
            name="fk_replenishment_task_evidence_tenant_task",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "transition_id"],
            ["inventory_transition_outbox.tenant_id", "inventory_transition_outbox.transition_id"],
            name="fk_replenishment_task_evidence_tenant_transition",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_replenishment_task_evidence"),
        sa.UniqueConstraint(
            "tenant_id",
            "transition_id",
            name="uq_replenishment_task_evidence_transition",
        ),
    )
    op.create_index(
        "ix_replenishment_task_evidence_task_observed",
        "replenishment_task_evidence",
        ["tenant_id", "task_id", "observed_at"],
    )
    op.create_index(
        "ix_replenishment_tasks_verification_candidate",
        "replenishment_tasks",
        ["tenant_id", "store_id", "sku_id", "status", "started_at"],
        postgresql_where=sa.text("status IN ('IN_PROGRESS', 'COMPLETED')"),
    )

    predicate = f"tenant_id = {TENANT_CONTEXT_SQL}"
    quoted_owner = _quoted_identifier(migration_owner)
    op.execute("ALTER TABLE public.replenishment_task_evidence ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.replenishment_task_evidence FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON public.replenishment_task_evidence "
        f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(
        "CREATE POLICY migration_owner_access ON public.replenishment_task_evidence "
        f"FOR ALL TO {quoted_owner} USING (true) WITH CHECK (true)"
    )
    if application_role_exists:
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            f"GRANT SELECT, INSERT ON TABLE public.replenishment_task_evidence TO {quoted_role}"
        )


def downgrade() -> None:
    application_role = _application_role()
    if _role_exists(application_role):
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            f"REVOKE SELECT, INSERT ON TABLE public.replenishment_task_evidence FROM {quoted_role}"
        )
    op.execute("DROP POLICY IF EXISTS migration_owner_access ON public.replenishment_task_evidence")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON public.replenishment_task_evidence")
    op.execute("ALTER TABLE public.replenishment_task_evidence NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.replenishment_task_evidence DISABLE ROW LEVEL SECURITY")
    op.drop_index(
        "ix_replenishment_tasks_verification_candidate",
        table_name="replenishment_tasks",
        postgresql_where=sa.text("status IN ('IN_PROGRESS', 'COMPLETED')"),
    )
    op.drop_index(
        "ix_replenishment_task_evidence_task_observed",
        table_name="replenishment_task_evidence",
    )
    op.drop_table("replenishment_task_evidence")
    op.drop_constraint(
        "uq_replenishment_tasks_tenant_id",
        "replenishment_tasks",
        type_="unique",
    )
    op.drop_constraint(
        "ck_replenishment_tasks_verified_quantity",
        "replenishment_tasks",
        type_="check",
    )
    op.drop_column("replenishment_tasks", "verification_deadline")
    op.drop_column("replenishment_tasks", "verified_quantity")
