"""add rotating refresh-token sessions

Revision ID: e9f2a4c6b831
Revises: b8e4c1a7d920
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e9f2a4c6b831"
down_revision: str | None = "b8e4c1a7d920"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ADDED_TENANT_TABLES: tuple[str, ...] = ("auth_sessions",)
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

    op.create_table(
        "auth_sessions",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("family_id", sa.Uuid(), nullable=False),
        sa.Column("refresh_token_hash", sa.String(length=64), nullable=False),
        sa.Column("token_version", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("rotated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint("token_version >= 1", name="ck_auth_sessions_token_version"),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_auth_sessions_tenant",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "user_id"],
            ["users.tenant_id", "users.id"],
            name="fk_auth_sessions_tenant_user",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_auth_sessions"),
        sa.UniqueConstraint("refresh_token_hash", name="uq_auth_sessions_refresh_hash"),
    )
    op.create_index(
        "ix_auth_sessions_tenant_user",
        "auth_sessions",
        ["tenant_id", "user_id", "expires_at"],
    )
    op.create_index(
        "ix_auth_sessions_family",
        "auth_sessions",
        ["tenant_id", "family_id"],
    )
    op.create_index(
        "ix_auth_sessions_expired",
        "auth_sessions",
        ["expires_at"],
        postgresql_where=sa.text("revoked_at IS NULL"),
    )

    predicate = f"tenant_id = {TENANT_CONTEXT_SQL}"
    quoted_owner = _quoted_identifier(migration_owner)
    op.execute("ALTER TABLE public.auth_sessions ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.auth_sessions FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON public.auth_sessions "
        f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(
        "CREATE POLICY migration_owner_access ON public.auth_sessions "
        f"FOR ALL TO {quoted_owner} USING (true) WITH CHECK (true)"
    )
    if application_role_exists:
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE public.auth_sessions TO {quoted_role}"
        )


def downgrade() -> None:
    application_role = _application_role()
    if _role_exists(application_role):
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            "REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE public.auth_sessions "
            f"FROM {quoted_role}"
        )
    op.execute("DROP POLICY IF EXISTS migration_owner_access ON public.auth_sessions")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON public.auth_sessions")
    op.execute("ALTER TABLE public.auth_sessions NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.auth_sessions DISABLE ROW LEVEL SECURITY")
    op.drop_index(
        "ix_auth_sessions_expired",
        table_name="auth_sessions",
        postgresql_where=sa.text("revoked_at IS NULL"),
    )
    op.drop_index("ix_auth_sessions_family", table_name="auth_sessions")
    op.drop_index("ix_auth_sessions_tenant_user", table_name="auth_sessions")
    op.drop_table("auth_sessions")
