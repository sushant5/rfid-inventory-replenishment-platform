"""add durable catalog import sources

Revision ID: e2f6a1b3c904
Revises: c9e8d4f2a715
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e2f6a1b3c904"
down_revision: str | None = "c9e8d4f2a715"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ADDED_TENANT_TABLES: tuple[str, ...] = ("catalog_import_sources",)
ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.tenant_id', true), '')::uuid"
CATALOG_IMPORT_RESOLVER_SIGNATURE = "public.abacus_resolve_catalog_import_tenant(uuid)"


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


def _create_catalog_import_resolver(application_role: str, *, role_exists: bool) -> None:
    """Restore the pre-JWT lookup boundary when this migration is downgraded."""

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.abacus_resolve_catalog_import_tenant(p_import_id uuid)
        RETURNS uuid
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT catalog_import.tenant_id
          FROM public.catalog_imports AS catalog_import
          JOIN public.tenants AS tenant ON tenant.id = catalog_import.tenant_id
          WHERE catalog_import.id = p_import_id
            AND tenant.status = 'ACTIVE'
          LIMIT 1
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {CATALOG_IMPORT_RESOLVER_SIGNATURE} FROM PUBLIC")
    if role_exists:
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            f"GRANT EXECUTE ON FUNCTION {CATALOG_IMPORT_RESOLVER_SIGNATURE} TO {quoted_role}"
        )


def upgrade() -> None:
    application_role = _application_role()
    migration_owner = str(op.get_bind().scalar(sa.text("SELECT current_user")))
    application_role_exists = _role_exists(application_role)
    if application_role_exists and application_role == migration_owner:
        raise RuntimeError("Migration owner and application database role must be different roles")

    # Catalog imports are now authorized and tenant-pinned by JWT. Remove the old
    # cross-tenant SECURITY DEFINER lookup that supported the platform-key route.
    op.execute(f"DROP FUNCTION IF EXISTS {CATALOG_IMPORT_RESOLVER_SIGNATURE}")

    op.create_unique_constraint(
        "uq_catalog_imports_tenant_id",
        "catalog_imports",
        ["tenant_id", "id"],
    )
    op.create_table(
        "catalog_import_sources",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("import_id", sa.Uuid(), nullable=False),
        sa.Column("content", sa.LargeBinary(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "octet_length(content) <= 10485760",
            name="ck_catalog_import_sources_max_size",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_catalog_import_sources_tenant_id_tenants",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "import_id"],
            ["catalog_imports.tenant_id", "catalog_imports.id"],
            name="fk_catalog_import_sources_tenant_import",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("import_id", name="pk_catalog_import_sources"),
    )

    predicate = f"tenant_id = {TENANT_CONTEXT_SQL}"
    quoted_owner = _quoted_identifier(migration_owner)
    op.execute("ALTER TABLE public.catalog_import_sources ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.catalog_import_sources FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON public.catalog_import_sources "
        f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(
        "CREATE POLICY migration_owner_access ON public.catalog_import_sources "
        f"FOR ALL TO {quoted_owner} USING (true) WITH CHECK (true)"
    )

    if application_role_exists:
        quoted_role = _quoted_identifier(application_role)
        # Source bytes are immutable after acceptance. Runtime processes may create
        # and read them, but cannot update or delete the audit input.
        op.execute(f"GRANT SELECT, INSERT ON TABLE public.catalog_import_sources TO {quoted_role}")


def downgrade() -> None:
    application_role = _application_role()
    application_role_exists = _role_exists(application_role)
    if application_role_exists:
        quoted_role = _quoted_identifier(application_role)
        op.execute(
            f"REVOKE SELECT, INSERT ON TABLE public.catalog_import_sources FROM {quoted_role}"
        )

    op.execute("DROP POLICY IF EXISTS migration_owner_access ON public.catalog_import_sources")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON public.catalog_import_sources")
    op.execute("ALTER TABLE public.catalog_import_sources NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.catalog_import_sources DISABLE ROW LEVEL SECURITY")
    op.drop_table("catalog_import_sources")
    op.drop_constraint(
        "uq_catalog_imports_tenant_id",
        "catalog_imports",
        type_="unique",
    )
    _create_catalog_import_resolver(
        application_role,
        role_exists=application_role_exists,
    )
