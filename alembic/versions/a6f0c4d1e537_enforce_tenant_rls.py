"""enforce tenant row-level security

Revision ID: a6f0c4d1e537
Revises: f5ea3c09b426
Create Date: 2026-08-02
"""

# Migration DDL interpolates only identifiers that are restricted by ROLE_NAME_PATTERN
# and quoted by SQLAlchemy's PostgreSQL dialect.
# ruff: noqa: S608

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a6f0c4d1e537"
down_revision: str | None = "f5ea3c09b426"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


# Keep this explicit: adding a tenant-owned table without deciding its RLS posture must
# be a visible migration change, not something inferred from today's catalog.
TENANT_OWNED_TABLES: tuple[str, ...] = (
    "tenants",
    "catalog_imports",
    "devices",
    "durable_jobs",
    "onboarding_batches",
    "organization_units",
    "product_styles",
    "replenishment_policy_imports",
    "users",
    "catalog_import_errors",
    "catalog_import_rows",
    "identity_audit_records",
    "skus",
    "stores",
    "epc_bindings",
    "legacy_replenishment_rules",
    "replenishment_runs",
    "user_access_grants",
    "zones",
    "device_assignments",
    "inventory_balances",
    "inventory_item_states",
    "legacy_replenishment_tasks",
    "rfid_observations",
    "inventory_changes",
    "replenishment_run_lines",
    "products",
    "product_variants",
    "user_roles",
    "user_store_assignments",
    "rfid_tags",
    "rfid_observation_batches",
    "rfid_observation_events",
    "rfid_observation_batch_events",
    "rfid_observation_outbox",
    "rfid_quarantine",
    "current_item_state",
    "inventory_transition_outbox",
    "inventory_projection",
    "applied_inventory_deltas",
    "store_connectivity",
    "replenishment_policies",
    "replenishment_policy_versions",
    "replenishment_policy_rules",
    "replenishment_tasks",
)

TENANT_CONTEXT_SQL = "NULLIF(pg_catalog.current_setting('app.tenant_id', true), '')::uuid"
ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
LOGIN_RESOLVER_SIGNATURE = "public.abacus_resolve_login_tenant(text)"
DEVICE_RESOLVER_SIGNATURE = "public.abacus_resolve_device_tenant(uuid)"
CATALOG_IMPORT_RESOLVER_SIGNATURE = "public.abacus_resolve_catalog_import_tenant(uuid)"
ACTIVE_TENANTS_SIGNATURE = "public.app_active_tenants()"
PENDING_OUTBOX_TENANTS_SIGNATURE = "public.app_pending_inventory_outbox_tenants()"
PENDING_RFID_OUTBOX_TENANTS_SIGNATURE = "public.app_pending_rfid_outbox_tenants()"


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


def _grant_runtime_access(role: str) -> None:
    quoted_role = _quoted_identifier(role)
    qualified_tables = ", ".join(f"public.{table}" for table in TENANT_OWNED_TABLES)
    op.execute(f"GRANT USAGE ON SCHEMA public TO {quoted_role}")
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {qualified_tables} TO {quoted_role}")
    op.execute(f"GRANT SELECT ON TABLE public.alembic_version TO {quoted_role}")
    op.execute(
        f"GRANT USAGE, SELECT ON SEQUENCE public.rfid_observation_acceptance_seq TO {quoted_role}"
    )
    op.execute(f"GRANT EXECUTE ON FUNCTION {LOGIN_RESOLVER_SIGNATURE} TO {quoted_role}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {DEVICE_RESOLVER_SIGNATURE} TO {quoted_role}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {CATALOG_IMPORT_RESOLVER_SIGNATURE} TO {quoted_role}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {ACTIVE_TENANTS_SIGNATURE} TO {quoted_role}")
    op.execute(f"GRANT EXECUTE ON FUNCTION {PENDING_OUTBOX_TENANTS_SIGNATURE} TO {quoted_role}")
    op.execute(
        f"GRANT EXECUTE ON FUNCTION {PENDING_RFID_OUTBOX_TENANTS_SIGNATURE} TO {quoted_role}"
    )


def _revoke_runtime_access(role: str) -> None:
    quoted_role = _quoted_identifier(role)
    qualified_tables = ", ".join(f"public.{table}" for table in TENANT_OWNED_TABLES)
    op.execute(
        f"REVOKE EXECUTE ON FUNCTION {PENDING_RFID_OUTBOX_TENANTS_SIGNATURE} FROM {quoted_role}"
    )
    op.execute(f"REVOKE EXECUTE ON FUNCTION {PENDING_OUTBOX_TENANTS_SIGNATURE} FROM {quoted_role}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {ACTIVE_TENANTS_SIGNATURE} FROM {quoted_role}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {LOGIN_RESOLVER_SIGNATURE} FROM {quoted_role}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {DEVICE_RESOLVER_SIGNATURE} FROM {quoted_role}")
    op.execute(f"REVOKE EXECUTE ON FUNCTION {CATALOG_IMPORT_RESOLVER_SIGNATURE} FROM {quoted_role}")
    op.execute(
        "REVOKE USAGE, SELECT ON SEQUENCE public.rfid_observation_acceptance_seq "
        f"FROM {quoted_role}"
    )
    op.execute(
        f"REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE {qualified_tables} FROM {quoted_role}"
    )
    op.execute(f"REVOKE SELECT ON TABLE public.alembic_version FROM {quoted_role}")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {quoted_role}")


def upgrade() -> None:
    bind = op.get_bind()
    application_role = _application_role()
    migration_owner = str(bind.scalar(sa.text("SELECT current_user")))
    application_role_exists = _role_exists(application_role)
    if application_role_exists and application_role == migration_owner:
        raise RuntimeError("Migration owner and application database role must be different roles")

    quoted_owner = _quoted_identifier(migration_owner)
    for table in TENANT_OWNED_TABLES:
        tenant_column = "id" if table == "tenants" else "tenant_id"
        predicate = f"{tenant_column} = {TENANT_CONTEXT_SQL}"
        op.execute(f"ALTER TABLE public.{table} ENABLE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE public.{table} FORCE ROW LEVEL SECURITY")
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON public.{table}")
        op.execute(
            f"CREATE POLICY tenant_isolation ON public.{table} "
            f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
        )
        # FORCE RLS also constrains the table owner. The trusted migration owner needs
        # an explicit policy for future migrations and owns the narrow SECURITY DEFINER
        # functions below. Runtime credentials must never use this role.
        op.execute(f"DROP POLICY IF EXISTS migration_owner_access ON public.{table}")
        op.execute(
            f"CREATE POLICY migration_owner_access ON public.{table} "
            f"FOR ALL TO {quoted_owner} USING (true) WITH CHECK (true)"
        )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.abacus_resolve_login_tenant(p_tenant_code text)
        RETURNS uuid
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT tenant.id
          FROM public.tenants AS tenant
          WHERE tenant.code = pg_catalog.lower(pg_catalog.btrim(p_tenant_code))
            AND tenant.status = 'ACTIVE'
          LIMIT 1
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {LOGIN_RESOLVER_SIGNATURE} FROM PUBLIC")

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.abacus_resolve_device_tenant(p_device_id uuid)
        RETURNS uuid
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT device.tenant_id
          FROM public.devices AS device
          JOIN public.tenants AS tenant ON tenant.id = device.tenant_id
          WHERE device.id = p_device_id
            AND device.status = 'ACTIVE'
            AND tenant.status = 'ACTIVE'
          LIMIT 1
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {DEVICE_RESOLVER_SIGNATURE} FROM PUBLIC")

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

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.app_active_tenants()
        RETURNS TABLE (tenant_id uuid)
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT tenant.id
          FROM public.tenants AS tenant
          WHERE tenant.status = 'ACTIVE'
          ORDER BY tenant.id
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {ACTIVE_TENANTS_SIGNATURE} FROM PUBLIC")

    op.execute(
        """
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
            AND tenant.status = 'ACTIVE'
          ORDER BY transition.tenant_id
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {PENDING_OUTBOX_TENANTS_SIGNATURE} FROM PUBLIC")

    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.app_pending_rfid_outbox_tenants()
        RETURNS TABLE (tenant_id uuid)
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT DISTINCT raw_event.tenant_id
          FROM public.rfid_observation_outbox AS raw_event
          JOIN public.tenants AS tenant ON tenant.id = raw_event.tenant_id
          WHERE raw_event.published_at IS NULL
            AND tenant.status = 'ACTIVE'
          ORDER BY raw_event.tenant_id
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {PENDING_RFID_OUTBOX_TENANTS_SIGNATURE} FROM PUBLIC")

    if application_role_exists:
        _grant_runtime_access(application_role)


def downgrade() -> None:
    application_role = _application_role()
    if _role_exists(application_role):
        _revoke_runtime_access(application_role)

    op.execute(f"DROP FUNCTION IF EXISTS {PENDING_RFID_OUTBOX_TENANTS_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {PENDING_OUTBOX_TENANTS_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {ACTIVE_TENANTS_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {LOGIN_RESOLVER_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {DEVICE_RESOLVER_SIGNATURE}")
    op.execute(f"DROP FUNCTION IF EXISTS {CATALOG_IMPORT_RESOLVER_SIGNATURE}")
    for table in reversed(TENANT_OWNED_TABLES):
        op.execute(f"DROP POLICY IF EXISTS migration_owner_access ON public.{table}")
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON public.{table}")
        op.execute(f"ALTER TABLE public.{table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE public.{table} DISABLE ROW LEVEL SECURITY")
