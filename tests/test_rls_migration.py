import importlib.util
import os
import uuid
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import DBAPIError

from abacus.models import Base


def _load_rls_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "a6f0c4d1e537_enforce_tenant_rls.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_tenant_rls_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load tenant RLS migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_retirement_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "c9e8d4f2a715_retire_compatibility_schema.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_compatibility_retirement_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load compatibility retirement migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_catalog_source_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "e2f6a1b3c904_add_catalog_import_sources.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_catalog_source_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load catalog source migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_business_event_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "f8c1d2e3a4b6_authoritative_business_events.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_business_event_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load business-event migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_replenishment_evidence_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "b8e4c1a7d920_verify_replenishment_moves.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_replenishment_evidence_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load replenishment-evidence migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_auth_session_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "e9f2a4c6b831_add_rotating_auth_sessions.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_auth_session_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load auth-session migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def _load_store_scope_migration() -> ModuleType:
    migration_path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "a1c5e7f9b042_enforce_database_store_scope.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_store_scope_migration",
        migration_path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load store-scope migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_rls_table_inventory_matches_all_tenant_owned_models() -> None:
    migration = _load_rls_migration()
    retirement = _load_retirement_migration()
    catalog_source = _load_catalog_source_migration()
    business_events = _load_business_event_migration()
    replenishment_evidence = _load_replenishment_evidence_migration()
    auth_sessions = _load_auth_session_migration()
    modeled_tenant_tables = {
        table.name for table in Base.metadata.tables.values() if "tenant_id" in table.columns
    }
    modeled_tenant_tables.add("tenants")

    historical_tables = set(migration.TENANT_OWNED_TABLES)
    retired_tables = set(retirement.RETIRED_TABLES)
    added_tables = (
        set(catalog_source.ADDED_TENANT_TABLES)
        | set(business_events.ADDED_TENANT_TABLES)
        | set(replenishment_evidence.ADDED_TENANT_TABLES)
        | set(auth_sessions.ADDED_TENANT_TABLES)
    )
    assert historical_tables | added_tables == modeled_tenant_tables | retired_tables
    assert historical_tables.isdisjoint(added_tables)
    assert modeled_tenant_tables.isdisjoint(retired_tables)
    assert "NULLIF" in migration.TENANT_CONTEXT_SQL
    assert "current_setting('app.tenant_id', true)" in migration.TENANT_CONTEXT_SQL


def test_store_scope_policy_inventory_matches_store_owned_models() -> None:
    migration = _load_store_scope_migration()
    identity_scope_tables = {"user_access_grants", "user_store_assignments"}
    modeled_direct_tables = {
        table.name
        for table in Base.metadata.tables.values()
        if "tenant_id" in table.columns and "store_id" in table.columns
    } - identity_scope_tables
    modeled_direct_tables.add("stores")

    assert set(migration.DIRECT_STORE_TABLES) == modeled_direct_tables
    assert set(migration.INDIRECT_STORE_TABLES) == {
        "devices",
        "replenishment_task_evidence",
    }


@pytest.mark.integration
def test_postgres_rls_and_security_definer_boundaries(postgres_engine: Engine) -> None:
    retirement = _load_retirement_migration()
    modeled_tenant_tables = {
        table.name for table in Base.metadata.tables.values() if "tenant_id" in table.columns
    }
    modeled_tenant_tables.add("tenants")
    role_name = f"abacus_rls_test_{uuid.uuid4().hex}"
    quoted_role = postgres_engine.dialect.identifier_preparer.quote(role_name)
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()
    store_a = uuid.uuid4()
    store_b = uuid.uuid4()
    transition_a = uuid.uuid4()
    code_a = f"rls-{uuid.uuid4().hex[:12]}"
    code_b = f"rls-{uuid.uuid4().hex[:12]}"

    with postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
        rls_relations = {
            row.relname: (row.relrowsecurity, row.relforcerowsecurity)
            for row in admin.execute(
                text(
                    "SELECT relname, relrowsecurity, relforcerowsecurity "
                    "FROM pg_catalog.pg_class "
                    "WHERE relnamespace = 'public'::regnamespace AND relkind = 'r'"
                )
            )
        }
        assert modeled_tenant_tables <= set(rls_relations)
        assert all(rls_relations[table] == (True, True) for table in modeled_tenant_tables)
        retired_relations = {
            row.relname: (row.relrowsecurity, row.relforcerowsecurity)
            for row in admin.execute(
                text(
                    "SELECT relname, relrowsecurity, relforcerowsecurity "
                    "FROM pg_catalog.pg_class "
                    "WHERE relnamespace = 'retired_compatibility'::regnamespace "
                    "AND relkind = 'r'"
                )
            )
        }
        assert set(retirement.RETIRED_TABLES) == set(retired_relations)
        assert all(value == (True, True) for value in retired_relations.values())
        assert admin.scalar(
            text("SELECT to_regclass('public.rfid_observation_acceptance_seq') IS NOT NULL")
        )
        assert admin.scalar(text("SELECT to_regprocedure('public.app_cutover_ready()') IS NULL"))
        assert admin.scalar(
            text(
                "SELECT to_regprocedure("
                "'public.abacus_resolve_catalog_import_tenant(uuid)') IS NULL"
            )
        )
        assert not admin.scalar(
            text("SELECT has_schema_privilege('public', 'retired_compatibility', 'USAGE')")
        )
        application_role = os.environ.get("APPLICATION_DATABASE_ROLE", "abacus_app")
        if admin.scalar(
            text("SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :role)"),
            {"role": application_role},
        ):
            assert not admin.scalar(
                text("SELECT has_schema_privilege(:role, 'retired_compatibility', 'USAGE')"),
                {"role": application_role},
            )
            for retired_table in retirement.RETIRED_TABLES:
                for privilege in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                    assert not admin.scalar(
                        text("SELECT has_table_privilege(:role, :relation, :privilege)"),
                        {
                            "role": application_role,
                            "relation": f"retired_compatibility.{retired_table}",
                            "privilege": privilege,
                        },
                    )
        assert not admin.scalar(
            text(
                "SELECT has_function_privilege("
                "'public', 'public.abacus_resolve_login_tenant(text)', 'EXECUTE')"
            )
        )
        assert not admin.scalar(
            text(
                "SELECT has_function_privilege('public', 'public.app_active_tenants()', 'EXECUTE')"
            )
        )
        assert not admin.scalar(
            text(
                "SELECT has_function_privilege("
                "'public', 'public.app_pending_inventory_outbox_tenants()', 'EXECUTE')"
            )
        )
        can_create_role = bool(
            admin.scalar(
                text(
                    "SELECT rolsuper OR rolcreaterole "
                    "FROM pg_catalog.pg_roles WHERE rolname = current_user"
                )
            )
        )
        if not can_create_role:
            pytest.skip("PostgreSQL test owner cannot create a non-superuser RLS role")
        admin.exec_driver_sql(
            f"CREATE ROLE {quoted_role} NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS"
        )
        admin.exec_driver_sql(f"GRANT USAGE ON SCHEMA public TO {quoted_role}")
        admin.exec_driver_sql(
            f"GRANT SELECT, INSERT ON TABLE public.tenants, public.stores TO {quoted_role}"
        )
        admin.exec_driver_sql(
            f"GRANT EXECUTE ON FUNCTION public.abacus_resolve_login_tenant(text) TO {quoted_role}"
        )
        admin.exec_driver_sql(
            f"GRANT EXECUTE ON FUNCTION public.app_active_tenants() TO {quoted_role}"
        )
        admin.exec_driver_sql(
            "GRANT EXECUTE ON FUNCTION public.app_pending_inventory_outbox_tenants() "
            f"TO {quoted_role}"
        )
        admin.execute(
            text(
                "INSERT INTO public.tenants (id, code, name, status) "
                "VALUES (:tenant_a, :code_a, 'RLS Tenant A', 'ACTIVE'), "
                "(:tenant_b, :code_b, 'RLS Tenant B', 'ACTIVE')"
            ),
            {
                "tenant_a": tenant_a,
                "tenant_b": tenant_b,
                "code_a": code_a,
                "code_b": code_b,
            },
        )
        admin.execute(
            text(
                "INSERT INTO public.stores "
                "(id, tenant_id, code, name, timezone, status, configuration) VALUES "
                "(:store_a, :tenant_a, 'store-a', 'Store A', 'UTC', 'ACTIVE', '{}'::jsonb), "
                "(:store_b, :tenant_b, 'store-b', 'Store B', 'UTC', 'ACTIVE', '{}'::jsonb)"
            ),
            {
                "store_a": store_a,
                "store_b": store_b,
                "tenant_a": tenant_a,
                "tenant_b": tenant_b,
            },
        )
        admin.execute(
            text(
                "INSERT INTO public.inventory_transition_outbox "
                "(transition_id, tenant_id, epc, state_version, deltas, publish_attempts) "
                "VALUES (:transition_id, :tenant_id, 'RLS-EPC-A', 1, '[]'::jsonb, 0)"
            ),
            {
                "transition_id": transition_a,
                "tenant_id": tenant_a,
            },
        )

    role_connection = postgres_engine.connect()
    try:
        role_connection.exec_driver_sql(f"SET ROLE {quoted_role}")
        role_connection.commit()
        role_properties = role_connection.execute(
            text(
                "SELECT rolsuper, rolbypassrls FROM pg_catalog.pg_roles "
                "WHERE rolname = current_user"
            )
        ).one()
        assert role_properties == (False, False)
        role_connection.commit()

        assert role_connection.scalar(text("SELECT count(*) FROM public.tenants")) == 0
        role_connection.commit()

        role_connection.execute(
            text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
            {"tenant_id": str(tenant_a)},
        )
        visible_tenants = role_connection.scalars(
            text("SELECT id FROM public.tenants ORDER BY id")
        ).all()
        visible_stores = role_connection.scalars(
            text("SELECT id FROM public.stores ORDER BY id")
        ).all()
        assert visible_tenants == [tenant_a]
        assert visible_stores == [store_a]
        role_connection.commit()

        # SET LOCAL semantics prevent a pooled connection from retaining tenant A.
        assert role_connection.scalar(text("SELECT count(*) FROM public.tenants")) == 0
        role_connection.commit()

        resolved_tenant = role_connection.scalar(
            text("SELECT public.abacus_resolve_login_tenant(:tenant_code)"),
            {"tenant_code": code_a.upper()},
        )
        active_tenants = set(
            role_connection.scalars(text("SELECT tenant_id FROM public.app_active_tenants()")).all()
        )
        pending_outbox_tenants = set(
            role_connection.scalars(
                text("SELECT tenant_id FROM public.app_pending_inventory_outbox_tenants()")
            ).all()
        )
        assert resolved_tenant == tenant_a
        assert {tenant_a, tenant_b} <= active_tenants
        assert tenant_a in pending_outbox_tenants
        role_connection.commit()

        with pytest.raises(DBAPIError):
            role_connection.execute(
                text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
                {"tenant_id": str(tenant_a)},
            )
            role_connection.execute(
                text(
                    "INSERT INTO public.stores "
                    "(id, tenant_id, code, name, timezone, status, configuration) VALUES "
                    "(:id, :tenant_id, 'cross-tenant', 'Cross Tenant', "
                    "'UTC', 'ACTIVE', '{}'::jsonb)"
                ),
                {"id": uuid.uuid4(), "tenant_id": tenant_b},
            )
        role_connection.rollback()
    finally:
        if role_connection.in_transaction():
            role_connection.rollback()
        role_connection.exec_driver_sql("RESET ROLE")
        role_connection.commit()
        role_connection.close()
        with postgres_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as admin:
            admin.execute(
                text("DELETE FROM public.tenants WHERE id IN (:tenant_a, :tenant_b)"),
                {"tenant_a": tenant_a, "tenant_b": tenant_b},
            )
            admin.exec_driver_sql(f"DROP OWNED BY {quoted_role}")
            admin.exec_driver_sql(f"DROP ROLE {quoted_role}")
