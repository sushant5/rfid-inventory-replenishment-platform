from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

from abacus.db import (
    TenantSession,
    engine,
    get_db,
    pin_session_to_store_scope,
    pin_session_to_tenant,
)
from abacus.main import create_app

pytestmark = pytest.mark.integration


def test_canonical_api_runs_through_restricted_rls_role(postgres_engine: object) -> None:
    """Catch grants/session bugs hidden by the owner-backed integration fixture."""

    del postgres_engine  # The fixture applies the complete migration chain first.
    app_database_url = os.environ.get("TEST_APP_DATABASE_URL")
    if not app_database_url:
        pytest.skip("TEST_APP_DATABASE_URL is required for the runtime-role smoke test")

    app_engine = create_engine(app_database_url, pool_pre_ping=True)
    app_sessions = sessionmaker(
        bind=app_engine,
        class_=TenantSession,
        expire_on_commit=False,
    )
    tenant_code = f"runtime-{uuid.uuid4().hex[:12]}"
    second_tenant_code = f"runtime-{uuid.uuid4().hex[:12]}"

    def override_get_db() -> Generator[Session]:
        with app_sessions() as session:
            yield session

    try:
        with app_engine.connect() as connection:
            role = connection.execute(
                text(
                    "SELECT current_user, rolsuper, rolbypassrls "
                    "FROM pg_catalog.pg_roles WHERE rolname = current_user"
                )
            ).one()
            assert role.current_user == "abacus_app"
            assert role.rolsuper is False
            assert role.rolbypassrls is False

        app = create_app()
        app.dependency_overrides[get_db] = override_get_db
        with TestClient(app) as client:
            ready = client.get("/health/ready")
            assert ready.status_code == 200, ready.text
            response = client.post(
                "/v1/tenants",
                headers={"X-Platform-Key": os.environ["PLATFORM_API_KEY"]},
                json={"code": tenant_code, "name": "Runtime Role Tenant"},
            )
            second_response = client.post(
                "/v1/tenants",
                headers={"X-Platform-Key": os.environ["PLATFORM_API_KEY"]},
                json={"code": second_tenant_code, "name": "Second Runtime Role Tenant"},
            )
        assert response.status_code == 201, response.text
        assert second_response.status_code == 201, second_response.text
        tenant_id = uuid.UUID(response.json()["id"])
        second_tenant_id = uuid.UUID(second_response.json()["id"])

        # Missing or wrong tenant context fails closed at PostgreSQL, independently
        # of application query predicates.
        with app_sessions() as unpinned:
            assert unpinned.scalar(text("SELECT count(*) FROM tenants")) == 0
        with app_sessions() as correct:
            pin_session_to_tenant(correct, tenant_id)
            # No tenant predicate: PostgreSQL RLS itself hides the second tenant.
            assert correct.scalar(text("SELECT count(*) FROM tenants")) == 1
        with app_sessions() as second:
            pin_session_to_tenant(second, second_tenant_id)
            assert second.scalar(text("SELECT count(*) FROM tenants")) == 1
        with app_sessions() as wrong:
            pin_session_to_tenant(wrong, uuid.uuid4())
            assert wrong.scalar(text("SELECT count(*) FROM tenants")) == 0
    finally:
        app_engine.dispose()


def test_runtime_engine_applies_database_timeouts(postgres_engine: object) -> None:
    del postgres_engine
    with engine.connect() as connection:
        configured = connection.execute(
            text(
                "SELECT current_setting('statement_timeout'), "
                "current_setting('lock_timeout'), "
                "current_setting('idle_in_transaction_session_timeout')"
            )
        ).one()

    assert configured == ("30s", "5s", "1min")


def test_runtime_role_enforces_store_scope_without_query_predicates(
    postgres_engine: Engine,
) -> None:
    app_database_url = os.environ.get("TEST_APP_DATABASE_URL")
    if not app_database_url:
        pytest.skip("TEST_APP_DATABASE_URL is required for the runtime-role smoke test")
    owner_engine = postgres_engine
    tenant_id = uuid.uuid4()
    store_one = uuid.uuid4()
    store_two = uuid.uuid4()
    suffix = uuid.uuid4().hex[:12]
    with owner_engine.connect() as owner:
        owner.execute(
            text(
                "INSERT INTO tenants (id, code, name, status) "
                "VALUES (:id, :code, 'Store Scope Tenant', 'ACTIVE')"
            ),
            {"id": tenant_id, "code": f"store-scope-{suffix}"},
        )
        owner.execute(
            text(
                "INSERT INTO stores "
                "(id, tenant_id, code, name, timezone, status, configuration) VALUES "
                "(:one, :tenant, 'one', 'Store One', 'UTC', 'ACTIVE', '{}'::jsonb), "
                "(:two, :tenant, 'two', 'Store Two', 'UTC', 'ACTIVE', '{}'::jsonb)"
            ),
            {"one": store_one, "two": store_two, "tenant": tenant_id},
        )
        owner.commit()

    app_engine = create_engine(app_database_url, pool_pre_ping=True)
    sessions = sessionmaker(bind=app_engine, class_=TenantSession, expire_on_commit=False)
    try:
        with sessions() as missing_scope:
            pin_session_to_tenant(missing_scope, tenant_id)
            assert missing_scope.scalar(text("SELECT count(*) FROM stores")) == 0
        with sessions() as store_scoped:
            pin_session_to_tenant(store_scoped, tenant_id)
            pin_session_to_store_scope(store_scoped, [store_one])
            assert store_scoped.scalars(text("SELECT id FROM stores ORDER BY id")).all() == [
                store_one
            ]
            with pytest.raises(DBAPIError):
                store_scoped.execute(
                    text(
                        "INSERT INTO zones (id, tenant_id, store_id, code, name, kind) "
                        "VALUES (:id, :tenant, :store, 'blocked', 'Blocked', 'SALES_FLOOR')"
                    ),
                    {"id": uuid.uuid4(), "tenant": tenant_id, "store": store_two},
                )
            store_scoped.rollback()
        with sessions() as tenant_wide_session:
            pin_session_to_tenant(tenant_wide_session, tenant_id)
            pin_session_to_store_scope(tenant_wide_session, tenant_wide=True)
            assert tenant_wide_session.scalar(text("SELECT count(*) FROM stores")) == 2
    finally:
        app_engine.dispose()
        with owner_engine.connect() as owner:
            owner.execute(text("DELETE FROM tenants WHERE id = :id"), {"id": tenant_id})
            owner.commit()
