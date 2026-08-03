from __future__ import annotations

import os
import uuid
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from abacus.db import TenantSession, engine, get_db, pin_session_to_tenant
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
