from __future__ import annotations

import os
from collections.abc import Generator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi import APIRouter
from fastapi.testclient import TestClient
from sqlalchemy import Engine, create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session, sessionmaker

# The application creates its module-level engine while it is imported. Select the
# explicitly supplied test database before test modules import any application code.
TEST_DATABASE_URL = os.environ.get("TEST_DATABASE_URL")
if TEST_DATABASE_URL:
    os.environ["DATABASE_URL"] = TEST_DATABASE_URL
os.environ.setdefault("APP_ENV", "test")
os.environ.setdefault("JWT_SECRET", "postgres-integration-test-jwt-secret-000000000000")
os.environ.setdefault("PLATFORM_API_KEY", "postgres-integration-platform-key")
os.environ.setdefault("LOGIN_THROTTLE_ENABLED", "false")


def _validated_test_database_url() -> str:
    if not TEST_DATABASE_URL:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL integration tests")
    database_name = make_url(TEST_DATABASE_URL).database or ""
    if "test" not in database_name.lower():
        raise pytest.UsageError(
            "Refusing to reset PostgreSQL schema: TEST_DATABASE_URL database name "
            f"{database_name!r} does not contain 'test'."
        )
    return TEST_DATABASE_URL


def _reset_public_schema(engine: Engine) -> None:
    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
        actual_database = str(connection.scalar(text("SELECT current_database()")) or "")
        if "test" not in actual_database.lower():
            raise pytest.UsageError(
                "Refusing to reset PostgreSQL schema: connected database name "
                f"{actual_database!r} does not contain 'test'."
            )
        connection.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        connection.execute(text("CREATE SCHEMA public"))


@pytest.fixture(scope="session")
def postgres_engine() -> Generator[Engine]:
    database_url = _validated_test_database_url()
    engine = create_engine(database_url, pool_pre_ping=True)
    _reset_public_schema(engine)

    project_root = Path(__file__).resolve().parents[1]
    alembic_config = Config(str(project_root / "alembic.ini"))
    alembic_config.set_main_option("script_location", str(project_root / "alembic"))
    alembic_config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    command.upgrade(alembic_config, "head")

    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def postgres_session_factory(
    postgres_engine: Engine,
) -> sessionmaker[Session]:
    return sessionmaker(bind=postgres_engine, class_=Session, expire_on_commit=False)


@pytest.fixture(scope="session")
def api_client(
    postgres_session_factory: sessionmaker[Session],
) -> Generator[TestClient]:
    from abacus.db import get_db
    from abacus.main import create_app

    def override_get_db() -> Generator[Session]:
        with postgres_session_factory() as session:
            yield session

    app = create_app()
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client


@pytest.fixture
def compatibility_api_client(
    postgres_session_factory: sessionmaker[Session],
) -> Generator[TestClient]:
    from abacus.api.routes.catalog import router as catalog_fixture_router
    from abacus.api.routes.onboarding import router as onboarding_fixture_router
    from abacus.api.routes.replenishment import router as replenishment_fixture_router
    from abacus.api.routes.rfid import device_router as rfid_device_fixture_router
    from abacus.api.routes.rfid import platform_router as rfid_platform_fixture_router
    from abacus.db import get_db
    from abacus.main import create_app

    def override_get_db() -> Generator[Session]:
        with postgres_session_factory() as session:
            yield session

    fixture_router = APIRouter()
    fixture_router.include_router(onboarding_fixture_router, include_in_schema=False)
    fixture_router.include_router(catalog_fixture_router, include_in_schema=False)
    fixture_router.include_router(rfid_device_fixture_router, include_in_schema=False)
    fixture_router.include_router(rfid_platform_fixture_router, include_in_schema=False)
    fixture_router.include_router(replenishment_fixture_router, include_in_schema=False)

    app = create_app()
    app.include_router(fixture_router)
    app.dependency_overrides[get_db] = override_get_db
    with TestClient(app) as client:
        yield client
    app.dependency_overrides.clear()
