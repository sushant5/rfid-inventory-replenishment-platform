from __future__ import annotations

import importlib.util
import io
from pathlib import Path
from types import ModuleType

import pytest
from alembic import command
from alembic.config import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    PROJECT_ROOT / "alembic" / "versions" / "c9e8d4f2a715_retire_compatibility_schema.py"
)


def _load_migration() -> ModuleType:
    specification = importlib.util.spec_from_file_location(
        "abacus_compatibility_retirement_test",
        MIGRATION_PATH,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load compatibility retirement migration")
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_retirement_upgrade_is_fail_closed_and_removes_runtime_acls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setattr(
        migration.op, "execute", lambda statement: statements.append(str(statement))
    )

    migration.upgrade()

    rendered = "\n".join(statements)
    assert "CREATE SCHEMA retired_compatibility" in statements
    assert "CREATE SCHEMA IF NOT EXISTS" not in rendered
    assert "IN SHARE ROW EXCLUSIVE MODE" in rendered
    assert "reservation_cutover_reviewed IS NOT TRUE" in rendered
    assert "status = 'RECEIVED'" in rendered
    assert "status = 'PROCESSING'" in rendered
    assert "kind IN ('RFID_OBSERVATION', 'REPLENISHMENT_RECALC')" in rendered
    assert "REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA retired_compatibility FROM %I" in rendered
    assert statements.index(
        "ALTER SEQUENCE public.rfid_observation_acceptance_seq OWNED BY NONE"
    ) < statements.index("ALTER TABLE public.rfid_observations SET SCHEMA retired_compatibility")


def test_retirement_downgrade_restores_runtime_acl_and_sequence_owner(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    migration = _load_migration()
    statements: list[str] = []
    monkeypatch.setattr(
        migration.op, "execute", lambda statement: statements.append(str(statement))
    )

    migration.downgrade()

    rendered = "\n".join(statements)
    assert (
        "ALTER SEQUENCE public.rfid_observation_acceptance_seq "
        "OWNED BY public.rfid_observations.acceptance_sequence"
    ) in statements
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" in rendered
    assert "GRANT EXECUTE ON FUNCTION public.app_cutover_ready() TO %I" in rendered


def test_retirement_offline_downgrade_sql_generation() -> None:
    output = io.StringIO()
    config = Config(str(PROJECT_ROOT / "alembic.ini"), output_buffer=output)
    config.set_main_option("script_location", str(PROJECT_ROOT / "alembic"))
    config.set_main_option(
        "sqlalchemy.url",
        "postgresql+psycopg://offline:offline@localhost/offline",
    )

    command.downgrade(
        config,
        "c9e8d4f2a715:b4f7a9c2d610",
        sql=True,
    )

    rendered = output.getvalue()
    assert "GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE" in rendered
    assert "GRANT EXECUTE ON FUNCTION public.app_cutover_ready() TO %I" in rendered
    assert "COMMIT;" in rendered
