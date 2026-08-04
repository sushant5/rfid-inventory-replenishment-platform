import importlib.util
from datetime import UTC, datetime
from pathlib import Path
from types import ModuleType

import pytest
from sqlalchemy import Engine, text


def _load_migration() -> ModuleType:
    path = (
        Path(__file__).resolve().parents[1]
        / "alembic"
        / "versions"
        / "c6f8a2d4e719_serialize_store_connectivity_status.py"
    )
    specification = importlib.util.spec_from_file_location(
        "abacus_connectivity_receipt_migration",
        path,
    )
    if specification is None or specification.loader is None:
        raise RuntimeError("Unable to load connectivity receipt migration")
    migration = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(migration)
    return migration


@pytest.mark.integration
def test_connectivity_watermark_backfill_uses_newest_known_timestamp(
    postgres_engine: Engine,
) -> None:
    migration = _load_migration()
    oldest = datetime(2026, 8, 1, 10, tzinfo=UTC)
    heartbeat = datetime(2026, 8, 1, 11, tzinfo=UTC)
    live_event = datetime(2026, 8, 1, 12, tzinfo=UTC)
    updated = datetime(2026, 8, 1, 13, tzinfo=UTC)
    migration_default = datetime(2026, 8, 1, 14, tzinfo=UTC)

    with postgres_engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TEMPORARY TABLE store_connectivity ("
                "id integer PRIMARY KEY, "
                "status_received_at timestamptz NOT NULL, "
                "gateway_last_heartbeat timestamptz, "
                "last_live_event_at timestamptz, "
                "updated_at timestamptz NOT NULL) ON COMMIT DROP"
            )
        )
        connection.execute(
            text(
                "INSERT INTO store_connectivity "
                "(id, status_received_at, gateway_last_heartbeat, last_live_event_at, updated_at) "
                "VALUES "
                "(1, :oldest, :heartbeat, :live_event, :updated), "
                "(2, :migration_default, :heartbeat, :live_event, :updated)"
            ),
            {
                "oldest": oldest,
                "heartbeat": heartbeat,
                "live_event": live_event,
                "updated": updated,
                "migration_default": migration_default,
            },
        )
        connection.execute(text(migration.STATUS_RECEIPT_BACKFILL_SQL))
        watermarks = (
            connection.execute(
                text("SELECT status_received_at FROM store_connectivity ORDER BY id")
            )
            .scalars()
            .all()
        )

    assert watermarks == [updated, migration_default]
