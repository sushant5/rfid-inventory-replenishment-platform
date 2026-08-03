from __future__ import annotations

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from abacus.config import get_settings
from abacus.enums import ObservationStatus
from abacus.models.rfid import RfidObservation
from abacus.models.tenancy import Device
from abacus.schemas.rfid import RfidBatchInput, RfidObservationInput
from abacus.services.cutover import reconcile_reservation_cutover_task
from abacus.services.replenishment import _unreflected_terminal_moved_quantity
from abacus.services.rfid import ingest_batch, process_rfid_observation_job

pytestmark = pytest.mark.integration


def _alembic_config(project_root: Path) -> Config:
    config = Config(str(project_root / "alembic.ini"))
    config.set_main_option("script_location", str(project_root / "alembic"))
    return config


def test_populated_active_task_cutover_requires_review_before_baselining() -> None:
    test_database_url = os.environ.get("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL is required for PostgreSQL migration tests")

    base_url = make_url(test_database_url)
    database_name = f"abacus_migration_test_{uuid.uuid4().hex}"
    assert database_name.replace("_", "").isalnum()
    admin_url = base_url.set(database="postgres")
    migration_url = base_url.set(database=database_name)
    admin_engine = create_engine(admin_url)
    migration_engine = None
    project_root = Path(__file__).resolve().parents[1]
    original_database_url = os.environ.get("DATABASE_URL")
    original_migration_database_url = os.environ.get("MIGRATION_DATABASE_URL")

    try:
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.exec_driver_sql(f'CREATE DATABASE "{database_name}"')

        migration_url_text = migration_url.render_as_string(hide_password=False)
        os.environ["DATABASE_URL"] = migration_url_text
        # Docker Compose supplies MIGRATION_DATABASE_URL for the main test database.
        # Point both settings at this isolated migration database so `make test`
        # exercises the same database that this test creates.
        os.environ["MIGRATION_DATABASE_URL"] = migration_url_text
        get_settings.cache_clear()
        command.upgrade(_alembic_config(project_root), "c421c8a25f4e")
        migration_engine = create_engine(migration_url)

        tenant_id = uuid.uuid4()
        store_id = uuid.uuid4()
        style_id = uuid.uuid4()
        sku_id = uuid.uuid4()
        policy_id = uuid.uuid4()
        task_id = uuid.uuid4()
        zero_moved_task_id = uuid.uuid4()
        direct_insert_task_id = uuid.uuid4()
        floor_zone_id = uuid.uuid4()
        backroom_zone_id = uuid.uuid4()
        floor_device_id = uuid.uuid4()
        backroom_device_id = uuid.uuid4()
        catalog_import_id = uuid.uuid4()
        epc_binding_id = uuid.uuid4()
        historical_observation_ids = (uuid.uuid4(), uuid.uuid4())
        historical_epc = "3074257BF7194E4000001B01"
        concurrent_epc = "3074257BF7194E4000001B02"
        now = datetime.now(UTC)
        with migration_engine.begin() as connection:
            connection.execute(
                text(
                    "INSERT INTO tenants (id, code, name, status) "
                    "VALUES (:id, 'migration-cutover', 'Migration Cutover', 'ACTIVE')"
                ),
                {"id": tenant_id},
            )
            connection.execute(
                text(
                    "INSERT INTO stores "
                    "(id, tenant_id, code, name, timezone, status, configuration) "
                    "VALUES (:id, :tenant_id, 'store-1', 'Store 1', 'UTC', 'ACTIVE', "
                    "CAST('{}' AS jsonb))"
                ),
                {"id": store_id, "tenant_id": tenant_id},
            )
            connection.execute(
                text(
                    "INSERT INTO product_styles "
                    "(id, tenant_id, code, name, attributes, active) "
                    "VALUES (:id, :tenant_id, 'STYLE-1', 'Style 1', "
                    "CAST('{}' AS jsonb), true)"
                ),
                {"id": style_id, "tenant_id": tenant_id},
            )
            connection.execute(
                text(
                    "INSERT INTO skus "
                    "(id, tenant_id, product_style_id, code, upc, color, size, "
                    "attributes, active) "
                    "VALUES (:id, :tenant_id, :style_id, 'SKU-1', '036000291452', "
                    "'Blue', 'M', CAST('{}' AS jsonb), true)"
                ),
                {"id": sku_id, "tenant_id": tenant_id, "style_id": style_id},
            )
            connection.execute(
                text(
                    "INSERT INTO zones (id, tenant_id, store_id, code, name, kind) VALUES "
                    "(:floor_id, :tenant_id, :store_id, 'floor', 'Sales Floor', "
                    "'SALES_FLOOR'), "
                    "(:backroom_id, :tenant_id, :store_id, 'backroom', 'Backroom', "
                    "'BACKROOM')"
                ),
                {
                    "floor_id": floor_zone_id,
                    "backroom_id": backroom_zone_id,
                    "tenant_id": tenant_id,
                    "store_id": store_id,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO devices "
                    "(id, tenant_id, serial_number, display_name, status) VALUES "
                    "(:floor_id, :tenant_id, 'CUTOVER-FLOOR', 'Cutover Floor', 'ACTIVE'), "
                    "(:backroom_id, :tenant_id, 'CUTOVER-BACK', 'Cutover Back', 'ACTIVE')"
                ),
                {
                    "floor_id": floor_device_id,
                    "backroom_id": backroom_device_id,
                    "tenant_id": tenant_id,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO device_assignments "
                    "(id, tenant_id, device_id, store_id, zone_id, effective_from) VALUES "
                    "(:floor_assignment_id, :tenant_id, :floor_device_id, :store_id, "
                    ":floor_zone_id, :effective_from), "
                    "(:back_assignment_id, :tenant_id, :backroom_device_id, :store_id, "
                    ":backroom_zone_id, :effective_from)"
                ),
                {
                    "floor_assignment_id": uuid.uuid4(),
                    "back_assignment_id": uuid.uuid4(),
                    "tenant_id": tenant_id,
                    "floor_device_id": floor_device_id,
                    "backroom_device_id": backroom_device_id,
                    "store_id": store_id,
                    "floor_zone_id": floor_zone_id,
                    "backroom_zone_id": backroom_zone_id,
                    "effective_from": now,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO catalog_imports "
                    "(id, tenant_id, idempotency_key, checksum, mode, status, filename, "
                    "content_type, size_bytes, total_rows, valid_rows, invalid_rows, "
                    "inserted_count, updated_count, unchanged_count, deactivated_count, "
                    "reconciliation) VALUES "
                    "(:id, :tenant_id, 'cutover-catalog', :checksum, 'DELTA', 'COMPLETED', "
                    "'cutover.csv', 'text/csv', 1, 1, 1, 0, 1, 0, 0, 0, "
                    "CAST('{}' AS jsonb))"
                ),
                {
                    "id": catalog_import_id,
                    "tenant_id": tenant_id,
                    "checksum": "c" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO epc_bindings "
                    "(id, tenant_id, sku_id, epc, effective_from, source_import_id) "
                    "VALUES (:id, :tenant_id, :sku_id, :epc, :effective_from, :import_id)"
                ),
                {
                    "id": epc_binding_id,
                    "tenant_id": tenant_id,
                    "sku_id": sku_id,
                    "epc": concurrent_epc,
                    "effective_from": now,
                    "import_id": catalog_import_id,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO replenishment_policies "
                    "(id, tenant_id, external_key, store_id, selector_type, selector_value, "
                    "minimum_floor_quantity, target_floor_quantity, priority, "
                    "effective_from, active, revision) "
                    "VALUES (:id, :tenant_id, 'policy-1', :store_id, 'SKU', 'SKU-1', "
                    "1, 3, 0, :effective_from, true, 1)"
                ),
                {
                    "id": policy_id,
                    "tenant_id": tenant_id,
                    "store_id": store_id,
                    "effective_from": now,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO replenishment_tasks "
                    "(id, tenant_id, store_id, sku_id, source_policy_id, status, "
                    "quantity, moved_quantity, version, completed_at) "
                    "VALUES (:id, :tenant_id, :store_id, :sku_id, :policy_id, "
                    "'CANCELLED', 1, 0, 1, :completed_at)"
                ),
                {
                    "id": zero_moved_task_id,
                    "tenant_id": tenant_id,
                    "store_id": store_id,
                    "sku_id": sku_id,
                    "policy_id": policy_id,
                    "completed_at": now,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO rfid_observations "
                    "(id, tenant_id, event_id, batch_id, device_id, store_id, zone_id, epc, "
                    "observed_at, ingested_at, payload_hash, status, raw_payload) VALUES "
                    "(:first_id, :tenant_id, :first_event, 'historical-batch-1', "
                    ":device_id, :store_id, :zone_id, :epc, :observed_at, :ingested_at, "
                    ":first_hash, 'RECEIVED', CAST('{}' AS jsonb)), "
                    "(:second_id, :tenant_id, :second_event, 'historical-batch-2', "
                    ":device_id, :store_id, :zone_id, :epc, :observed_at, :ingested_at, "
                    ":second_hash, 'RECEIVED', CAST('{}' AS jsonb))"
                ),
                {
                    "first_id": historical_observation_ids[1],
                    "second_id": historical_observation_ids[0],
                    "tenant_id": tenant_id,
                    "first_event": "00000000-0000-0000-0000-000000000001",
                    "second_event": "00000000-0000-0000-0000-000000000002",
                    "device_id": floor_device_id,
                    "store_id": store_id,
                    "zone_id": floor_zone_id,
                    "epc": historical_epc,
                    "observed_at": now,
                    "ingested_at": now,
                    "first_hash": "a" * 64,
                    "second_hash": "b" * 64,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO replenishment_tasks "
                    "(id, tenant_id, store_id, sku_id, source_policy_id, status, "
                    "quantity, moved_quantity, version, claimed_by_subject, claimed_at) "
                    "VALUES (:id, :tenant_id, :store_id, :sku_id, :policy_id, "
                    "'IN_PROGRESS', 3, 2, 1, 'legacy-associate', :claimed_at)"
                ),
                {
                    "id": task_id,
                    "tenant_id": tenant_id,
                    "store_id": store_id,
                    "sku_id": sku_id,
                    "policy_id": policy_id,
                    "claimed_at": now,
                },
            )

        command.upgrade(_alembic_config(project_root), "head")
        with migration_engine.begin() as connection:
            historical_sequences = connection.execute(
                text(
                    "SELECT event_id, acceptance_sequence FROM rfid_observations "
                    "WHERE id = ANY(:observation_ids) ORDER BY acceptance_sequence"
                ),
                {"observation_ids": list(historical_observation_ids)},
            ).all()
            assert historical_sequences == [
                ("00000000-0000-0000-0000-000000000001", 1),
                ("00000000-0000-0000-0000-000000000002", 2),
            ]
            sequence_cache = connection.scalar(
                text(
                    "SELECT cache_size FROM pg_sequences "
                    "WHERE schemaname = 'public' "
                    "AND sequencename = 'rfid_observation_acceptance_seq'"
                )
            )
            assert sequence_cache == 1
            cutover = connection.execute(
                text(
                    "SELECT reconciled_before_tracking_quantity, "
                    "reservation_cutover_reviewed "
                    "FROM legacy_replenishment_tasks WHERE id = :task_id"
                ),
                {"task_id": task_id},
            ).one()
            assert cutover.reconciled_before_tracking_quantity == 0
            assert cutover.reservation_cutover_reviewed is False
            zero_moved_reviewed = connection.scalar(
                text(
                    "SELECT reservation_cutover_reviewed "
                    "FROM legacy_replenishment_tasks WHERE id = :task_id"
                ),
                {"task_id": zero_moved_task_id},
            )
            assert zero_moved_reviewed is True
            connection.execute(
                text(
                    "INSERT INTO legacy_replenishment_tasks "
                    "(id, tenant_id, store_id, sku_id, source_policy_id, status, "
                    "quantity, moved_quantity, version, completed_at) "
                    "VALUES (:id, :tenant_id, :store_id, :sku_id, :policy_id, "
                    "'CANCELLED', 1, 0, 1, clock_timestamp())"
                ),
                {
                    "id": direct_insert_task_id,
                    "tenant_id": tenant_id,
                    "store_id": store_id,
                    "sku_id": sku_id,
                    "policy_id": policy_id,
                },
            )
            direct_insert_reviewed = connection.scalar(
                text(
                    "SELECT reservation_cutover_reviewed "
                    "FROM legacy_replenishment_tasks WHERE id = :task_id"
                ),
                {"task_id": direct_insert_task_id},
            )
            assert direct_insert_reviewed is False

        # The operator compares legacy task records with physical/RFID evidence,
        # then uses the audited offline path. This fixture establishes that the two
        # pre-cutover moved units are already reflected.
        with Session(migration_engine) as session:
            with pytest.raises(ValueError, match="between 0 and moved_quantity"):
                reconcile_reservation_cutover_task(
                    session,
                    task_id=task_id,
                    baseline=3,
                    reviewed_by="migration-test-operator",
                    note="Invalid baseline probe.",
                )
            reviewed = reconcile_reservation_cutover_task(
                session,
                task_id=task_id,
                baseline=2,
                reviewed_by="migration-test-operator",
                note="Two legacy units confirmed in the RFID projection.",
            )
            session.commit()
            assert reviewed.reservation_cutover_reviewed is True
            assert reviewed.reservation_cutover_reviewed_at is not None
            assert reviewed.reservation_cutover_reviewed_by == "migration-test-operator"
            repeated = reconcile_reservation_cutover_task(
                session,
                task_id=task_id,
                baseline=2,
                reviewed_by="migration-test-operator",
                note="Idempotent retry.",
            )
            assert repeated.id == task_id
            with pytest.raises(ValueError, match="different baseline"):
                reconcile_reservation_cutover_task(
                    session,
                    task_id=task_id,
                    baseline=1,
                    reviewed_by="migration-test-operator",
                    note="Conflicting retry.",
                )
            reconcile_reservation_cutover_task(
                session,
                task_id=direct_insert_task_id,
                baseline=0,
                reviewed_by="migration-test-operator",
                note="Fail-closed direct insert contains no legacy movement.",
            )
            session.commit()

        with migration_engine.begin() as connection:
            # One unit is recorded after cutover, then the still-active task closes.
            connection.execute(
                text(
                    "UPDATE legacy_replenishment_tasks "
                    "SET moved_quantity = 3, status = 'EXCEPTION', "
                    "completed_at = clock_timestamp() WHERE id = :task_id"
                ),
                {"task_id": task_id},
            )

        with Session(migration_engine) as session:
            assert (
                _unreflected_terminal_moved_quantity(
                    session,
                    tenant_id,
                    store_id,
                    sku_id,
                )
                == 1
            )

        # Two independent database sessions accept conflicting locations for the
        # same EPC/event time. Whichever transaction obtains the EPC lock first gets
        # the lower sequence, and remains canonical even when workers run in reverse.
        acceptance_barrier = Barrier(2)
        concurrent_observed_at = datetime.now(UTC)
        concurrent_event_ids = (str(uuid.uuid4()), str(uuid.uuid4()))

        def accept_concurrently(device_id: uuid.UUID, event_id: str, batch_id: str) -> uuid.UUID:
            with Session(migration_engine) as session:
                device = session.get(Device, device_id)
                assert device is not None
                acceptance_barrier.wait(timeout=10)
                receipt = ingest_batch(
                    session,
                    device,
                    RfidBatchInput(
                        batch_id=batch_id,
                        observations=[
                            RfidObservationInput(
                                event_id=event_id,
                                epc=concurrent_epc,
                                observed_at=concurrent_observed_at,
                            )
                        ],
                    ),
                )
                observation_id = receipt.results[0].observation_id
                assert observation_id is not None
                return observation_id

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (
                executor.submit(
                    accept_concurrently,
                    floor_device_id,
                    concurrent_event_ids[0],
                    "concurrent-floor",
                ),
                executor.submit(
                    accept_concurrently,
                    backroom_device_id,
                    concurrent_event_ids[1],
                    "concurrent-backroom",
                ),
            )
            concurrent_observation_ids = [future.result(timeout=20) for future in futures]

        with Session(migration_engine) as session:
            accepted_rows = list(
                session.scalars(
                    select(RfidObservation)
                    .where(RfidObservation.id.in_(concurrent_observation_ids))
                    .order_by(RfidObservation.acceptance_sequence)
                ).all()
            )
        assert len(accepted_rows) == 2
        assert accepted_rows[0].acceptance_sequence < accepted_rows[1].acceptance_sequence
        assert accepted_rows[0].zone_id != accepted_rows[1].zone_id

        for row in reversed(accepted_rows):
            with Session(migration_engine) as session:
                process_rfid_observation_job(session, {"observation_id": str(row.id)})
                session.commit()

        with Session(migration_engine) as session:
            processed_rows = list(
                session.scalars(
                    select(RfidObservation)
                    .where(RfidObservation.id.in_(concurrent_observation_ids))
                    .order_by(RfidObservation.acceptance_sequence)
                ).all()
            )
            assert processed_rows[0].status is ObservationStatus.PROCESSED
            assert processed_rows[1].status is ObservationStatus.QUARANTINED
            assert processed_rows[1].quarantine_reason == "AMBIGUOUS_SAME_TIMESTAMP_LOCATION"

        # Exercise the sequence migration in both directions with populated data.
        command.downgrade(_alembic_config(project_root), "a2c7e91f4b6d")
        with migration_engine.begin() as connection:
            acceptance_column_exists = connection.scalar(
                text(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'rfid_observations' "
                    "AND column_name = 'acceptance_sequence')"
                )
            )
            assert acceptance_column_exists is False
        command.upgrade(_alembic_config(project_root), "head")
        with migration_engine.begin() as connection:
            sequence_integrity = connection.execute(
                text(
                    "SELECT count(*) AS total, count(acceptance_sequence) AS populated, "
                    "count(DISTINCT acceptance_sequence) AS distinct_values, "
                    "max(acceptance_sequence) AS maximum FROM rfid_observations"
                )
            ).one()
            assert sequence_integrity.total == sequence_integrity.populated
            assert sequence_integrity.total == sequence_integrity.distinct_values
            next_sequence = connection.scalar(
                text(
                    "INSERT INTO rfid_observations "
                    "(id, tenant_id, event_id, batch_id, device_id, store_id, zone_id, epc, "
                    "observed_at, ingested_at, payload_hash, status, raw_payload) "
                    "VALUES (:id, :tenant_id, :event_id, 'post-reupgrade', :device_id, "
                    ":store_id, :zone_id, :epc, clock_timestamp(), clock_timestamp(), "
                    ":payload_hash, 'RECEIVED', CAST('{}' AS jsonb)) "
                    "RETURNING acceptance_sequence"
                ),
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_id,
                    "event_id": str(uuid.uuid4()),
                    "device_id": floor_device_id,
                    "store_id": store_id,
                    "zone_id": floor_zone_id,
                    "epc": historical_epc,
                    "payload_hash": "d" * 64,
                },
            )
            assert next_sequence == sequence_integrity.maximum + 1
    finally:
        if migration_engine is not None:
            migration_engine.dispose()
        if original_database_url is None:
            os.environ.pop("DATABASE_URL", None)
        else:
            os.environ["DATABASE_URL"] = original_database_url
        if original_migration_database_url is None:
            os.environ.pop("MIGRATION_DATABASE_URL", None)
        else:
            os.environ["MIGRATION_DATABASE_URL"] = original_migration_database_url
        get_settings.cache_clear()
        with admin_engine.connect().execution_options(isolation_level="AUTOCOMMIT") as connection:
            connection.execute(
                text(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE datname = :database_name AND pid <> pg_backend_pid()"
                ),
                {"database_name": database_name},
            )
            connection.exec_driver_sql(f'DROP DATABASE IF EXISTS "{database_name}"')
        admin_engine.dispose()
