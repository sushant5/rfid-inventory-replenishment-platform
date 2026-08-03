from __future__ import annotations

import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from queue import Queue

import pytest
from sqlalchemy import select, text
from sqlalchemy.orm import Session, sessionmaker

from abacus.enums import DeviceStatus, ObservationStatus, StoreStatus, TenantStatus, ZoneKind
from abacus.models.catalog import (
    CatalogImport,
    CatalogImportMode,
    CatalogImportStatus,
    EpcBinding,
    ProductStyle,
    Sku,
)
from abacus.models.rfid import (
    InventoryBalance,
    InventoryChange,
    InventoryItemState,
    RfidObservation,
)
from abacus.models.tenancy import Device, Store, Tenant, Zone
from abacus.services.rfid import process_rfid_observation_job

pytestmark = pytest.mark.integration


def _wait_for_lock_wait(
    session_factory: sessionmaker[Session],
    backend_pid: int,
    *,
    timeout_seconds: float = 5.0,
) -> None:
    deadline = time.monotonic() + timeout_seconds
    with session_factory() as inspector:
        while time.monotonic() < deadline:
            wait_event_type = inspector.scalar(
                text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
                {"pid": backend_pid},
            )
            if wait_event_type == "Lock":
                return
            time.sleep(0.01)
    pytest.fail("the concurrent balance writer did not wait on the first transaction")


def test_parallel_first_sightings_create_and_increment_one_balance(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    """A second EPC must wait for an uncommitted first balance row.

    Without store/SKU serialization, PostgreSQL cannot row-lock the balance before it
    exists: both workers construct an INSERT and one loses the unique-constraint race.
    """

    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    style_id = uuid.uuid4()
    sku_id = uuid.uuid4()
    device_id = uuid.uuid4()
    catalog_import_id = uuid.uuid4()
    observation_ids = (uuid.uuid4(), uuid.uuid4())
    epcs = ("3074257BF7194E4000002A01", "3074257BF7194E4000002A02")
    suffix = uuid.uuid4().hex[:12]
    observed_at = datetime.now(UTC)

    with postgres_session_factory() as setup:
        setup.add(
            Tenant(
                id=tenant_id,
                code=f"race-{suffix}",
                name="RFID Balance Race Tenant",
                status=TenantStatus.ACTIVE,
            )
        )
        setup.flush()
        setup.add_all(
            [
                ProductStyle(
                    id=style_id,
                    tenant_id=tenant_id,
                    code=f"STYLE-{suffix.upper()}",
                    name="Concurrency Style",
                    attributes={},
                    active=True,
                ),
                Store(
                    id=store_id,
                    tenant_id=tenant_id,
                    code=f"store-{suffix}",
                    name="Concurrency Store",
                    timezone="UTC",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                Device(
                    id=device_id,
                    tenant_id=tenant_id,
                    serial_number=f"reader-{suffix}",
                    display_name="Concurrency Reader",
                    status=DeviceStatus.ACTIVE,
                ),
                CatalogImport(
                    id=catalog_import_id,
                    tenant_id=tenant_id,
                    idempotency_key=f"catalog-{suffix}",
                    checksum="0" * 64,
                    mode=CatalogImportMode.DELTA,
                    status=CatalogImportStatus.COMPLETED,
                    filename="concurrency.csv",
                    content_type="text/csv",
                    size_bytes=0,
                    total_rows=0,
                    valid_rows=0,
                    invalid_rows=0,
                    inserted_count=0,
                    updated_count=0,
                    unchanged_count=0,
                    deactivated_count=0,
                    reconciliation={},
                    promoted_at=observed_at,
                ),
            ]
        )
        setup.flush()
        setup.add_all(
            [
                Sku(
                    id=sku_id,
                    tenant_id=tenant_id,
                    product_style_id=style_id,
                    code=f"SKU-{suffix.upper()}",
                    upc=f"9{str(tenant_id.int)[:11]}",
                    color="Orange",
                    size="M",
                    attributes={},
                    active=True,
                ),
                Zone(
                    id=zone_id,
                    tenant_id=tenant_id,
                    store_id=store_id,
                    code="sales-floor",
                    name="Sales Floor",
                    kind=ZoneKind.SALES_FLOOR,
                ),
            ]
        )
        setup.flush()
        for index, (observation_id, epc) in enumerate(
            zip(observation_ids, epcs, strict=True),
            start=1,
        ):
            setup.add_all(
                [
                    EpcBinding(
                        tenant_id=tenant_id,
                        sku_id=sku_id,
                        epc=epc,
                        effective_from=observed_at,
                        source_import_id=catalog_import_id,
                    ),
                    RfidObservation(
                        id=observation_id,
                        tenant_id=tenant_id,
                        event_id=f"first-sighting-{index}-{suffix}",
                        batch_id=f"concurrency-{suffix}",
                        device_id=device_id,
                        store_id=store_id,
                        zone_id=zone_id,
                        epc=epc,
                        observed_at=observed_at,
                        ingested_at=observed_at,
                        payload_hash=str(index) * 64,
                        status=ObservationStatus.RECEIVED,
                        raw_payload={"epc": epc},
                    ),
                ]
            )
        setup.commit()

    second_backend_pid: Queue[int] = Queue(maxsize=1)

    def add_second_first_sighting() -> None:
        with postgres_session_factory() as second:
            backend_pid = second.scalar(text("SELECT pg_backend_pid()"))
            assert backend_pid is not None
            second_backend_pid.put(backend_pid)
            process_rfid_observation_job(
                second,
                {"observation_id": str(observation_ids[1])},
            )
            second.commit()

    with postgres_session_factory() as first:
        process_rfid_observation_job(
            first,
            {"observation_id": str(observation_ids[0])},
        )
        first.flush()

        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(add_second_first_sighting)
            backend_pid = second_backend_pid.get(timeout=2)

            try:
                _wait_for_lock_wait(postgres_session_factory, backend_pid)
            finally:
                # Release both the uncommitted row and its transaction-scoped lock.
                first.commit()
            future.result(timeout=5)

    with postgres_session_factory() as verify:
        balances = list(
            verify.scalars(
                select(InventoryBalance).where(
                    InventoryBalance.tenant_id == tenant_id,
                    InventoryBalance.store_id == store_id,
                    InventoryBalance.zone_id == zone_id,
                    InventoryBalance.sku_id == sku_id,
                )
            ).all()
        )
        assert len(balances) == 1
        assert balances[0].quantity == 2
        assert (
            len(
                verify.scalars(
                    select(InventoryItemState).where(
                        InventoryItemState.tenant_id == tenant_id,
                        InventoryItemState.epc.in_(epcs),
                    )
                ).all()
            )
            == 2
        )
        assert (
            len(
                verify.scalars(
                    select(InventoryChange).where(
                        InventoryChange.tenant_id == tenant_id,
                        InventoryChange.observation_id.in_(observation_ids),
                    )
                ).all()
            )
            == 2
        )
        processed_statuses = list(
            verify.scalars(
                select(RfidObservation.status).where(RfidObservation.id.in_(observation_ids))
            ).all()
        )
        assert processed_statuses == [ObservationStatus.PROCESSED, ObservationStatus.PROCESSED]
