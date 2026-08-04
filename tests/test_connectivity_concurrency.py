import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier

import pytest
from sqlalchemy.orm import Session, sessionmaker

from abacus.config import Settings
from abacus.enums import StoreStatus, TenantStatus
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import FreshnessStatus, StoreConnectivity
from abacus.models.tenancy import Store, Tenant
from abacus.services.connectivity import lock_store_connectivity_for_receipt
from abacus.services.streaming_inventory import _update_connectivity

pytestmark = pytest.mark.integration


def _event(
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    received_at: datetime,
    backlog_drained: bool,
    reader_coverage_ok: bool,
) -> RfidObservationEvent:
    return RfidObservationEvent(
        tenant_id=tenant_id,
        batch_id=uuid.uuid4(),
        event_id=str(uuid.uuid4()),
        device_id=uuid.uuid4(),
        store_id=store_id,
        zone_id=uuid.uuid4(),
        epc=f"3034{uuid.uuid4().int % 10**20:020d}",
        observed_at=received_at,
        received_at=received_at,
        rssi=-42,
        backlog_drained=backlog_drained,
        reader_coverage_ok=reader_coverage_ok,
    )


def _store(postgres_session_factory: sessionmaker[Session]) -> tuple[uuid.UUID, uuid.UUID]:
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    suffix = uuid.uuid4().hex[:12]
    with postgres_session_factory() as db:
        tenant = Tenant(
            id=tenant_id,
            code=f"connectivity-{suffix}",
            name="Connectivity test tenant",
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        db.flush()
        db.add(
            Store(
                id=store_id,
                tenant_id=tenant_id,
                code=f"store-{suffix}",
                name="Connectivity test store",
                timezone="UTC",
                status=StoreStatus.ACTIVE,
                configuration={},
            )
        )
        db.commit()
    return tenant_id, store_id


def test_delayed_worker_cannot_replace_newer_connectivity_status(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id, store_id = _store(postgres_session_factory)
    newest_receipt = datetime.now(UTC)

    with postgres_session_factory() as db:
        locked_connectivity, _ = lock_store_connectivity_for_receipt(
            db,
            tenant_id=tenant_id,
            store_id=store_id,
            received_at=newest_receipt,
            backlog_drained=False,
            reader_coverage_ok=False,
        )
        locked_connectivity.freshness_status = FreshnessStatus.STALE
        db.commit()

    delayed_event = _event(
        tenant_id=tenant_id,
        store_id=store_id,
        received_at=newest_receipt - timedelta(minutes=5),
        backlog_drained=True,
        reader_coverage_ok=True,
    )
    with postgres_session_factory() as db:
        _update_connectivity(db, delayed_event, Settings())
        db.commit()

    with postgres_session_factory() as db:
        connectivity = db.get(StoreConnectivity, (tenant_id, store_id))
        assert connectivity is not None
        assert connectivity.status_received_at == newest_receipt
        assert connectivity.gateway_last_heartbeat == newest_receipt
        assert connectivity.backlog_drained is False
        assert connectivity.reader_coverage_ok is False
        assert connectivity.freshness_status == FreshnessStatus.STALE


def test_first_connectivity_write_race_converges_on_newest_receipt(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id, store_id = _store(postgres_session_factory)
    newest_receipt = datetime.now(UTC)
    barrier = Barrier(2)

    def write_ingress_status() -> None:
        with postgres_session_factory() as db:
            barrier.wait()
            lock_store_connectivity_for_receipt(
                db,
                tenant_id=tenant_id,
                store_id=store_id,
                received_at=newest_receipt,
                backlog_drained=False,
                reader_coverage_ok=False,
            )
            db.commit()

    def process_delayed_event() -> None:
        event = _event(
            tenant_id=tenant_id,
            store_id=store_id,
            received_at=newest_receipt - timedelta(minutes=5),
            backlog_drained=True,
            reader_coverage_ok=True,
        )
        with postgres_session_factory() as db:
            barrier.wait()
            _update_connectivity(db, event, Settings())
            db.commit()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(write_ingress_status), executor.submit(process_delayed_event)]
        for future in futures:
            future.result(timeout=10)

    with postgres_session_factory() as db:
        connectivity = db.get(StoreConnectivity, (tenant_id, store_id))
        assert connectivity is not None
        assert connectivity.status_received_at == newest_receipt
        assert connectivity.gateway_last_heartbeat == newest_receipt
        assert connectivity.backlog_drained is False
        assert connectivity.reader_coverage_ok is False
        assert connectivity.freshness_status == FreshnessStatus.STALE
