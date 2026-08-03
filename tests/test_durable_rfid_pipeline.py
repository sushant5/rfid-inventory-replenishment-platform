from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, sessionmaker

import abacus.processes.event_worker as event_worker
from abacus.api.errors import ApiError
from abacus.config import Settings
from abacus.enums import DeviceStatus, StoreStatus, TenantStatus, ZoneKind
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import (
    AppliedInventoryDelta,
    CurrentItemState,
    InventoryProjection,
    InventoryTransitionOutbox,
    ObservationBatchStatus,
    Product,
    ProductVariant,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidObservationOutbox,
    RfidQuarantine,
    RfidTag,
    StoreConnectivity,
)
from abacus.models.catalog import (
    CatalogImport,
    CatalogImportMode,
    CatalogImportStatus,
    ProductStyle,
    Sku,
)
from abacus.models.tenancy import Device, DeviceAssignment, Store, Tenant, Zone
from abacus.schemas.architecture import (
    CanonicalObservationBatchCreate,
    CanonicalObservationInput,
)
from abacus.services.rfid_ingress import accept_observation_batch
from abacus.services.streaming_inventory import (
    ProcessingResult,
    RecentObservationState,
    rebuild_inventory_projection,
)
from abacus.services.streaming_inventory import (
    process_observation as process_observation_service,
)

pytestmark = pytest.mark.integration


@dataclass(frozen=True, slots=True)
class DurablePipelineFixture:
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    zone_id: uuid.UUID
    device_id: uuid.UUID
    sku_id: uuid.UUID
    epc: str
    observed_at: datetime


@pytest.fixture
def durable_pipeline(
    postgres_session_factory: sessionmaker[Session],
) -> Iterator[DurablePipelineFixture]:
    suffix = uuid.uuid4().hex[:12]
    observed_at = datetime.now(UTC) - timedelta(seconds=5)
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    device_id = uuid.uuid4()
    style_id = uuid.uuid4()
    product_id = uuid.uuid4()
    catalog_import_id = uuid.uuid4()
    tenant = Tenant(
        id=tenant_id,
        code=f"durable-{suffix}",
        name="Durable RFID Pipeline",
        status=TenantStatus.ACTIVE,
    )
    store = Store(
        id=store_id,
        tenant_id=tenant_id,
        code=f"store-{suffix}",
        name="Durable Store",
        timezone="UTC",
        status=StoreStatus.ACTIVE,
        configuration={},
    )
    zone = Zone(
        id=zone_id,
        tenant_id=tenant_id,
        store_id=store_id,
        code="sales-floor",
        name="Sales Floor",
        kind=ZoneKind.SALES_FLOOR,
    )
    device = Device(
        id=device_id,
        tenant_id=tenant_id,
        serial_number=f"durable-reader-{suffix}",
        display_name="Durable Reader",
        status=DeviceStatus.ACTIVE,
    )
    style = ProductStyle(
        id=style_id,
        tenant_id=tenant_id,
        code=f"STYLE-{suffix.upper()}",
        name="Durable Style",
        attributes={},
        active=True,
    )
    product = Product(
        id=product_id,
        tenant_id=tenant_id,
        style_code=f"STYLE-{suffix.upper()}",
        name="Durable Product",
        category="Apparel",
        attributes={},
        active=True,
    )
    catalog_import = CatalogImport(
        id=catalog_import_id,
        tenant_id=tenant_id,
        idempotency_key=f"durable-{suffix}",
        checksum=suffix.ljust(64, "0"),
        mode=CatalogImportMode.DELTA,
        status=CatalogImportStatus.COMPLETED,
        filename="durable.csv",
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
    )

    with postgres_session_factory() as db:
        db.add(tenant)
        db.flush()
        db.add_all([store, device, style, product, catalog_import])
        db.flush()
        variant = ProductVariant(
            tenant_id=tenant_id,
            product_id=product_id,
            color="Orange",
            attributes={},
            active=True,
        )
        db.add_all([zone, variant])
        db.flush()
        sku = Sku(
            tenant_id=tenant_id,
            product_style_id=style_id,
            product_variant_id=variant.id,
            code=f"SKU-{suffix.upper()}",
            upc=str(tenant_id.int)[:12],
            color="Orange",
            size="M",
            attributes={},
            active=True,
        )
        epc = f"3034{tenant_id.hex[:20].upper()}"
        db.add_all(
            [
                sku,
                DeviceAssignment(
                    tenant_id=tenant_id,
                    device_id=device_id,
                    store_id=store_id,
                    zone_id=zone_id,
                    effective_from=observed_at - timedelta(minutes=1),
                ),
            ]
        )
        db.flush()
        db.add(
            RfidTag(
                tenant_id=tenant_id,
                epc=epc,
                sku_id=sku.id,
                source_import_id=catalog_import_id,
                active=True,
            )
        )
        db.commit()
        fixture = DurablePipelineFixture(
            tenant_id=tenant_id,
            store_id=store_id,
            zone_id=zone_id,
            device_id=device_id,
            sku_id=sku.id,
            epc=epc,
            observed_at=observed_at,
        )

    try:
        yield fixture
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == fixture.tenant_id))
            db.commit()


def _request(
    fixture: DurablePipelineFixture,
    event_ids: tuple[str, ...],
    *,
    epc: str | None = None,
    rssi_offset: float = 0,
) -> CanonicalObservationBatchCreate:
    return CanonicalObservationBatchCreate(
        device_id=fixture.device_id,
        observations=[
            CanonicalObservationInput(
                event_id=event_id,
                epc=epc or fixture.epc,
                observed_at=fixture.observed_at + timedelta(seconds=index),
                rssi=-42 + rssi_offset,
            )
            for index, event_id in enumerate(event_ids)
        ],
    )


def _accept(
    postgres_session_factory: sessionmaker[Session],
    fixture: DurablePipelineFixture,
    request: CanonicalObservationBatchCreate,
    *,
    received_at: datetime,
) -> uuid.UUID:
    with postgres_session_factory() as db:
        device = db.get(Device, fixture.device_id)
        assert device is not None
        batch, _ = accept_observation_batch(
            db,
            device=device,
            request=request,
            received_at=received_at,
        )
        return batch.id


def test_durable_acceptance_retry_and_projection_drain(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    # Deliberately differs from UUID sort order: the sequence must encode request order.
    event_ids = (
        "00000000-0000-0000-0000-000000000003",
        "00000000-0000-0000-0000-000000000001",
        "00000000-0000-0000-0000-000000000002",
    )
    request = _request(durable_pipeline, event_ids)
    received_at = durable_pipeline.observed_at + timedelta(seconds=4)

    first_batch_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at,
    )
    pending_retry_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at + timedelta(seconds=1),
    )

    with postgres_session_factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationEventLedger)
                .where(RfidObservationEventLedger.tenant_id == durable_pipeline.tenant_id)
            )
            == 3
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationOutbox)
                .where(RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id)
            )
            == 3
        )
        accepted_event_ids = list(
            db.scalars(
                select(RfidObservationOutbox.event_id)
                .where(RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id)
                .order_by(RfidObservationOutbox.acceptance_sequence)
            ).all()
        )
        assert accepted_event_ids == list(event_ids)
        for batch_id in (first_batch_id, pending_retry_id):
            batch = db.get(RfidObservationBatch, batch_id)
            assert batch is not None
            assert batch.status is ObservationBatchStatus.ACCEPTED
            assert batch.pending_count == 3

    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (3, 1)

    with postgres_session_factory() as db:
        for batch_id in (first_batch_id, pending_retry_id):
            batch = db.get(RfidObservationBatch, batch_id)
            assert batch is not None
            assert batch.status is ObservationBatchStatus.COMPLETED
            assert (batch.accepted_count, batch.processed_count, batch.rejected_count) == (3, 3, 0)
            assert batch.pending_count == 0
            assert batch.completed_at is not None

        links = list(
            db.scalars(
                select(RfidObservationBatchEvent).where(
                    RfidObservationBatchEvent.tenant_id == durable_pipeline.tenant_id
                )
            ).all()
        )
        assert len(links) == 6
        assert all(link.processing_status is RfidEventProcessingStatus.PROCESSED for link in links)

        item = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        assert item is not None
        assert item.zone_id == durable_pipeline.zone_id
        assert item.state_version == 1
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert projection is not None
        assert projection.quantity == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(AppliedInventoryDelta)
                .where(AppliedInventoryDelta.tenant_id == durable_pipeline.tenant_id)
            )
            == 1
        )

    terminal_retry_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at + timedelta(seconds=2),
    )
    with postgres_session_factory() as db:
        terminal_retry = db.get(RfidObservationBatch, terminal_retry_id)
        assert terminal_retry is not None
        assert terminal_retry.status is ObservationBatchStatus.COMPLETED
        assert terminal_retry.pending_count == 0
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationOutbox)
                .where(RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id)
            )
            == 3
        )

        transition = db.scalar(
            select(InventoryTransitionOutbox).where(
                InventoryTransitionOutbox.tenant_id == durable_pipeline.tenant_id
            )
        )
        assert transition is not None
        transition.published_at = None
        db.commit()

    # Replaying a transition is harmless because delta_id is the projection boundary.
    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (0, 1)
    with postgres_session_factory() as db:
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert projection is not None
        assert projection.quantity == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(AppliedInventoryDelta)
                .where(AppliedInventoryDelta.tenant_id == durable_pipeline.tenant_id)
            )
            == 1
        )

    conflicting = _request(durable_pipeline, event_ids, rssi_offset=-1)
    with pytest.raises(ApiError) as exc_info:
        _accept(
            postgres_session_factory,
            durable_pipeline,
            conflicting,
            received_at=received_at + timedelta(seconds=3),
        )
    assert exc_info.value.status_code == 409
    assert exc_info.value.code == "rfid_event_id_conflict"


def test_projection_rebuild_rejects_pending_transition(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    request = _request(
        durable_pipeline,
        (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())),
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=durable_pipeline.observed_at + timedelta(seconds=4),
    )

    recent = RecentObservationState()
    with postgres_session_factory() as db:
        rows = list(
            db.scalars(
                select(RfidObservationOutbox)
                .where(RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id)
                .order_by(RfidObservationOutbox.acceptance_sequence)
            ).all()
        )
        for row in rows:
            process_observation_service(
                db,
                RfidObservationEvent.model_validate(row.payload),
                recent,
                Settings(),
            )
            row.published_at = datetime.now(UTC)
        db.commit()

    with postgres_session_factory() as db:
        assert (
            db.scalar(
                select(func.count())
                .select_from(InventoryTransitionOutbox)
                .where(
                    InventoryTransitionOutbox.tenant_id == durable_pipeline.tenant_id,
                    InventoryTransitionOutbox.published_at.is_(None),
                )
            )
            == 1
        )
        with pytest.raises(RuntimeError, match="transition deltas are pending"):
            rebuild_inventory_projection(db, durable_pipeline.tenant_id)
        db.rollback()

    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (0, 1)
    with postgres_session_factory() as db:
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert projection is not None
        assert projection.quantity == 1


def test_event_worker_confirms_timed_out_removal(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    request = _request(
        durable_pipeline,
        (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())),
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=durable_pipeline.observed_at + timedelta(seconds=4),
    )
    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (3, 1)

    now = datetime.now(UTC)
    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        connectivity = db.get(
            StoreConnectivity,
            (durable_pipeline.tenant_id, durable_pipeline.store_id),
        )
        assert state is not None
        assert connectivity is not None
        state.last_observed_at = now - timedelta(minutes=31)
        connectivity.gateway_last_heartbeat = now
        connectivity.last_live_event_at = now
        connectivity.oldest_buffered_event_at = None
        connectivity.backlog_drained = True
        connectivity.reader_coverage_ok = True
        db.commit()

    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
        sweep_removals=True,
    ) == (0, 1)
    with postgres_session_factory() as db:
        removed = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert removed is not None
        assert removed.store_id is None
        assert removed.zone_id is None
        assert removed.state_version == 2
        assert projection is not None
        assert projection.quantity == 0


def test_unknown_epc_rejects_every_link_and_completes_retry_batches(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    event_id = str(uuid.uuid4())
    unknown_epc = "3034FFFFFFFFFFFFFFFFFFFF"
    request = _request(durable_pipeline, (event_id,), epc=unknown_epc)
    received_at = durable_pipeline.observed_at + timedelta(seconds=4)
    first_batch_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at,
    )
    retry_batch_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at + timedelta(seconds=1),
    )

    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (1, 0)

    with postgres_session_factory() as db:
        ledger = db.get(RfidObservationEventLedger, (durable_pipeline.tenant_id, event_id))
        assert ledger is not None
        assert ledger.processing_status is RfidEventProcessingStatus.REJECTED
        assert ledger.disposition == "QUARANTINED"
        assert ledger.rejection_reason == "UNKNOWN_EPC"
        for batch_id in (first_batch_id, retry_batch_id):
            batch = db.get(RfidObservationBatch, batch_id)
            assert batch is not None
            assert batch.status is ObservationBatchStatus.COMPLETED_WITH_ERRORS
            assert (batch.accepted_count, batch.processed_count, batch.rejected_count) == (1, 0, 1)
            assert batch.pending_count == 0
        quarantine = db.scalar(
            select(RfidQuarantine).where(
                RfidQuarantine.tenant_id == durable_pipeline.tenant_id,
                RfidQuarantine.event_id == event_id,
            )
        )
        assert quarantine is not None

    terminal_retry_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at + timedelta(seconds=2),
    )
    with postgres_session_factory() as db:
        terminal_retry = db.get(RfidObservationBatch, terminal_retry_id)
        assert terminal_retry is not None
        assert terminal_retry.status is ObservationBatchStatus.COMPLETED_WITH_ERRORS
        assert terminal_retry.rejected_count == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationOutbox)
                .where(RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id)
            )
            == 1
        )


def test_worker_rollback_restores_recent_evidence_and_records_failure(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event_ids = tuple(str(uuid.uuid4()) for _ in range(3))
    request = _request(durable_pipeline, event_ids)
    received_at = durable_pipeline.observed_at + timedelta(seconds=4)
    _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at,
    )
    recent = RecentObservationState()
    original_process = process_observation_service
    calls = 0

    def fail_after_third_event(
        db: Session,
        event: RfidObservationEvent,
        state: RecentObservationState,
        settings: Settings,
    ) -> ProcessingResult:
        nonlocal calls
        calls += 1
        result = original_process(db, event, state, settings)
        if calls == 3:
            raise RuntimeError("injected transaction failure")
        return result

    monkeypatch.setattr(
        "abacus.processes.event_worker.process_observation",
        fail_after_third_event,
    )
    with pytest.raises(RuntimeError, match="injected transaction failure"):
        event_worker.process_tenant_once(durable_pipeline.tenant_id, recent)

    with postgres_session_factory() as db:
        assert db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc)) is None
        batch = db.scalar(
            select(RfidObservationBatch).where(
                RfidObservationBatch.tenant_id == durable_pipeline.tenant_id
            )
        )
        assert batch is not None
        assert (batch.processed_count, batch.rejected_count, batch.pending_count) == (2, 0, 1)
        failed = db.scalar(
            select(RfidObservationOutbox).where(
                RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                RfidObservationOutbox.event_id == event_ids[2],
            )
        )
        assert failed is not None
        assert failed.publish_attempts == 1
        assert failed.last_error == "RuntimeError: injected transaction failure"

    monkeypatch.setattr(
        "abacus.processes.event_worker.process_observation",
        original_process,
    )
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (1, 1)
    with postgres_session_factory() as db:
        item = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        assert item is not None
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert projection is not None
        assert projection.quantity == 1
