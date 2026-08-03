from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, func, select, update
from sqlalchemy.orm import Session, sessionmaker

import abacus.processes.event_worker as event_worker
from abacus.api.errors import ApiError
from abacus.api.routes.rfid import (
    get_item_state_endpoint,
    get_store_inventory_endpoint,
    list_rfid_quarantine_endpoint,
)
from abacus.config import Settings
from abacus.enums import DeviceStatus, StoreStatus, TenantStatus, ZoneKind
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import (
    AppliedInventoryDelta,
    CanonicalReplenishmentTask,
    CurrentItemState,
    FreshnessStatus,
    InventoryProjection,
    InventoryTransitionOutbox,
    ObservationBatchStatus,
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
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
    EpcBinding,
    ProductStyle,
    Sku,
)
from abacus.models.identity import IdentityRole
from abacus.models.tenancy import Device, DeviceAssignment, Store, Tenant, Zone
from abacus.schemas.architecture import (
    CanonicalObservationBatchCreate,
    CanonicalObservationInput,
)
from abacus.schemas.canonical_replenishment import ReplenishmentEvaluationCreate
from abacus.security import Principal, RoleScope
from abacus.services.canonical_replenishment import evaluate_replenishment
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
            EpcBinding(
                tenant_id=tenant_id,
                epc=epc,
                sku_id=sku.id,
                effective_from=observed_at - timedelta(minutes=1),
                source_import_id=catalog_import_id,
            )
        )
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
            # Activated versions are immutable, including during FK cascades.
            db.scalar(select(func.set_config("app.tenant_id", str(fixture.tenant_id), True)))
            db.execute(
                update(PolicyVersion)
                .where(PolicyVersion.tenant_id == fixture.tenant_id)
                .values(status=PolicyVersionStatus.DRAFT)
            )
            # Delete rules while their DRAFT parent still exists. A tenant cascade
            # may remove the version before the rule trigger checks its status.
            db.execute(delete(PolicyRule).where(PolicyRule.tenant_id == fixture.tenant_id))
            db.commit()
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


def _establish_projection(
    postgres_session_factory: sessionmaker[Session],
    fixture: DurablePipelineFixture,
) -> None:
    request = _request(
        fixture,
        (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())),
    )
    _accept(
        postgres_session_factory,
        fixture,
        request,
        received_at=fixture.observed_at + timedelta(seconds=4),
    )
    assert event_worker.process_tenant_once(
        fixture.tenant_id,
        RecentObservationState(),
    ) == (3, 1)


def _tenant_admin(fixture: DurablePipelineFixture) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=fixture.tenant_id,
        email="admin@example.test",
        display_name="Test tenant administrator",
        role_scopes=(RoleScope(IdentityRole.CORPORATE_ADMIN, None),),
    )


def test_inventory_api_reads_bucket_metadata_from_current_item_state(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    _establish_projection(postgres_session_factory, durable_pipeline)
    authoritative_as_of = durable_pipeline.observed_at + timedelta(minutes=1)

    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert state is not None
        assert projection is not None
        assert projection.confidence > 0.7
        assert projection.as_of < authoritative_as_of

        state.last_observed_at = authoritative_as_of
        state.confidence = 0.2
        db.commit()

        inventory = get_store_inventory_endpoint(
            durable_pipeline.store_id,
            db,
            Settings(),
            _tenant_admin(durable_pipeline),
        )

        assert len(inventory) == 1
        assert inventory[0].quantity == 1
        assert inventory[0].as_of == authoritative_as_of
        assert inventory[0].confidence == 0.2
        # The projection remains deliberately unchanged: quantity is projected, while
        # read-time metadata comes from authoritative item state.
        db.refresh(projection)
        assert projection.confidence > 0.7


def test_item_and_bucket_confidence_age_while_store_remains_live(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    _establish_projection(postgres_session_factory, durable_pipeline)
    now = datetime.now(UTC)
    settings = Settings(rfid_confidence_half_life_seconds=1800)

    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        connectivity = db.get(
            StoreConnectivity,
            (durable_pipeline.tenant_id, durable_pipeline.store_id),
        )
        assert state is not None
        assert connectivity is not None
        state.confidence = 1.0
        state.last_observed_at = now - timedelta(minutes=30)
        connectivity.gateway_last_heartbeat = now
        connectivity.last_live_event_at = now
        connectivity.oldest_buffered_event_at = None
        connectivity.backlog_drained = True
        connectivity.reader_coverage_ok = True
        db.commit()

        inventory = get_store_inventory_endpoint(
            durable_pipeline.store_id,
            db,
            settings,
            _tenant_admin(durable_pipeline),
        )
        item = get_item_state_endpoint(
            durable_pipeline.epc,
            db,
            settings,
            _tenant_admin(durable_pipeline),
        )

        assert inventory[0].freshness_status == FreshnessStatus.LIVE
        assert inventory[0].confidence == pytest.approx(0.5, abs=0.001)
        assert item.freshness_status == FreshnessStatus.LIVE
        assert item.confidence == pytest.approx(0.5, abs=0.001)

        # Rebuilding snapshot metadata must not make old item evidence look fresh.
        assert rebuild_inventory_projection(db, durable_pipeline.tenant_id) == 1
        db.commit()
        rebuilt_inventory = get_store_inventory_endpoint(
            durable_pipeline.store_id,
            db,
            settings,
            _tenant_admin(durable_pipeline),
        )
        assert rebuilt_inventory[0].confidence == pytest.approx(0.5, abs=0.001)


def test_replenishment_suppresses_old_item_evidence_while_store_remains_live(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    _establish_projection(postgres_session_factory, durable_pipeline)
    now = datetime.now(UTC)
    backroom_zone_id = uuid.uuid4()

    with postgres_session_factory() as db:
        floor_state = db.get(
            CurrentItemState,
            (durable_pipeline.tenant_id, durable_pipeline.epc),
        )
        floor_projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        connectivity = db.get(
            StoreConnectivity,
            (durable_pipeline.tenant_id, durable_pipeline.store_id),
        )
        assert floor_state is not None
        assert floor_projection is not None
        assert connectivity is not None
        assert floor_projection.confidence > 0.7

        # The materialized projection keeps its prior snapshot confidence, while
        # current item evidence ages independently without a quantity transition.
        floor_state.confidence = 1.0
        floor_state.last_observed_at = now - timedelta(minutes=30)
        connectivity.gateway_last_heartbeat = now
        connectivity.last_live_event_at = now
        connectivity.oldest_buffered_event_at = None
        connectivity.backlog_drained = True
        connectivity.reader_coverage_ok = True

        backroom = Zone(
            id=backroom_zone_id,
            tenant_id=durable_pipeline.tenant_id,
            store_id=durable_pipeline.store_id,
            code="backroom-confidence",
            name="Backroom",
            kind=ZoneKind.BACKROOM,
        )
        policy = PolicyDefinition(
            tenant_id=durable_pipeline.tenant_id,
            name="Current-state confidence policy",
            description=None,
        )
        db.add_all([backroom, policy])
        db.flush()
        version = PolicyVersion(
            tenant_id=durable_pipeline.tenant_id,
            policy_id=policy.id,
            version_number=1,
            status=PolicyVersionStatus.DRAFT,
        )
        db.add(version)
        db.flush()
        rule = PolicyRule(
            tenant_id=durable_pipeline.tenant_id,
            version_id=version.id,
            min_floor_qty=2,
            target_floor_qty=3,
            priority=0,
        )
        db.add(rule)
        db.flush()
        version.status = PolicyVersionStatus.ACTIVE
        version.activated_at = now
        for index in range(2):
            db.add(
                CurrentItemState(
                    tenant_id=durable_pipeline.tenant_id,
                    epc=f"{durable_pipeline.epc}-BACK-{index}",
                    sku_id=durable_pipeline.sku_id,
                    store_id=durable_pipeline.store_id,
                    zone_id=backroom_zone_id,
                    last_observed_at=now,
                    last_received_at=now,
                    confidence=0.95,
                    state_version=1,
                )
            )
        db.add(
            InventoryProjection(
                tenant_id=durable_pipeline.tenant_id,
                store_id=durable_pipeline.store_id,
                sku_id=durable_pipeline.sku_id,
                zone_id=backroom_zone_id,
                quantity=2,
                as_of=now,
                confidence=0.95,
                freshness_status=FreshnessStatus.LIVE,
            )
        )
        db.commit()

        result = evaluate_replenishment(
            db,
            _tenant_admin(durable_pipeline),
            ReplenishmentEvaluationCreate(store_id=durable_pipeline.store_id),
            settings=Settings(rfid_confidence_half_life_seconds=1800),
            minimum_confidence=0.7,
        )

        assert result.tasks == ()
        assert result.suppressed_connectivity is False
        assert result.suppressed_low_confidence == 1
        assert (
            db.scalar(
                select(func.count())
                .select_from(CanonicalReplenishmentTask)
                .where(CanonicalReplenishmentTask.tenant_id == durable_pipeline.tenant_id)
            )
            == 0
        )


def test_throttled_item_refresh_keeps_a_durable_restart_watermark(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    _establish_projection(postgres_session_factory, durable_pipeline)
    repeated_at = durable_pipeline.observed_at + timedelta(seconds=10)
    repeated_id = str(uuid.uuid4())
    repeated = CanonicalObservationBatchCreate(
        device_id=durable_pipeline.device_id,
        observations=[
            CanonicalObservationInput(
                event_id=repeated_id,
                epc=durable_pipeline.epc,
                observed_at=repeated_at,
                rssi=-42,
            )
        ],
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        repeated,
        received_at=repeated_at + timedelta(seconds=1),
    )
    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (1, 0)

    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        repeated_ledger = db.get(
            RfidObservationEventLedger,
            (durable_pipeline.tenant_id, repeated_id),
        )
        assert state is not None
        assert repeated_ledger is not None
        assert state.last_observed_at == durable_pipeline.observed_at + timedelta(seconds=2)
        assert repeated_ledger.processing_status is RfidEventProcessingStatus.PROCESSED
        assert repeated_ledger.disposition == "AMBIGUOUS"

    # Simulate a processor restart. This event is newer than the throttled item row
    # but older than the processed ledger watermark, so it must not regress state.
    late_at = repeated_at - timedelta(seconds=1)
    late_id = str(uuid.uuid4())
    late = CanonicalObservationBatchCreate(
        device_id=durable_pipeline.device_id,
        observations=[
            CanonicalObservationInput(
                event_id=late_id,
                epc=durable_pipeline.epc,
                observed_at=late_at,
                rssi=-42,
            )
        ],
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        late,
        received_at=repeated_at + timedelta(seconds=2),
    )
    assert event_worker.process_tenant_once(
        durable_pipeline.tenant_id,
        RecentObservationState(),
    ) == (1, 0)

    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        late_ledger = db.get(
            RfidObservationEventLedger,
            (durable_pipeline.tenant_id, late_id),
        )
        assert state is not None
        assert late_ledger is not None
        assert state.last_observed_at == durable_pipeline.observed_at + timedelta(seconds=2)
        assert late_ledger.disposition == "LATE"


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


def test_effective_epc_binding_controls_historical_and_current_sku_projection(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    effective_at = durable_pipeline.observed_at + timedelta(seconds=30)
    with postgres_session_factory() as db:
        original_sku = db.get(Sku, durable_pipeline.sku_id)
        assert original_sku is not None
        current_binding = db.scalar(
            select(EpcBinding).where(
                EpcBinding.tenant_id == durable_pipeline.tenant_id,
                EpcBinding.epc == durable_pipeline.epc,
                EpcBinding.effective_to.is_(None),
            )
        )
        tag = db.get(RfidTag, (durable_pipeline.tenant_id, durable_pipeline.epc))
        assert current_binding is not None
        assert tag is not None
        current_binding.effective_to = effective_at
        replacement_sku = Sku(
            tenant_id=durable_pipeline.tenant_id,
            product_style_id=original_sku.product_style_id,
            product_variant_id=original_sku.product_variant_id,
            code=f"{original_sku.code}-REBOUND",
            upc=str(uuid.uuid4().int)[:12],
            color=original_sku.color,
            size="L",
            attributes={},
            active=True,
        )
        db.add(replacement_sku)
        db.flush()
        db.add(
            EpcBinding(
                tenant_id=durable_pipeline.tenant_id,
                sku_id=replacement_sku.id,
                epc=durable_pipeline.epc,
                effective_from=effective_at,
                source_import_id=current_binding.source_import_id,
            )
        )
        tag.sku_id = replacement_sku.id
        db.commit()
        replacement_sku_id = replacement_sku.id

    recent = RecentObservationState()
    historical = _request(
        durable_pipeline,
        (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())),
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        historical,
        received_at=effective_at + timedelta(seconds=1),
    )
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (3, 1)

    with postgres_session_factory() as db:
        historical_state = db.get(
            CurrentItemState,
            (durable_pipeline.tenant_id, durable_pipeline.epc),
        )
        assert historical_state is not None
        assert historical_state.sku_id == durable_pipeline.sku_id

    current = CanonicalObservationBatchCreate(
        device_id=durable_pipeline.device_id,
        observations=[
            CanonicalObservationInput(
                event_id=str(uuid.uuid4()),
                epc=durable_pipeline.epc,
                observed_at=effective_at + timedelta(seconds=index),
                rssi=-41,
            )
            for index in range(1, 4)
        ],
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        current,
        received_at=effective_at + timedelta(seconds=5),
    )
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (3, 1)

    with postgres_session_factory() as db:
        current_state = db.get(
            CurrentItemState,
            (durable_pipeline.tenant_id, durable_pipeline.epc),
        )
        assert current_state is not None
        assert current_state.sku_id == replacement_sku_id
        assert current_state.state_version == 2
        old_projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        new_projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                replacement_sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert old_projection is not None and old_projection.quantity == 0
        assert new_projection is not None and new_projection.quantity == 1


def test_equal_timestamp_reads_can_confirm_a_stable_zone_move(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
) -> None:
    recent = RecentObservationState()
    initial = _request(
        durable_pipeline,
        (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())),
    )
    _accept(
        postgres_session_factory,
        durable_pipeline,
        initial,
        received_at=durable_pipeline.observed_at + timedelta(seconds=4),
    )
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (3, 1)

    move_at = durable_pipeline.observed_at + timedelta(seconds=30)
    backroom_zone_id = uuid.uuid4()
    backroom_device_id = uuid.uuid4()
    with postgres_session_factory() as db:
        db.add_all(
            [
                Zone(
                    id=backroom_zone_id,
                    tenant_id=durable_pipeline.tenant_id,
                    store_id=durable_pipeline.store_id,
                    code="backroom",
                    name="Backroom",
                    kind=ZoneKind.BACKROOM,
                ),
                Device(
                    id=backroom_device_id,
                    tenant_id=durable_pipeline.tenant_id,
                    serial_number=f"backroom-{uuid.uuid4().hex[:12]}",
                    display_name="Backroom Reader",
                    status=DeviceStatus.ACTIVE,
                ),
            ]
        )
        db.flush()
        db.add(
            DeviceAssignment(
                tenant_id=durable_pipeline.tenant_id,
                device_id=backroom_device_id,
                store_id=durable_pipeline.store_id,
                zone_id=backroom_zone_id,
                effective_from=move_at - timedelta(seconds=1),
            )
        )
        db.commit()

    move_request = CanonicalObservationBatchCreate(
        device_id=backroom_device_id,
        observations=[
            CanonicalObservationInput(
                event_id=str(uuid.uuid4()),
                epc=durable_pipeline.epc,
                observed_at=move_at,
                rssi=-39,
            )
            for _ in range(3)
        ],
    )
    with postgres_session_factory() as db:
        device = db.get(Device, backroom_device_id)
        assert device is not None
        accept_observation_batch(
            db,
            device=device,
            request=move_request,
            received_at=move_at + timedelta(seconds=1),
        )
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (3, 1)

    with postgres_session_factory() as db:
        state = db.get(CurrentItemState, (durable_pipeline.tenant_id, durable_pipeline.epc))
        floor = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        backroom = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                backroom_zone_id,
            ),
        )
        assert state is not None
        assert state.zone_id == backroom_zone_id
        assert state.state_version == 2
        assert floor is not None and floor.quantity == 0
        assert backroom is not None and backroom.quantity == 1


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
        other_tenant_id = uuid.uuid4()
        other_store_id = uuid.uuid4()
        other_zone_id = uuid.uuid4()
        other_device_id = uuid.uuid4()
        other_batch_id = uuid.uuid4()
        db.add(
            Tenant(
                id=other_tenant_id,
                code=f"quarantine-{other_tenant_id.hex}",
                name="Other quarantine tenant",
                status=TenantStatus.ACTIVE,
            )
        )
        db.flush()
        db.add_all(
            [
                Store(
                    id=other_store_id,
                    tenant_id=other_tenant_id,
                    code="other-store",
                    name="Other store",
                    timezone="UTC",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                Device(
                    id=other_device_id,
                    tenant_id=other_tenant_id,
                    serial_number=f"OTHER-{other_tenant_id.hex}",
                    display_name="Other device",
                    status=DeviceStatus.ACTIVE,
                ),
            ]
        )
        db.flush()
        db.add(
            Zone(
                id=other_zone_id,
                tenant_id=other_tenant_id,
                store_id=other_store_id,
                code="other-zone",
                name="Other zone",
                kind=ZoneKind.SALES_FLOOR,
            )
        )
        db.flush()
        db.add(
            RfidObservationBatch(
                id=other_batch_id,
                tenant_id=other_tenant_id,
                device_id=other_device_id,
                store_id=other_store_id,
                zone_id=other_zone_id,
                status=ObservationBatchStatus.COMPLETED_WITH_ERRORS,
                accepted_count=1,
                processed_count=0,
                rejected_count=1,
                received_at=received_at,
                completed_at=received_at,
            )
        )
        db.flush()
        db.add(
            RfidQuarantine(
                tenant_id=other_tenant_id,
                batch_id=other_batch_id,
                event_id=event_id,
                reason="OTHER_TENANT_EVENT",
                payload={"tenant_marker": "other"},
            )
        )
        db.commit()
        try:
            page = list_rfid_quarantine_endpoint(
                db,
                _tenant_admin(durable_pipeline),
                event_id=event_id,
            )
            assert page.total == 1
            assert page.items[0].event_id == event_id
            assert page.items[0].reason == "UNKNOWN_EPC"
            assert page.items[0].payload.get("tenant_marker") is None
            store_reader = Principal(
                user_id=uuid.uuid4(),
                tenant_id=durable_pipeline.tenant_id,
                email="store-reader@example.test",
                display_name="Store reader",
                role_scopes=(RoleScope(IdentityRole.STORE_ASSOCIATE, durable_pipeline.store_id),),
            )
            with pytest.raises(ApiError) as forbidden:
                list_rfid_quarantine_endpoint(db, store_reader)
            assert forbidden.value.status_code == 403
            assert forbidden.value.code == "tenant_inventory_scope_required"
        finally:
            db.execute(delete(Tenant).where(Tenant.id == other_tenant_id))
            db.commit()

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


def test_worker_quarantines_exhausted_poison_event_and_continues(
    postgres_session_factory: sessionmaker[Session],
    durable_pipeline: DurablePipelineFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    poison_event_id = str(uuid.uuid4())
    later_event_ids = tuple(str(uuid.uuid4()) for _ in range(3))
    request = _request(durable_pipeline, (poison_event_id, *later_event_ids))
    received_at = durable_pipeline.observed_at + timedelta(seconds=5)
    first_batch_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        request,
        received_at=received_at,
    )
    retry_batch_id = _accept(
        postgres_session_factory,
        durable_pipeline,
        _request(durable_pipeline, (poison_event_id,)),
        received_at=received_at + timedelta(seconds=1),
    )

    with postgres_session_factory() as db:
        poison = db.scalar(
            select(RfidObservationOutbox).where(
                RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                RfidObservationOutbox.event_id == poison_event_id,
            )
        )
        assert poison is not None
        poison.payload = {**poison.payload, "unexpected_poison_field": True}
        db.commit()

    monkeypatch.setattr(
        event_worker,
        "get_settings",
        lambda: Settings(worker_max_attempts=2),
    )
    recent = RecentObservationState()

    # Attempts below the configured limit stay pending and retain head-of-line order.
    with pytest.raises(ValidationError):
        event_worker.process_tenant_once(durable_pipeline.tenant_id, recent)

    with postgres_session_factory() as db:
        poison = db.scalar(
            select(RfidObservationOutbox).where(
                RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                RfidObservationOutbox.event_id == poison_event_id,
            )
        )
        ledger = db.get(
            RfidObservationEventLedger,
            (durable_pipeline.tenant_id, poison_event_id),
        )
        assert poison is not None
        assert poison.published_at is None
        assert poison.publish_attempts == 1
        assert ledger is not None
        assert ledger.processing_status is RfidEventProcessingStatus.PENDING
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationOutbox)
                .where(
                    RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                    RfidObservationOutbox.event_id.in_(later_event_ids),
                    RfidObservationOutbox.published_at.is_not(None),
                )
            )
            == 0
        )

    # The limit attempt terminally rejects the poison row, then drains later events.
    assert event_worker.process_tenant_once(durable_pipeline.tenant_id, recent) == (4, 1)

    with postgres_session_factory() as db:
        poison = db.scalar(
            select(RfidObservationOutbox).where(
                RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                RfidObservationOutbox.event_id == poison_event_id,
            )
        )
        ledger = db.get(
            RfidObservationEventLedger,
            (durable_pipeline.tenant_id, poison_event_id),
        )
        quarantine = db.scalar(
            select(RfidQuarantine).where(
                RfidQuarantine.tenant_id == durable_pipeline.tenant_id,
                RfidQuarantine.event_id == poison_event_id,
            )
        )
        assert poison is not None
        assert poison.published_at is not None
        assert poison.publish_attempts == 2
        assert poison.last_error is not None
        assert poison.last_error.startswith("ValidationError:")
        assert ledger is not None
        assert ledger.processing_status is RfidEventProcessingStatus.REJECTED
        assert ledger.disposition == "QUARANTINED"
        assert ledger.rejection_reason == event_worker.PROCESSING_ATTEMPTS_EXHAUSTED
        assert quarantine is not None
        assert quarantine.reason == event_worker.PROCESSING_ATTEMPTS_EXHAUSTED
        assert quarantine.payload["unexpected_poison_field"] is True

        for batch_id, expected_counts in (
            (first_batch_id, (4, 3, 1)),
            (retry_batch_id, (1, 0, 1)),
        ):
            batch = db.get(RfidObservationBatch, batch_id)
            assert batch is not None
            assert batch.status is ObservationBatchStatus.COMPLETED_WITH_ERRORS
            assert (
                batch.accepted_count,
                batch.processed_count,
                batch.rejected_count,
            ) == expected_counts
            assert batch.pending_count == 0
            assert batch.completed_at is not None

        poison_links = list(
            db.scalars(
                select(RfidObservationBatchEvent).where(
                    RfidObservationBatchEvent.tenant_id == durable_pipeline.tenant_id,
                    RfidObservationBatchEvent.event_id == poison_event_id,
                )
            ).all()
        )
        assert len(poison_links) == 2
        assert all(
            link.processing_status is RfidEventProcessingStatus.REJECTED
            and link.rejection_reason == event_worker.PROCESSING_ATTEMPTS_EXHAUSTED
            for link in poison_links
        )
        assert (
            db.scalar(
                select(func.count())
                .select_from(RfidObservationOutbox)
                .where(
                    RfidObservationOutbox.tenant_id == durable_pipeline.tenant_id,
                    RfidObservationOutbox.event_id.in_(later_event_ids),
                    RfidObservationOutbox.published_at.is_not(None),
                )
            )
            == 3
        )
        state = db.get(
            CurrentItemState,
            (durable_pipeline.tenant_id, durable_pipeline.epc),
        )
        projection = db.get(
            InventoryProjection,
            (
                durable_pipeline.tenant_id,
                durable_pipeline.store_id,
                durable_pipeline.sku_id,
                durable_pipeline.zone_id,
            ),
        )
        assert state is not None
        assert state.state_version == 1
        assert projection is not None
        assert projection.quantity == 1
