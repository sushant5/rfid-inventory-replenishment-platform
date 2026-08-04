import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest
from sqlalchemy.orm import Session, sessionmaker

from abacus.api.errors import ApiError
from abacus.api.routes.rfid import get_observation_batch_endpoint
from abacus.enums import DeviceStatus, StoreStatus, TenantStatus, ZoneKind
from abacus.models.architecture import (
    CanonicalIdentityRole,
    ObservationBatchStatus,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
)
from abacus.models.tenancy import Device, DeviceAssignment, Store, Tenant, Zone
from abacus.security import Principal, RoleScope


def _store_principal(tenant_id: uuid.UUID, store_id: uuid.UUID) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="associate@orange.example",
        display_name="Store Associate",
        role_scopes=(RoleScope(CanonicalIdentityRole.STORE_ASSOCIATE, store_id),),
        canonical_roles=(CanonicalIdentityRole.STORE_ASSOCIATE,),
        assigned_store_ids=(store_id,),
    )


def _tenant_principal(tenant_id: uuid.UUID) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="corporate@orange.example",
        display_name="Corporate User",
        role_scopes=(RoleScope(CanonicalIdentityRole.CORPORATE_USER, None),),
        canonical_roles=(CanonicalIdentityRole.CORPORATE_USER,),
    )


def _batch(tenant_id: uuid.UUID, store_id: uuid.UUID) -> RfidObservationBatch:
    return RfidObservationBatch(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        device_id=uuid.uuid4(),
        store_id=store_id,
        zone_id=uuid.uuid4(),
        status=ObservationBatchStatus.COMPLETED,
        accepted_count=2,
        processed_count=2,
        rejected_count=0,
        received_at=datetime.now(UTC),
    )


def test_batch_status_requires_access_to_every_resolved_event_store() -> None:
    tenant_id = uuid.uuid4()
    assigned_store_id = uuid.uuid4()
    other_store_id = uuid.uuid4()
    batch = _batch(tenant_id, assigned_store_id)
    db = Mock(spec=Session)
    db.scalar.return_value = batch
    db.scalars.return_value.all.return_value = [assigned_store_id, other_store_id]

    with pytest.raises(ApiError) as caught:
        get_observation_batch_endpoint(
            batch.id,
            db,
            _store_principal(tenant_id, assigned_store_id),
        )

    assert caught.value.status_code == 403


def test_batch_status_allows_store_scoped_user_when_all_events_match() -> None:
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    batch = _batch(tenant_id, store_id)
    db = Mock(spec=Session)
    db.scalar.return_value = batch
    db.scalars.return_value.all.return_value = [store_id]

    result = get_observation_batch_endpoint(
        batch.id,
        db,
        _store_principal(tenant_id, store_id),
    )

    assert result.batch_id == batch.id
    assert result.processed == 2
    assert result.pending == 0


@pytest.mark.integration
def test_batch_status_sql_resolves_stores_across_effective_dated_device_move(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    now = datetime.now(UTC)
    moved_at = now - timedelta(minutes=1)
    tenant_id = uuid.uuid4()
    first_store_id = uuid.uuid4()
    second_store_id = uuid.uuid4()
    first_zone_id = uuid.uuid4()
    second_zone_id = uuid.uuid4()
    device_id = uuid.uuid4()
    batch_id = uuid.uuid4()
    suffix = uuid.uuid4().hex[:12]

    with postgres_session_factory() as db:
        db.add(
            Tenant(
                id=tenant_id,
                code=f"batch-auth-{suffix}",
                name="Batch authorization tenant",
                status=TenantStatus.ACTIVE,
            )
        )
        db.flush()
        db.add_all(
            [
                Store(
                    id=first_store_id,
                    tenant_id=tenant_id,
                    code=f"first-{suffix}",
                    name="First store",
                    timezone="UTC",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                Store(
                    id=second_store_id,
                    tenant_id=tenant_id,
                    code=f"second-{suffix}",
                    name="Second store",
                    timezone="UTC",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
            ]
        )
        db.flush()
        db.add_all(
            [
                Zone(
                    id=first_zone_id,
                    tenant_id=tenant_id,
                    store_id=first_store_id,
                    code="floor",
                    name="First sales floor",
                    kind=ZoneKind.SALES_FLOOR,
                ),
                Zone(
                    id=second_zone_id,
                    tenant_id=tenant_id,
                    store_id=second_store_id,
                    code="floor",
                    name="Second sales floor",
                    kind=ZoneKind.SALES_FLOOR,
                ),
            ]
        )
        db.add(
            Device(
                id=device_id,
                tenant_id=tenant_id,
                serial_number=f"BATCH-AUTH-{suffix}",
                display_name="Moved reader",
                status=DeviceStatus.ACTIVE,
            )
        )
        db.flush()
        db.add_all(
            [
                DeviceAssignment(
                    tenant_id=tenant_id,
                    device_id=device_id,
                    store_id=first_store_id,
                    zone_id=first_zone_id,
                    effective_from=now - timedelta(days=1),
                    effective_to=moved_at,
                ),
                DeviceAssignment(
                    tenant_id=tenant_id,
                    device_id=device_id,
                    store_id=second_store_id,
                    zone_id=second_zone_id,
                    effective_from=moved_at,
                ),
            ]
        )
        db.add(
            RfidObservationBatch(
                id=batch_id,
                tenant_id=tenant_id,
                device_id=device_id,
                store_id=second_store_id,
                zone_id=second_zone_id,
                status=ObservationBatchStatus.COMPLETED,
                accepted_count=2,
                processed_count=2,
                rejected_count=0,
                received_at=now,
                completed_at=now,
            )
        )
        db.flush()

        event_rows = (
            ("before-move", first_store_id, first_zone_id, moved_at - timedelta(seconds=1)),
            ("after-move", second_store_id, second_zone_id, moved_at + timedelta(seconds=1)),
        )
        for event_id, store_id, zone_id, observed_at in event_rows:
            db.add(
                RfidObservationEventLedger(
                    tenant_id=tenant_id,
                    event_id=event_id,
                    payload_fingerprint=("a" if store_id == first_store_id else "b") * 64,
                    device_id=device_id,
                    store_id=store_id,
                    zone_id=zone_id,
                    epc=f"3034{uuid.uuid4().int % 10**20:020d}",
                    observed_at=observed_at,
                    first_received_at=now,
                    rssi=-42,
                    reader_health=1.0,
                    is_buffered=observed_at < moved_at,
                    backlog_drained=True,
                    reader_coverage_ok=True,
                    processing_status=RfidEventProcessingStatus.PROCESSED,
                    disposition="PROCESSED",
                    processed_at=now,
                )
            )
            db.flush()
            db.add(
                RfidObservationBatchEvent(
                    tenant_id=tenant_id,
                    batch_id=batch_id,
                    event_id=event_id,
                    processing_status=RfidEventProcessingStatus.PROCESSED,
                    disposition="PROCESSED",
                    finalized_at=now,
                )
            )
        db.flush()

        with pytest.raises(ApiError) as caught:
            get_observation_batch_endpoint(
                batch_id,
                db,
                _store_principal(tenant_id, second_store_id),
            )
        assert caught.value.status_code == 403

        result = get_observation_batch_endpoint(
            batch_id,
            db,
            _tenant_principal(tenant_id),
        )
        assert result.batch_id == batch_id
        assert result.processed == 2
        assert result.pending == 0

        db.rollback()
