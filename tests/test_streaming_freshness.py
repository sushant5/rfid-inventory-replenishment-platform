import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.api.routes.rfid import get_item_state_endpoint, list_rfid_quarantine_endpoint
from abacus.config import Settings
from abacus.enums import DeviceStatus
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import CurrentItemState, FreshnessStatus, StoreConnectivity
from abacus.models.catalog import EpcBinding, Sku
from abacus.models.identity import IdentityRole
from abacus.models.tenancy import Device, DeviceAssignment
from abacus.security import Principal, RoleScope
from abacus.services import streaming_inventory
from abacus.services.streaming_inventory import (
    ProcessingResult,
    RecentObservationState,
    _resolve_effective_assignment,
    effective_bucket_confidence,
    effective_freshness,
    effective_item_confidence,
    process_observation,
)


@pytest.mark.parametrize(
    ("projected_quantity", "current_item_count", "current_confidence", "expected"),
    [
        (0, None, None, 1.0),
        (1, None, None, 0.0),
        (2, 1, 0.9, 0.0),
        (2, 2, 0.9, 0.9),
    ],
)
def test_bucket_confidence_fails_safe_while_projection_quantity_lags(
    projected_quantity: int,
    current_item_count: int | None,
    current_confidence: float | None,
    expected: float,
) -> None:
    assert (
        effective_bucket_confidence(
            projected_quantity=projected_quantity,
            current_item_count=current_item_count,
            current_confidence=current_confidence,
        )
        == expected
    )


def _settings() -> Settings:
    return Settings(
        connectivity_live_window_seconds=120,
        connectivity_stale_window_seconds=600,
        rfid_move_confirmation_reads=1,
        rfid_last_seen_flush_seconds=30,
        rfid_max_future_skew_seconds=300,
    )


def _connectivity(now: datetime) -> StoreConnectivity:
    return StoreConnectivity(
        tenant_id=uuid.uuid4(),
        store_id=uuid.uuid4(),
        gateway_last_heartbeat=now,
        last_live_event_at=now,
        backlog_drained=True,
        reader_coverage_ok=True,
        freshness_status=FreshnessStatus.LIVE,
    )


def _event(
    *,
    observed_at: datetime,
    received_at: datetime,
    store_id: uuid.UUID | None = None,
    zone_id: uuid.UUID | None = None,
) -> RfidObservationEvent:
    return RfidObservationEvent(
        tenant_id=uuid.uuid4(),
        batch_id=uuid.uuid4(),
        event_id=str(uuid.uuid4()),
        device_id=uuid.uuid4(),
        store_id=store_id or uuid.uuid4(),
        zone_id=zone_id or uuid.uuid4(),
        epc="303400000000000000000001",
        observed_at=observed_at,
        received_at=received_at,
        rssi=-42,
    )


def test_batch_finalization_requires_a_durable_event_ledger() -> None:
    now = datetime.now(UTC)
    db = MagicMock(spec=Session)
    db.scalar.return_value = None

    with pytest.raises(ValueError, match="RFID event ledger does not exist"):
        streaming_inventory._advance_batch(
            db,
            _event(observed_at=now, received_at=now),
            rejected=False,
        )


def test_effective_freshness_decays_without_a_database_write() -> None:
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)
    settings = _settings()
    connectivity = _connectivity(now)

    assert effective_freshness(connectivity, settings, now=now) == FreshnessStatus.LIVE
    assert (
        effective_freshness(connectivity, settings, now=now + timedelta(seconds=121))
        == FreshnessStatus.DEGRADED
    )
    assert (
        effective_freshness(connectivity, settings, now=now + timedelta(seconds=601))
        == FreshnessStatus.STALE
    )


def test_effective_freshness_fails_closed_until_inventory_reconciliation() -> None:
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)
    connectivity = _connectivity(now)
    connectivity.inventory_reconciliation_required_at = now

    assert effective_freshness(connectivity, _settings(), now=now) == FreshnessStatus.DEGRADED

    # A connectivity outage remains the stronger signal.
    connectivity.gateway_last_heartbeat = now - timedelta(minutes=11)
    assert effective_freshness(connectivity, _settings(), now=now) == FreshnessStatus.STALE


def test_item_confidence_decays_without_a_database_write() -> None:
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)

    assert effective_item_confidence(
        stored_confidence=0.8,
        last_observed_at=now,
        evaluated_at=now,
        half_life_seconds=1800,
    ) == pytest.approx(0.8)
    assert effective_item_confidence(
        stored_confidence=0.8,
        last_observed_at=now - timedelta(minutes=30),
        evaluated_at=now,
        half_life_seconds=1800,
    ) == pytest.approx(0.4)
    assert effective_item_confidence(
        stored_confidence=0.8,
        last_observed_at=now - timedelta(minutes=60),
        evaluated_at=now,
        half_life_seconds=1800,
    ) == pytest.approx(0.2)
    assert effective_item_confidence(
        stored_confidence=0.8,
        last_observed_at=now + timedelta(minutes=1),
        evaluated_at=now,
        half_life_seconds=1800,
    ) == pytest.approx(0.8)


def test_effective_freshness_requires_drained_backlog_and_healthy_coverage() -> None:
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)
    connectivity = _connectivity(now)
    connectivity.backlog_drained = False
    assert effective_freshness(connectivity, _settings(), now=now) == FreshnessStatus.STALE

    connectivity.backlog_drained = True
    connectivity.reader_coverage_ok = False
    assert effective_freshness(connectivity, _settings(), now=now) == FreshnessStatus.DEGRADED

    connectivity.reader_coverage_ok = True
    connectivity.oldest_buffered_event_at = now - timedelta(minutes=1)
    assert effective_freshness(connectivity, _settings(), now=now) == FreshnessStatus.STALE


def test_assignment_is_resolved_at_event_time_and_payload_mismatch_is_rejected() -> None:
    observed_at = datetime(2026, 8, 2, 12, tzinfo=UTC)
    event = _event(observed_at=observed_at, received_at=observed_at)
    device = Device(
        id=event.device_id,
        tenant_id=event.tenant_id,
        serial_number="READER-1",
        display_name="Reader 1",
        status=DeviceStatus.ACTIVE,
    )
    assignment = DeviceAssignment(
        tenant_id=event.tenant_id,
        device_id=event.device_id,
        store_id=uuid.uuid4(),
        zone_id=uuid.uuid4(),
        effective_from=observed_at - timedelta(days=1),
    )
    db = MagicMock(spec=Session)
    db.scalar.side_effect = [device, assignment]

    resolved, error = _resolve_effective_assignment(db, event)

    assert resolved is None
    assert error == "DEVICE_ASSIGNMENT_MISMATCH"


def test_inactive_device_is_rejected_before_assignment_resolution() -> None:
    now = datetime(2026, 8, 2, 12, tzinfo=UTC)
    event = _event(observed_at=now, received_at=now)
    device = Device(
        id=event.device_id,
        tenant_id=event.tenant_id,
        serial_number="READER-1",
        display_name="Reader 1",
        status=DeviceStatus.INACTIVE,
    )
    db = MagicMock(spec=Session)
    db.scalar.return_value = device

    resolved, error = _resolve_effective_assignment(db, event)

    assert resolved is None
    assert error == "INACTIVE_DEVICE"
    assert db.scalar.call_count == 1


def test_future_observation_is_quarantined_before_connectivity_or_assignment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    received_at = datetime.now(UTC)
    event = _event(
        observed_at=received_at + timedelta(seconds=301),
        received_at=received_at,
    )
    quarantined: list[str] = []

    def fake_quarantine(
        _db: Session,
        _event: RfidObservationEvent,
        *,
        reason: str,
    ) -> ProcessingResult:
        quarantined.append(reason)
        return ProcessingResult("QUARANTINED", reason=reason)

    monkeypatch.setattr(streaming_inventory, "_quarantine", fake_quarantine)
    monkeypatch.setattr(
        streaming_inventory,
        "_resolve_effective_assignment",
        MagicMock(side_effect=AssertionError("assignment must not be resolved")),
    )
    monkeypatch.setattr(
        streaming_inventory,
        "_update_connectivity",
        MagicMock(side_effect=AssertionError("connectivity must not be updated")),
    )

    result = process_observation(
        MagicMock(spec=Session), event, RecentObservationState(), _settings()
    )

    assert result.disposition == "QUARANTINED"
    assert quarantined == ["OBSERVED_AT_TOO_FAR_IN_FUTURE"]


@pytest.mark.parametrize(("event_offset_seconds", "should_flush"), [(10, False), (31, True)])
def test_same_zone_read_throttles_current_item_last_seen_refresh(
    monkeypatch: pytest.MonkeyPatch,
    event_offset_seconds: int,
    should_flush: bool,
) -> None:
    base = datetime.now(UTC) - timedelta(minutes=1)
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    event = _event(
        observed_at=base + timedelta(seconds=event_offset_seconds),
        received_at=base + timedelta(seconds=event_offset_seconds),
        store_id=store_id,
        zone_id=zone_id,
    )
    assignment = DeviceAssignment(
        tenant_id=event.tenant_id,
        device_id=event.device_id,
        store_id=store_id,
        zone_id=zone_id,
        effective_from=base - timedelta(days=1),
    )
    state = CurrentItemState(
        tenant_id=event.tenant_id,
        epc=event.epc,
        sku_id=uuid.uuid4(),
        store_id=store_id,
        zone_id=zone_id,
        last_observed_at=base,
        last_received_at=base,
        confidence=0.9,
        state_version=1,
    )
    binding = MagicMock(spec=EpcBinding, sku_id=state.sku_id)
    db = MagicMock(spec=Session)
    db.scalar.side_effect = [binding, state, base]
    monkeypatch.setattr(
        streaming_inventory,
        "_resolve_effective_assignment",
        MagicMock(return_value=(assignment, None)),
    )
    monkeypatch.setattr(streaming_inventory, "_update_connectivity", MagicMock())
    monkeypatch.setattr(streaming_inventory, "_advance_batch", MagicMock())

    result = process_observation(db, event, RecentObservationState(), _settings())

    assert result.disposition == "PROCESSED"
    expected_last_seen = event.observed_at if should_flush else base
    assert state.last_observed_at == expected_last_seen
    assert state.last_received_at == expected_last_seen


def test_same_zone_read_after_worker_restart_preserves_confirmed_confidence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = datetime.now(UTC) - timedelta(minutes=1)
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    event = _event(
        observed_at=base + timedelta(seconds=10),
        received_at=base + timedelta(seconds=10),
        store_id=store_id,
        zone_id=zone_id,
    )
    assignment = DeviceAssignment(
        tenant_id=event.tenant_id,
        device_id=event.device_id,
        store_id=store_id,
        zone_id=zone_id,
        effective_from=base - timedelta(days=1),
    )
    state = CurrentItemState(
        tenant_id=event.tenant_id,
        epc=event.epc,
        sku_id=uuid.uuid4(),
        store_id=store_id,
        zone_id=zone_id,
        last_observed_at=base,
        last_received_at=base,
        confidence=0.9,
        state_version=1,
    )
    binding = MagicMock(spec=EpcBinding, sku_id=state.sku_id)
    db = MagicMock(spec=Session)
    db.scalar.side_effect = [binding, state, base]
    monkeypatch.setattr(
        streaming_inventory,
        "_resolve_effective_assignment",
        MagicMock(return_value=(assignment, None)),
    )
    monkeypatch.setattr(streaming_inventory, "_update_connectivity", MagicMock())
    monkeypatch.setattr(streaming_inventory, "_advance_batch", MagicMock())
    settings = _settings()
    settings.rfid_move_confirmation_reads = 3

    result = process_observation(db, event, RecentObservationState(), settings)

    assert result.disposition == "AMBIGUOUS"
    assert state.confidence == 0.9


def test_rebuilt_stable_window_recovers_confidence_before_last_seen_flush(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = datetime.now(UTC) - timedelta(minutes=1)
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    event = _event(
        observed_at=base + timedelta(seconds=10),
        received_at=base + timedelta(seconds=10),
        store_id=store_id,
        zone_id=zone_id,
    )
    assignment = DeviceAssignment(
        tenant_id=event.tenant_id,
        device_id=event.device_id,
        store_id=store_id,
        zone_id=zone_id,
        effective_from=base - timedelta(days=1),
    )
    state = CurrentItemState(
        tenant_id=event.tenant_id,
        epc=event.epc,
        sku_id=uuid.uuid4(),
        store_id=store_id,
        zone_id=zone_id,
        last_observed_at=base + timedelta(seconds=5),
        last_received_at=base + timedelta(seconds=5),
        confidence=0.49,
        state_version=1,
    )
    binding = MagicMock(spec=EpcBinding, sku_id=state.sku_id)
    db = MagicMock(spec=Session)
    db.scalar.side_effect = [binding, state, state.last_observed_at]
    monkeypatch.setattr(
        streaming_inventory,
        "_resolve_effective_assignment",
        MagicMock(return_value=(assignment, None)),
    )
    monkeypatch.setattr(streaming_inventory, "_update_connectivity", MagicMock())
    monkeypatch.setattr(streaming_inventory, "_advance_batch", MagicMock())
    settings = _settings()
    settings.rfid_move_confirmation_reads = 3
    recent = RecentObservationState()
    for offset in (8, 9):
        prior = event.model_copy(
            update={
                "event_id": str(uuid.uuid4()),
                "observed_at": base + timedelta(seconds=offset),
                "received_at": base + timedelta(seconds=offset),
            }
        )
        recent.add(prior, settings.rfid_move_confirmation_window_seconds)

    result = process_observation(db, event, recent, settings)

    assert result.disposition == "PROCESSED"
    assert state.confidence > 0.7
    assert state.last_received_at == base + timedelta(seconds=5)


def test_unlocated_item_requires_tenant_wide_inventory_permission() -> None:
    tenant_id = uuid.uuid4()
    item = CurrentItemState(
        tenant_id=tenant_id,
        epc="303400000000000000000001",
        sku_id=uuid.uuid4(),
        store_id=None,
        zone_id=None,
        last_observed_at=datetime.now(UTC),
        last_received_at=datetime.now(UTC),
        confidence=0.8,
        state_version=2,
    )
    sku = MagicMock(spec=Sku, code="SKU-1")
    db = MagicMock(spec=Session)
    db.execute.return_value.one_or_none.return_value = (item, sku)
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="associate@example.com",
        display_name="Associate",
        role_scopes=(RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()),),
    )

    with pytest.raises(ApiError) as error:
        get_item_state_endpoint(item.epc, db, _settings(), principal)

    assert error.value.code == "tenant_inventory_scope_required"


def test_invalid_item_epc_is_a_client_error() -> None:
    db = MagicMock(spec=Session)
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        email="admin@example.com",
        display_name="Administrator",
        role_scopes=(RoleScope(IdentityRole.CORPORATE_ADMIN, None),),
    )

    with pytest.raises(ApiError) as error:
        get_item_state_endpoint("not-an-epc", db, _settings(), principal)

    assert error.value.status_code == 422
    assert error.value.code == "invalid_epc"
    db.execute.assert_not_called()


def test_store_scoped_user_cannot_inspect_tenant_quarantine() -> None:
    db = MagicMock(spec=Session)
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        email="associate@example.com",
        display_name="Associate",
        role_scopes=(RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()),),
    )

    with pytest.raises(ApiError) as error:
        list_rfid_quarantine_endpoint(db, principal)

    assert error.value.status_code == 403
    assert error.value.code == "tenant_inventory_scope_required"
    db.scalar.assert_not_called()
