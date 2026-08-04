"""PostgreSQL-backed event worker used by the submitted hosted scope."""

import signal
import socket
import time
import uuid
from datetime import UTC, datetime

import structlog
from pydantic import ValidationError
from sqlalchemy import select, text
from sqlalchemy.orm import Session

from abacus.config import get_settings
from abacus.db import SessionLocal, tenant_session_scope
from abacus.events.inventory import InventoryDeltaEvent
from abacus.events.rfid import RfidObservationEvent
from abacus.logging import configure_logging
from abacus.models.architecture import (
    CurrentItemState,
    FreshnessStatus,
    InventoryTransitionOutbox,
    RfidEventProcessingStatus,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidObservationOutbox,
    StoreConnectivity,
)
from abacus.services.streaming_inventory import (
    RecentObservationState,
    apply_inventory_deltas,
    confirm_timed_out_removals,
    process_observation,
    quarantine_observation,
)

configure_logging()
logger = structlog.get_logger(__name__)
_stop_requested = False
PROCESSING_ATTEMPTS_EXHAUSTED = "PROCESSING_ATTEMPTS_EXHAUSTED"
TRANSITION_ATTEMPTS_EXHAUSTED = "TRANSITION_ATTEMPTS_EXHAUSTED"


def _request_stop(*_: object) -> None:
    global _stop_requested
    _stop_requested = True


def _active_tenants() -> list[uuid.UUID]:
    with SessionLocal() as db:
        return list(db.scalars(text("SELECT tenant_id FROM app_active_tenants()")))


def _terminal_event(
    db: Session,
    row: RfidObservationOutbox,
) -> RfidObservationEvent:
    """Recover the canonical event from its ledger if an outbox payload is malformed."""

    try:
        return RfidObservationEvent.model_validate(row.payload)
    except ValidationError as exc:
        # The ledger is written atomically with the outbox and contains the validated
        # immutable event fields. This fallback lets a schema-incompatible payload be
        # quarantined instead of blocking the tenant forever.
        ledger = db.scalar(
            select(RfidObservationEventLedger)
            .where(
                RfidObservationEventLedger.tenant_id == row.tenant_id,
                RfidObservationEventLedger.event_id == row.event_id,
            )
            .with_for_update()
        )
        batch_id = db.scalar(
            select(RfidObservationBatchEvent.batch_id)
            .where(
                RfidObservationBatchEvent.tenant_id == row.tenant_id,
                RfidObservationBatchEvent.event_id == row.event_id,
            )
            .order_by(RfidObservationBatchEvent.batch_id)
            .limit(1)
        )
        if ledger is None or batch_id is None:
            raise RuntimeError("RFID poison event has no durable ledger or batch link") from exc
        return RfidObservationEvent(
            tenant_id=ledger.tenant_id,
            batch_id=batch_id,
            event_id=ledger.event_id,
            device_id=ledger.device_id,
            store_id=ledger.store_id,
            zone_id=ledger.zone_id,
            epc=ledger.epc,
            observed_at=ledger.observed_at,
            received_at=ledger.first_received_at,
            rssi=ledger.rssi,
            antenna_id=ledger.antenna_id,
            reader_health=ledger.reader_health,
            is_buffered=ledger.is_buffered,
            backlog_drained=ledger.backlog_drained,
            reader_coverage_ok=ledger.reader_coverage_ok,
        )


def _transition_store_ids(
    db: Session,
    transition: InventoryTransitionOutbox,
) -> set[uuid.UUID]:
    """Best-effort blast-radius recovery for a schema-invalid transition."""

    store_ids: set[uuid.UUID] = set()
    if isinstance(transition.deltas, list):
        for payload in transition.deltas:
            if not isinstance(payload, dict):
                continue
            try:
                store_ids.add(uuid.UUID(str(payload["store_id"])))
            except (KeyError, TypeError, ValueError, AttributeError):
                continue
    state = db.get(CurrentItemState, (transition.tenant_id, transition.epc))
    if state is not None and state.store_id is not None:
        store_ids.add(state.store_id)
    return store_ids


def _validated_transition_deltas(
    transition: InventoryTransitionOutbox,
) -> list[InventoryDeltaEvent]:
    if not isinstance(transition.deltas, list) or not transition.deltas:
        raise ValueError("inventory transition must contain at least one delta")
    deltas = [InventoryDeltaEvent.model_validate(item) for item in transition.deltas]
    if any(item.transition_id != transition.transition_id for item in deltas):
        raise ValueError("inventory delta transition_id does not match its outbox row")
    if any(item.tenant_id != transition.tenant_id for item in deltas):
        raise ValueError("inventory delta tenant_id does not match its outbox row")
    if any(item.epc != transition.epc for item in deltas):
        raise ValueError("inventory delta EPC does not match its outbox row")
    if len({item.delta_id for item in deltas}) != len(deltas):
        raise ValueError("inventory transition contains duplicate delta IDs")
    return deltas


def _quarantine_transition(
    db: Session,
    transition: InventoryTransitionOutbox,
    *,
    error: Exception,
    quarantined_at: datetime,
) -> None:
    """Terminalize one poison delta and fail closed until projection rebuild."""

    transition.quarantined_at = quarantined_at
    transition.quarantine_reason = TRANSITION_ATTEMPTS_EXHAUSTED
    transition.last_error = f"{type(error).__name__}: {error}"[:2000]
    store_ids = _transition_store_ids(db, transition)
    connectivity_query = select(StoreConnectivity).where(
        StoreConnectivity.tenant_id == transition.tenant_id
    )
    if store_ids:
        connectivity_query = connectivity_query.where(StoreConnectivity.store_id.in_(store_ids))
    connectivity_rows = list(db.scalars(connectivity_query.with_for_update()).all())
    if store_ids and not connectivity_rows:
        # If the corrupt payload names no real store and current state cannot recover
        # one, the safe blast radius is the tenant rather than no degradation at all.
        connectivity_rows = list(
            db.scalars(
                select(StoreConnectivity)
                .where(StoreConnectivity.tenant_id == transition.tenant_id)
                .with_for_update()
            ).all()
        )
    for connectivity in connectivity_rows:
        connectivity.inventory_reconciliation_required_at = quarantined_at
        if connectivity.freshness_status == FreshnessStatus.LIVE:
            connectivity.freshness_status = FreshnessStatus.DEGRADED


def process_tenant_once(
    tenant_id: uuid.UUID,
    recent: RecentObservationState,
    *,
    limit: int = 250,
    sweep_removals: bool = True,
) -> tuple[int, int]:
    """Drain raw events, then their ordered inventory transitions in one tenant."""

    settings = get_settings()
    raw_count = 0
    transition_count = 0
    raw_error: Exception | None = None
    with tenant_session_scope(tenant_id) as db:
        raw_ids = list(
            db.scalars(
                select(RfidObservationOutbox.id)
                .where(
                    RfidObservationOutbox.tenant_id == tenant_id,
                    RfidObservationOutbox.published_at.is_(None),
                )
                .order_by(RfidObservationOutbox.acceptance_sequence)
                .limit(limit)
            ).all()
        )

    # Commit each raw event independently. A poison record cannot roll back earlier
    # progress, and the per-event checkpoint keeps processor memory consistent with
    # PostgreSQL when the current transaction fails.
    for raw_id in raw_ids:
        checkpoint = None
        try:
            with tenant_session_scope(tenant_id) as db:
                row = db.scalar(
                    select(RfidObservationOutbox)
                    .where(
                        RfidObservationOutbox.tenant_id == tenant_id,
                        RfidObservationOutbox.id == raw_id,
                        RfidObservationOutbox.published_at.is_(None),
                    )
                    .with_for_update()
                )
                if row is None:
                    continue
                event = RfidObservationEvent.model_validate(row.payload)
                checkpoint = recent.checkpoint(event)
                ledger = db.scalar(
                    select(RfidObservationEventLedger)
                    .where(
                        RfidObservationEventLedger.tenant_id == tenant_id,
                        RfidObservationEventLedger.event_id == row.event_id,
                    )
                    .with_for_update()
                )
                if ledger is None:
                    raise RuntimeError("RFID outbox has no event ledger")
                if ledger.processing_status == RfidEventProcessingStatus.PENDING:
                    process_observation(db, event, recent, settings)
                row.published_at = datetime.now(UTC)
                row.publish_attempts += 1
                row.last_error = None
                db.commit()
                raw_count += 1
        except Exception as exc:
            if checkpoint is not None:
                recent.restore(checkpoint)
            terminalized = False
            with tenant_session_scope(tenant_id) as db:
                failed_raw = db.scalar(
                    select(RfidObservationOutbox)
                    .where(
                        RfidObservationOutbox.tenant_id == tenant_id,
                        RfidObservationOutbox.id == raw_id,
                        RfidObservationOutbox.published_at.is_(None),
                    )
                    .with_for_update()
                )
                if failed_raw is not None:
                    failed_raw.publish_attempts += 1
                    failed_raw.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    if failed_raw.publish_attempts >= settings.worker_max_attempts:
                        failed_event = _terminal_event(db, failed_raw)
                        quarantine_observation(
                            db,
                            failed_event,
                            reason=PROCESSING_ATTEMPTS_EXHAUSTED,
                            payload=failed_raw.payload,
                        )
                        failed_raw.published_at = datetime.now(UTC)
                        terminalized = True
                    db.commit()
            if terminalized:
                raw_count += 1
                logger.warning(
                    "rfid_observation_attempts_exhausted",
                    tenant_id=str(tenant_id),
                    outbox_id=str(raw_id),
                    error=failed_raw.last_error if failed_raw is not None else None,
                )
                continue
            raw_error = exc
            break

    if sweep_removals:
        with tenant_session_scope(tenant_id) as db:
            confirm_timed_out_removals(
                db,
                tenant_id=tenant_id,
                settings=settings,
                limit=limit,
            )
            db.commit()

    # Each transition commits independently. A malformed payload is retried without
    # rolling back valid work or blocking unrelated items behind it.
    with tenant_session_scope(tenant_id) as db:
        transition_ids = list(
            db.scalars(
                select(InventoryTransitionOutbox.transition_id)
                .where(
                    InventoryTransitionOutbox.tenant_id == tenant_id,
                    InventoryTransitionOutbox.published_at.is_(None),
                    InventoryTransitionOutbox.quarantined_at.is_(None),
                )
                .order_by(
                    InventoryTransitionOutbox.created_at,
                    InventoryTransitionOutbox.state_version,
                    InventoryTransitionOutbox.transition_id,
                )
                .limit(limit)
            ).all()
        )
    for transition_id in transition_ids:
        try:
            with tenant_session_scope(tenant_id) as db:
                transition = db.scalar(
                    select(InventoryTransitionOutbox)
                    .where(
                        InventoryTransitionOutbox.tenant_id == tenant_id,
                        InventoryTransitionOutbox.transition_id == transition_id,
                        InventoryTransitionOutbox.published_at.is_(None),
                        InventoryTransitionOutbox.quarantined_at.is_(None),
                    )
                    .with_for_update(skip_locked=True)
                )
                if transition is None:
                    continue
                deltas = _validated_transition_deltas(transition)
                apply_inventory_deltas(db, deltas)
                transition.published_at = datetime.now(UTC)
                transition.publish_attempts += 1
                transition.last_error = None
                db.commit()
                transition_count += 1
        except Exception as exc:
            terminalized = False
            error_message = f"{type(exc).__name__}: {exc}"[:2000]
            with tenant_session_scope(tenant_id) as db:
                failed_transition = db.scalar(
                    select(InventoryTransitionOutbox)
                    .where(
                        InventoryTransitionOutbox.tenant_id == tenant_id,
                        InventoryTransitionOutbox.transition_id == transition_id,
                        InventoryTransitionOutbox.published_at.is_(None),
                        InventoryTransitionOutbox.quarantined_at.is_(None),
                    )
                    .with_for_update()
                )
                if failed_transition is not None:
                    failed_transition.publish_attempts += 1
                    failed_transition.last_error = error_message
                    if failed_transition.publish_attempts >= settings.worker_max_attempts:
                        _quarantine_transition(
                            db,
                            failed_transition,
                            error=exc,
                            quarantined_at=datetime.now(UTC),
                        )
                        terminalized = True
                    db.commit()
            logger.warning(
                "inventory_transition_failed",
                tenant_id=str(tenant_id),
                transition_id=str(transition_id),
                terminalized=terminalized,
                error=error_message,
            )
    if raw_error is not None:
        raise raw_error
    return raw_count, transition_count


def run() -> None:
    settings = get_settings()
    worker_id = f"event-{socket.gethostname()}-{uuid.uuid4()}"
    recent = RecentObservationState()
    next_removal_sweep: dict[uuid.UUID, float] = {}
    logger.info("event_worker_started", worker_id=worker_id)
    while not _stop_requested:
        processed = 0
        for tenant_id in _active_tenants():
            try:
                now = time.monotonic()
                sweep_removals = now >= next_removal_sweep.get(tenant_id, 0.0)
                raw_count, transition_count = process_tenant_once(
                    tenant_id,
                    recent,
                    sweep_removals=sweep_removals,
                )
                if sweep_removals:
                    next_removal_sweep[tenant_id] = (
                        now + settings.rfid_removal_sweep_interval_seconds
                    )
                processed += raw_count + transition_count
            except Exception:
                logger.exception("event_worker_tenant_failed", tenant_id=str(tenant_id))
        if processed == 0:
            time.sleep(settings.worker_poll_interval_ms / 1000)
    logger.info("event_worker_stopped", worker_id=worker_id)


def main() -> None:
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    run()


if __name__ == "__main__":
    main()
