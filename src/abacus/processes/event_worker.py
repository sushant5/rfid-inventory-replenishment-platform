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
    InventoryTransitionOutbox,
    RfidEventProcessingStatus,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidObservationOutbox,
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

    # A separate transaction makes item state visible before its derived projection.
    failed_transition_id: uuid.UUID | None = None
    try:
        with tenant_session_scope(tenant_id) as db:
            transitions = list(
                db.scalars(
                    select(InventoryTransitionOutbox)
                    .where(
                        InventoryTransitionOutbox.tenant_id == tenant_id,
                        InventoryTransitionOutbox.published_at.is_(None),
                    )
                    .order_by(
                        InventoryTransitionOutbox.created_at,
                        InventoryTransitionOutbox.state_version,
                    )
                    .with_for_update()
                    .limit(limit)
                ).all()
            )
            for transition in transitions:
                failed_transition_id = transition.transition_id
                deltas = [InventoryDeltaEvent.model_validate(item) for item in transition.deltas]
                apply_inventory_deltas(db, deltas)
                transition.published_at = datetime.now(UTC)
                transition.publish_attempts += 1
                transition.last_error = None
                transition_count += 1
            db.commit()
    except Exception as exc:
        if failed_transition_id is not None:
            with tenant_session_scope(tenant_id) as db:
                failed_transition = db.scalar(
                    select(InventoryTransitionOutbox)
                    .where(
                        InventoryTransitionOutbox.tenant_id == tenant_id,
                        InventoryTransitionOutbox.transition_id == failed_transition_id,
                        InventoryTransitionOutbox.published_at.is_(None),
                    )
                    .with_for_update()
                )
                if failed_transition is not None:
                    failed_transition.publish_attempts += 1
                    failed_transition.last_error = f"{type(exc).__name__}: {exc}"[:2000]
                    db.commit()
        raise
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
