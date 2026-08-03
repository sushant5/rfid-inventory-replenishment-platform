import hashlib
import json
import uuid
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import (
    FreshnessStatus,
    ObservationBatchStatus,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidObservationOutbox,
    StoreConnectivity,
)
from abacus.models.tenancy import Device, DeviceAssignment
from abacus.schemas.architecture import CanonicalObservationBatchCreate


def _assignment_unavailable() -> ApiError:
    return ApiError(
        409,
        "Device assignment unavailable",
        "The device has no store and zone assignment effective for an observation.",
        code="device_assignment_unavailable",
    )


def _load_effective_assignment_history(
    db: Session,
    device: Device,
    *,
    window_start: datetime,
    window_end: datetime,
) -> tuple[DeviceAssignment, ...]:
    """Load every assignment that can cover a timestamp in the batch window."""

    return tuple(
        db.scalars(
            select(DeviceAssignment)
            .where(
                DeviceAssignment.tenant_id == device.tenant_id,
                DeviceAssignment.device_id == device.id,
                DeviceAssignment.effective_from <= window_end,
                or_(
                    DeviceAssignment.effective_to.is_(None),
                    DeviceAssignment.effective_to > window_start,
                ),
            )
            .order_by(DeviceAssignment.effective_from.desc())
        ).all()
    )


def _resolve_effective_assignment(
    assignments: Sequence[DeviceAssignment],
    at: datetime,
) -> DeviceAssignment:
    """Resolve one timestamp using the database's half-open interval semantics."""

    for assignment in assignments:
        if assignment.effective_from <= at and (
            assignment.effective_to is None or assignment.effective_to > at
        ):
            return assignment
    raise _assignment_unavailable()


def observation_payload_fingerprint(event: RfidObservationEvent) -> str:
    """Hash the immutable, device-observed and server-resolved event fields.

    Receipt time and connectivity flags describe a delivery attempt, so they are
    intentionally excluded. A retry with the same event ID may arrive later, but it
    may not change the physical observation or its effective assignment.
    """

    observed_at = event.observed_at.astimezone(UTC).isoformat(timespec="microseconds")
    canonical_payload = {
        "antenna_id": event.antenna_id,
        "device_id": str(event.device_id),
        "epc": event.epc,
        "observed_at": observed_at,
        "rssi": float(event.rssi),
        "store_id": str(event.store_id),
        "zone_id": str(event.zone_id),
    }
    encoded = json.dumps(canonical_payload, separators=(",", ":"), sort_keys=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _set_initial_batch_outcome(
    batch: RfidObservationBatch,
    ledgers: list[RfidObservationEventLedger],
    *,
    received_at: datetime,
) -> None:
    batch.processed_count = sum(
        row.processing_status == RfidEventProcessingStatus.PROCESSED for row in ledgers
    )
    batch.rejected_count = sum(
        row.processing_status == RfidEventProcessingStatus.REJECTED for row in ledgers
    )
    accounted = batch.processed_count + batch.rejected_count
    if accounted == batch.accepted_count:
        batch.status = (
            ObservationBatchStatus.COMPLETED_WITH_ERRORS
            if batch.rejected_count
            else ObservationBatchStatus.COMPLETED
        )
        batch.completed_at = received_at
    elif accounted:
        batch.status = ObservationBatchStatus.PROCESSING


def accept_observation_batch(
    db: Session,
    *,
    device: Device,
    request: CanonicalObservationBatchCreate,
    received_at: datetime,
) -> tuple[RfidObservationBatch, list[RfidObservationEvent]]:
    """Atomically accept a batch, its event identities, links, and raw outbox rows."""

    if request.device_id != device.id:
        raise ApiError(
            403,
            "Device token mismatch",
            "The authenticated device does not match device_id.",
            code="device_token_mismatch",
        )

    try:
        assignment_times = [received_at, *(item.observed_at for item in request.observations)]
        assignments = _load_effective_assignment_history(
            db,
            device,
            window_start=min(assignment_times),
            window_end=max(assignment_times),
        )
        current_assignment = _resolve_effective_assignment(assignments, received_at)
        batch_id = uuid.uuid4()
        events: list[RfidObservationEvent] = []
        for observation in request.observations:
            assignment = _resolve_effective_assignment(assignments, observation.observed_at)
            buffered = received_at - observation.observed_at > timedelta(seconds=60)
            events.append(
                RfidObservationEvent(
                    tenant_id=device.tenant_id,
                    batch_id=batch_id,
                    event_id=observation.event_id,
                    device_id=device.id,
                    store_id=assignment.store_id,
                    zone_id=assignment.zone_id,
                    epc=observation.epc,
                    observed_at=observation.observed_at,
                    received_at=received_at,
                    rssi=observation.rssi,
                    antenna_id=observation.antenna_id,
                    reader_health=1.0 if request.reader_coverage_ok else 0.0,
                    is_buffered=buffered,
                    backlog_drained=request.backlog_drained,
                    reader_coverage_ok=request.reader_coverage_ok,
                )
            )

        batch = RfidObservationBatch(
            id=batch_id,
            tenant_id=device.tenant_id,
            device_id=device.id,
            store_id=current_assignment.store_id,
            zone_id=current_assignment.zone_id,
            status=ObservationBatchStatus.ACCEPTED,
            accepted_count=len(events),
            processed_count=0,
            rejected_count=0,
            received_at=received_at,
        )
        db.add(batch)
        db.flush()

        event_by_id = {event.event_id: event for event in events}
        ledger_values = [
            {
                "tenant_id": event.tenant_id,
                "event_id": event.event_id,
                "payload_fingerprint": observation_payload_fingerprint(event),
                "device_id": event.device_id,
                "store_id": event.store_id,
                "zone_id": event.zone_id,
                "epc": event.epc,
                "observed_at": event.observed_at,
                "first_received_at": event.received_at,
                "rssi": event.rssi,
                "antenna_id": event.antenna_id,
                "reader_health": event.reader_health,
                "is_buffered": event.is_buffered,
                "backlog_drained": event.backlog_drained,
                "reader_coverage_ok": event.reader_coverage_ok,
                "processing_status": RfidEventProcessingStatus.PENDING,
            }
            for event in sorted(events, key=lambda item: item.event_id)
        ]
        inserted_event_ids = set(
            db.scalars(
                insert(RfidObservationEventLedger)
                .values(ledger_values)
                .on_conflict_do_nothing(index_elements=["tenant_id", "event_id"])
                .returning(RfidObservationEventLedger.event_id)
            ).all()
        )
        ledgers = list(
            db.scalars(
                select(RfidObservationEventLedger)
                .where(
                    RfidObservationEventLedger.tenant_id == device.tenant_id,
                    RfidObservationEventLedger.event_id.in_(sorted(event_by_id)),
                )
                .order_by(RfidObservationEventLedger.event_id)
                .with_for_update()
            ).all()
        )
        if len(ledgers) != len(events):
            raise RuntimeError("RFID event ledger insertion did not reconcile")

        for ledger in ledgers:
            event = event_by_id[ledger.event_id]
            if ledger.payload_fingerprint != observation_payload_fingerprint(event):
                raise ApiError(
                    409,
                    "Conflicting RFID event",
                    "event_id already exists with a different immutable observation payload.",
                    code="rfid_event_id_conflict",
                )

            db.add(
                RfidObservationBatchEvent(
                    tenant_id=device.tenant_id,
                    batch_id=batch.id,
                    event_id=ledger.event_id,
                    processing_status=ledger.processing_status,
                    disposition=ledger.disposition,
                    rejection_reason=ledger.rejection_reason,
                    finalized_at=ledger.processed_at,
                )
            )

        # Sequence-backed outbox rows are added in the device's request order. The
        # ledger operations above intentionally sort event IDs to avoid lock-order
        # deadlocks, but that must not redefine observation acceptance order.
        for event in events:
            if event.event_id in inserted_event_ids:
                db.add(
                    RfidObservationOutbox(
                        tenant_id=device.tenant_id,
                        event_id=event.event_id,
                        partition_key=event.partition_key,
                        payload=event.model_dump(mode="json"),
                        publish_attempts=0,
                    )
                )

        _set_initial_batch_outcome(batch, ledgers, received_at=received_at)

        connectivity = db.get(
            StoreConnectivity,
            (device.tenant_id, current_assignment.store_id),
        )
        if connectivity is None:
            connectivity = StoreConnectivity(
                tenant_id=device.tenant_id,
                store_id=current_assignment.store_id,
                backlog_drained=request.backlog_drained,
                reader_coverage_ok=request.reader_coverage_ok,
                freshness_status=FreshnessStatus.STALE,
            )
            db.add(connectivity)
        connectivity.gateway_last_heartbeat = received_at
        connectivity.backlog_drained = request.backlog_drained
        connectivity.reader_coverage_ok = request.reader_coverage_ok
        if not request.backlog_drained or not request.reader_coverage_ok:
            connectivity.freshness_status = FreshnessStatus.STALE

        db.commit()
        db.refresh(batch)
        return batch, events
    except Exception:
        db.rollback()
        raise
