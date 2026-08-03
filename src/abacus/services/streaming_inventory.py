from __future__ import annotations

import statistics
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import delete, func, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from abacus.config import Settings
from abacus.enums import DeviceStatus
from abacus.events.inventory import InventoryDeltaEvent
from abacus.events.rfid import RfidObservationEvent
from abacus.models.architecture import (
    AppliedInventoryDelta,
    CurrentItemState,
    FreshnessStatus,
    InventoryProjection,
    InventoryTransitionOutbox,
    ObservationBatchStatus,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidQuarantine,
    RfidTag,
    StoreConnectivity,
)
from abacus.models.tenancy import Device, DeviceAssignment


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    disposition: Literal["PROCESSED", "DUPLICATE", "LATE", "QUARANTINED", "AMBIGUOUS"]
    reason: str | None = None
    state_changed: bool = False


@dataclass(frozen=True, slots=True)
class StableZoneDecision:
    store_id: uuid.UUID
    zone_id: uuid.UUID
    confidence: float
    observed_at: datetime
    received_at: datetime


@dataclass(frozen=True, slots=True)
class RecentObservationCheckpoint:
    event_key: tuple[uuid.UUID, str]
    event_preexisting: bool
    oldest_event_key: tuple[uuid.UUID, str] | None
    window_key: tuple[uuid.UUID, str]
    window_before: tuple[RfidObservationEvent, ...] | None
    oldest_window: (
        tuple[
            tuple[uuid.UUID, str],
            tuple[RfidObservationEvent, ...],
        ]
        | None
    )


class RecentObservationState:
    """Bounded evidence window used by the single hosted event worker."""

    def __init__(self, *, maximum_epcs: int = 100_000, maximum_event_ids: int = 500_000) -> None:
        self._windows: OrderedDict[tuple[uuid.UUID, str], deque[RfidObservationEvent]] = (
            OrderedDict()
        )
        self._event_ids: OrderedDict[tuple[uuid.UUID, str], None] = OrderedDict()
        self._maximum_epcs = maximum_epcs
        self._maximum_event_ids = maximum_event_ids

    def remember_event(self, event: RfidObservationEvent) -> bool:
        key = (event.tenant_id, event.event_id)
        if key in self._event_ids:
            self._event_ids.move_to_end(key)
            return False
        self._event_ids[key] = None
        if len(self._event_ids) > self._maximum_event_ids:
            self._event_ids.popitem(last=False)
        return True

    def add(
        self, event: RfidObservationEvent, window_seconds: int
    ) -> tuple[RfidObservationEvent, ...]:
        key = (event.tenant_id, event.epc)
        window = self._windows.setdefault(key, deque())
        self._windows.move_to_end(key)
        window.append(event)
        newest = max(item.observed_at for item in window)
        cutoff = newest - timedelta(seconds=window_seconds)
        retained = deque(item for item in window if item.observed_at >= cutoff)
        self._windows[key] = retained
        if len(self._windows) > self._maximum_epcs:
            self._windows.popitem(last=False)
        return tuple(retained)

    def checkpoint(self, event: RfidObservationEvent) -> RecentObservationCheckpoint:
        """Capture the bounded state one event can mutate before a DB transaction."""

        event_key = (event.tenant_id, event.event_id)
        window_key = (event.tenant_id, event.epc)
        window = self._windows.get(window_key)
        oldest_window_entry = next(iter(self._windows.items()), None)
        return RecentObservationCheckpoint(
            event_key=event_key,
            event_preexisting=event_key in self._event_ids,
            oldest_event_key=next(iter(self._event_ids), None),
            window_key=window_key,
            window_before=tuple(window) if window is not None else None,
            oldest_window=(
                (oldest_window_entry[0], tuple(oldest_window_entry[1]))
                if oldest_window_entry is not None
                else None
            ),
        )

    def restore(self, checkpoint: RecentObservationCheckpoint) -> None:
        """Restore a checkpoint after the corresponding DB transaction rolls back."""

        if checkpoint.event_preexisting:
            self._event_ids[checkpoint.event_key] = None
        else:
            self._event_ids.pop(checkpoint.event_key, None)
        if checkpoint.oldest_event_key is not None:
            self._event_ids[checkpoint.oldest_event_key] = None
            self._event_ids.move_to_end(checkpoint.oldest_event_key, last=False)

        if checkpoint.window_before is None:
            self._windows.pop(checkpoint.window_key, None)
        else:
            self._windows[checkpoint.window_key] = deque(checkpoint.window_before)
        if checkpoint.oldest_window is not None:
            oldest_key, oldest_values = checkpoint.oldest_window
            if oldest_key != checkpoint.window_key or checkpoint.window_before is not None:
                self._windows[oldest_key] = deque(oldest_values)
            self._windows.move_to_end(oldest_key, last=False)


def _confidence(
    *,
    read_count: int,
    required_reads: int,
    rssi_separation: float | None,
    newest: RfidObservationEvent,
    window_seconds: int,
) -> float:
    consistency = min(1.0, read_count / required_reads)
    separation = 1.0 if rssi_separation is None else min(1.0, max(0.0, rssi_separation) / 12)
    age = max(0.0, (newest.received_at - newest.observed_at).total_seconds())
    recency = max(0.0, 1.0 - age / window_seconds)
    score = 0.40 * consistency + 0.25 * separation + 0.25 * recency + 0.10 * newest.reader_health
    return round(max(0.0, min(1.0, score)), 4)


def infer_stable_zone(
    observations: tuple[RfidObservationEvent, ...],
    settings: Settings,
) -> StableZoneDecision | None:
    if not observations:
        return None
    grouped: dict[tuple[uuid.UUID, uuid.UUID], list[RfidObservationEvent]] = defaultdict(list)
    for item in observations:
        grouped[(item.store_id, item.zone_id)].append(item)
    ranked = sorted(
        grouped.items(),
        key=lambda entry: (
            len(entry[1]),
            statistics.median(item.rssi for item in entry[1]),
            str(entry[0][1]),
        ),
        reverse=True,
    )
    (store_id, zone_id), winning = ranked[0]
    if len(winning) < settings.rfid_move_confirmation_reads:
        return None
    winner_median = statistics.median(item.rssi for item in winning)
    separation: float | None = None
    if len(ranked) > 1:
        competing_median = statistics.median(item.rssi for item in ranked[1][1])
        separation = winner_median - competing_median
        same_read_count = len(winning) == len(ranked[1][1])
        if same_read_count and separation < 3.0:
            return None
    newest = max(winning, key=lambda item: (item.observed_at, item.received_at, item.event_id))
    return StableZoneDecision(
        store_id=store_id,
        zone_id=zone_id,
        confidence=_confidence(
            read_count=len(winning),
            required_reads=settings.rfid_move_confirmation_reads,
            rssi_separation=separation,
            newest=newest,
            window_seconds=settings.rfid_move_confirmation_window_seconds,
        ),
        observed_at=newest.observed_at,
        received_at=newest.received_at,
    )


def deterministic_transition_id(tenant_id: uuid.UUID, epc: str, state_version: int) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"abacus:{tenant_id}:{epc}:{state_version}")


def effective_freshness(
    connectivity: StoreConnectivity | None,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> FreshnessStatus:
    """Derive freshness at read/evaluation time instead of trusting a stale snapshot."""

    if (
        connectivity is None
        or connectivity.gateway_last_heartbeat is None
        or connectivity.last_live_event_at is None
        or not connectivity.backlog_drained
        or connectivity.oldest_buffered_event_at is not None
    ):
        return FreshnessStatus.STALE

    current_time = now or datetime.now(UTC)
    heartbeat_age = max(
        0.0,
        (current_time - connectivity.gateway_last_heartbeat).total_seconds(),
    )
    live_event_age = max(
        0.0,
        (current_time - connectivity.last_live_event_at).total_seconds(),
    )
    if (
        heartbeat_age > settings.connectivity_stale_window_seconds
        or live_event_age > settings.connectivity_stale_window_seconds
    ):
        return FreshnessStatus.STALE
    if (
        connectivity.reader_coverage_ok
        and heartbeat_age <= settings.connectivity_live_window_seconds
        and live_event_age <= settings.connectivity_live_window_seconds
    ):
        return FreshnessStatus.LIVE
    return FreshnessStatus.DEGRADED


def _quarantine(
    db: Session,
    event: RfidObservationEvent,
    *,
    reason: str,
) -> ProcessingResult:
    existing_quarantine = db.scalar(
        select(RfidQuarantine).where(
            RfidQuarantine.tenant_id == event.tenant_id,
            RfidQuarantine.event_id == event.event_id,
        )
    )
    if existing_quarantine is None:
        db.add(
            RfidQuarantine(
                tenant_id=event.tenant_id,
                batch_id=event.batch_id,
                event_id=event.event_id,
                reason=reason,
                payload=event.model_dump(mode="json"),
            )
        )
    _advance_batch(
        db,
        event,
        rejected=True,
        disposition="QUARANTINED",
        reason=reason,
    )
    return ProcessingResult("QUARANTINED", reason=reason)


def _resolve_effective_assignment(
    db: Session,
    event: RfidObservationEvent,
) -> tuple[DeviceAssignment | None, str | None]:
    """Resolve location from trusted registry data at event time."""

    device = db.scalar(
        select(Device).where(
            Device.tenant_id == event.tenant_id,
            Device.id == event.device_id,
        )
    )
    if device is None:
        return None, "UNKNOWN_DEVICE"
    if device.status != DeviceStatus.ACTIVE:
        return None, "INACTIVE_DEVICE"
    assignment = db.scalar(
        select(DeviceAssignment)
        .where(
            DeviceAssignment.tenant_id == event.tenant_id,
            DeviceAssignment.device_id == event.device_id,
            DeviceAssignment.effective_from <= event.observed_at,
            or_(
                DeviceAssignment.effective_to.is_(None),
                DeviceAssignment.effective_to > event.observed_at,
            ),
        )
        .order_by(DeviceAssignment.effective_from.desc())
        .limit(1)
    )
    if assignment is None:
        return None, "DEVICE_ASSIGNMENT_UNAVAILABLE"
    if assignment.store_id != event.store_id or assignment.zone_id != event.zone_id:
        return None, "DEVICE_ASSIGNMENT_MISMATCH"
    return assignment, None


def _delta_payload(
    *,
    transition_id: uuid.UUID,
    tenant_id: uuid.UUID,
    epc: str,
    sku_id: uuid.UUID,
    store_id: uuid.UUID,
    zone_id: uuid.UUID,
    quantity_delta: int,
    confidence: float,
    observed_at: datetime,
) -> dict[str, object]:
    return InventoryDeltaEvent(
        delta_id=f"{transition_id}:{zone_id}",
        transition_id=transition_id,
        tenant_id=tenant_id,
        store_id=store_id,
        sku_id=sku_id,
        zone_id=zone_id,
        epc=epc,
        quantity_delta=quantity_delta,
        confidence=confidence,
        observed_at=observed_at,
    ).model_dump(mode="json")


def _update_connectivity(
    db: Session,
    event: RfidObservationEvent,
    settings: Settings,
) -> StoreConnectivity:
    connectivity = db.get(StoreConnectivity, (event.tenant_id, event.store_id))
    if connectivity is None:
        connectivity = StoreConnectivity(
            tenant_id=event.tenant_id,
            store_id=event.store_id,
            backlog_drained=False,
            reader_coverage_ok=event.reader_coverage_ok,
            freshness_status=FreshnessStatus.STALE,
        )
        db.add(connectivity)
    connectivity.gateway_last_heartbeat = max(
        event.received_at,
        connectivity.gateway_last_heartbeat or event.received_at,
    )
    connectivity.reader_coverage_ok = event.reader_coverage_ok
    if event.is_buffered:
        if (
            connectivity.oldest_buffered_event_at is None
            or event.observed_at < connectivity.oldest_buffered_event_at
        ):
            connectivity.oldest_buffered_event_at = event.observed_at
        connectivity.backlog_drained = False
        connectivity.freshness_status = FreshnessStatus.STALE
    else:
        if (
            connectivity.last_live_event_at is None
            or event.observed_at - connectivity.last_live_event_at
            >= timedelta(seconds=settings.rfid_last_seen_flush_seconds)
        ):
            connectivity.last_live_event_at = event.observed_at
        connectivity.backlog_drained = event.backlog_drained
        if event.backlog_drained:
            connectivity.oldest_buffered_event_at = None
        connectivity.freshness_status = effective_freshness(
            connectivity,
            settings,
            now=event.received_at,
        )
    return connectivity


def _advance_batch(
    db: Session,
    event: RfidObservationEvent,
    *,
    rejected: bool,
    disposition: str | None = None,
    reason: str | None = None,
) -> None:
    """Finalize one durable event and every request batch that retried it."""

    terminal_status = (
        RfidEventProcessingStatus.REJECTED if rejected else RfidEventProcessingStatus.PROCESSED
    )
    ledger = db.scalar(
        select(RfidObservationEventLedger)
        .where(
            RfidObservationEventLedger.tenant_id == event.tenant_id,
            RfidObservationEventLedger.event_id == event.event_id,
        )
        .with_for_update()
    )
    if ledger is not None:
        if ledger.processing_status != RfidEventProcessingStatus.PENDING:
            return
        finalized_at = datetime.now(UTC)
        ledger.processing_status = terminal_status
        ledger.disposition = disposition or ("QUARANTINED" if rejected else "PROCESSED")
        ledger.rejection_reason = reason if rejected else None
        ledger.processed_at = finalized_at
        links = list(
            db.scalars(
                select(RfidObservationBatchEvent)
                .where(
                    RfidObservationBatchEvent.tenant_id == event.tenant_id,
                    RfidObservationBatchEvent.event_id == event.event_id,
                )
                .with_for_update()
            ).all()
        )
        batch_ids: set[uuid.UUID] = set()
        for link in links:
            link.processing_status = terminal_status
            link.disposition = ledger.disposition
            link.rejection_reason = ledger.rejection_reason
            link.finalized_at = finalized_at
            batch_ids.add(link.batch_id)
        for batch_id in batch_ids:
            batch = db.scalar(
                select(RfidObservationBatch)
                .where(
                    RfidObservationBatch.tenant_id == event.tenant_id,
                    RfidObservationBatch.id == batch_id,
                )
                .with_for_update()
            )
            if batch is None:
                raise ValueError("RFID retry batch does not exist")
            batch_links = list(
                db.scalars(
                    select(RfidObservationBatchEvent).where(
                        RfidObservationBatchEvent.tenant_id == event.tenant_id,
                        RfidObservationBatchEvent.batch_id == batch_id,
                    )
                ).all()
            )
            batch.processed_count = sum(
                link.processing_status == RfidEventProcessingStatus.PROCESSED
                for link in batch_links
            )
            batch.rejected_count = sum(
                link.processing_status == RfidEventProcessingStatus.REJECTED for link in batch_links
            )
            accounted = batch.processed_count + batch.rejected_count
            if accounted == batch.accepted_count:
                batch.status = (
                    ObservationBatchStatus.COMPLETED_WITH_ERRORS
                    if batch.rejected_count
                    else ObservationBatchStatus.COMPLETED
                )
                batch.completed_at = finalized_at
            elif accounted:
                batch.status = ObservationBatchStatus.PROCESSING
        return

    # Compatibility for legacy/unit paths created before the durable event ledger.
    batch = db.scalar(
        select(RfidObservationBatch)
        .where(
            RfidObservationBatch.id == event.batch_id,
            RfidObservationBatch.tenant_id == event.tenant_id,
        )
        .with_for_update()
    )
    if batch is None:
        raise ValueError("RFID batch does not exist")
    if batch.processed_count + batch.rejected_count < batch.accepted_count:
        if rejected:
            batch.rejected_count += 1
        else:
            batch.processed_count += 1
    if batch.processed_count + batch.rejected_count == batch.accepted_count:
        batch.status = (
            ObservationBatchStatus.COMPLETED_WITH_ERRORS
            if batch.rejected_count
            else ObservationBatchStatus.COMPLETED
        )
        batch.completed_at = event.received_at
    else:
        batch.status = ObservationBatchStatus.PROCESSING


def process_observation(
    db: Session,
    event: RfidObservationEvent,
    recent: RecentObservationState,
    settings: Settings,
) -> ProcessingResult:
    """Apply one partition-ordered observation and emit an outbox transition."""

    if not recent.remember_event(event):
        _advance_batch(db, event, rejected=False, disposition="DUPLICATE")
        return ProcessingResult("DUPLICATE")

    processor_time = datetime.now(UTC)
    future_reference = min(event.received_at, processor_time)
    if event.observed_at > future_reference + timedelta(
        seconds=settings.rfid_max_future_skew_seconds
    ):
        return _quarantine(db, event, reason="OBSERVED_AT_TOO_FAR_IN_FUTURE")

    assignment, assignment_error = _resolve_effective_assignment(db, event)
    if assignment_error is not None or assignment is None:
        return _quarantine(
            db,
            event,
            reason=assignment_error or "DEVICE_ASSIGNMENT_UNAVAILABLE",
        )

    # The payload fields are checked above only to detect a stale/spoofed producer.
    # Every downstream location decision uses the registry-resolved values.
    resolved_event = event.model_copy(
        update={"store_id": assignment.store_id, "zone_id": assignment.zone_id}
    )
    _update_connectivity(db, resolved_event, settings)
    tag = db.get(RfidTag, (event.tenant_id, event.epc))
    if tag is None or not tag.active:
        return _quarantine(db, event, reason="UNKNOWN_EPC")

    state = db.scalar(
        select(CurrentItemState)
        .where(
            CurrentItemState.tenant_id == event.tenant_id,
            CurrentItemState.epc == event.epc,
        )
        .with_for_update()
    )
    if state is not None and event.observed_at < state.last_observed_at:
        _advance_batch(db, event, rejected=False, disposition="LATE")
        return ProcessingResult("LATE", reason="OBSERVED_AT_BEFORE_CURRENT_STATE")

    window = recent.add(resolved_event, settings.rfid_move_confirmation_window_seconds)
    decision = infer_stable_zone(window, settings)
    if decision is None:
        if state is not None:
            # This watermark is deliberately durable for every accepted observation.
            # Otherwise a processor restart could accept an older event and move the
            # item backwards while the more recent read existed only in memory.
            state.last_observed_at = max(state.last_observed_at, event.observed_at)
            state.confidence = min(state.confidence, 0.49)
            if event.received_at - state.last_received_at >= timedelta(
                seconds=settings.rfid_last_seen_flush_seconds
            ):
                state.last_received_at = event.received_at
        _advance_batch(db, event, rejected=False, disposition="AMBIGUOUS")
        return ProcessingResult("AMBIGUOUS")

    if state is not None and decision.observed_at <= state.last_observed_at:
        state.last_observed_at = max(state.last_observed_at, event.observed_at)
        state.confidence = min(state.confidence, 0.49)
        if event.received_at - state.last_received_at >= timedelta(
            seconds=settings.rfid_last_seen_flush_seconds
        ):
            state.last_received_at = event.received_at
        _advance_batch(db, event, rejected=False, disposition="AMBIGUOUS")
        return ProcessingResult("AMBIGUOUS", reason="STABLE_EVIDENCE_BEFORE_CURRENT_STATE")

    if (
        state is not None
        and state.store_id == decision.store_id
        and state.zone_id == decision.zone_id
    ):
        state.last_observed_at = max(
            state.last_observed_at,
            decision.observed_at,
            event.observed_at,
        )
        if event.received_at - state.last_received_at >= timedelta(
            seconds=settings.rfid_last_seen_flush_seconds
        ):
            state.last_received_at = max(state.last_received_at, decision.received_at)
            state.confidence = decision.confidence
        _advance_batch(db, event, rejected=False, disposition="PROCESSED")
        return ProcessingResult("PROCESSED")

    next_version = 1 if state is None else state.state_version + 1
    transition_id = deterministic_transition_id(event.tenant_id, event.epc, next_version)
    watermark_observed_at = max(event.observed_at, decision.observed_at)
    watermark_received_at = max(event.received_at, decision.received_at)
    deltas: list[dict[str, object]] = []
    if state is not None and state.store_id is not None and state.zone_id is not None:
        deltas.append(
            _delta_payload(
                transition_id=transition_id,
                tenant_id=state.tenant_id,
                epc=state.epc,
                sku_id=state.sku_id,
                store_id=state.store_id,
                zone_id=state.zone_id,
                quantity_delta=-1,
                confidence=decision.confidence,
                observed_at=decision.observed_at,
            )
        )
    deltas.append(
        _delta_payload(
            transition_id=transition_id,
            tenant_id=event.tenant_id,
            epc=event.epc,
            sku_id=tag.sku_id,
            store_id=decision.store_id,
            zone_id=decision.zone_id,
            quantity_delta=1,
            confidence=decision.confidence,
            observed_at=decision.observed_at,
        )
    )
    if state is None:
        state = CurrentItemState(
            tenant_id=event.tenant_id,
            epc=event.epc,
            sku_id=tag.sku_id,
            store_id=decision.store_id,
            zone_id=decision.zone_id,
            last_observed_at=watermark_observed_at,
            last_received_at=watermark_received_at,
            confidence=decision.confidence,
            state_version=next_version,
        )
        db.add(state)
    else:
        state.sku_id = tag.sku_id
        state.store_id = decision.store_id
        state.zone_id = decision.zone_id
        state.last_observed_at = watermark_observed_at
        state.last_received_at = watermark_received_at
        state.confidence = decision.confidence
        state.state_version = next_version
    db.add(
        InventoryTransitionOutbox(
            transition_id=transition_id,
            tenant_id=event.tenant_id,
            epc=event.epc,
            state_version=next_version,
            deltas=deltas,
            publish_attempts=0,
        )
    )
    _advance_batch(db, event, rejected=False, disposition="PROCESSED")
    return ProcessingResult("PROCESSED", state_changed=True)


def confirm_timed_out_removals(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    settings: Settings,
    now: datetime | None = None,
    limit: int = 1000,
) -> int:
    current_time = now or datetime.now(UTC)
    cutoff = current_time - timedelta(seconds=settings.rfid_removal_timeout_seconds)
    live_cutoff = current_time - timedelta(seconds=settings.connectivity_live_window_seconds)
    states = list(
        db.scalars(
            select(CurrentItemState)
            .join(
                StoreConnectivity,
                (StoreConnectivity.tenant_id == CurrentItemState.tenant_id)
                & (StoreConnectivity.store_id == CurrentItemState.store_id),
            )
            .where(
                CurrentItemState.tenant_id == tenant_id,
                CurrentItemState.zone_id.is_not(None),
                CurrentItemState.last_observed_at < cutoff,
                StoreConnectivity.backlog_drained.is_(True),
                StoreConnectivity.reader_coverage_ok.is_(True),
                StoreConnectivity.oldest_buffered_event_at.is_(None),
                StoreConnectivity.gateway_last_heartbeat >= live_cutoff,
                StoreConnectivity.last_live_event_at >= live_cutoff,
            )
            .order_by(CurrentItemState.last_observed_at)
            .with_for_update(skip_locked=True)
            .limit(limit)
        ).all()
    )
    for state in states:
        assert state.store_id is not None and state.zone_id is not None
        next_version = state.state_version + 1
        transition_id = deterministic_transition_id(tenant_id, state.epc, next_version)
        delta = _delta_payload(
            transition_id=transition_id,
            tenant_id=tenant_id,
            epc=state.epc,
            sku_id=state.sku_id,
            store_id=state.store_id,
            zone_id=state.zone_id,
            quantity_delta=-1,
            confidence=state.confidence,
            observed_at=current_time,
        )
        state.store_id = None
        state.zone_id = None
        state.state_version = next_version
        db.add(
            InventoryTransitionOutbox(
                transition_id=transition_id,
                tenant_id=tenant_id,
                epc=state.epc,
                state_version=next_version,
                deltas=[delta],
                publish_attempts=0,
            )
        )
    return len(states)


def apply_inventory_deltas(db: Session, deltas: list[InventoryDeltaEvent]) -> int:
    """Deduplicate, combine, and atomically upsert one consumer batch."""

    if not deltas:
        return 0
    tenant_ids = {item.tenant_id for item in deltas}
    if len(tenant_ids) != 1:
        raise ValueError("one database transaction may apply deltas for only one tenant")
    tenant_id = next(iter(tenant_ids))
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"inventory-projection:{tenant_id}"},
    )
    unique_by_id = {item.delta_id: item for item in deltas}
    inserted_ids = set(
        db.scalars(
            insert(AppliedInventoryDelta)
            .values(
                [
                    {
                        "delta_id": item.delta_id,
                        "tenant_id": item.tenant_id,
                        "store_id": item.store_id,
                        "sku_id": item.sku_id,
                        "zone_id": item.zone_id,
                        "quantity_delta": item.quantity_delta,
                    }
                    for item in unique_by_id.values()
                ]
            )
            .on_conflict_do_nothing(index_elements=[AppliedInventoryDelta.delta_id])
            .returning(AppliedInventoryDelta.delta_id)
        ).all()
    )
    pending = [item for key, item in unique_by_id.items() if key in inserted_ids]
    grouped: dict[tuple[uuid.UUID, uuid.UUID, uuid.UUID], list[InventoryDeltaEvent]] = defaultdict(
        list
    )
    for item in pending:
        grouped[(item.store_id, item.sku_id, item.zone_id)].append(item)
    for (store_id, sku_id, zone_id), items in grouped.items():
        projection = db.scalar(
            select(InventoryProjection)
            .where(
                InventoryProjection.tenant_id == tenant_id,
                InventoryProjection.store_id == store_id,
                InventoryProjection.sku_id == sku_id,
                InventoryProjection.zone_id == zone_id,
            )
            .with_for_update()
        )
        quantity_delta = sum(item.quantity_delta for item in items)
        as_of = max(item.observed_at for item in items)
        confidence = min(item.confidence for item in items)
        connectivity = db.get(StoreConnectivity, (tenant_id, store_id))
        freshness = connectivity.freshness_status if connectivity else FreshnessStatus.STALE
        if projection is None:
            if quantity_delta < 0:
                raise ValueError("inventory projection cannot start with a negative delta")
            projection = InventoryProjection(
                tenant_id=tenant_id,
                store_id=store_id,
                sku_id=sku_id,
                zone_id=zone_id,
                quantity=quantity_delta,
                as_of=as_of,
                confidence=confidence,
                freshness_status=freshness,
            )
            db.add(projection)
        else:
            next_quantity = projection.quantity + quantity_delta
            if next_quantity < 0:
                raise ValueError("inventory projection would become negative")
            projection.quantity = next_quantity
            projection.as_of = max(projection.as_of, as_of)
            projection.confidence = confidence
            projection.freshness_status = freshness
    return len(inserted_ids)


def rebuild_inventory_projection(db: Session, tenant_id: uuid.UUID) -> int:
    """Reconstruct the derived projection from current physical-item state."""

    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"inventory-projection:{tenant_id}"},
    )
    pending_before = db.scalar(
        select(func.count())
        .select_from(InventoryTransitionOutbox)
        .where(
            InventoryTransitionOutbox.tenant_id == tenant_id,
            InventoryTransitionOutbox.published_at.is_(None),
        )
    )
    if pending_before:
        raise RuntimeError(
            "inventory projection cannot be rebuilt while transition deltas are pending"
        )
    db.execute(delete(InventoryProjection).where(InventoryProjection.tenant_id == tenant_id))
    rows = db.execute(
        select(
            CurrentItemState.store_id,
            CurrentItemState.sku_id,
            CurrentItemState.zone_id,
            func.count().label("quantity"),
            func.max(CurrentItemState.last_observed_at).label("as_of"),
            func.min(CurrentItemState.confidence).label("confidence"),
        )
        .where(
            CurrentItemState.tenant_id == tenant_id,
            CurrentItemState.store_id.is_not(None),
            CurrentItemState.zone_id.is_not(None),
        )
        .group_by(
            CurrentItemState.store_id,
            CurrentItemState.sku_id,
            CurrentItemState.zone_id,
        )
    ).all()
    for row in rows:
        connectivity = db.get(StoreConnectivity, (tenant_id, row.store_id))
        db.add(
            InventoryProjection(
                tenant_id=tenant_id,
                store_id=row.store_id,
                sku_id=row.sku_id,
                zone_id=row.zone_id,
                quantity=int(row.quantity),
                as_of=row.as_of,
                confidence=float(row.confidence),
                freshness_status=(
                    connectivity.freshness_status
                    if connectivity is not None
                    else FreshnessStatus.STALE
                ),
            )
        )
    # READ COMMITTED gives this statement a fresh snapshot. If an item transition
    # committed after the reconstruction query, it is either visible here (rollback
    # and retry) or committed afterward, in which case its delta correctly applies to
    # the snapshot we just built. The shared advisory lock serializes delta applying.
    pending_after = db.scalar(
        select(func.count())
        .select_from(InventoryTransitionOutbox)
        .where(
            InventoryTransitionOutbox.tenant_id == tenant_id,
            InventoryTransitionOutbox.published_at.is_(None),
        )
    )
    if pending_after:
        raise RuntimeError(
            "an inventory transition arrived during projection rebuild; retry after draining"
        )
    return len(rows)
