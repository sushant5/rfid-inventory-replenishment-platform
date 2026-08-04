from __future__ import annotations

import statistics
import uuid
from collections import OrderedDict, defaultdict, deque
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy import delete, func, literal, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.selectable import Subquery

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
    ItemPresenceStatus,
    ObservationBatchStatus,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidQuarantine,
    StoreConnectivity,
)
from abacus.models.catalog import EpcBinding
from abacus.models.tenancy import Device, DeviceAssignment
from abacus.services.connectivity import lock_store_connectivity_for_receipt


@dataclass(frozen=True, slots=True)
class ProcessingResult:
    disposition: Literal[
        "PROCESSED",
        "DUPLICATE",
        "LATE",
        "QUARANTINED",
        "AMBIGUOUS",
        "REMOVED_ITEM_OBSERVED",
    ]
    reason: str | None = None
    state_changed: bool = False


@dataclass(frozen=True, slots=True)
class StableZoneDecision:
    store_id: uuid.UUID
    zone_id: uuid.UUID
    confidence: float
    observed_at: datetime
    received_at: datetime


def effective_item_confidence(
    *,
    stored_confidence: float,
    last_observed_at: datetime,
    evaluated_at: datetime,
    half_life_seconds: int,
) -> float:
    """Age location evidence without rewriting the authoritative item-state row."""

    age_seconds = max(0.0, (evaluated_at - last_observed_at).total_seconds())
    decay = 0.5 ** (age_seconds / half_life_seconds)
    return float(max(0.0, min(1.0, stored_confidence * decay)))


def current_inventory_bucket_metadata(
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    evaluated_at: datetime,
    confidence_half_life_seconds: int,
) -> Subquery:
    """Aggregate authoritative metadata for each currently occupied inventory bucket.

    ``inventory_projection`` is an eventually consistent quantity projection. Item
    confidence and observation time can change without a quantity transition, so its
    copied metadata is not authoritative for reads or replenishment decisions.
    """

    age_seconds = func.greatest(
        0.0,
        func.extract(
            "epoch",
            literal(evaluated_at) - CurrentItemState.last_observed_at,
        ),
    )
    effective_confidence = CurrentItemState.confidence * func.power(
        0.5,
        age_seconds / confidence_half_life_seconds,
    )

    return (
        select(
            CurrentItemState.tenant_id.label("tenant_id"),
            CurrentItemState.store_id.label("store_id"),
            CurrentItemState.sku_id.label("sku_id"),
            CurrentItemState.zone_id.label("zone_id"),
            func.count(CurrentItemState.epc).label("item_count"),
            func.max(CurrentItemState.last_observed_at).label("as_of"),
            func.min(CurrentItemState.last_observed_at).label("oldest_item_observed_at"),
            func.min(effective_confidence).label("confidence"),
        )
        .where(
            CurrentItemState.tenant_id == tenant_id,
            CurrentItemState.store_id == store_id,
            CurrentItemState.zone_id.is_not(None),
        )
        .group_by(
            CurrentItemState.tenant_id,
            CurrentItemState.store_id,
            CurrentItemState.sku_id,
            CurrentItemState.zone_id,
        )
        .subquery("current_inventory_bucket_metadata")
    )


def effective_bucket_confidence(
    *,
    projected_quantity: int,
    current_item_count: int | None,
    current_confidence: float | None,
) -> float:
    """Return current-state confidence, failing safe on a projection mismatch.

    A zero-quantity bucket with no current items is a normal empty bucket. A positive
    projection without any matching current item means the asynchronous projection is
    behind current state, so automatic work must not trust it.
    """

    if current_item_count is None:
        return 1.0 if projected_quantity == 0 else 0.0
    if projected_quantity != current_item_count or current_confidence is None:
        return 0.0
    return float(current_confidence)


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

    def forget_event(self, event: RfidObservationEvent) -> None:
        """Allow an explicit operator replay to rebuild evidence for one event."""

        self._event_ids.pop((event.tenant_id, event.event_id), None)

    def add(
        self,
        event: RfidObservationEvent,
        window_seconds: int,
        *,
        evidence_not_before: datetime | None = None,
    ) -> tuple[RfidObservationEvent, ...]:
        key = (event.tenant_id, event.epc)
        window = self._windows.setdefault(key, deque())
        self._windows.move_to_end(key)
        window.append(event)
        newest = max(item.observed_at for item in window)
        cutoff = newest - timedelta(seconds=window_seconds)
        retained = deque(
            item
            for item in window
            if item.observed_at >= cutoff
            and (evidence_not_before is None or item.observed_at >= evidence_not_before)
        )
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
    if connectivity.inventory_reconciliation_required_at is not None:
        return FreshnessStatus.DEGRADED
    if (
        connectivity.reader_coverage_ok
        and heartbeat_age <= settings.connectivity_live_window_seconds
        and live_event_age <= settings.connectivity_live_window_seconds
    ):
        return FreshnessStatus.LIVE
    return FreshnessStatus.DEGRADED


def effective_presence_status(
    state: CurrentItemState,
    settings: Settings,
    *,
    now: datetime | None = None,
) -> ItemPresenceStatus:
    """Classify observation age without mutating or decrementing inventory."""

    if state.authoritative_removal_event_id is not None:
        return ItemPresenceStatus.REMOVED
    if state.store_id is None or state.zone_id is None:
        # Releases before the authoritative-event ledger used a null location for
        # timeout tombstones. Leave those rows recoverable by a stable RFID window.
        return ItemPresenceStatus.LOCATION_UNKNOWN
    current_time = now or datetime.now(UTC)
    age_seconds = max(0.0, (current_time - state.last_observed_at).total_seconds())
    if age_seconds > settings.rfid_unobserved_after_seconds:
        return ItemPresenceStatus.UNOBSERVED
    return ItemPresenceStatus.OBSERVED


def _quarantine(
    db: Session,
    event: RfidObservationEvent,
    *,
    reason: str,
    payload: dict[str, object] | None = None,
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
                payload=payload if payload is not None else event.model_dump(mode="json"),
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


def quarantine_observation(
    db: Session,
    event: RfidObservationEvent,
    *,
    reason: str,
    payload: dict[str, object] | None = None,
) -> ProcessingResult:
    """Terminally reject one accepted observation and finalize every retry batch."""

    return _quarantine(db, event, reason=reason, payload=payload)


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
        # SKU is part of the inventory bucket identity. Including it keeps both
        # sides of an effective-dated EPC rebind distinct even when the item stays
        # in the same physical zone.
        delta_id=f"{transition_id}:{sku_id}:{zone_id}",
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


def _apply_connectivity_observation(
    connectivity: StoreConnectivity,
    event: RfidObservationEvent,
    settings: Settings,
    *,
    is_current_receipt: bool,
) -> None:
    """Apply event evidence without making same-receipt results order-dependent."""

    if event.is_buffered:
        if is_current_receipt:
            live_has_resumed = (
                event.backlog_drained
                and connectivity.last_live_received_at is not None
                and connectivity.last_live_received_at >= event.received_at
            )
            if not live_has_resumed:
                if (
                    connectivity.oldest_buffered_event_at is None
                    or event.observed_at < connectivity.oldest_buffered_event_at
                ):
                    connectivity.oldest_buffered_event_at = event.observed_at
                connectivity.backlog_drained = False
                connectivity.freshness_status = FreshnessStatus.STALE
    else:
        if (
            connectivity.last_live_received_at is None
            or event.received_at > connectivity.last_live_received_at
        ):
            connectivity.last_live_received_at = event.received_at
        if (
            connectivity.last_live_event_at is None
            or event.observed_at - connectivity.last_live_event_at
            >= timedelta(seconds=settings.rfid_last_seen_flush_seconds)
        ):
            connectivity.last_live_event_at = event.observed_at
        if is_current_receipt:
            if event.backlog_drained:
                connectivity.oldest_buffered_event_at = None
            connectivity.freshness_status = effective_freshness(
                connectivity,
                settings,
                now=event.received_at,
            )


def _update_connectivity(
    db: Session,
    event: RfidObservationEvent,
    settings: Settings,
) -> StoreConnectivity:
    connectivity, is_current_receipt = lock_store_connectivity_for_receipt(
        db,
        tenant_id=event.tenant_id,
        store_id=event.store_id,
        received_at=event.received_at,
        backlog_drained=event.backlog_drained,
        reader_coverage_ok=event.reader_coverage_ok,
    )
    _apply_connectivity_observation(
        connectivity,
        event,
        settings,
        is_current_receipt=is_current_receipt,
    )
    return connectivity


def processed_observation_watermark(
    db: Session,
    tenant_id: uuid.UUID,
    epc: str,
) -> datetime | None:
    """Return the latest durable event-time watermark for one physical item.

    The immutable event ledger is updated for every accepted event anyway. Using it
    as replay protection lets ``current_item_state`` throttle repeated last-seen
    refreshes without allowing a worker restart to regress item state.
    """

    return db.scalar(
        select(func.max(RfidObservationEventLedger.observed_at)).where(
            RfidObservationEventLedger.tenant_id == tenant_id,
            RfidObservationEventLedger.epc == epc,
            RfidObservationEventLedger.processing_status == RfidEventProcessingStatus.PROCESSED,
        )
    )


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

    raise ValueError("RFID event ledger does not exist")


def process_observation(
    db: Session,
    event: RfidObservationEvent,
    recent: RecentObservationState,
    settings: Settings,
) -> ProcessingResult:
    """Apply one partition-ordered observation and emit an outbox transition."""

    if event.replayed_from_quarantine_id is not None:
        # A terminally quarantined event has already passed through this process's
        # bounded in-memory dedupe set. The durable ledger remains the authoritative
        # idempotency boundary; an explicit recovery request may therefore release
        # only this process-local marker before retrying the immutable event.
        recent.forget_event(event)
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
    binding = db.scalar(
        select(EpcBinding)
        .where(
            EpcBinding.tenant_id == event.tenant_id,
            EpcBinding.epc == event.epc,
            EpcBinding.effective_from <= event.observed_at,
            or_(EpcBinding.effective_to.is_(None), EpcBinding.effective_to > event.observed_at),
        )
        .order_by(EpcBinding.effective_from.desc())
        .limit(1)
    )
    binding_evidence_start = binding.effective_from if binding is not None else None
    if binding is None and event.replayed_from_quarantine_id is not None:
        quarantine = db.scalar(
            select(RfidQuarantine).where(
                RfidQuarantine.id == event.replayed_from_quarantine_id,
                RfidQuarantine.tenant_id == event.tenant_id,
                RfidQuarantine.event_id == event.event_id,
                RfidQuarantine.reason == "UNKNOWN_EPC",
            )
        )
        if quarantine is not None:
            # Product-master data may legitimately arrive after a physical read.
            # The explicit replay is the operator's authorization to use the current
            # active binding while retaining the observation's original event time.
            binding = db.scalar(
                select(EpcBinding)
                .where(
                    EpcBinding.tenant_id == event.tenant_id,
                    EpcBinding.epc == event.epc,
                    EpcBinding.effective_from <= datetime.now(UTC),
                    EpcBinding.effective_to.is_(None),
                )
                .order_by(EpcBinding.effective_from.desc())
                .limit(1)
            )
    if binding is None:
        return _quarantine(db, event, reason="UNKNOWN_EPC")

    state = db.scalar(
        select(CurrentItemState)
        .where(
            CurrentItemState.tenant_id == event.tenant_id,
            CurrentItemState.epc == event.epc,
        )
        .with_for_update()
    )
    processed_watermark = processed_observation_watermark(db, event.tenant_id, event.epc)
    state_watermark = state.last_observed_at if state is not None else None
    durable_watermark = max(
        (value for value in (processed_watermark, state_watermark) if value is not None),
        default=None,
    )
    if durable_watermark is not None and event.observed_at < durable_watermark:
        _advance_batch(db, event, rejected=False, disposition="LATE")
        return ProcessingResult("LATE", reason="OBSERVED_AT_BEFORE_CURRENT_STATE")

    if state is not None and state.authoritative_removal_event_id is not None:
        # RFID is evidence of location, not authority to reverse a POS/WMS removal.
        # Keep the observation in the durable ledger for investigation, but require
        # an explicit future receipt/return command before this EPC can be counted.
        _advance_batch(db, event, rejected=False, disposition="REMOVED_ITEM_OBSERVED")
        return ProcessingResult(
            "REMOVED_ITEM_OBSERVED",
            reason="AUTHORITATIVELY_REMOVED",
        )

    window = recent.add(
        resolved_event,
        settings.rfid_move_confirmation_window_seconds,
        evidence_not_before=binding_evidence_start,
    )
    decision = infer_stable_zone(window, settings)
    if decision is None:
        if state is not None:
            observed_locations = {(item.store_id, item.zone_id) for item in window}
            current_location = (state.store_id, state.zone_id)
            # A worker restart starts with an empty process-local evidence window.
            # One read from the already-confirmed location is insufficient to make
            # a new decision, but it is not conflicting evidence and must not lower
            # confidence. Competing locations or a catalog rebind remain ambiguous.
            same_current_evidence = (
                state.sku_id == binding.sku_id
                and state.zone_id is not None
                and observed_locations == {current_location}
            )
            if not same_current_evidence:
                state.confidence = min(state.confidence, 0.49)
            if event.received_at - state.last_received_at >= timedelta(
                seconds=settings.rfid_last_seen_flush_seconds
            ):
                state.last_observed_at = max(state.last_observed_at, event.observed_at)
                state.last_received_at = event.received_at
        _advance_batch(db, event, rejected=False, disposition="AMBIGUOUS")
        return ProcessingResult("AMBIGUOUS")

    if durable_watermark is not None and decision.observed_at < durable_watermark:
        if state is not None:
            state.confidence = min(state.confidence, 0.49)
            if event.received_at - state.last_received_at >= timedelta(
                seconds=settings.rfid_last_seen_flush_seconds
            ):
                state.last_observed_at = max(state.last_observed_at, event.observed_at)
                state.last_received_at = event.received_at
        _advance_batch(db, event, rejected=False, disposition="AMBIGUOUS")
        return ProcessingResult("AMBIGUOUS", reason="STABLE_EVIDENCE_BEFORE_CURRENT_STATE")

    if (
        state is not None
        and state.sku_id == binding.sku_id
        and state.store_id == decision.store_id
        and state.zone_id == decision.zone_id
    ):
        # Recover confidence as soon as a complete stable window is rebuilt after
        # a worker handoff. Timestamp refreshes remain throttled, but waiting for
        # that timer here would suppress safe replenishment despite fresh evidence.
        if decision.confidence > state.confidence:
            state.confidence = decision.confidence
        if event.received_at - state.last_received_at >= timedelta(
            seconds=settings.rfid_last_seen_flush_seconds
        ):
            state.last_observed_at = max(
                state.last_observed_at,
                decision.observed_at,
                event.observed_at,
            )
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
            sku_id=binding.sku_id,
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
            sku_id=binding.sku_id,
            store_id=decision.store_id,
            zone_id=decision.zone_id,
            last_observed_at=watermark_observed_at,
            last_received_at=watermark_received_at,
            confidence=decision.confidence,
            state_version=next_version,
        )
        db.add(state)
    else:
        state.sku_id = binding.sku_id
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
            InventoryTransitionOutbox.quarantined_at.is_(None),
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
            InventoryTransitionOutbox.quarantined_at.is_(None),
        )
    )
    if pending_after:
        raise RuntimeError(
            "an inventory transition arrived during projection rebuild; retry after draining"
        )
    reconciled_at = datetime.now(UTC)
    db.execute(
        update(InventoryTransitionOutbox)
        .where(
            InventoryTransitionOutbox.tenant_id == tenant_id,
            InventoryTransitionOutbox.quarantined_at.is_not(None),
            InventoryTransitionOutbox.reconciled_at.is_(None),
        )
        .values(reconciled_at=reconciled_at)
    )
    db.execute(
        update(StoreConnectivity)
        .where(StoreConnectivity.tenant_id == tenant_id)
        .values(inventory_reconciliation_required_at=None)
    )
    return len(rows)
