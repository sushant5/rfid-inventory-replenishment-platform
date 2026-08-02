import hashlib
import json
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import case, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.config import get_settings
from abacus.enums import DeviceStatus, JobKind, ObservationStatus, TenantStatus, ZoneKind
from abacus.models.catalog import Sku
from abacus.models.replenishment import ReplenishmentTask, ReplenishmentTaskStatus
from abacus.models.rfid import (
    InventoryBalance,
    InventoryChange,
    InventoryItemState,
    RfidObservation,
)
from abacus.models.tenancy import Device, DeviceAssignment, Tenant, Zone
from abacus.schemas.rfid import RfidBatchInput, RfidBatchReceipt, RfidEventIngressResult
from abacus.services.catalog import resolve_active_epc
from abacus.services.cutover import require_reservation_cutover_ready
from abacus.services.jobs import enqueue_job
from abacus.services.locks import lock_replenishment_store_sku

TERMINAL_REPLENISHMENT_STATUSES = (
    ReplenishmentTaskStatus.VERIFIED,
    ReplenishmentTaskStatus.CANCELLED,
    ReplenishmentTaskStatus.EXCEPTION,
)
EXECUTING_REPLENISHMENT_STATUSES = (
    ReplenishmentTaskStatus.IN_PROGRESS,
    ReplenishmentTaskStatus.AWAITING_VERIFICATION,
)
ALLOCATABLE_REPLENISHMENT_STATUSES = (
    *EXECUTING_REPLENISHMENT_STATUSES,
    *TERMINAL_REPLENISHMENT_STATUSES,
)


def authenticate_device(db: Session, raw_api_key: str | None) -> Device:
    if not raw_api_key or "." not in raw_api_key:
        raise ApiError(401, "Unauthorized device", "A valid device API key is required.")
    raw_id, secret = raw_api_key.split(".", 1)
    try:
        device_id = uuid.UUID(raw_id)
    except ValueError as exc:
        raise ApiError(401, "Unauthorized device", "The device API key is malformed.") from exc
    device = db.get(Device, device_id)
    candidate = hashlib.sha256(secret.encode()).hexdigest()
    tenant_status = (
        db.scalar(select(Tenant.status).where(Tenant.id == device.tenant_id))
        if device is not None
        else None
    )
    if (
        device is None
        or device.status != DeviceStatus.ACTIVE
        or tenant_status != TenantStatus.ACTIVE
        or device.credential_hash is None
        or not secrets.compare_digest(candidate, device.credential_hash)
    ):
        raise ApiError(401, "Unauthorized device", "The device API key is invalid.")
    return device


def _hash_observation(device_id: uuid.UUID, raw: dict[str, Any]) -> str:
    canonical = json.dumps(
        {"device_id": str(device_id), **raw},
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _assignment_at(
    db: Session,
    device_id: uuid.UUID,
    observed_at: datetime,
) -> DeviceAssignment | None:
    return db.scalar(
        select(DeviceAssignment)
        .where(
            DeviceAssignment.device_id == device_id,
            DeviceAssignment.effective_from <= observed_at,
            or_(
                DeviceAssignment.effective_to.is_(None),
                DeviceAssignment.effective_to > observed_at,
            ),
        )
        .order_by(DeviceAssignment.effective_from.desc())
        .limit(1)
    )


def _advisory_lock_epc(db: Session, tenant_id: uuid.UUID, epc: str) -> None:
    """Serialize acceptance and projection work for one tenant/EPC."""

    lock_key = f"{tenant_id}:{epc}"
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": lock_key},
    )


def ingest_batch(db: Session, device: Device, request: RfidBatchInput) -> RfidBatchReceipt:
    require_reservation_cutover_ready(db)
    # Use the same transaction lock as the worker. Sorted acquisition prevents
    # deadlocks between multi-EPC batches. PostgreSQL assigns the durable acceptance
    # sequence while these locks are held, so concurrent requests for one EPC cannot
    # invert their canonical precedence.
    for epc in sorted({item.epc for item in request.observations}):
        _advisory_lock_epc(db, device.tenant_id, epc)

    event_ids = [item.event_id for item in request.observations]
    existing_by_id = {
        row.event_id: row
        for row in db.scalars(
            select(RfidObservation).where(
                RfidObservation.tenant_id == device.tenant_id,
                RfidObservation.event_id.in_(event_ids),
            )
        ).all()
    }
    now = db.scalar(select(func.clock_timestamp()))
    if now is None:  # pragma: no cover - PostgreSQL always returns a value
        raise RuntimeError("database clock is unavailable")
    max_future_time = now + timedelta(seconds=get_settings().rfid_max_future_skew_seconds)
    results: list[RfidEventIngressResult] = []
    accepted = duplicates = conflicts = 0

    for event in request.observations:
        raw = event.model_dump(mode="json")
        digest = _hash_observation(device.id, raw)
        existing = existing_by_id.get(event.event_id)
        if existing is not None:
            if existing.payload_hash == digest:
                duplicates += 1
                results.append(
                    RfidEventIngressResult(
                        event_id=event.event_id,
                        disposition="DUPLICATE",
                        observation_id=existing.id,
                    )
                )
            else:
                conflicts += 1
                results.append(
                    RfidEventIngressResult(
                        event_id=event.event_id,
                        disposition="CONFLICT",
                        observation_id=existing.id,
                        detail="event_id already exists with a different payload",
                    )
                )
            continue

        assignment = _assignment_at(db, device.id, event.observed_at)
        quarantine_reason: str | None = None
        if event.observed_at > max_future_time:
            quarantine_reason = "OBSERVED_AT_TOO_FAR_IN_FUTURE"
        elif assignment is None:
            quarantine_reason = "NO_EFFECTIVE_DEVICE_ASSIGNMENT"
        observation = RfidObservation(
            tenant_id=device.tenant_id,
            event_id=event.event_id,
            batch_id=request.batch_id,
            device_id=device.id,
            store_id=assignment.store_id if assignment else None,
            zone_id=assignment.zone_id if assignment else None,
            epc=event.epc,
            observed_at=event.observed_at,
            ingested_at=now,
            reader_sequence=event.reader_sequence,
            antenna_port=event.antenna_port,
            rssi_dbm=event.rssi_dbm,
            payload_hash=digest,
            status=(
                ObservationStatus.RECEIVED
                if quarantine_reason is None
                else ObservationStatus.QUARANTINED
            ),
            quarantine_reason=quarantine_reason,
            raw_payload=raw,
        )
        try:
            db.add(observation)
            db.flush()
            if quarantine_reason is None:
                enqueue_job(
                    db,
                    tenant_id=device.tenant_id,
                    kind=JobKind.RFID_OBSERVATION,
                    payload={"observation_id": str(observation.id)},
                )
        except IntegrityError as exc:
            db.rollback()
            raise ApiError(
                409,
                "Concurrent event conflict",
                "One or more event IDs were concurrently ingested; retry the batch.",
                code="concurrent_event_conflict",
            ) from exc
        accepted += 1
        results.append(
            RfidEventIngressResult(
                event_id=event.event_id,
                disposition="ACCEPTED",
                observation_id=observation.id,
                detail=(
                    f"accepted into quarantine: {quarantine_reason}"
                    if quarantine_reason is not None
                    else None
                ),
            )
        )

    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Concurrent event conflict",
            "One or more event IDs were concurrently ingested; retry the batch.",
            code="concurrent_event_conflict",
        ) from exc

    return RfidBatchReceipt(
        batch_id=request.batch_id,
        accepted_count=accepted,
        duplicate_count=duplicates,
        conflict_count=conflicts,
        results=results,
    )


def _change_balance(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    zone_id: uuid.UUID,
    sku_id: uuid.UUID,
    delta: int,
    observed_at: datetime,
) -> None:
    balance = db.scalar(
        select(InventoryBalance)
        .where(
            InventoryBalance.tenant_id == tenant_id,
            InventoryBalance.store_id == store_id,
            InventoryBalance.zone_id == zone_id,
            InventoryBalance.sku_id == sku_id,
        )
        .with_for_update()
    )
    # Use wall-clock database time only after acquiring the aggregate row lock.
    # Transaction-start defaults can otherwise regress when concurrent writers commit
    # in the opposite order from when their transactions began.
    processed_at = db.scalar(select(func.clock_timestamp()))
    if processed_at is None:  # pragma: no cover - PostgreSQL always returns a value
        raise RuntimeError("database clock is unavailable")
    if balance is None:
        if delta <= 0:
            raise ValueError("cannot decrement or reaffirm a missing inventory balance")
        db.add(
            InventoryBalance(
                tenant_id=tenant_id,
                store_id=store_id,
                zone_id=zone_id,
                sku_id=sku_id,
                quantity=delta,
                quantity_changed_at=processed_at,
                last_relevant_observation_at=observed_at,
                created_at=processed_at,
                updated_at=processed_at,
            )
        )
        return
    if balance.quantity + delta < 0:
        raise ValueError("inventory balance would become negative")
    balance.quantity += delta
    balance.updated_at = processed_at
    if delta != 0:
        balance.quantity_changed_at = processed_at
    if (
        balance.last_relevant_observation_at is None
        or observed_at > balance.last_relevant_observation_at
    ):
        balance.last_relevant_observation_at = observed_at


def _canonical_same_timestamp_observation(
    db: Session,
    observation: RfidObservation,
) -> RfidObservation:
    """Return the first non-quarantined accepted observation at one event time.

    Device timestamps and per-reader sequences cannot globally order simultaneous
    cross-reader evidence. Ingestion is serialized per EPC and PostgreSQL assigns a
    unique monotonic sequence, giving deterministic, auditable precedence independent
    of application clocks or which worker leases the jobs first.
    """

    assert observation.store_id is not None
    assert observation.zone_id is not None
    canonical = db.scalar(
        select(RfidObservation)
        .where(
            RfidObservation.tenant_id == observation.tenant_id,
            RfidObservation.epc == observation.epc,
            RfidObservation.observed_at == observation.observed_at,
            RfidObservation.store_id.is_not(None),
            RfidObservation.zone_id.is_not(None),
            RfidObservation.status != ObservationStatus.QUARANTINED,
        )
        .order_by(RfidObservation.acceptance_sequence.asc())
        .limit(1)
    )
    if canonical is None:  # pragma: no cover - the current persisted row always qualifies
        raise RuntimeError("RFID observation disappeared during timestamp ordering")
    return canonical


def _allocate_replenishment_reservation(
    db: Session,
    change: InventoryChange,
) -> None:
    """FIFO-link one confirmed floorward EPC move to one task unit.

    The caller holds the tenant/store/SKU advisory transaction lock. That lock—not
    wall-clock comparison—defines ordering. Executing tasks expose their requested
    capacity because RFID may confirm a physical move before the associate records
    moved_quantity; terminal tasks expose only their recorded moved_quantity.
    """

    consumed_units = (
        select(func.count(InventoryChange.id))
        .where(InventoryChange.replenishment_task_id == ReplenishmentTask.id)
        .correlate(ReplenishmentTask)
        .scalar_subquery()
    )
    allocatable_units = case(
        (
            ReplenishmentTask.status.in_(TERMINAL_REPLENISHMENT_STATUSES),
            ReplenishmentTask.moved_quantity
            - ReplenishmentTask.reconciled_before_tracking_quantity,
        ),
        else_=(ReplenishmentTask.quantity - ReplenishmentTask.reconciled_before_tracking_quantity),
    )
    task = db.scalar(
        select(ReplenishmentTask)
        .where(
            ReplenishmentTask.tenant_id == change.tenant_id,
            ReplenishmentTask.store_id == change.to_store_id,
            ReplenishmentTask.sku_id == change.sku_id,
            ReplenishmentTask.status.in_(ALLOCATABLE_REPLENISHMENT_STATUSES),
            allocatable_units > consumed_units,
        )
        .order_by(
            case(
                (ReplenishmentTask.status.in_(EXECUTING_REPLENISHMENT_STATUSES), 0),
                else_=1,
            ),
            ReplenishmentTask.created_at.asc(),
            ReplenishmentTask.id.asc(),
        )
        .with_for_update()
        .limit(1)
    )
    if task is not None:
        change.replenishment_task_id = task.id


def process_rfid_observation_job(db: Session, payload: dict[str, object]) -> None:
    observation_id = uuid.UUID(str(payload["observation_id"]))
    observation = db.get(RfidObservation, observation_id)
    if observation is None:
        raise ValueError("RFID observation does not exist")

    # All acceptance, replay and projection paths acquire EPC then row. Keeping one
    # global order prevents replay/worker deadlocks while still serializing state.
    _advisory_lock_epc(db, observation.tenant_id, observation.epc)
    db.refresh(observation, with_for_update=True)
    if observation.status in {ObservationStatus.PROCESSED, ObservationStatus.LATE_IGNORED}:
        return
    tenant_status = db.scalar(select(Tenant.status).where(Tenant.id == observation.tenant_id))
    if tenant_status != TenantStatus.ACTIVE:
        observation.status = ObservationStatus.QUARANTINED
        observation.quarantine_reason = "TENANT_NOT_ACTIVE"
        return
    if observation.store_id is None or observation.zone_id is None:
        observation.status = ObservationStatus.QUARANTINED
        observation.quarantine_reason = "NO_EFFECTIVE_DEVICE_ASSIGNMENT"
        return

    binding = resolve_active_epc(
        db,
        observation.tenant_id,
        observation.epc,
        observation.observed_at,
    )
    resolution_strategy = "OBSERVED_AT"
    if binding is None and payload.get("use_current_epc_binding") is True:
        binding = resolve_active_epc(
            db,
            observation.tenant_id,
            observation.epc,
            datetime.now(UTC),
        )
        resolution_strategy = "REPLAY_CURRENT"
    if binding is None:
        observation.status = ObservationStatus.QUARANTINED
        observation.quarantine_reason = "UNKNOWN_EPC"
        return
    observation.resolved_epc_binding_id = binding.id
    observation.resolution_strategy = resolution_strategy

    state = db.scalar(
        select(InventoryItemState).where(
            InventoryItemState.tenant_id == observation.tenant_id,
            InventoryItemState.epc == observation.epc,
        )
    )

    if state is not None and state.sku_id != binding.sku_id:
        observation.status = ObservationStatus.QUARANTINED
        observation.quarantine_reason = "EPC_REBIND_REQUIRES_RECONCILIATION"
        return

    if state is not None and observation.observed_at < state.last_observed_at:
        observation.status = ObservationStatus.LATE_IGNORED
        observation.quarantine_reason = "LATE_EVENT_DOES_NOT_REGRESS_STATE"
        return

    canonical = _canonical_same_timestamp_observation(db, observation)
    if canonical.id != observation.id:
        same_location = (
            canonical.store_id == observation.store_id and canonical.zone_id == observation.zone_id
        )
        if not same_location:
            observation.status = ObservationStatus.QUARANTINED
            observation.quarantine_reason = "AMBIGUOUS_SAME_TIMESTAMP_LOCATION"
        else:
            # Only the canonical accepted observation may affect candidate or
            # aggregate state. Same-time repetitions are retained processed no-ops.
            observation.status = ObservationStatus.PROCESSED
            observation.quarantine_reason = None
        return

    if state is not None and observation.observed_at == state.last_observed_at:
        matches_confirmed_location = (
            state.store_id == observation.store_id and state.zone_id == observation.zone_id
        )
        matches_candidate_location = (
            state.candidate_store_id == observation.store_id
            and state.candidate_zone_id == observation.zone_id
        )
        if not matches_confirmed_location and not matches_candidate_location:
            observation.status = ObservationStatus.QUARANTINED
            observation.quarantine_reason = "AMBIGUOUS_SAME_TIMESTAMP_LOCATION"
            return
        # Equal event time at the latest-evidence location is valid retained evidence,
        # but it is not a second temporal confirmation and must not alter candidate
        # movement state. reader_sequence is device-local metadata and is deliberately
        # not used as a global cross-reader tie-breaker. Only evidence at the confirmed
        # location reaffirms the aggregate balance.
        if matches_confirmed_location:
            _change_balance(
                db,
                tenant_id=state.tenant_id,
                store_id=state.store_id,
                zone_id=state.zone_id,
                sku_id=state.sku_id,
                delta=0,
                observed_at=observation.observed_at,
            )
        observation.status = ObservationStatus.PROCESSED
        observation.quarantine_reason = None
        return

    if state is None:
        state = InventoryItemState(
            tenant_id=observation.tenant_id,
            epc=observation.epc,
            sku_id=binding.sku_id,
            store_id=observation.store_id,
            zone_id=observation.zone_id,
            candidate_count=0,
            last_observed_at=observation.observed_at,
            last_event_id=observation.event_id,
            confidence=1.0,
        )
        db.add(state)
        _change_balance(
            db,
            tenant_id=observation.tenant_id,
            store_id=observation.store_id,
            zone_id=observation.zone_id,
            sku_id=binding.sku_id,
            delta=1,
            observed_at=observation.observed_at,
        )
        db.add(
            InventoryChange(
                tenant_id=observation.tenant_id,
                epc=observation.epc,
                sku_id=binding.sku_id,
                observation_id=observation.id,
                to_store_id=observation.store_id,
                to_zone_id=observation.zone_id,
                observed_at=observation.observed_at,
            )
        )
        _enqueue_replenishment(db, observation.tenant_id, observation.store_id, binding.sku_id)
    elif state.store_id == observation.store_id and state.zone_id == observation.zone_id:
        _change_balance(
            db,
            tenant_id=state.tenant_id,
            store_id=state.store_id,
            zone_id=state.zone_id,
            sku_id=state.sku_id,
            delta=0,
            observed_at=observation.observed_at,
        )
        state.candidate_store_id = None
        state.candidate_zone_id = None
        state.candidate_count = 0
        state.candidate_started_at = None
        state.last_observed_at = observation.observed_at
        state.last_event_id = observation.event_id
        state.confidence = 1.0
    else:
        settings = get_settings()
        same_candidate_within_window = (
            state.candidate_store_id == observation.store_id
            and state.candidate_zone_id == observation.zone_id
            and state.candidate_started_at is not None
            and observation.observed_at - state.candidate_started_at
            <= timedelta(seconds=settings.rfid_move_confirmation_window_seconds)
        )
        if same_candidate_within_window:
            state.candidate_count += 1
        else:
            state.candidate_store_id = observation.store_id
            state.candidate_zone_id = observation.zone_id
            state.candidate_count = 1
            state.candidate_started_at = observation.observed_at
        state.last_observed_at = observation.observed_at
        state.last_event_id = observation.event_id
        state.confidence = 0.5

        if state.candidate_count >= settings.rfid_move_confirmation_reads:
            old_store_id = state.store_id
            old_zone_id = state.zone_id
            old_zone = db.get(Zone, old_zone_id)
            new_zone = db.get(Zone, observation.zone_id)
            if old_zone is None or new_zone is None:  # pragma: no cover - protected by FKs
                raise ValueError("RFID state references a missing zone")
            consumes_replenishment_reservation = (
                old_store_id == observation.store_id
                and old_zone.kind is ZoneKind.BACKROOM
                and new_zone.kind is ZoneKind.SALES_FLOOR
            )
            # Serialize every affected store/SKU in deterministic order before
            # touching either balance. This prevents opposite-direction or
            # cross-store moves from locking the two aggregate rows in reverse order.
            for affected_store_id in sorted(
                {old_store_id, observation.store_id},
                key=str,
            ):
                lock_replenishment_store_sku(
                    db,
                    state.tenant_id,
                    affected_store_id,
                    state.sku_id,
                )
            _change_balance(
                db,
                tenant_id=state.tenant_id,
                store_id=old_store_id,
                zone_id=old_zone_id,
                sku_id=state.sku_id,
                delta=-1,
                observed_at=observation.observed_at,
            )
            _change_balance(
                db,
                tenant_id=state.tenant_id,
                store_id=observation.store_id,
                zone_id=observation.zone_id,
                sku_id=state.sku_id,
                delta=1,
                observed_at=observation.observed_at,
            )
            state.store_id = observation.store_id
            state.zone_id = observation.zone_id
            state.candidate_store_id = None
            state.candidate_zone_id = None
            state.candidate_count = 0
            state.candidate_started_at = None
            state.confidence = 1.0
            change = InventoryChange(
                tenant_id=state.tenant_id,
                epc=state.epc,
                sku_id=state.sku_id,
                observation_id=observation.id,
                from_store_id=old_store_id,
                from_zone_id=old_zone_id,
                to_store_id=state.store_id,
                to_zone_id=state.zone_id,
                observed_at=observation.observed_at,
            )
            db.add(change)
            db.flush()
            if consumes_replenishment_reservation:
                _allocate_replenishment_reservation(db, change)
            _enqueue_replenishment(db, state.tenant_id, old_store_id, state.sku_id)
            if old_store_id != state.store_id:
                _enqueue_replenishment(db, state.tenant_id, state.store_id, state.sku_id)

    observation.status = ObservationStatus.PROCESSED
    observation.quarantine_reason = None


def _enqueue_replenishment(
    db: Session,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> None:
    enqueue_job(
        db,
        tenant_id=tenant_id,
        kind=JobKind.REPLENISHMENT_RECALC,
        payload={"store_id": str(store_id), "sku_id": str(sku_id)},
    )


def list_balances(
    db: Session,
    tenant_id: uuid.UUID,
    *,
    store_id: uuid.UUID | None = None,
    limit: int,
    offset: int,
) -> tuple[list[tuple[InventoryBalance, Zone, Sku]], int]:
    predicates: list[Any] = [InventoryBalance.tenant_id == tenant_id]
    if store_id is not None:
        predicates.append(InventoryBalance.store_id == store_id)
    total = db.scalar(select(func.count()).select_from(InventoryBalance).where(*predicates))
    statement = (
        select(InventoryBalance, Zone, Sku)
        .join(Zone, Zone.id == InventoryBalance.zone_id)
        .join(Sku, Sku.id == InventoryBalance.sku_id)
        .where(*predicates)
        .order_by(InventoryBalance.store_id, Sku.code, Zone.code)
        .limit(limit)
        .offset(offset)
    )
    return list(db.execute(statement).tuples().all()), int(total or 0)


def list_observations(
    db: Session,
    tenant_id: uuid.UUID,
    *,
    status: ObservationStatus | None,
    epc: str | None,
    limit: int,
    offset: int,
) -> tuple[list[RfidObservation], int]:
    predicates: list[Any] = [RfidObservation.tenant_id == tenant_id]
    if status is not None:
        predicates.append(RfidObservation.status == status)
    if epc is not None:
        predicates.append(RfidObservation.epc == epc.strip().upper())
    total = db.scalar(select(func.count()).select_from(RfidObservation).where(*predicates))
    observations = list(
        db.scalars(
            select(RfidObservation)
            .where(*predicates)
            .order_by(RfidObservation.acceptance_sequence.desc())
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return observations, int(total or 0)


def replay_quarantined_observation(
    db: Session,
    tenant_id: uuid.UUID,
    observation_id: uuid.UUID,
) -> RfidObservation:
    require_reservation_cutover_ready(db)
    observation = db.scalar(
        select(RfidObservation).where(
            RfidObservation.id == observation_id,
            RfidObservation.tenant_id == tenant_id,
        )
    )
    if observation is None:
        raise ApiError(404, "Observation not found", "The RFID observation does not exist.")

    # Replay changes canonical eligibility. Serialize it with both new acceptance
    # and worker projection, then refresh under a row lock so concurrent replay calls
    # cannot both act on the same stale QUARANTINED object.
    _advisory_lock_epc(db, observation.tenant_id, observation.epc)
    db.refresh(observation, with_for_update=True)
    if observation.status != ObservationStatus.QUARANTINED:
        raise ApiError(409, "Observation is not quarantined", "Only quarantined events can replay.")

    if (
        observation.quarantine_reason == "OBSERVED_AT_TOO_FAR_IN_FUTURE"
        and observation.observed_at
        > datetime.now(UTC) + timedelta(seconds=get_settings().rfid_max_future_skew_seconds)
    ):
        raise ApiError(
            409,
            "Observation timestamp is still in the future",
            "Wait until the device timestamp falls within the accepted clock-skew window.",
            code="future_observation_unresolved",
        )

    assignment = _assignment_at(db, observation.device_id, observation.observed_at)
    if assignment is None:
        raise ApiError(
            409,
            "Device assignment is still unresolved",
            "Create or correct the effective-dated device assignment before replaying this event.",
            code="device_assignment_unresolved",
        )
    observation.store_id = assignment.store_id
    observation.zone_id = assignment.zone_id
    use_current_epc_binding = observation.quarantine_reason == "UNKNOWN_EPC"
    observation.status = ObservationStatus.RECEIVED
    observation.quarantine_reason = None
    enqueue_job(
        db,
        tenant_id=tenant_id,
        kind=JobKind.RFID_OBSERVATION,
        payload={
            "observation_id": str(observation.id),
            "use_current_epc_binding": use_current_epc_binding,
        },
    )
    db.commit()
    db.refresh(observation)
    return observation
