import hashlib
import json
import secrets
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sqlalchemy import func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.config import get_settings
from abacus.enums import DeviceStatus, JobKind, ObservationStatus, TenantStatus
from abacus.models.catalog import Sku
from abacus.models.rfid import (
    InventoryBalance,
    InventoryChange,
    InventoryItemState,
    RfidObservation,
)
from abacus.models.tenancy import Device, DeviceAssignment, Tenant, Zone
from abacus.schemas.rfid import RfidBatchInput, RfidBatchReceipt, RfidEventIngressResult
from abacus.services.catalog import resolve_active_epc
from abacus.services.jobs import enqueue_job


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


def ingest_batch(db: Session, device: Device, request: RfidBatchInput) -> RfidBatchReceipt:
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
    now = datetime.now(UTC)
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
    if balance is None:
        if delta < 0:
            raise ValueError("inventory balance would become negative")
        db.add(
            InventoryBalance(
                tenant_id=tenant_id,
                store_id=store_id,
                zone_id=zone_id,
                sku_id=sku_id,
                quantity=delta,
            )
        )
        return
    if balance.quantity + delta < 0:
        raise ValueError("inventory balance would become negative")
    balance.quantity += delta


def process_rfid_observation_job(db: Session, payload: dict[str, object]) -> None:
    observation_id = uuid.UUID(str(payload["observation_id"]))
    observation = db.scalar(
        select(RfidObservation).where(RfidObservation.id == observation_id).with_for_update()
    )
    if observation is None:
        raise ValueError("RFID observation does not exist")
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

    lock_key = f"{observation.tenant_id}:{observation.epc}"
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": lock_key},
    )
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
            _change_balance(
                db,
                tenant_id=state.tenant_id,
                store_id=old_store_id,
                zone_id=old_zone_id,
                sku_id=state.sku_id,
                delta=-1,
            )
            _change_balance(
                db,
                tenant_id=state.tenant_id,
                store_id=observation.store_id,
                zone_id=observation.zone_id,
                sku_id=state.sku_id,
                delta=1,
            )
            state.store_id = observation.store_id
            state.zone_id = observation.zone_id
            state.candidate_store_id = None
            state.candidate_zone_id = None
            state.candidate_count = 0
            state.candidate_started_at = None
            state.confidence = 1.0
            db.add(
                InventoryChange(
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
            )
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
            .order_by(RfidObservation.ingested_at.desc(), RfidObservation.id.desc())
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
    observation = db.scalar(
        select(RfidObservation).where(
            RfidObservation.id == observation_id,
            RfidObservation.tenant_id == tenant_id,
        )
    )
    if observation is None:
        raise ApiError(404, "Observation not found", "The RFID observation does not exist.")
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
