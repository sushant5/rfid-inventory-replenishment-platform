import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.config import Settings
from abacus.events.inventory import InventoryDeltaEvent
from abacus.models.architecture import (
    BusinessEvent,
    BusinessEventStatus,
    CurrentItemState,
    InventoryTransitionOutbox,
)
from abacus.models.tenancy import Store
from abacus.schemas.business_events import BusinessEventCreate
from abacus.services.streaming_inventory import (
    deterministic_transition_id,
    processed_observation_watermark,
)


@dataclass(frozen=True, slots=True)
class AcceptedBusinessEvent:
    event: BusinessEvent
    processing_status: BusinessEventStatus
    created: bool


def _request_fingerprint(store_id: uuid.UUID, request: BusinessEventCreate) -> str:
    canonical = {
        "store_id": str(store_id),
        **request.model_dump(mode="json"),
    }
    encoded = json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded, usedforsecurity=False).hexdigest()


def business_event_status(db: Session, event: BusinessEvent) -> BusinessEventStatus:
    transition = db.get(InventoryTransitionOutbox, event.transition_id)
    if transition is None:
        return BusinessEventStatus.FAILED
    if transition.published_at is not None or transition.reconciled_at is not None:
        return BusinessEventStatus.PROJECTED
    if transition.quarantined_at is not None:
        return BusinessEventStatus.FAILED
    return BusinessEventStatus.PENDING_PROJECTION


def accept_authoritative_removal(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    request: BusinessEventCreate,
    settings: Settings,
) -> AcceptedBusinessEvent:
    """Apply one idempotent authoritative removal and enqueue its projection delta."""

    store_exists = db.scalar(
        select(Store.id).where(Store.tenant_id == tenant_id, Store.id == store_id)
    )
    if store_exists is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    if request.occurred_at > datetime.now(UTC) + timedelta(
        seconds=settings.rfid_max_future_skew_seconds
    ):
        raise ApiError(
            422,
            "Business event time is too far in the future",
            "occurred_at exceeds the configured clock-skew allowance.",
            code="business_event_time_too_far_in_future",
        )

    fingerprint = _request_fingerprint(store_id, request)
    lock_key = f"business-event:{tenant_id}:{request.source_system}:{request.external_event_id}"
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": lock_key},
    )
    existing = db.scalar(
        select(BusinessEvent).where(
            BusinessEvent.tenant_id == tenant_id,
            BusinessEvent.source_system == request.source_system,
            BusinessEvent.external_event_id == request.external_event_id,
        )
    )
    if existing is not None:
        if existing.request_fingerprint != fingerprint:
            raise ApiError(
                409,
                "Business event conflict",
                "The source event identifier was already used with different content.",
                code="business_event_idempotency_conflict",
            )
        return AcceptedBusinessEvent(existing, business_event_status(db, existing), False)

    state = db.scalar(
        select(CurrentItemState)
        .where(CurrentItemState.tenant_id == tenant_id, CurrentItemState.epc == request.epc)
        .with_for_update()
    )
    if state is None:
        raise ApiError(
            404,
            "Item not found",
            "The EPC has no confirmed current state to remove.",
            code="business_event_item_not_found",
        )

    if state.authoritative_removal_event_id is not None:
        raise ApiError(
            409,
            "Item already removed",
            "The EPC already has a different authoritative removal event.",
            code="business_event_item_already_removed",
        )
    if state.store_id is None or state.zone_id is None:
        raise ApiError(
            409,
            "Item location unavailable",
            "The EPC has no current store location; confirm it with RFID before removal.",
            code="business_event_location_unavailable",
        )
    if state.store_id != store_id:
        raise ApiError(
            409,
            "Item location conflict",
            "The EPC is not currently located in the requested store.",
            code="business_event_store_mismatch",
        )
    latest_processed_read = processed_observation_watermark(db, tenant_id, state.epc)
    durable_observation_watermark = max(
        value for value in (state.last_observed_at, latest_processed_read) if value is not None
    )
    if request.occurred_at < durable_observation_watermark:
        raise ApiError(
            409,
            "Stale business event",
            "The event occurred before the item's latest confirmed observation.",
            code="stale_business_event",
        )
    previous_store_id = state.store_id
    previous_zone_id = state.zone_id
    next_version = state.state_version + 1
    transition_id = deterministic_transition_id(tenant_id, state.epc, next_version)
    delta = InventoryDeltaEvent(
        delta_id=f"{transition_id}:{state.sku_id}:{previous_zone_id}",
        transition_id=transition_id,
        tenant_id=tenant_id,
        store_id=previous_store_id,
        sku_id=state.sku_id,
        zone_id=previous_zone_id,
        epc=state.epc,
        quantity_delta=-1,
        confidence=state.confidence,
        observed_at=request.occurred_at,
    )
    transition = InventoryTransitionOutbox(
        transition_id=transition_id,
        tenant_id=tenant_id,
        epc=state.epc,
        state_version=next_version,
        deltas=[delta.model_dump(mode="json")],
        publish_attempts=0,
    )
    db.add(transition)

    event_id = uuid.uuid4()
    event = BusinessEvent(
        id=event_id,
        tenant_id=tenant_id,
        store_id=store_id,
        source_system=request.source_system,
        external_event_id=request.external_event_id,
        request_fingerprint=fingerprint,
        event_type=request.event_type,
        epc=request.epc,
        occurred_at=request.occurred_at,
        transition_id=transition_id,
        state_version=next_version,
        note=request.note,
    )
    db.add(event)
    try:
        # Explicit flush order satisfies both the original and tenant-qualified
        # event-to-outbox foreign keys. All writes remain in this transaction.
        db.flush([transition])
        db.flush([event])
        state.store_id = None
        state.zone_id = None
        state.state_version = next_version
        state.authoritative_removal_event_id = event_id
        state.authoritative_removed_at = request.occurred_at
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Business event conflict",
            "The event could not be accepted because related state changed concurrently.",
            code="business_event_conflict",
        ) from exc
    db.refresh(event)
    return AcceptedBusinessEvent(event, BusinessEventStatus.PENDING_PROJECTION, True)


def get_business_event(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    event_id: uuid.UUID,
) -> tuple[BusinessEvent, BusinessEventStatus]:
    event = db.scalar(
        select(BusinessEvent).where(
            BusinessEvent.tenant_id == tenant_id,
            BusinessEvent.store_id == store_id,
            BusinessEvent.id == event_id,
        )
    )
    if event is None:
        raise ApiError(404, "Business event not found", "The requested event does not exist.")
    return event, business_event_status(db, event)
