import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from fastapi.security import APIKeyHeader
from sqlalchemy import func, select

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.api.errors import ApiError
from abacus.models.architecture import (
    CurrentItemState,
    InventoryProjection,
    RfidEventProcessingStatus,
    RfidObservationBatch,
    RfidObservationBatchEvent,
    RfidObservationEventLedger,
    RfidQuarantine,
    StoreConnectivity,
)
from abacus.models.catalog import Sku
from abacus.models.tenancy import Store, Zone
from abacus.schemas.architecture import (
    InventoryProjectionPage,
    InventoryProjectionRead,
    ItemStateRead,
    ObservationBatchAccepted,
    ObservationBatchCreate,
    ObservationBatchRead,
    RfidQuarantinePage,
    RfidQuarantineRead,
    RfidQuarantineReplayRead,
)
from abacus.schemas.catalog import normalize_epc
from abacus.security import Permission, Principal, require_permission
from abacus.services.device_auth import authenticate_device
from abacus.services.rfid_ingress import (
    accept_observation_batch,
    queue_quarantined_observation_replay,
)
from abacus.services.streaming_inventory import (
    current_inventory_bucket_metadata,
    effective_bucket_confidence,
    effective_freshness,
    effective_item_confidence,
    effective_presence_status,
)

router = APIRouter(prefix="/v1", tags=["3. RFID and Inventory"])
CanReadInventory = Annotated[
    Principal,
    Depends(require_permission(Permission.INVENTORY_READ)),
]
CanConfigureTenant = Annotated[
    Principal,
    Depends(require_permission(Permission.TENANT_CONFIGURE)),
]
_device_token_header = APIKeyHeader(
    name="X-Device-Token",
    scheme_name="DeviceToken",
    description="Opaque device credential returned only once during registration.",
    auto_error=False,
)
DeviceToken = Annotated[str | None, Depends(_device_token_header)]


@router.post(
    "/rfid/observation-batches",
    response_model=ObservationBatchAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="submitRfidObservationBatch",
)
def submit_observation_batch_endpoint(
    request: ObservationBatchCreate,
    db: DatabaseSession,
    x_device_token: DeviceToken,
) -> ObservationBatchAccepted:
    device = authenticate_device(db, x_device_token)
    batch, _events = accept_observation_batch(
        db,
        device=device,
        request=request,
        received_at=datetime.now(UTC),
    )
    return ObservationBatchAccepted(
        batch_id=batch.id,
        status=batch.status,
        accepted=batch.accepted_count,
    )


@router.get(
    "/rfid/observation-batches/{batch_id}",
    response_model=ObservationBatchRead,
    operation_id="getRfidObservationBatch",
)
def get_observation_batch_endpoint(
    batch_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadInventory,
) -> ObservationBatchRead:
    batch = db.scalar(
        select(RfidObservationBatch).where(
            RfidObservationBatch.id == batch_id,
            RfidObservationBatch.tenant_id == principal.tenant_id,
        )
    )
    if batch is None:
        raise ApiError(404, "RFID batch not found", "The requested batch does not exist.")
    event_store_ids = set(
        db.scalars(
            select(RfidObservationEventLedger.store_id)
            .join(
                RfidObservationBatchEvent,
                (RfidObservationBatchEvent.tenant_id == RfidObservationEventLedger.tenant_id)
                & (RfidObservationBatchEvent.event_id == RfidObservationEventLedger.event_id),
            )
            .where(
                RfidObservationBatchEvent.tenant_id == principal.tenant_id,
                RfidObservationBatchEvent.batch_id == batch.id,
            )
            .distinct()
        ).all()
    )
    batch_store_ids = event_store_ids | {batch.store_id}
    if any(
        not principal.can_access_store(Permission.INVENTORY_READ, store_id)
        for store_id in batch_store_ids
    ):
        raise ApiError(403, "Forbidden", "The batch contains stores outside the user's scope.")
    return ObservationBatchRead(
        batch_id=batch.id,
        status=batch.status,
        accepted=batch.accepted_count,
        processed=batch.processed_count,
        rejected=batch.rejected_count,
        pending=batch.pending_count,
    )


@router.get(
    "/rfid/quarantine",
    response_model=RfidQuarantinePage,
    operation_id="listRfidQuarantine",
)
def list_rfid_quarantine_endpoint(
    db: DatabaseSession,
    principal: CanReadInventory,
    batch_id: Annotated[uuid.UUID | None, Query()] = None,
    event_id: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    reason: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> RfidQuarantinePage:
    if not principal.has_tenant_permission(Permission.INVENTORY_READ):
        raise ApiError(
            403,
            "Forbidden",
            "Quarantine inspection requires tenant-wide inventory access.",
            code="tenant_inventory_scope_required",
        )
    predicates = [RfidQuarantine.tenant_id == principal.tenant_id]
    if batch_id is not None:
        predicates.append(RfidQuarantine.batch_id == batch_id)
    if event_id is not None:
        predicates.append(RfidQuarantine.event_id == event_id)
    if reason is not None:
        predicates.append(RfidQuarantine.reason == reason)
    total = db.scalar(select(func.count(RfidQuarantine.id)).where(*predicates)) or 0
    records = db.execute(
        select(
            RfidQuarantine,
            RfidObservationEventLedger.processing_status,
            RfidObservationEventLedger.processed_at,
            RfidObservationEventLedger.rejection_reason,
        )
        .outerjoin(
            RfidObservationEventLedger,
            (RfidObservationEventLedger.tenant_id == RfidQuarantine.tenant_id)
            & (RfidObservationEventLedger.event_id == RfidQuarantine.event_id),
        )
        .where(*predicates)
        .order_by(
            RfidQuarantine.quarantined_at.desc(),
            RfidQuarantine.id.desc(),
        )
        .limit(limit)
        .offset(offset)
    ).all()
    items = [
        RfidQuarantineRead(
            id=record.id,
            batch_id=record.batch_id,
            event_id=record.event_id,
            reason=record.reason,
            current_rejection_reason=current_rejection_reason,
            payload=record.payload,
            quarantined_at=record.quarantined_at,
            processing_status=processing_status,
            resolved_at=(
                processed_at if processing_status is RfidEventProcessingStatus.PROCESSED else None
            ),
        )
        for record, processing_status, processed_at, current_rejection_reason in records
    ]
    return RfidQuarantinePage(
        items=items,
        total=total,
        limit=limit,
        offset=offset,
    )


@router.post(
    "/rfid/quarantine/{quarantine_id}:replay",
    response_model=RfidQuarantineReplayRead,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="replayRfidQuarantine",
)
def replay_rfid_quarantine_endpoint(
    quarantine_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanConfigureTenant,
) -> RfidQuarantineReplayRead:
    result = queue_quarantined_observation_replay(
        db,
        tenant_id=principal.tenant_id,
        quarantine_id=quarantine_id,
    )
    return RfidQuarantineReplayRead(
        quarantine_id=result.quarantine_id,
        batch_id=result.batch_id,
        event_id=result.event_id,
        processing_status=result.processing_status,
        queued=result.queued,
    )


@router.get(
    "/stores/{store_id}/inventory",
    response_model=InventoryProjectionPage,
    operation_id="getStoreInventory",
)
def get_store_inventory_endpoint(
    store_id: uuid.UUID,
    db: DatabaseSession,
    settings: SettingsDependency,
    principal: CanReadInventory,
    sku_id: uuid.UUID | None = None,
    zone_id: uuid.UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InventoryProjectionPage:
    if not principal.can_access_store(Permission.INVENTORY_READ, store_id):
        raise ApiError(403, "Forbidden", "The store is outside the current user's scope.")
    store_exists = db.scalar(
        select(Store.id).where(
            Store.id == store_id,
            Store.tenant_id == principal.tenant_id,
        )
    )
    if store_exists is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    predicates = [
        InventoryProjection.tenant_id == principal.tenant_id,
        InventoryProjection.store_id == store_id,
    ]
    if sku_id is not None:
        predicates.append(InventoryProjection.sku_id == sku_id)
    if zone_id is not None:
        predicates.append(InventoryProjection.zone_id == zone_id)
    total = db.scalar(select(func.count()).select_from(InventoryProjection).where(*predicates)) or 0
    evaluated_at = datetime.now(UTC)
    current_metadata = current_inventory_bucket_metadata(
        tenant_id=principal.tenant_id,
        store_id=store_id,
        evaluated_at=evaluated_at,
        confidence_half_life_seconds=settings.rfid_confidence_half_life_seconds,
    )
    rows = db.execute(
        select(
            InventoryProjection,
            Sku,
            Zone,
            current_metadata.c.item_count,
            current_metadata.c.as_of,
            current_metadata.c.oldest_item_observed_at,
            current_metadata.c.confidence,
        )
        .join(Sku, Sku.id == InventoryProjection.sku_id)
        .join(Zone, Zone.id == InventoryProjection.zone_id)
        .outerjoin(
            current_metadata,
            (current_metadata.c.tenant_id == InventoryProjection.tenant_id)
            & (current_metadata.c.store_id == InventoryProjection.store_id)
            & (current_metadata.c.sku_id == InventoryProjection.sku_id)
            & (current_metadata.c.zone_id == InventoryProjection.zone_id),
        )
        .where(*predicates)
        .order_by(Sku.code, Zone.code, InventoryProjection.sku_id, InventoryProjection.zone_id)
        .limit(limit)
        .offset(offset)
    ).all()
    connectivity = db.get(StoreConnectivity, (principal.tenant_id, store_id))
    freshness = effective_freshness(connectivity, settings, now=evaluated_at)
    return InventoryProjectionPage(
        items=[
            InventoryProjectionRead(
                sku_id=projection.sku_id,
                sku=sku.code,
                zone_id=projection.zone_id,
                zone=zone.code,
                quantity=projection.quantity,
                as_of=current_as_of or projection.as_of,
                oldest_item_observed_at=current_oldest_observed_at or projection.as_of,
                confidence=effective_bucket_confidence(
                    projected_quantity=projection.quantity,
                    current_item_count=current_item_count,
                    current_confidence=current_confidence,
                ),
                freshness_status=freshness,
            )
            for (
                projection,
                sku,
                zone,
                current_item_count,
                current_as_of,
                current_oldest_observed_at,
                current_confidence,
            ) in rows
        ],
        total=int(total),
        limit=limit,
        offset=offset,
    )


@router.get(
    "/items/{epc}",
    response_model=ItemStateRead,
    operation_id="getCurrentItemState",
)
def get_item_state_endpoint(
    epc: str,
    db: DatabaseSession,
    settings: SettingsDependency,
    principal: CanReadInventory,
) -> ItemStateRead:
    try:
        normalized_epc = normalize_epc(epc)
    except ValueError as exc:
        raise ApiError(
            422,
            "Invalid EPC",
            str(exc),
            code="invalid_epc",
        ) from exc
    row = db.execute(
        select(CurrentItemState, Sku)
        .join(Sku, Sku.id == CurrentItemState.sku_id)
        .where(
            CurrentItemState.tenant_id == principal.tenant_id,
            CurrentItemState.epc == normalized_epc,
        )
    ).one_or_none()
    if row is None:
        raise ApiError(404, "Item not found", "The EPC has no confirmed current state.")
    item, sku = row
    if item.store_id is None and not principal.has_tenant_permission(Permission.INVENTORY_READ):
        raise ApiError(
            403,
            "Forbidden",
            "Unlocated items require tenant-wide inventory access.",
            code="tenant_inventory_scope_required",
        )
    if item.store_id is not None and not principal.can_access_store(
        Permission.INVENTORY_READ, item.store_id
    ):
        raise ApiError(403, "Forbidden", "The item's store is outside the user's scope.")
    connectivity = (
        db.get(StoreConnectivity, (principal.tenant_id, item.store_id))
        if item.store_id is not None
        else None
    )
    evaluated_at = datetime.now(UTC)
    return ItemStateRead(
        epc=item.epc,
        sku_id=item.sku_id,
        sku=sku.code,
        store_id=item.store_id,
        zone_id=item.zone_id,
        last_observed_at=item.last_observed_at,
        last_received_at=item.last_received_at,
        confidence=effective_item_confidence(
            stored_confidence=item.confidence,
            last_observed_at=item.last_observed_at,
            evaluated_at=evaluated_at,
            half_life_seconds=settings.rfid_confidence_half_life_seconds,
        ),
        presence_status=effective_presence_status(
            item,
            settings,
            now=evaluated_at,
        ),
        authoritative_removal_event_id=item.authoritative_removal_event_id,
        authoritative_removed_at=item.authoritative_removed_at,
        state_version=item.state_version,
        freshness_status=effective_freshness(connectivity, settings, now=evaluated_at),
    )
