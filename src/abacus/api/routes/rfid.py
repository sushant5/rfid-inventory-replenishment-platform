import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from fastapi.security import APIKeyHeader
from sqlalchemy import func, select

from abacus.api.dependencies import (
    DatabaseSession,
    PlatformAccess,
    SettingsDependency,
)
from abacus.api.errors import ApiError
from abacus.enums import ObservationStatus
from abacus.models.architecture import (
    CurrentItemState,
    InventoryProjection,
    RfidObservationBatch,
    RfidQuarantine,
    StoreConnectivity,
)
from abacus.models.catalog import Sku
from abacus.models.tenancy import Store, Zone
from abacus.schemas.architecture import (
    CanonicalObservationBatchCreate,
    InventoryProjectionPage,
    InventoryProjectionRead,
    ItemStateRead,
    ObservationBatchAccepted,
    ObservationBatchRead,
    RfidQuarantinePage,
    RfidQuarantineRead,
)
from abacus.schemas.catalog import normalize_epc
from abacus.schemas.rfid import (
    InventoryBalanceListRead,
    InventoryBalanceRead,
    RfidBatchInput,
    RfidBatchReceipt,
    RfidObservationListRead,
    RfidObservationRead,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.rfid import (
    authenticate_device,
    ingest_batch,
    list_balances,
    list_observations,
    replay_quarantined_observation,
)
from abacus.services.rfid_ingress import accept_observation_batch
from abacus.services.streaming_inventory import (
    current_inventory_bucket_metadata,
    effective_bucket_confidence,
    effective_freshness,
    effective_item_confidence,
)

device_router = APIRouter(prefix="/v1/device", tags=["3. RFID Ingestion"])
platform_router = APIRouter(tags=["3. RFID Inventory"])
canonical_router = APIRouter(prefix="/v1", tags=["3. RFID and Inventory"])
CanReadInventory = Annotated[
    Principal,
    Depends(require_permission(Permission.INVENTORY_READ)),
]
_device_key_header = APIKeyHeader(
    name="X-Device-Key",
    scheme_name="DeviceApiKey",
    description=(
        "RFID reader or gateway API key; plaintext is returned once and remains valid "
        "until rotation."
    ),
    auto_error=False,
)
DeviceKey = Annotated[str | None, Depends(_device_key_header)]
_device_token_header = APIKeyHeader(
    name="X-Device-Token",
    scheme_name="DeviceToken",
    description="Demo device credential returned once during registration.",
    auto_error=False,
)
DeviceToken = Annotated[str | None, Depends(_device_token_header)]


@canonical_router.post(
    "/rfid/observation-batches",
    response_model=ObservationBatchAccepted,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="submitRfidObservationBatch",
)
def submit_observation_batch_endpoint(
    request: CanonicalObservationBatchCreate,
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


@canonical_router.get(
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
    if not principal.can_access_store(Permission.INVENTORY_READ, batch.store_id):
        raise ApiError(403, "Forbidden", "The batch's store is outside the user's scope.")
    return ObservationBatchRead(
        batch_id=batch.id,
        status=batch.status,
        accepted=batch.accepted_count,
        processed=batch.processed_count,
        rejected=batch.rejected_count,
        pending=batch.pending_count,
    )


@canonical_router.get(
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
    records = list(
        db.scalars(
            select(RfidQuarantine)
            .where(*predicates)
            .order_by(
                RfidQuarantine.quarantined_at.desc(),
                RfidQuarantine.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return RfidQuarantinePage(
        items=[RfidQuarantineRead.model_validate(record) for record in records],
        total=total,
        limit=limit,
        offset=offset,
    )


@canonical_router.get(
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
    store_exists = db.scalar(
        select(Store.id).where(
            Store.id == store_id,
            Store.tenant_id == principal.tenant_id,
        )
    )
    if store_exists is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    if not principal.can_access_store(Permission.INVENTORY_READ, store_id):
        raise ApiError(403, "Forbidden", "The store is outside the current user's scope.")
    predicates = [
        InventoryProjection.tenant_id == principal.tenant_id,
        InventoryProjection.store_id == store_id,
    ]
    if sku_id is not None:
        predicates.append(InventoryProjection.sku_id == sku_id)
    if zone_id is not None:
        predicates.append(InventoryProjection.zone_id == zone_id)
    total = (
        db.scalar(
            select(func.count())
            .select_from(InventoryProjection)
            .where(*predicates)
        )
        or 0
    )
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


@canonical_router.get(
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
        state_version=item.state_version,
        freshness_status=effective_freshness(connectivity, settings, now=evaluated_at),
    )


@device_router.post(
    "/read-batches",
    response_model=RfidBatchReceipt,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="ingestRfidReadBatch",
)
def ingest_rfid_batch_endpoint(
    request: RfidBatchInput,
    db: DatabaseSession,
    x_device_key: DeviceKey,
) -> RfidBatchReceipt:
    device = authenticate_device(db, x_device_key)
    return ingest_batch(db, device, request)


@platform_router.get(
    "/v1/tenants/{tenant_id}/inventory",
    response_model=InventoryBalanceListRead,
    operation_id="listInventoryBalances",
)
def list_inventory_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadInventory,
    store_id: uuid.UUID | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> InventoryBalanceListRead:
    if principal.tenant_id != tenant_id:
        raise ApiError(
            403,
            "Forbidden",
            "The requested tenant is outside the current user's access scope.",
            code="tenant_scope_denied",
        )
    if store_id is None and not principal.has_tenant_permission(Permission.INVENTORY_READ):
        raise ApiError(
            400,
            "Store filter required",
            "Store-scoped users must specify store_id when listing inventory.",
            code="store_filter_required",
        )
    if store_id is not None and not principal.can_access_store(Permission.INVENTORY_READ, store_id):
        raise ApiError(
            403,
            "Forbidden",
            "The requested store is outside the current user's access scope.",
            code="store_scope_denied",
        )
    rows, total = list_balances(
        db,
        tenant_id,
        store_id=store_id,
        limit=limit,
        offset=offset,
    )
    return InventoryBalanceListRead(
        items=[
            InventoryBalanceRead(
                tenant_id=balance.tenant_id,
                store_id=balance.store_id,
                zone_id=balance.zone_id,
                zone_kind=zone.kind,
                sku_id=balance.sku_id,
                sku_code=sku.code,
                quantity=balance.quantity,
                projection_updated_at=balance.updated_at,
                last_relevant_observation_at=balance.last_relevant_observation_at,
            )
            for balance, zone, sku in rows
        ],
        total=total,
        limit=limit,
        offset=offset,
    )


@platform_router.post(
    "/v1/platform/tenants/{tenant_id}/rfid/observations/{observation_id}:replay",
    response_model=RfidObservationRead,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="replayQuarantinedObservation",
)
def replay_observation_endpoint(
    tenant_id: uuid.UUID,
    observation_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> RfidObservationRead:
    observation = replay_quarantined_observation(db, tenant_id, observation_id)
    return RfidObservationRead.model_validate(observation)


@platform_router.get(
    "/v1/platform/tenants/{tenant_id}/rfid/observations",
    response_model=RfidObservationListRead,
    operation_id="listRfidObservations",
)
def list_observations_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
    observation_status: Annotated[
        ObservationStatus | None,
        Query(alias="status"),
    ] = None,
    epc: Annotated[str | None, Query(min_length=4, max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> RfidObservationListRead:
    observations, total = list_observations(
        db,
        tenant_id,
        status=observation_status,
        epc=epc,
        limit=limit,
        offset=offset,
    )
    return RfidObservationListRead(
        items=[RfidObservationRead.model_validate(item) for item in observations],
        total=total,
        limit=limit,
        offset=offset,
    )
