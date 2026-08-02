import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status
from fastapi.security import APIKeyHeader

from abacus.api.dependencies import DatabaseSession, PlatformAccess
from abacus.api.errors import ApiError
from abacus.enums import ObservationStatus
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

device_router = APIRouter(prefix="/v1/device", tags=["3. RFID Ingestion"])
platform_router = APIRouter(tags=["3. RFID Inventory"])
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
