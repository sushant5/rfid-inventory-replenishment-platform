import uuid
from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, status
from sqlalchemy import or_, select

from abacus.api.dependencies import DatabaseSession, PlatformAccess
from abacus.api.errors import ApiError
from abacus.models.tenancy import Device, DeviceAssignment, Store, Zone
from abacus.schemas.tenancy import (
    BulkStoreOnboardingRequest,
    DeviceAssignmentRead,
    DeviceRead,
    DeviceTokenRead,
    OnboardingBatchRead,
    StoreDeviceCreate,
    StoreDeviceMappingRead,
    StoreDeviceRegistrationRead,
    StorePage,
    StoreRead,
    TenantCreate,
    TenantRead,
    ZoneCreate,
    ZoneRead,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.onboarding import (
    create_store_zone,
    create_tenant,
    list_stores,
    list_visible_stores,
    onboard_stores,
    register_store_device,
    rotate_device_credential,
)

router = APIRouter(prefix="/v1", tags=["1. Onboarding"])

CanConfigureTenant = Annotated[
    Principal,
    Depends(require_permission(Permission.TENANT_CONFIGURE)),
]
CanReadInventory = Annotated[
    Principal,
    Depends(require_permission(Permission.INVENTORY_READ)),
]


@router.post(
    "/tenants",
    response_model=TenantRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createTenant",
)
def create_tenant_endpoint(
    request: TenantCreate,
    db: DatabaseSession,
    _: PlatformAccess,
) -> TenantRead:
    return TenantRead.model_validate(create_tenant(db, request))


@router.post(
    "/tenants/{tenant_id}/store-imports",
    response_model=OnboardingBatchRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createStoreImport",
)
def bulk_onboard_stores_endpoint(
    tenant_id: uuid.UUID,
    request: BulkStoreOnboardingRequest,
    db: DatabaseSession,
    _: PlatformAccess,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=8, max_length=128)],
) -> OnboardingBatchRead:
    if not idempotency_key.strip():
        raise ApiError(400, "Invalid idempotency key", "Idempotency-Key cannot be blank.")
    batch = onboard_stores(db, tenant_id, idempotency_key.strip(), request)
    return OnboardingBatchRead.model_validate(batch)


@router.post(
    "/stores/{store_id}/zones",
    response_model=ZoneRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createStoreZone",
)
def create_store_zone_endpoint(
    store_id: uuid.UUID,
    request: ZoneCreate,
    db: DatabaseSession,
    principal: CanConfigureTenant,
) -> ZoneRead:
    return ZoneRead.model_validate(create_store_zone(db, principal, store_id, request))


@router.post(
    "/stores/{store_id}/devices",
    response_model=StoreDeviceRegistrationRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="registerStoreDevice",
)
def register_store_device_endpoint(
    store_id: uuid.UUID,
    request: StoreDeviceCreate,
    db: DatabaseSession,
    principal: CanConfigureTenant,
) -> StoreDeviceRegistrationRead:
    registration = register_store_device(db, principal, store_id, request)
    return StoreDeviceRegistrationRead(
        device=DeviceRead.model_validate(registration.device),
        assignment=DeviceAssignmentRead.model_validate(registration.assignment),
        device_token=registration.api_key,
    )


@router.get(
    "/stores/{store_id}/devices",
    response_model=list[StoreDeviceMappingRead],
    operation_id="listStoreDevices",
)
def list_store_devices_endpoint(
    store_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadInventory,
) -> list[StoreDeviceMappingRead]:
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
    effective_at = datetime.now(UTC)
    mappings = db.execute(
        select(Device, DeviceAssignment)
        .join(
            DeviceAssignment,
            (DeviceAssignment.device_id == Device.id)
            & (DeviceAssignment.tenant_id == Device.tenant_id),
        )
        .where(
            Device.tenant_id == principal.tenant_id,
            DeviceAssignment.store_id == store_id,
            DeviceAssignment.effective_from <= effective_at,
            or_(
                DeviceAssignment.effective_to.is_(None),
                DeviceAssignment.effective_to > effective_at,
            ),
        )
        .order_by(Device.serial_number)
    ).all()
    return [
        StoreDeviceMappingRead(
            device=DeviceRead.model_validate(device),
            assignment=DeviceAssignmentRead.model_validate(assignment),
        )
        for device, assignment in mappings
    ]


@router.post(
    "/devices/{device_id}/credentials:rotate",
    response_model=DeviceTokenRead,
    operation_id="rotateDeviceCredential",
)
def rotate_device_credential_endpoint(
    device_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanConfigureTenant,
) -> DeviceTokenRead:
    api_key = rotate_device_credential(db, principal.tenant_id, device_id)
    return DeviceTokenRead(device_id=device_id, device_token=api_key)


@router.get(
    "/stores",
    response_model=StorePage,
    operation_id="listStores",
)
def list_visible_stores_endpoint(
    db: DatabaseSession,
    principal: CanReadInventory,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> StorePage:
    stores, total = list_visible_stores(
        db,
        principal,
        limit=limit,
        offset=offset,
    )
    return StorePage(
        items=[StoreRead.model_validate(store) for store in stores],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/tenants/{tenant_id}/stores",
    response_model=list[StoreRead],
    operation_id="listTenantStores",
)
def list_tenant_stores_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> list[StoreRead]:
    return [StoreRead.model_validate(store) for store in list_stores(db, tenant_id)]


@router.get(
    "/stores/{store_id}/zones",
    response_model=list[ZoneRead],
    operation_id="listStoreZones",
)
def list_store_zones_endpoint(
    store_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadInventory,
) -> list[ZoneRead]:
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
    zones = db.scalars(
        select(Zone)
        .where(
            Zone.tenant_id == principal.tenant_id,
            Zone.store_id == store_id,
        )
        .order_by(Zone.code)
    ).all()
    return [ZoneRead.model_validate(zone) for zone in zones]
