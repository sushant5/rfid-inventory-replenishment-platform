import uuid
from typing import Annotated

from fastapi import APIRouter, Header, status

from abacus.api.dependencies import DatabaseSession, PlatformAccess
from abacus.api.errors import ApiError
from abacus.schemas.tenancy import (
    BulkStoreOnboardingRequest,
    DeviceAssignmentCreate,
    DeviceAssignmentRead,
    DeviceCredentialRead,
    DeviceRead,
    OnboardingBatchRead,
    StoreRead,
    TenantCreate,
    TenantRead,
)
from abacus.services.onboarding import (
    assign_device,
    create_tenant,
    list_device_assignments,
    list_devices,
    list_stores,
    onboard_stores,
    rotate_device_credential,
)

router = APIRouter(prefix="/v1/platform", tags=["1. Onboarding"])


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
    "/tenants/{tenant_id}/stores:bulk-onboard",
    response_model=OnboardingBatchRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="bulkOnboardStores",
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
    "/tenants/{tenant_id}/devices",
    response_model=list[DeviceRead],
    operation_id="listTenantDevices",
)
def list_tenant_devices_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> list[DeviceRead]:
    return [DeviceRead.model_validate(device) for device in list_devices(db, tenant_id)]


@router.post(
    "/tenants/{tenant_id}/devices/{device_id}/credentials:rotate",
    response_model=DeviceCredentialRead,
    operation_id="rotateDeviceCredential",
)
def rotate_device_credential_endpoint(
    tenant_id: uuid.UUID,
    device_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> DeviceCredentialRead:
    api_key = rotate_device_credential(db, tenant_id, device_id)
    return DeviceCredentialRead(device_id=device_id, api_key=api_key)


@router.post(
    "/tenants/{tenant_id}/devices/{device_id}/assignments",
    response_model=DeviceAssignmentRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="assignDevice",
)
def assign_device_endpoint(
    tenant_id: uuid.UUID,
    device_id: uuid.UUID,
    request: DeviceAssignmentCreate,
    db: DatabaseSession,
    _: PlatformAccess,
) -> DeviceAssignmentRead:
    return DeviceAssignmentRead.model_validate(assign_device(db, tenant_id, device_id, request))


@router.get(
    "/tenants/{tenant_id}/devices/{device_id}/assignments",
    response_model=list[DeviceAssignmentRead],
    operation_id="listDeviceAssignments",
)
def list_device_assignments_endpoint(
    tenant_id: uuid.UUID,
    device_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> list[DeviceAssignmentRead]:
    return [
        DeviceAssignmentRead.model_validate(item)
        for item in list_device_assignments(db, tenant_id, device_id)
    ]
