import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.api.errors import ApiError
from abacus.schemas.business_events import BusinessEventCreate, BusinessEventRead
from abacus.security import Permission, Principal, require_permission
from abacus.services.business_events import (
    AcceptedBusinessEvent,
    accept_authoritative_removal,
    get_business_event,
)

router = APIRouter(prefix="/v1", tags=["3. RFID and Inventory"])
CanAdjustInventory = Annotated[
    Principal,
    Depends(require_permission(Permission.INVENTORY_ADJUST)),
]
CanReadInventory = Annotated[
    Principal,
    Depends(require_permission(Permission.INVENTORY_READ)),
]


def _read(result: AcceptedBusinessEvent) -> BusinessEventRead:
    event = result.event
    return BusinessEventRead(
        id=event.id,
        store_id=event.store_id,
        source_system=event.source_system,
        external_event_id=event.external_event_id,
        event_type=event.event_type,
        epc=event.epc,
        occurred_at=event.occurred_at,
        processing_status=result.processing_status,
        transition_id=event.transition_id,
        state_version=event.state_version,
        note=event.note,
        created_at=event.created_at,
        idempotent_replay=not result.created,
    )


@router.post(
    "/stores/{store_id}/business-events",
    response_model=BusinessEventRead,
    status_code=status.HTTP_201_CREATED,
    responses={
        status.HTTP_200_OK: {
            "model": BusinessEventRead,
            "description": "The identical source event was already accepted",
        }
    },
    operation_id="createBusinessEvent",
)
def create_business_event_endpoint(
    store_id: uuid.UUID,
    request: BusinessEventCreate,
    response: Response,
    db: DatabaseSession,
    principal: CanAdjustInventory,
    settings: SettingsDependency,
) -> BusinessEventRead:
    if not principal.can_access_store(Permission.INVENTORY_ADJUST, store_id):
        raise ApiError(403, "Forbidden", "The store is outside the user's scope.")
    result = accept_authoritative_removal(
        db,
        tenant_id=principal.tenant_id,
        store_id=store_id,
        request=request,
        settings=settings,
    )
    if not result.created:
        response.status_code = status.HTTP_200_OK
    return _read(result)


@router.get(
    "/stores/{store_id}/business-events/{event_id}",
    response_model=BusinessEventRead,
    operation_id="getBusinessEvent",
)
def get_business_event_endpoint(
    store_id: uuid.UUID,
    event_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadInventory,
) -> BusinessEventRead:
    if not principal.can_access_store(Permission.INVENTORY_READ, store_id):
        raise ApiError(403, "Forbidden", "The store is outside the user's scope.")
    event, processing_status = get_business_event(
        db,
        tenant_id=principal.tenant_id,
        store_id=store_id,
        event_id=event_id,
    )
    return _read(AcceptedBusinessEvent(event, processing_status, True))
