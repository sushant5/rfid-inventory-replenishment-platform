import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from abacus.api.dependencies import DatabaseSession
from abacus.models.identity import IdentityAuditRecord
from abacus.schemas.identity import (
    CanonicalUserCreate,
    CanonicalUserPage,
    CanonicalUserRead,
    IdentityAuditPage,
    IdentityAuditRecordRead,
    UserRolesRead,
    UserRolesReplace,
    UserStoreAssignmentsRead,
    UserStoreAssignmentsReplace,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.identity import (
    CanonicalUserRecord,
    canonicalize_user_record,
    create_canonical_user,
    get_user,
    list_audit_records,
    list_users,
    replace_user_roles,
    replace_user_store_assignments,
    suspend_user,
)

router = APIRouter(prefix="/v1/users", tags=["4. Identity and Access"])

CanCreateUsers = Annotated[Principal, Depends(require_permission(Permission.USERS_CREATE))]
CanReadUsers = Annotated[Principal, Depends(require_permission(Permission.USERS_READ))]
CanSuspendUsers = Annotated[Principal, Depends(require_permission(Permission.USERS_SUSPEND))]
CanReadIdentityAudit = Annotated[
    Principal,
    Depends(require_permission(Permission.IDENTITY_AUDIT_READ)),
]


def _user_read(record: CanonicalUserRecord) -> CanonicalUserRead:
    user = record.user
    return CanonicalUserRead(
        id=user.id,
        tenant_id=user.tenant_id,
        email=user.email,
        display_name=user.display_name,
        status=user.status,
        roles=list(record.roles),
        store_ids=list(record.store_ids),
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


def _audit_read(record: IdentityAuditRecord) -> IdentityAuditRecordRead:
    return IdentityAuditRecordRead.model_validate(record)


@router.post(
    "",
    response_model=CanonicalUserRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createUser",
)
def create_user_endpoint(
    request: CanonicalUserCreate,
    db: DatabaseSession,
    principal: CanCreateUsers,
) -> CanonicalUserRead:
    return _user_read(create_canonical_user(db, principal, request))


@router.get(
    "",
    response_model=CanonicalUserPage,
    operation_id="listUsers",
)
def list_users_endpoint(
    db: DatabaseSession,
    principal: CanReadUsers,
    store_id: Annotated[uuid.UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CanonicalUserPage:
    records, total = list_users(
        db,
        principal,
        store_id=store_id,
        limit=limit,
        offset=offset,
    )
    return CanonicalUserPage(
        items=[_user_read(canonicalize_user_record(db, record, principal)) for record in records],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/audit-records",
    response_model=IdentityAuditPage,
    operation_id="listIdentityAuditRecords",
)
def list_identity_audit_records_endpoint(
    db: DatabaseSession,
    principal: CanReadIdentityAudit,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> IdentityAuditPage:
    records, total = list_audit_records(db, principal, limit=limit, offset=offset)
    return IdentityAuditPage(
        items=[_audit_read(record) for record in records],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/{user_id}",
    response_model=CanonicalUserRead,
    operation_id="getUser",
)
def get_user_endpoint(
    user_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadUsers,
) -> CanonicalUserRead:
    return _user_read(canonicalize_user_record(db, get_user(db, principal, user_id), principal))


@router.put(
    "/{user_id}/roles",
    response_model=UserRolesRead,
    operation_id="replaceUserRoles",
)
def replace_user_roles_endpoint(
    user_id: uuid.UUID,
    request: UserRolesReplace,
    db: DatabaseSession,
    principal: CanCreateUsers,
) -> UserRolesRead:
    access = replace_user_roles(db, principal, user_id, request)
    return UserRolesRead(user_id=access.user_id, roles=list(access.roles))


@router.put(
    "/{user_id}/store-assignments",
    response_model=UserStoreAssignmentsRead,
    operation_id="replaceUserStoreAssignments",
)
def replace_user_store_assignments_endpoint(
    user_id: uuid.UUID,
    request: UserStoreAssignmentsReplace,
    db: DatabaseSession,
    principal: CanCreateUsers,
) -> UserStoreAssignmentsRead:
    access = replace_user_store_assignments(db, principal, user_id, request)
    return UserStoreAssignmentsRead(user_id=access.user_id, store_ids=list(access.store_ids))


@router.post(
    "/{user_id}:suspend",
    response_model=CanonicalUserRead,
    operation_id="suspendUser",
)
def suspend_user_endpoint(
    user_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanSuspendUsers,
) -> CanonicalUserRead:
    return _user_read(canonicalize_user_record(db, suspend_user(db, principal, user_id), principal))
