import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from abacus.api.dependencies import DatabaseSession
from abacus.models.identity import IdentityAuditRecord, UserAccessGrant
from abacus.schemas.identity import (
    IdentityAuditPage,
    IdentityAuditRecordRead,
    RoleAssignmentRead,
    UserCreate,
    UserPage,
    UserRead,
    UserRolesRead,
    UserRolesReplace,
    UserStoreAssignmentsRead,
    UserStoreAssignmentsReplace,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.identity import (
    UserRecord,
    create_user,
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


def _role_assignment_read(grant: UserAccessGrant) -> RoleAssignmentRead:
    return RoleAssignmentRead(role=grant.role, store_id=grant.store_id)


def _user_read(record: UserRecord) -> UserRead:
    user = record.user
    return UserRead(
        id=user.id,
        tenant_id=user.tenant_id,
        email=user.email,
        display_name=user.display_name,
        status=user.status,
        role_assignments=[_role_assignment_read(grant) for grant in record.grants],
        last_login_at=user.last_login_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )


def _audit_read(record: IdentityAuditRecord) -> IdentityAuditRecordRead:
    return IdentityAuditRecordRead.model_validate(record)


@router.post(
    "",
    response_model=UserRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createUser",
)
def create_user_endpoint(
    request: UserCreate,
    db: DatabaseSession,
    principal: CanCreateUsers,
) -> UserRead:
    return _user_read(create_user(db, principal, request))


@router.get(
    "",
    response_model=UserPage,
    operation_id="listUsers",
)
def list_users_endpoint(
    db: DatabaseSession,
    principal: CanReadUsers,
    store_id: Annotated[uuid.UUID | None, Query()] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> UserPage:
    records, total = list_users(
        db,
        principal,
        store_id=store_id,
        limit=limit,
        offset=offset,
    )
    return UserPage(
        items=[_user_read(record) for record in records],
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
    response_model=UserRead,
    operation_id="getUser",
)
def get_user_endpoint(
    user_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadUsers,
) -> UserRead:
    return _user_read(get_user(db, principal, user_id))


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
    response_model=UserRead,
    operation_id="suspendUser",
)
def suspend_user_endpoint(
    user_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanSuspendUsers,
) -> UserRead:
    return _user_read(suspend_user(db, principal, user_id))
