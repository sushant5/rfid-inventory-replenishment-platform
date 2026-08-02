import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.enums import StoreStatus, TenantStatus
from abacus.models.identity import (
    IdentityAuditAction,
    IdentityAuditRecord,
    IdentityRole,
    User,
    UserAccessGrant,
    UserStatus,
)
from abacus.models.tenancy import Store, Tenant
from abacus.schemas.identity import UserCreate
from abacus.schemas.tenancy import TenantCreate
from abacus.security import (
    Permission,
    Principal,
    RoleScope,
    ensure_can_assign_roles,
    hash_password,
    verify_password_and_update,
)


@dataclass(frozen=True, slots=True)
class UserRecord:
    user: User
    grants: tuple[UserAccessGrant, ...]


def bootstrap_corporate_admin(
    db: Session,
    tenant_request: TenantCreate,
    user_request: UserCreate,
) -> UserRecord:
    """Create the first tenant administrator from a trusted deployment command."""

    if len(user_request.role_assignments) != 1 or (
        user_request.role_assignments[0].role is not IdentityRole.CORPORATE_ADMIN
    ):
        raise ValueError("bootstrap user must have exactly one CORPORATE_ADMIN grant")

    tenant = db.scalar(select(Tenant).where(Tenant.code == tenant_request.code))
    if tenant is None:
        tenant = Tenant(
            code=tenant_request.code,
            name=tenant_request.name,
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        db.flush()
    elif tenant.name != tenant_request.name:
        raise ApiError(
            409,
            "Tenant conflict",
            "The configured bootstrap tenant code already has another name.",
            code="bootstrap_tenant_conflict",
        )

    configured_admin = db.execute(
        select(User, UserAccessGrant)
        .join(UserAccessGrant, UserAccessGrant.user_id == User.id)
        .where(
            User.tenant_id == tenant.id,
            User.email == str(user_request.email),
            UserAccessGrant.tenant_id == tenant.id,
            UserAccessGrant.role == IdentityRole.CORPORATE_ADMIN,
            UserAccessGrant.store_id.is_(None),
        )
    ).first()
    if configured_admin is not None:
        user, grant = configured_admin
        return UserRecord(user=user, grants=(grant,))

    any_admin_exists = db.scalar(
        select(UserAccessGrant.id)
        .where(
            UserAccessGrant.tenant_id == tenant.id,
            UserAccessGrant.role == IdentityRole.CORPORATE_ADMIN,
            UserAccessGrant.store_id.is_(None),
        )
        .limit(1)
    )
    if any_admin_exists is not None:
        raise ApiError(
            409,
            "Tenant already bootstrapped",
            "A different corporate administrator already exists for this tenant.",
            code="bootstrap_admin_exists",
        )

    existing_user = db.scalar(
        select(User).where(
            User.tenant_id == tenant.id,
            User.email == str(user_request.email),
        )
    )
    if existing_user is not None:
        raise ApiError(
            409,
            "Bootstrap user conflict",
            "The configured email already exists without the corporate administrator grant.",
            code="bootstrap_user_conflict",
        )

    user = User(
        tenant_id=tenant.id,
        email=str(user_request.email),
        display_name=user_request.display_name,
        password_hash=hash_password(user_request.password.get_secret_value()),
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    db.add(user)
    db.flush()
    grant = UserAccessGrant(
        tenant_id=tenant.id,
        user_id=user.id,
        role=IdentityRole.CORPORATE_ADMIN,
        store_id=None,
    )
    db.add(grant)
    _add_audit_record(
        db,
        tenant_id=tenant.id,
        actor_user_id=None,
        action=IdentityAuditAction.USER_CREATED,
        target_user_id=user.id,
        details={"email": user.email, "bootstrap": True, "role": grant.role.value},
    )
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Bootstrap conflict",
            "The tenant or initial administrator was created concurrently.",
            code="bootstrap_conflict",
        ) from exc
    db.refresh(user)
    return UserRecord(user=user, grants=(grant,))


def _add_audit_record(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    actor_user_id: uuid.UUID | None,
    action: IdentityAuditAction,
    target_user_id: uuid.UUID | None,
    details: dict[str, object],
) -> None:
    db.add(
        IdentityAuditRecord(
            tenant_id=tenant_id,
            actor_user_id=actor_user_id,
            action=action,
            target_user_id=target_user_id,
            details=details,
        )
    )


def _load_grants(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> tuple[UserAccessGrant, ...]:
    return tuple(
        db.scalars(
            select(UserAccessGrant)
            .where(
                UserAccessGrant.tenant_id == tenant_id,
                UserAccessGrant.user_id == user_id,
            )
            .order_by(UserAccessGrant.role.asc(), UserAccessGrant.store_id.asc())
        ).all()
    )


def authenticate_user(
    db: Session,
    *,
    tenant_code: str,
    email: str,
    password: str,
) -> User:
    tenant = db.scalar(select(Tenant).where(Tenant.code == tenant_code))
    user: User | None = None
    if tenant is not None:
        user = db.scalar(
            select(User).where(
                User.tenant_id == tenant.id,
                User.email == email,
            )
        )

    verified, replacement_hash = verify_password_and_update(
        password,
        user.password_hash if user is not None else None,
    )
    authentication_allowed = (
        tenant is not None
        and tenant.status == TenantStatus.ACTIVE
        and user is not None
        and user.status == UserStatus.ACTIVE
        and verified
    )
    if not authentication_allowed:
        if tenant is not None:
            _add_audit_record(
                db,
                tenant_id=tenant.id,
                actor_user_id=user.id if user is not None else None,
                action=IdentityAuditAction.LOGIN_FAILED,
                target_user_id=user.id if user is not None else None,
                details={"email": email, "result": "denied"},
            )
            db.commit()
        raise ApiError(
            401,
            "Unauthorized",
            "The tenant code, email, or password is invalid.",
            code="invalid_credentials",
        )

    assert user is not None
    if replacement_hash is not None:
        user.password_hash = replacement_hash
    user.last_login_at = datetime.now(UTC)
    _add_audit_record(
        db,
        tenant_id=user.tenant_id,
        actor_user_id=user.id,
        action=IdentityAuditAction.LOGIN_SUCCEEDED,
        target_user_id=user.id,
        details={"email": user.email},
    )
    db.commit()
    db.refresh(user)
    return user


def create_user(db: Session, principal: Principal, request: UserCreate) -> UserRecord:
    assignments = tuple(
        RoleScope(role=assignment.role, store_id=assignment.store_id)
        for assignment in request.role_assignments
    )
    ensure_can_assign_roles(principal, assignments)

    scoped_store_ids = {
        assignment.store_id for assignment in assignments if assignment.store_id is not None
    }
    if scoped_store_ids:
        active_store_ids = set(
            db.scalars(
                select(Store.id).where(
                    Store.tenant_id == principal.tenant_id,
                    Store.id.in_(scoped_store_ids),
                    Store.status == StoreStatus.ACTIVE,
                )
            ).all()
        )
        if active_store_ids != scoped_store_ids:
            raise ApiError(
                422,
                "Invalid store scope",
                "Every role assignment must reference an active store in the current tenant.",
                code="invalid_store_scope",
            )

    existing_user = db.scalar(
        select(User.id).where(
            User.tenant_id == principal.tenant_id,
            User.email == str(request.email),
        )
    )
    if existing_user is not None:
        raise ApiError(
            409,
            "User already exists",
            "A user with this email already exists in the tenant.",
            code="user_email_conflict",
        )

    user = User(
        tenant_id=principal.tenant_id,
        email=str(request.email),
        display_name=request.display_name,
        password_hash=hash_password(request.password.get_secret_value()),
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    db.add(user)
    try:
        db.flush()
        grants = tuple(
            UserAccessGrant(
                tenant_id=principal.tenant_id,
                user_id=user.id,
                role=assignment.role,
                store_id=assignment.store_id,
            )
            for assignment in assignments
        )
        db.add_all(grants)
        _add_audit_record(
            db,
            tenant_id=principal.tenant_id,
            actor_user_id=principal.user_id,
            action=IdentityAuditAction.USER_CREATED,
            target_user_id=user.id,
            details={
                "email": user.email,
                "role_assignments": [
                    {
                        "role": grant.role.value,
                        "store_id": str(grant.store_id) if grant.store_id else None,
                    }
                    for grant in grants
                ],
            },
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "User creation conflict",
            "The user or one of its role assignments was created concurrently.",
            code="user_creation_conflict",
        ) from exc
    db.refresh(user)
    return UserRecord(user=user, grants=grants)


def _visible_grants(
    principal: Principal,
    permission: Permission,
    grants: tuple[UserAccessGrant, ...],
) -> tuple[UserAccessGrant, ...]:
    if principal.has_tenant_permission(permission):
        return grants
    allowed_stores = principal.store_ids_for_permission(permission)
    return tuple(grant for grant in grants if grant.store_id in allowed_stores)


def list_users(
    db: Session,
    principal: Principal,
    *,
    store_id: uuid.UUID | None,
    limit: int,
    offset: int,
) -> tuple[list[UserRecord], int]:
    permission = Permission.USERS_READ
    if store_id is not None and not principal.can_access_store(permission, store_id):
        raise ApiError(
            403,
            "Forbidden",
            "The requested store is outside the current user's access scope.",
            code="store_scope_denied",
        )

    eligible_user_ids = None
    if store_id is not None:
        eligible_user_ids = select(UserAccessGrant.user_id).where(
            UserAccessGrant.tenant_id == principal.tenant_id,
            UserAccessGrant.store_id == store_id,
        )
    elif not principal.has_tenant_permission(permission):
        allowed_store_ids = principal.store_ids_for_permission(permission)
        eligible_user_ids = select(UserAccessGrant.user_id).where(
            UserAccessGrant.tenant_id == principal.tenant_id,
            UserAccessGrant.store_id.in_(allowed_store_ids),
        )

    filters = [User.tenant_id == principal.tenant_id]
    if eligible_user_ids is not None:
        filters.append(User.id.in_(eligible_user_ids))

    total = db.scalar(select(func.count(User.id)).where(*filters)) or 0
    users = list(
        db.scalars(
            select(User)
            .where(*filters)
            .order_by(User.email.asc(), User.id.asc())
            .limit(limit)
            .offset(offset)
        ).all()
    )
    records = [
        UserRecord(
            user=user,
            grants=_visible_grants(
                principal,
                permission,
                _load_grants(db, principal.tenant_id, user.id),
            ),
        )
        for user in users
    ]
    return records, total


def get_user(db: Session, principal: Principal, user_id: uuid.UUID) -> UserRecord:
    user = db.scalar(
        select(User).where(
            User.id == user_id,
            User.tenant_id == principal.tenant_id,
        )
    )
    if user is None:
        raise ApiError(404, "User not found", "The requested user does not exist.")

    grants = _load_grants(db, principal.tenant_id, user.id)
    visible_grants = _visible_grants(principal, Permission.USERS_READ, grants)
    if not visible_grants and not principal.has_tenant_permission(Permission.USERS_READ):
        raise ApiError(
            403,
            "Forbidden",
            "The requested user is outside the current user's access scope.",
            code="user_scope_denied",
        )
    return UserRecord(user=user, grants=visible_grants)


def _ensure_can_suspend(principal: Principal, target_grants: tuple[UserAccessGrant, ...]) -> None:
    if principal.has_tenant_permission(Permission.USERS_SUSPEND):
        return
    allowed_stores = principal.store_ids_for_permission(Permission.USERS_SUSPEND)
    if not target_grants or any(
        grant.role != IdentityRole.STORE_ASSOCIATE or grant.store_id not in allowed_stores
        for grant in target_grants
    ):
        raise ApiError(
            403,
            "Forbidden",
            "Store managers may suspend associates only within stores they manage.",
            code="user_suspension_forbidden",
        )


def suspend_user(db: Session, principal: Principal, user_id: uuid.UUID) -> UserRecord:
    if user_id == principal.user_id:
        raise ApiError(
            409,
            "Cannot suspend current user",
            "A user cannot suspend their own account.",
            code="self_suspension_forbidden",
        )

    user = db.scalar(
        select(User).where(
            User.id == user_id,
            User.tenant_id == principal.tenant_id,
        )
    )
    if user is None:
        raise ApiError(404, "User not found", "The requested user does not exist.")

    grants = _load_grants(db, principal.tenant_id, user.id)
    _ensure_can_suspend(principal, grants)
    if user.status == UserStatus.SUSPENDED:
        return UserRecord(
            user=user,
            grants=_visible_grants(principal, Permission.USERS_READ, grants),
        )

    user.status = UserStatus.SUSPENDED
    user.token_version += 1
    _add_audit_record(
        db,
        tenant_id=principal.tenant_id,
        actor_user_id=principal.user_id,
        action=IdentityAuditAction.USER_SUSPENDED,
        target_user_id=user.id,
        details={"email": user.email, "new_token_version": user.token_version},
    )
    db.commit()
    db.refresh(user)
    return UserRecord(
        user=user,
        grants=_visible_grants(principal, Permission.USERS_READ, grants),
    )


def list_audit_records(
    db: Session,
    principal: Principal,
    *,
    limit: int,
    offset: int,
) -> tuple[list[IdentityAuditRecord], int]:
    total = (
        db.scalar(
            select(func.count(IdentityAuditRecord.id)).where(
                IdentityAuditRecord.tenant_id == principal.tenant_id
            )
        )
        or 0
    )
    records = list(
        db.scalars(
            select(IdentityAuditRecord)
            .where(IdentityAuditRecord.tenant_id == principal.tenant_id)
            .order_by(
                IdentityAuditRecord.occurred_at.desc(),
                IdentityAuditRecord.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return records, total
