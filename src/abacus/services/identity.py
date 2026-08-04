import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from sqlalchemy import delete, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.db import TenantSession, pin_session_to_tenant
from abacus.enums import StoreStatus, TenantStatus
from abacus.models.architecture import (
    CanonicalIdentityRole,
    UserRole,
    UserStoreAssignment,
)
from abacus.models.identity import (
    IdentityAuditAction,
    IdentityAuditRecord,
    IdentityRole,
    User,
    UserAccessGrant,
    UserStatus,
)
from abacus.models.tenancy import Store, Tenant
from abacus.schemas.identity import (
    CanonicalUserCreate,
    UserAccessReplace,
    UserCreate,
    UserRolesReplace,
    UserStoreAssignmentsReplace,
)
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


@dataclass(frozen=True, slots=True)
class CanonicalUserAccess:
    user_id: uuid.UUID
    roles: tuple[CanonicalIdentityRole, ...]
    store_ids: tuple[uuid.UUID, ...]


@dataclass(frozen=True, slots=True)
class CanonicalUserRecord:
    user: User
    roles: tuple[CanonicalIdentityRole, ...]
    store_ids: tuple[uuid.UUID, ...]


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

    tenant_id: uuid.UUID | None = None
    if isinstance(db, TenantSession):
        # This narrow SECURITY DEFINER resolver is the only unscoped lookup. End its
        # transaction before permanently pinning the session so every following SQL
        # statement is protected by the tenant's RLS context.
        resolved_tenant_id = db.scalar(
            text("SELECT abacus_resolve_login_tenant(:tenant_code)"),
            {"tenant_code": tenant_request.code},
        )
        db.rollback()
        tenant_id = (
            uuid.UUID(str(resolved_tenant_id)) if resolved_tenant_id is not None else uuid.uuid4()
        )
        pin_session_to_tenant(db, tenant_id)

    tenant = db.scalar(select(Tenant).where(Tenant.code == tenant_request.code))
    if tenant is None:
        tenant = Tenant(
            id=tenant_id or uuid.uuid4(),
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
        canonical_admin = db.get(
            UserRole,
            {
                "tenant_id": tenant.id,
                "user_id": user.id,
                "role": CanonicalIdentityRole.TENANT_ADMIN,
            },
        )
        if canonical_admin is None:
            db.add(
                UserRole(
                    tenant_id=tenant.id,
                    user_id=user.id,
                    role=CanonicalIdentityRole.TENANT_ADMIN,
                )
            )
            db.commit()
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
    db.add_all(
        [
            grant,
            UserRole(
                tenant_id=tenant.id,
                user_id=user.id,
                role=CanonicalIdentityRole.TENANT_ADMIN,
            ),
        ]
    )
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


def bootstrap_public_reviewer(
    db: Session,
    *,
    tenant_code: str,
    request: CanonicalUserCreate,
) -> CanonicalUserRecord:
    """Create or reconcile the explicitly configured read-only demo reviewer.

    The public account deliberately has only the canonical ``CORPORATE_USER`` role.
    It has no compatibility grants or store assignments, so it cannot inherit a
    write permission from the legacy access model. Re-running the bootstrap performs
    no writes when the configured identity and password are unchanged.
    """

    expected_roles = frozenset({CanonicalIdentityRole.CORPORATE_USER})
    if frozenset(request.roles) != expected_roles or request.store_ids:
        raise ValueError(
            "public reviewer must have exactly CORPORATE_USER and no store assignments"
        )

    normalized_tenant_code = tenant_code.strip().lower()
    if isinstance(db, TenantSession):
        resolved_tenant_id = db.scalar(
            text("SELECT abacus_resolve_login_tenant(:tenant_code)"),
            {"tenant_code": normalized_tenant_code},
        )
        db.rollback()
        if resolved_tenant_id is None:
            raise ApiError(
                404,
                "Bootstrap tenant not found",
                "Create the configured tenant administrator before the public reviewer.",
                code="public_reviewer_tenant_not_found",
            )
        tenant_id = uuid.UUID(str(resolved_tenant_id))
        pin_session_to_tenant(db, tenant_id)
        tenant = db.scalar(select(Tenant).where(Tenant.id == tenant_id).with_for_update())
    else:
        tenant = db.scalar(
            select(Tenant).where(Tenant.code == normalized_tenant_code).with_for_update()
        )

    if tenant is None:
        raise ApiError(
            404,
            "Bootstrap tenant not found",
            "Create the configured tenant administrator before the public reviewer.",
            code="public_reviewer_tenant_not_found",
        )
    if tenant.status != TenantStatus.ACTIVE:
        raise ApiError(
            409,
            "Bootstrap tenant is inactive",
            "The public reviewer cannot be enabled for an inactive tenant.",
            code="public_reviewer_tenant_inactive",
        )

    email = str(request.email)
    user = db.scalar(
        select(User)
        .where(
            User.tenant_id == tenant.id,
            User.email == email,
        )
        .with_for_update()
    )
    password = request.password.get_secret_value()
    if user is None:
        user = User(
            tenant_id=tenant.id,
            email=email,
            display_name=request.display_name,
            password_hash=hash_password(password),
            status=UserStatus.ACTIVE,
            token_version=1,
        )
        db.add(user)
        try:
            db.flush()
            db.add(
                UserRole(
                    tenant_id=tenant.id,
                    user_id=user.id,
                    role=CanonicalIdentityRole.CORPORATE_USER,
                )
            )
            _add_audit_record(
                db,
                tenant_id=tenant.id,
                actor_user_id=None,
                action=IdentityAuditAction.USER_CREATED,
                target_user_id=user.id,
                details={
                    "email": user.email,
                    "bootstrap": True,
                    "public_reviewer": True,
                    "roles": [CanonicalIdentityRole.CORPORATE_USER.value],
                    "store_ids": [],
                },
            )
            db.commit()
        except IntegrityError as exc:
            db.rollback()
            raise ApiError(
                409,
                "Public reviewer bootstrap conflict",
                "The public reviewer was created concurrently or conflicts with existing data.",
                code="public_reviewer_conflict",
            ) from exc
        db.refresh(user)
        return CanonicalUserRecord(
            user=user,
            roles=(CanonicalIdentityRole.CORPORATE_USER,),
            store_ids=(),
        )

    roles = frozenset(
        db.scalars(
            select(UserRole.role).where(
                UserRole.tenant_id == tenant.id,
                UserRole.user_id == user.id,
            )
        ).all()
    )
    store_ids = frozenset(
        db.scalars(
            select(UserStoreAssignment.store_id).where(
                UserStoreAssignment.tenant_id == tenant.id,
                UserStoreAssignment.user_id == user.id,
            )
        ).all()
    )
    legacy_grants = tuple(
        db.scalars(
            select(UserAccessGrant).where(
                UserAccessGrant.tenant_id == tenant.id,
                UserAccessGrant.user_id == user.id,
            )
        ).all()
    )
    if CanonicalIdentityRole.TENANT_ADMIN in roles or any(
        grant.role is IdentityRole.CORPORATE_ADMIN for grant in legacy_grants
    ):
        db.rollback()
        raise ApiError(
            409,
            "Protected administrator conflict",
            "The configured public-reviewer email belongs to a tenant administrator.",
            code="public_reviewer_admin_conflict",
        )

    changes: list[str] = []
    invalidate_tokens = False
    if user.display_name != request.display_name:
        user.display_name = request.display_name
        changes.append("display_name")
    if user.status is not UserStatus.ACTIVE:
        user.status = UserStatus.ACTIVE
        changes.append("status_reactivated")
        invalidate_tokens = True

    password_matches, replacement_hash = verify_password_and_update(password, user.password_hash)
    if not password_matches:
        user.password_hash = hash_password(password)
        changes.append("password_rotated")
        invalidate_tokens = True
    elif replacement_hash is not None and replacement_hash != user.password_hash:
        user.password_hash = replacement_hash
        changes.append("password_hash_upgraded")
    if roles != expected_roles:
        _persist_role_rows(db, tenant.id, user.id, expected_roles)
        changes.append("roles_reconciled")
        invalidate_tokens = True
    if store_ids:
        _persist_assignment_rows(db, tenant.id, user.id, frozenset())
        changes.append("store_assignments_removed")
        invalidate_tokens = True
    if legacy_grants:
        db.execute(
            delete(UserAccessGrant).where(
                UserAccessGrant.tenant_id == tenant.id,
                UserAccessGrant.user_id == user.id,
            )
        )
        changes.append("legacy_grants_removed")
        invalidate_tokens = True

    if invalidate_tokens:
        user.token_version += 1
    if changes:
        _add_audit_record(
            db,
            tenant_id=tenant.id,
            actor_user_id=None,
            action=IdentityAuditAction.USER_ACCESS_CHANGED,
            target_user_id=user.id,
            details={
                "email": user.email,
                "bootstrap": True,
                "public_reviewer": True,
                "changes": sorted(changes),
                "new_token_version": user.token_version,
            },
        )
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Public reviewer bootstrap conflict",
            "The public reviewer changed concurrently or conflicts with existing data.",
            code="public_reviewer_conflict",
        ) from exc
    return CanonicalUserRecord(
        user=user,
        roles=(CanonicalIdentityRole.CORPORATE_USER,),
        store_ids=(),
    )


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
    if isinstance(db, TenantSession):
        tenant_id = db.scalar(
            text("SELECT abacus_resolve_login_tenant(:tenant_code)"),
            {"tenant_code": tenant_code},
        )
        db.rollback()
        if tenant_id is not None:
            pin_session_to_tenant(db, uuid.UUID(str(tenant_id)))
        tenant = (
            db.scalar(select(Tenant).where(Tenant.id == tenant_id))
            if tenant_id is not None
            else None
        )
    else:
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


def create_canonical_user(
    db: Session,
    principal: Principal,
    request: CanonicalUserCreate,
) -> CanonicalUserRecord:
    roles = frozenset(request.roles)
    store_ids = frozenset(request.store_ids)
    if not principal.has_tenant_permission(Permission.USERS_CREATE):
        allowed_store_ids = principal.store_ids_for_permission(Permission.USERS_CREATE)
        if roles != {CanonicalIdentityRole.STORE_ASSOCIATE} or not store_ids.issubset(
            allowed_store_ids
        ):
            raise ApiError(
                403,
                "Forbidden",
                "Store managers may create associates only within stores they manage.",
                code="role_assignment_forbidden",
            )

    if store_ids:
        active_store_ids = frozenset(
            db.scalars(
                select(Store.id).where(
                    Store.tenant_id == principal.tenant_id,
                    Store.id.in_(store_ids),
                    Store.status == StoreStatus.ACTIVE,
                )
            ).all()
        )
        if active_store_ids != store_ids:
            raise ApiError(
                422,
                "Invalid store scope",
                "Every store assignment must reference an active store in the current tenant.",
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
        _persist_role_rows(db, principal.tenant_id, user.id, roles)
        _persist_assignment_rows(db, principal.tenant_id, user.id, store_ids)
        compatibility_grants = _compatibility_grant_specs(roles, store_ids)
        _persist_compatibility_grants(
            db,
            principal.tenant_id,
            user.id,
            compatibility_grants,
        )
        _add_audit_record(
            db,
            tenant_id=principal.tenant_id,
            actor_user_id=principal.user_id,
            action=IdentityAuditAction.USER_CREATED,
            target_user_id=user.id,
            details={
                "email": user.email,
                "roles": [role.value for role in sorted(roles, key=lambda item: item.value)],
                "store_ids": [str(store_id) for store_id in sorted(store_ids, key=str)],
            },
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "User creation conflict",
            "The user or one of its access assignments was created concurrently.",
            code="user_creation_conflict",
        ) from exc
    db.refresh(user)
    return CanonicalUserRecord(
        user=user,
        roles=tuple(sorted(roles, key=lambda item: item.value)),
        store_ids=tuple(sorted(store_ids, key=str)),
    )


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


def _canonical_role_for_legacy(role: IdentityRole) -> CanonicalIdentityRole:
    if role is IdentityRole.CORPORATE_ADMIN:
        return CanonicalIdentityRole.TENANT_ADMIN
    if role is IdentityRole.STORE_MANAGER:
        return CanonicalIdentityRole.STORE_MANAGER
    return CanonicalIdentityRole.STORE_ASSOCIATE


def _load_canonical_access(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
) -> tuple[
    frozenset[CanonicalIdentityRole],
    frozenset[uuid.UUID],
    bool,
    bool,
    tuple[UserAccessGrant, ...],
]:
    role_rows = frozenset(
        db.scalars(
            select(UserRole.role).where(
                UserRole.tenant_id == tenant_id,
                UserRole.user_id == user_id,
            )
        ).all()
    )
    assignment_rows = frozenset(
        db.scalars(
            select(UserStoreAssignment.store_id).where(
                UserStoreAssignment.tenant_id == tenant_id,
                UserStoreAssignment.user_id == user_id,
            )
        ).all()
    )
    grants = _load_grants(db, tenant_id, user_id)

    # Compatibility fallback covers users created through the pre-canonical API after
    # the migration backfill ran. The next PUT persists the missing canonical half.
    roles = role_rows or frozenset(_canonical_role_for_legacy(grant.role) for grant in grants)
    store_ids = assignment_rows or frozenset(
        grant.store_id for grant in grants if grant.store_id is not None
    )
    return roles, store_ids, not role_rows, not assignment_rows, grants


def canonicalize_user_record(
    db: Session,
    record: UserRecord,
    principal: Principal | None = None,
) -> CanonicalUserRecord:
    roles, store_ids, _roles_missing, _assignments_missing, _grants = _load_canonical_access(
        db,
        record.user.tenant_id,
        record.user.id,
    )
    if principal is not None and not principal.has_tenant_permission(Permission.USERS_READ):
        store_ids = store_ids.intersection(
            principal.store_ids_for_permission(Permission.USERS_READ)
        )
    return CanonicalUserRecord(
        user=record.user,
        roles=tuple(sorted(roles, key=lambda item: item.value)),
        store_ids=tuple(sorted(store_ids, key=str)),
    )


def _compatibility_grant_specs(
    roles: frozenset[CanonicalIdentityRole],
    store_ids: frozenset[uuid.UUID],
) -> frozenset[tuple[IdentityRole, uuid.UUID | None]]:
    specs: set[tuple[IdentityRole, uuid.UUID | None]] = set()
    if CanonicalIdentityRole.TENANT_ADMIN in roles:
        specs.add((IdentityRole.CORPORATE_ADMIN, None))

    # The compatibility model has no CORPORATE_USER. Map it to the least-privilege
    # store role for explicitly assigned stores; never map it to CORPORATE_ADMIN.
    for store_id in store_ids:
        if CanonicalIdentityRole.STORE_MANAGER in roles:
            specs.add((IdentityRole.STORE_MANAGER, store_id))
        elif roles.intersection(
            {
                CanonicalIdentityRole.STORE_ASSOCIATE,
                CanonicalIdentityRole.CORPORATE_USER,
            }
        ):
            specs.add((IdentityRole.STORE_ASSOCIATE, store_id))
    return frozenset(specs)


def _persist_role_rows(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    roles: frozenset[CanonicalIdentityRole],
) -> None:
    db.execute(
        delete(UserRole).where(
            UserRole.tenant_id == tenant_id,
            UserRole.user_id == user_id,
        )
    )
    db.add_all(
        UserRole(tenant_id=tenant_id, user_id=user_id, role=role)
        for role in sorted(roles, key=lambda item: item.value)
    )


def _persist_assignment_rows(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    store_ids: frozenset[uuid.UUID],
) -> None:
    db.execute(
        delete(UserStoreAssignment).where(
            UserStoreAssignment.tenant_id == tenant_id,
            UserStoreAssignment.user_id == user_id,
        )
    )
    db.add_all(
        UserStoreAssignment(tenant_id=tenant_id, user_id=user_id, store_id=store_id)
        for store_id in sorted(store_ids, key=str)
    )


def _persist_compatibility_grants(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    specs: frozenset[tuple[IdentityRole, uuid.UUID | None]],
) -> None:
    db.execute(
        delete(UserAccessGrant).where(
            UserAccessGrant.tenant_id == tenant_id,
            UserAccessGrant.user_id == user_id,
        )
    )
    db.add_all(
        UserAccessGrant(
            tenant_id=tenant_id,
            user_id=user_id,
            role=role,
            store_id=store_id,
        )
        for role, store_id in sorted(specs, key=lambda item: (item[0].value, str(item[1])))
    )


def _lock_access_target(
    db: Session,
    principal: Principal,
    user_id: uuid.UUID,
) -> User:
    user = db.scalar(
        select(User)
        .where(
            User.id == user_id,
            User.tenant_id == principal.tenant_id,
        )
        .with_for_update()
    )
    if user is None:
        # Same response for a missing user and another tenant's user.
        raise ApiError(404, "User not found", "The requested user does not exist.")
    return user


def _ensure_valid_canonical_access_pair(
    roles: frozenset[CanonicalIdentityRole],
    store_ids: frozenset[uuid.UUID],
) -> None:
    """Reject role/store combinations that would leave misleading access state."""

    has_store_scoped_role = bool(
        roles.intersection(
            {
                CanonicalIdentityRole.STORE_ASSOCIATE,
                CanonicalIdentityRole.STORE_MANAGER,
            }
        )
    )
    if has_store_scoped_role and not store_ids:
        raise ApiError(
            422,
            "Store assignment required",
            "Store-scoped roles require at least one store assignment.",
            code="store_assignment_required",
        )
    if not has_store_scoped_role and store_ids:
        raise ApiError(
            422,
            "Store assignment not allowed",
            "Store assignments require a store-scoped role.",
            code="store_assignment_not_allowed",
        )


def _ensure_valid_store_ids(
    db: Session,
    tenant_id: uuid.UUID,
    store_ids: frozenset[uuid.UUID],
) -> None:
    if not store_ids:
        return
    valid_store_ids = frozenset(
        db.scalars(
            select(Store.id).where(
                Store.tenant_id == tenant_id,
                Store.id.in_(store_ids),
                Store.status != StoreStatus.INACTIVE,
            )
        ).all()
    )
    if valid_store_ids != store_ids:
        raise ApiError(
            422,
            "Invalid store scope",
            "Every assignment must reference a non-inactive store in the current tenant.",
            code="invalid_store_scope",
        )


def _ensure_not_removing_last_tenant_admin(
    db: Session,
    tenant_id: uuid.UUID,
    user_id: uuid.UUID,
    existing_roles: frozenset[CanonicalIdentityRole],
    requested_roles: frozenset[CanonicalIdentityRole],
) -> None:
    if not (
        CanonicalIdentityRole.TENANT_ADMIN in existing_roles
        and CanonicalIdentityRole.TENANT_ADMIN not in requested_roles
    ):
        return
    another_canonical_admin = db.scalar(
        select(UserRole.user_id)
        .join(User, User.id == UserRole.user_id)
        .where(
            UserRole.tenant_id == tenant_id,
            UserRole.user_id != user_id,
            UserRole.role == CanonicalIdentityRole.TENANT_ADMIN,
            User.status == UserStatus.ACTIVE,
        )
        .limit(1)
    )
    another_legacy_admin = db.scalar(
        select(UserAccessGrant.user_id)
        .join(User, User.id == UserAccessGrant.user_id)
        .where(
            UserAccessGrant.tenant_id == tenant_id,
            UserAccessGrant.user_id != user_id,
            UserAccessGrant.role == IdentityRole.CORPORATE_ADMIN,
            UserAccessGrant.store_id.is_(None),
            User.status == UserStatus.ACTIVE,
        )
        .limit(1)
    )
    if another_canonical_admin is None and another_legacy_admin is None:
        raise ApiError(
            409,
            "Last tenant administrator",
            "Assign another tenant administrator before removing this role.",
            code="last_tenant_admin",
        )


def _audit_access_change(
    db: Session,
    *,
    principal: Principal,
    user: User,
    previous_roles: frozenset[CanonicalIdentityRole],
    previous_store_ids: frozenset[uuid.UUID],
    requested_roles: frozenset[CanonicalIdentityRole],
    requested_store_ids: frozenset[uuid.UUID],
) -> None:
    _add_audit_record(
        db,
        tenant_id=principal.tenant_id,
        actor_user_id=principal.user_id,
        action=IdentityAuditAction.USER_ACCESS_CHANGED,
        target_user_id=user.id,
        details={
            "previous_roles": sorted(role.value for role in previous_roles),
            "previous_store_ids": sorted(str(store_id) for store_id in previous_store_ids),
            "roles": sorted(role.value for role in requested_roles),
            "store_ids": sorted(str(store_id) for store_id in requested_store_ids),
            "new_token_version": user.token_version,
        },
    )


def replace_user_access(
    db: Session,
    principal: Principal,
    user_id: uuid.UUID,
    request: UserAccessReplace,
) -> CanonicalUserAccess:
    """Replace roles and scopes atomically so valid transitions cannot deadlock."""

    if not principal.has_tenant_permission(Permission.USERS_CREATE):
        raise ApiError(
            403,
            "Forbidden",
            "Only a tenant administrator may replace complete user access.",
            code="role_assignment_forbidden",
        )
    db.scalar(select(Tenant.id).where(Tenant.id == principal.tenant_id).with_for_update())
    user = _lock_access_target(db, principal, user_id)
    existing_roles, existing_store_ids, roles_missing, assignments_missing, grants = (
        _load_canonical_access(db, principal.tenant_id, user.id)
    )
    requested_roles = frozenset(request.roles)
    requested_store_ids = frozenset(request.store_ids)
    _ensure_valid_canonical_access_pair(requested_roles, requested_store_ids)
    _ensure_valid_store_ids(db, principal.tenant_id, requested_store_ids)
    _ensure_not_removing_last_tenant_admin(
        db,
        principal.tenant_id,
        user.id,
        existing_roles,
        requested_roles,
    )

    existing_specs = frozenset((grant.role, grant.store_id) for grant in grants)
    desired_specs = _compatibility_grant_specs(requested_roles, requested_store_ids)
    access_changed = (
        requested_roles != existing_roles
        or requested_store_ids != existing_store_ids
        or desired_specs != existing_specs
    )
    if not access_changed and not roles_missing and not assignments_missing:
        return CanonicalUserAccess(
            user_id=user.id,
            roles=tuple(sorted(requested_roles, key=lambda item: item.value)),
            store_ids=tuple(sorted(requested_store_ids, key=str)),
        )

    try:
        _persist_role_rows(db, principal.tenant_id, user.id, requested_roles)
        _persist_assignment_rows(db, principal.tenant_id, user.id, requested_store_ids)
        _persist_compatibility_grants(db, principal.tenant_id, user.id, desired_specs)
        if access_changed:
            user.token_version += 1
            _audit_access_change(
                db,
                principal=principal,
                user=user,
                previous_roles=existing_roles,
                previous_store_ids=existing_store_ids,
                requested_roles=requested_roles,
                requested_store_ids=requested_store_ids,
            )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Access update conflict",
            "The user's access changed concurrently.",
            code="user_access_update_conflict",
        ) from exc
    return CanonicalUserAccess(
        user_id=user.id,
        roles=tuple(sorted(requested_roles, key=lambda item: item.value)),
        store_ids=tuple(sorted(requested_store_ids, key=str)),
    )


def replace_user_roles(
    db: Session,
    principal: Principal,
    user_id: uuid.UUID,
    request: UserRolesReplace,
) -> CanonicalUserAccess:
    if not principal.has_tenant_permission(Permission.USERS_CREATE):
        raise ApiError(
            403,
            "Forbidden",
            "Only a tenant administrator may replace user roles.",
            code="role_assignment_forbidden",
        )

    # Serializes the last-admin invariant across role changes in this tenant.
    db.scalar(select(Tenant.id).where(Tenant.id == principal.tenant_id).with_for_update())
    user = _lock_access_target(db, principal, user_id)
    existing_roles, store_ids, roles_missing, assignments_missing, grants = _load_canonical_access(
        db, principal.tenant_id, user.id
    )
    requested_roles = frozenset(request.roles)
    _ensure_valid_canonical_access_pair(requested_roles, store_ids)

    _ensure_not_removing_last_tenant_admin(
        db,
        principal.tenant_id,
        user.id,
        existing_roles,
        requested_roles,
    )

    existing_specs = frozenset((grant.role, grant.store_id) for grant in grants)
    desired_specs = _compatibility_grant_specs(requested_roles, store_ids)
    access_changed = requested_roles != existing_roles or desired_specs != existing_specs
    if not access_changed and not roles_missing and not assignments_missing:
        return CanonicalUserAccess(
            user_id=user.id,
            roles=tuple(sorted(requested_roles, key=lambda item: item.value)),
            store_ids=tuple(sorted(store_ids, key=str)),
        )

    try:
        _persist_role_rows(db, principal.tenant_id, user.id, requested_roles)
        if assignments_missing:
            _persist_assignment_rows(db, principal.tenant_id, user.id, store_ids)
        if desired_specs != existing_specs:
            _persist_compatibility_grants(db, principal.tenant_id, user.id, desired_specs)
        if access_changed:
            user.token_version += 1
            _audit_access_change(
                db,
                principal=principal,
                user=user,
                previous_roles=existing_roles,
                previous_store_ids=store_ids,
                requested_roles=requested_roles,
                requested_store_ids=store_ids,
            )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Role update conflict",
            "The user's roles changed concurrently.",
            code="user_role_update_conflict",
        ) from exc
    return CanonicalUserAccess(
        user_id=user.id,
        roles=tuple(sorted(requested_roles, key=lambda item: item.value)),
        store_ids=tuple(sorted(store_ids, key=str)),
    )


def replace_user_store_assignments(
    db: Session,
    principal: Principal,
    user_id: uuid.UUID,
    request: UserStoreAssignmentsReplace,
) -> CanonicalUserAccess:
    user = _lock_access_target(db, principal, user_id)
    roles, existing_store_ids, roles_missing, assignments_missing, grants = _load_canonical_access(
        db, principal.tenant_id, user.id
    )
    if not roles:
        raise ApiError(
            422,
            "Missing role",
            "Assign at least one role before assigning stores.",
            code="user_role_required",
        )
    requested_store_ids = frozenset(request.store_ids)
    _ensure_valid_canonical_access_pair(roles, requested_store_ids)

    _ensure_valid_store_ids(db, principal.tenant_id, requested_store_ids)

    if not principal.has_tenant_permission(Permission.USERS_CREATE):
        allowed_store_ids = principal.store_ids_for_permission(Permission.USERS_CREATE)
        touched_store_ids = requested_store_ids.union(existing_store_ids)
        if roles != {CanonicalIdentityRole.STORE_ASSOCIATE} or not touched_store_ids.issubset(
            allowed_store_ids
        ):
            raise ApiError(
                403,
                "Forbidden",
                "Store managers may assign associates only within stores they manage.",
                code="role_assignment_forbidden",
            )

    existing_specs = frozenset((grant.role, grant.store_id) for grant in grants)
    desired_specs = _compatibility_grant_specs(roles, requested_store_ids)
    access_changed = requested_store_ids != existing_store_ids or desired_specs != existing_specs
    if not access_changed and not roles_missing and not assignments_missing:
        return CanonicalUserAccess(
            user_id=user.id,
            roles=tuple(sorted(roles, key=lambda item: item.value)),
            store_ids=tuple(sorted(requested_store_ids, key=str)),
        )

    try:
        if roles_missing:
            _persist_role_rows(db, principal.tenant_id, user.id, roles)
        _persist_assignment_rows(db, principal.tenant_id, user.id, requested_store_ids)
        if desired_specs != existing_specs:
            _persist_compatibility_grants(db, principal.tenant_id, user.id, desired_specs)
        if access_changed:
            user.token_version += 1
            _audit_access_change(
                db,
                principal=principal,
                user=user,
                previous_roles=roles,
                previous_store_ids=existing_store_ids,
                requested_roles=roles,
                requested_store_ids=requested_store_ids,
            )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Store assignment conflict",
            "The user's store assignments changed concurrently.",
            code="user_store_assignment_conflict",
        ) from exc
    return CanonicalUserAccess(
        user_id=user.id,
        roles=tuple(sorted(roles, key=lambda item: item.value)),
        store_ids=tuple(sorted(requested_store_ids, key=str)),
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
