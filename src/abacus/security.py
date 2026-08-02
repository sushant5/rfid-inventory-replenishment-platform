import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Annotated, Any

import jwt
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pwdlib import PasswordHash
from pwdlib.exceptions import PwdlibError
from pwdlib.hashers.argon2 import Argon2Hasher
from sqlalchemy import select

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.api.errors import ApiError
from abacus.enums import TenantStatus
from abacus.models.identity import IdentityRole, User, UserAccessGrant, UserStatus
from abacus.models.tenancy import Tenant


class Permission(StrEnum):
    TENANT_CONFIGURE = "tenant:configure"
    USERS_CREATE = "users:create"
    USERS_READ = "users:read"
    USERS_SUSPEND = "users:suspend"
    IDENTITY_AUDIT_READ = "identity-audit:read"
    CATALOG_INGEST = "catalog:ingest"
    CATALOG_READ = "catalog:read"
    INVENTORY_READ = "inventory:read"
    POLICY_READ = "policy:read"
    POLICY_MANAGE = "policy:manage"
    REPLENISHMENT_READ = "replenishment:read"
    REPLENISHMENT_MANAGE = "replenishment:manage"
    REPLENISHMENT_EXECUTE = "replenishment:execute"


ROLE_PERMISSIONS: dict[IdentityRole, frozenset[Permission]] = {
    IdentityRole.CORPORATE_ADMIN: frozenset(Permission),
    IdentityRole.STORE_MANAGER: frozenset(
        {
            Permission.USERS_CREATE,
            Permission.USERS_READ,
            Permission.USERS_SUSPEND,
            Permission.CATALOG_READ,
            Permission.INVENTORY_READ,
            Permission.POLICY_READ,
            Permission.REPLENISHMENT_READ,
            Permission.REPLENISHMENT_MANAGE,
            Permission.REPLENISHMENT_EXECUTE,
        }
    ),
    IdentityRole.STORE_ASSOCIATE: frozenset(
        {
            Permission.CATALOG_READ,
            Permission.INVENTORY_READ,
            Permission.POLICY_READ,
            Permission.REPLENISHMENT_READ,
            Permission.REPLENISHMENT_EXECUTE,
        }
    ),
}


@dataclass(frozen=True, slots=True)
class RoleScope:
    role: IdentityRole
    store_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class Principal:
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    email: str
    display_name: str
    role_scopes: tuple[RoleScope, ...]

    @property
    def permissions(self) -> frozenset[Permission]:
        return frozenset(
            permission for scope in self.role_scopes for permission in ROLE_PERMISSIONS[scope.role]
        )

    def has_permission(self, permission: Permission) -> bool:
        return permission in self.permissions

    def has_tenant_permission(self, permission: Permission) -> bool:
        return any(
            scope.store_id is None and permission in ROLE_PERMISSIONS[scope.role]
            for scope in self.role_scopes
        )

    def store_ids_for_permission(self, permission: Permission) -> frozenset[uuid.UUID]:
        return frozenset(
            scope.store_id
            for scope in self.role_scopes
            if scope.store_id is not None and permission in ROLE_PERMISSIONS[scope.role]
        )

    def can_access_store(self, permission: Permission, store_id: uuid.UUID) -> bool:
        return self.has_tenant_permission(permission) or store_id in self.store_ids_for_permission(
            permission
        )


@dataclass(frozen=True, slots=True)
class AccessTokenClaims:
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    token_version: int
    expires_at: datetime


_password_hash = PasswordHash((Argon2Hasher(),))
_dummy_password_hash = _password_hash.hash("not-a-real-account-password")
_bearer_scheme = HTTPBearer(auto_error=False, description="Short-lived Abacus access token")


def hash_password(password: str) -> str:
    return _password_hash.hash(password)


def verify_password_and_update(password: str, encoded_hash: str | None) -> tuple[bool, str | None]:
    candidate_hash = encoded_hash or _dummy_password_hash
    try:
        return _password_hash.verify_and_update(password, candidate_hash)
    except (PwdlibError, ValueError):
        return False, None


def create_access_token(
    *,
    user_id: uuid.UUID,
    tenant_id: uuid.UUID,
    token_version: int,
    secret: str,
    issuer: str,
    audience: str,
    lifetime: timedelta,
    now: datetime | None = None,
) -> tuple[str, datetime]:
    issued_at = now or datetime.now(UTC)
    expires_at = issued_at + lifetime
    payload: dict[str, Any] = {
        "iss": issuer,
        "aud": audience,
        "iat": issued_at,
        "exp": expires_at,
        "sub": str(user_id),
        "tid": str(tenant_id),
        "token_version": token_version,
    }
    return jwt.encode(payload, secret, algorithm="HS256"), expires_at


def decode_access_token(
    token: str,
    *,
    secret: str,
    issuer: str,
    audience: str,
) -> AccessTokenClaims:
    try:
        payload = jwt.decode(
            token,
            secret,
            algorithms=["HS256"],
            issuer=issuer,
            audience=audience,
            options={
                "require": ["iss", "aud", "iat", "exp", "sub", "tid", "token_version"],
            },
        )
        raw_subject = payload["sub"]
        raw_tenant_id = payload["tid"]
        raw_version = payload["token_version"]
        raw_expiry = payload["exp"]
        if not isinstance(raw_subject, str) or not isinstance(raw_tenant_id, str):
            raise ValueError("JWT identifiers must be strings")
        if isinstance(raw_version, bool) or not isinstance(raw_version, int) or raw_version < 1:
            raise ValueError("JWT token_version must be a positive integer")
        if isinstance(raw_expiry, bool) or not isinstance(raw_expiry, int | float):
            raise ValueError("JWT exp must be a numeric date")
        return AccessTokenClaims(
            user_id=uuid.UUID(raw_subject),
            tenant_id=uuid.UUID(raw_tenant_id),
            token_version=raw_version,
            expires_at=datetime.fromtimestamp(raw_expiry, tz=UTC),
        )
    except (jwt.InvalidTokenError, KeyError, TypeError, ValueError) as exc:
        raise ApiError(
            401,
            "Unauthorized",
            "The access token is missing, invalid, or expired.",
            code="invalid_access_token",
        ) from exc


def ensure_can_assign_roles(principal: Principal, assignments: Iterable[RoleScope]) -> None:
    assignments_tuple = tuple(assignments)
    if principal.has_tenant_permission(Permission.USERS_CREATE):
        return

    allowed_stores = principal.store_ids_for_permission(Permission.USERS_CREATE)
    if not assignments_tuple or any(
        assignment.role != IdentityRole.STORE_ASSOCIATE or assignment.store_id not in allowed_stores
        for assignment in assignments_tuple
    ):
        raise ApiError(
            403,
            "Forbidden",
            "Store managers may create associates only within stores they manage.",
            code="role_assignment_forbidden",
        )


def _invalid_access_token() -> ApiError:
    return ApiError(
        401,
        "Unauthorized",
        "The access token is missing, invalid, or expired.",
        code="invalid_access_token",
    )


BearerCredentials = Annotated[
    HTTPAuthorizationCredentials | None,
    Depends(_bearer_scheme),
]


def get_current_principal(
    credentials: BearerCredentials,
    db: DatabaseSession,
    settings: SettingsDependency,
) -> Principal:
    if credentials is None or credentials.scheme.lower() != "bearer":
        raise _invalid_access_token()

    claims = decode_access_token(
        credentials.credentials,
        secret=settings.jwt_secret,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
    )
    user = db.scalar(
        select(User).where(
            User.id == claims.user_id,
            User.tenant_id == claims.tenant_id,
        )
    )
    tenant_status = db.scalar(select(Tenant.status).where(Tenant.id == claims.tenant_id))
    if (
        user is None
        or user.status != UserStatus.ACTIVE
        or user.token_version != claims.token_version
        or tenant_status != TenantStatus.ACTIVE
    ):
        raise _invalid_access_token()

    grants = tuple(
        RoleScope(role=grant.role, store_id=grant.store_id)
        for grant in db.scalars(
            select(UserAccessGrant)
            .where(
                UserAccessGrant.tenant_id == claims.tenant_id,
                UserAccessGrant.user_id == claims.user_id,
            )
            .order_by(UserAccessGrant.role.asc(), UserAccessGrant.store_id.asc())
        ).all()
    )
    if not grants:
        raise _invalid_access_token()
    return Principal(
        user_id=user.id,
        tenant_id=user.tenant_id,
        email=user.email,
        display_name=user.display_name,
        role_scopes=grants,
    )


CurrentPrincipal = Annotated[Principal, Depends(get_current_principal)]


def require_permission(permission: Permission) -> Callable[[CurrentPrincipal], Principal]:
    def dependency(principal: CurrentPrincipal) -> Principal:
        if not principal.has_permission(permission):
            raise ApiError(
                403,
                "Forbidden",
                "The current user is not permitted to perform this operation.",
                code="permission_denied",
            )
        return principal

    return dependency
