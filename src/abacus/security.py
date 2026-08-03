import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import lru_cache
from hashlib import sha256
from math import ceil
from threading import Lock
from time import monotonic
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
from abacus.db import TenantSession, pin_session_to_tenant
from abacus.enums import TenantStatus
from abacus.models.architecture import (
    CanonicalIdentityRole,
    UserRole,
    UserStoreAssignment,
)
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


AuthorizationRole = IdentityRole | CanonicalIdentityRole

ROLE_PERMISSIONS: dict[AuthorizationRole, frozenset[Permission]] = {
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
    CanonicalIdentityRole.TENANT_ADMIN: frozenset(Permission),
    CanonicalIdentityRole.CORPORATE_USER: frozenset(
        {
            Permission.CATALOG_READ,
            Permission.INVENTORY_READ,
            Permission.POLICY_READ,
            Permission.REPLENISHMENT_READ,
        }
    ),
    CanonicalIdentityRole.STORE_MANAGER: frozenset(
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
    CanonicalIdentityRole.STORE_ASSOCIATE: frozenset(
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
    role: AuthorizationRole
    store_id: uuid.UUID | None


@dataclass(frozen=True, slots=True)
class Principal:
    user_id: uuid.UUID
    tenant_id: uuid.UUID
    email: str
    display_name: str
    role_scopes: tuple[RoleScope, ...]
    canonical_roles: tuple[CanonicalIdentityRole, ...] = ()
    assigned_store_ids: tuple[uuid.UUID, ...] = ()

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


@dataclass(slots=True)
class _LoginAttemptWindow:
    started_at: float
    attempts: int = 0


@dataclass(slots=True)
class LoginAttemptReservation:
    _ip_key: tuple[str, str]
    _account_key: tuple[str, str]
    _ip_window: _LoginAttemptWindow
    _account_window: _LoginAttemptWindow
    _finalized: bool = False


class LoginAttemptOutcome(StrEnum):
    SUCCESS = "SUCCESS"
    AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    ABORTED = "ABORTED"


@dataclass(frozen=True, slots=True)
class LoginThrottleDecision:
    reservation: LoginAttemptReservation | None = None
    retry_after: int | None = None


class LoginThrottle:
    """Bounded, process-local fixed-window login throttling.

    An in-flight attempt reserves both budgets. Every completed authentication attempt
    remains in the source-IP budget, while only failed authentication remains in the
    normalized tenant/account budget. Reserving both counters under one lock prevents
    concurrent requests from exceeding either limit in this single-service demo.
    """

    def __init__(
        self,
        *,
        enabled: bool,
        window_seconds: int,
        ip_limit: int,
        account_limit: int,
        max_entries: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if window_seconds < 1 or ip_limit < 1 or account_limit < 1 or max_entries < 2:
            raise ValueError("Login throttle limits must be positive and max_entries at least 2")
        self.enabled = enabled
        self.window_seconds = window_seconds
        self.ip_limit = ip_limit
        self.account_limit = account_limit
        self.max_entries = max_entries
        self._clock = clock
        self._entries: dict[tuple[str, str], _LoginAttemptWindow] = {}
        self._lock = Lock()

    @property
    def entry_count(self) -> int:
        with self._lock:
            return len(self._entries)

    @staticmethod
    def _ip_key(source_ip: str) -> tuple[str, str]:
        return "ip", source_ip.strip().lower() or "unknown"

    @staticmethod
    def _account_key(tenant_code: str, email: str) -> tuple[str, str]:
        normalized_account = f"{tenant_code.strip().lower()}\0{email.strip().lower()}"
        return "account", sha256(normalized_account.encode(), usedforsecurity=False).hexdigest()

    def _active_window(
        self,
        key: tuple[str, str],
        now: float,
    ) -> _LoginAttemptWindow | None:
        window = self._entries.get(key)
        if window is None:
            return None
        if now - window.started_at >= self.window_seconds:
            del self._entries[key]
            return None
        return window

    def _new_window(self, key: tuple[str, str], now: float) -> _LoginAttemptWindow:
        window = _LoginAttemptWindow(started_at=now)
        self._entries[key] = window
        return window

    def _remove_expired_windows(self, now: float) -> None:
        expired_keys = [
            key
            for key, window in self._entries.items()
            if now - window.started_at >= self.window_seconds
        ]
        for key in expired_keys:
            del self._entries[key]

    def _capacity_retry_after(self, now: float) -> int:
        remaining = [
            self.window_seconds - (now - window.started_at) for window in self._entries.values()
        ]
        return max(1, ceil(min(remaining, default=self.window_seconds)))

    def begin_attempt(
        self,
        *,
        source_ip: str,
        tenant_code: str,
        email: str,
    ) -> LoginThrottleDecision:
        """Atomically reserve an attempt, or return a Retry-After decision."""

        if not self.enabled:
            return LoginThrottleDecision()

        ip_key = self._ip_key(source_ip)
        account_key = self._account_key(tenant_code, email)
        now = self._clock()
        with self._lock:
            ip_window = self._active_window(ip_key, now)
            account_window = self._active_window(account_key, now)
            retry_after = [
                self.window_seconds - (now - window.started_at)
                for window, limit in (
                    (ip_window, self.ip_limit),
                    (account_window, self.account_limit),
                )
                if window is not None and window.attempts >= limit
            ]
            if retry_after:
                return LoginThrottleDecision(retry_after=max(1, ceil(max(retry_after))))

            required_entries = int(ip_window is None) + int(account_window is None)
            if len(self._entries) + required_entries > self.max_entries:
                self._remove_expired_windows(now)
                ip_window = self._active_window(ip_key, now)
                account_window = self._active_window(account_key, now)
                required_entries = int(ip_window is None) + int(account_window is None)
                if len(self._entries) + required_entries > self.max_entries:
                    return LoginThrottleDecision(retry_after=self._capacity_retry_after(now))

            if ip_window is None:
                ip_window = self._new_window(ip_key, now)
            if account_window is None:
                account_window = self._new_window(account_key, now)
            ip_window.attempts += 1
            account_window.attempts += 1
            reservation = LoginAttemptReservation(
                _ip_key=ip_key,
                _account_key=account_key,
                _ip_window=ip_window,
                _account_window=account_window,
            )
        return LoginThrottleDecision(reservation=reservation)

    def finish_attempt(
        self,
        reservation: LoginAttemptReservation | None,
        *,
        outcome: LoginAttemptOutcome,
    ) -> None:
        """Finalize exactly one reservation according to the authentication outcome."""

        if reservation is None:
            return
        with self._lock:
            if reservation._finalized:
                return
            reservation._finalized = True
            retained_keys: set[tuple[str, str]] = set()
            if outcome in {
                LoginAttemptOutcome.SUCCESS,
                LoginAttemptOutcome.AUTHENTICATION_FAILED,
            }:
                retained_keys.add(reservation._ip_key)
            if outcome is LoginAttemptOutcome.AUTHENTICATION_FAILED:
                retained_keys.add(reservation._account_key)
            for key, window in (
                (reservation._ip_key, reservation._ip_window),
                (reservation._account_key, reservation._account_window),
            ):
                if key in retained_keys:
                    continue
                if self._entries.get(key) is not window:
                    continue
                window.attempts -= 1
                if window.attempts == 0:
                    del self._entries[key]


@lru_cache(maxsize=16)
def _configured_login_throttle(
    enabled: bool,
    window_seconds: int,
    ip_limit: int,
    account_limit: int,
    max_entries: int,
) -> LoginThrottle:
    return LoginThrottle(
        enabled=enabled,
        window_seconds=window_seconds,
        ip_limit=ip_limit,
        account_limit=account_limit,
        max_entries=max_entries,
    )


def get_login_throttle(settings: SettingsDependency) -> LoginThrottle:
    return _configured_login_throttle(
        settings.login_throttle_enabled,
        settings.login_throttle_window_seconds,
        settings.login_throttle_ip_limit,
        settings.login_throttle_account_limit,
        settings.login_throttle_max_entries,
    )


LoginThrottleDependency = Annotated[LoginThrottle, Depends(get_login_throttle)]


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
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, claims.tenant_id)
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

    legacy_grants = tuple(
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
    canonical_roles = tuple(
        db.scalars(
            select(UserRole.role)
            .where(
                UserRole.tenant_id == claims.tenant_id,
                UserRole.user_id == claims.user_id,
            )
            .order_by(UserRole.role.asc())
        ).all()
    )
    assigned_store_ids = tuple(
        db.scalars(
            select(UserStoreAssignment.store_id)
            .where(
                UserStoreAssignment.tenant_id == claims.tenant_id,
                UserStoreAssignment.user_id == claims.user_id,
            )
            .order_by(UserStoreAssignment.store_id.asc())
        ).all()
    )
    if canonical_roles:
        scopes = tuple(
            RoleScope(role=role, store_id=store_id)
            for role in canonical_roles
            for store_id in (
                (None,)
                if role
                in {
                    CanonicalIdentityRole.TENANT_ADMIN,
                    CanonicalIdentityRole.CORPORATE_USER,
                }
                else assigned_store_ids
            )
        )
    else:
        scopes = legacy_grants
        canonical_roles = tuple(
            sorted(
                {
                    (
                        CanonicalIdentityRole.TENANT_ADMIN
                        if scope.role is IdentityRole.CORPORATE_ADMIN
                        else CanonicalIdentityRole(scope.role.value)
                    )
                    for scope in legacy_grants
                },
                key=lambda role: role.value,
            )
        )
        assigned_store_ids = tuple(
            sorted(
                {scope.store_id for scope in legacy_grants if scope.store_id is not None},
                key=str,
            )
        )
    if not canonical_roles:
        raise _invalid_access_token()
    return Principal(
        user_id=user.id,
        tenant_id=user.tenant_id,
        email=user.email,
        display_name=user.display_name,
        role_scopes=scopes,
        canonical_roles=canonical_roles,
        assigned_store_ids=assigned_store_ids,
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
