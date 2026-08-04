from datetime import timedelta

from fastapi import APIRouter, Request, Response, status

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.api.errors import ApiError
from abacus.schemas.identity import (
    AccessTokenRead,
    CanonicalPrincipalRead,
    LoginRequest,
    RefreshTokenRequest,
)
from abacus.security import (
    CurrentPrincipal,
    LoginAttemptOutcome,
    LoginThrottleDependency,
    create_access_token,
)
from abacus.services.identity import (
    authenticate_user,
    create_auth_session,
    revoke_auth_session,
    rotate_auth_session,
)

router = APIRouter(prefix="/v1", tags=["4. Identity and Access"])


@router.post(
    "/auth/login",
    response_model=AccessTokenRead,
    operation_id="login",
)
def login_endpoint(
    request: LoginRequest,
    http_request: Request,
    db: DatabaseSession,
    settings: SettingsDependency,
    login_throttle: LoginThrottleDependency,
) -> AccessTokenRead:
    source_ip = http_request.client.host if http_request.client is not None else "unknown"
    throttle_decision = login_throttle.begin_attempt(
        source_ip=source_ip,
        tenant_code=request.tenant_code,
        email=str(request.email),
    )
    if throttle_decision.retry_after is not None:
        raise ApiError(
            429,
            "Too many login attempts",
            "Too many login attempts were made. Try again later.",
            code="login_rate_limited",
            headers={"Retry-After": str(throttle_decision.retry_after)},
        )

    try:
        user = authenticate_user(
            db,
            tenant_code=request.tenant_code,
            email=str(request.email),
            password=request.password.get_secret_value(),
        )
        lifetime = timedelta(minutes=settings.access_token_minutes)
        refresh_lifetime = timedelta(days=settings.refresh_token_days)
        session_token = create_auth_session(db, user, lifetime=refresh_lifetime)
        token, _ = create_access_token(
            user_id=user.id,
            tenant_id=user.tenant_id,
            token_version=user.token_version,
            secret=settings.jwt_secret,
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            lifetime=lifetime,
            session_id=session_token.session.id,
        )
        response = AccessTokenRead(
            access_token=token,
            expires_in=int(lifetime.total_seconds()),
            refresh_token=session_token.refresh_token,
            refresh_expires_in=int(refresh_lifetime.total_seconds()),
        )
    except ApiError as exc:
        login_throttle.finish_attempt(
            throttle_decision.reservation,
            outcome=(
                LoginAttemptOutcome.AUTHENTICATION_FAILED
                if exc.status_code == 401
                else LoginAttemptOutcome.ABORTED
            ),
        )
        raise
    except Exception:
        login_throttle.finish_attempt(
            throttle_decision.reservation,
            outcome=LoginAttemptOutcome.ABORTED,
        )
        raise

    login_throttle.finish_attempt(
        throttle_decision.reservation,
        outcome=LoginAttemptOutcome.SUCCESS,
    )
    return response


@router.post(
    "/auth/refresh",
    response_model=AccessTokenRead,
    operation_id="refreshAccessToken",
)
def refresh_endpoint(
    request: RefreshTokenRequest,
    db: DatabaseSession,
    settings: SettingsDependency,
) -> AccessTokenRead:
    access_lifetime = timedelta(minutes=settings.access_token_minutes)
    refresh_lifetime = timedelta(days=settings.refresh_token_days)
    user, session_token = rotate_auth_session(
        db,
        tenant_code=request.tenant_code,
        raw_refresh_token=request.refresh_token.get_secret_value(),
        lifetime=refresh_lifetime,
    )
    access_token, _ = create_access_token(
        user_id=user.id,
        tenant_id=user.tenant_id,
        token_version=user.token_version,
        secret=settings.jwt_secret,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        lifetime=access_lifetime,
        session_id=session_token.session.id,
    )
    return AccessTokenRead(
        access_token=access_token,
        expires_in=int(access_lifetime.total_seconds()),
        refresh_token=session_token.refresh_token,
        refresh_expires_in=int(refresh_lifetime.total_seconds()),
    )


@router.post(
    "/auth/logout",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="logout",
)
def logout_endpoint(
    db: DatabaseSession,
    principal: CurrentPrincipal,
) -> Response:
    revoke_auth_session(db, principal)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get(
    "/me",
    response_model=CanonicalPrincipalRead,
    operation_id="getCurrentUser",
)
def get_current_user_endpoint(principal: CurrentPrincipal) -> CanonicalPrincipalRead:
    return CanonicalPrincipalRead(
        user_id=principal.user_id,
        tenant_id=principal.tenant_id,
        email=principal.email,
        display_name=principal.display_name,
        roles=list(principal.canonical_roles),
        store_ids=list(principal.assigned_store_ids),
        permissions=sorted(permission.value for permission in principal.permissions),
    )
