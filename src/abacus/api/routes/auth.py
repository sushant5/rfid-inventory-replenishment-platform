from datetime import timedelta

from fastapi import APIRouter, Request

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.api.errors import ApiError
from abacus.schemas.identity import (
    AccessTokenRead,
    CanonicalPrincipalRead,
    LoginRequest,
)
from abacus.security import (
    CurrentPrincipal,
    LoginAttemptOutcome,
    LoginThrottleDependency,
    create_access_token,
)
from abacus.services.identity import authenticate_user

router = APIRouter(prefix="/v1/auth", tags=["4. Identity and Access"])
canonical_router = APIRouter(prefix="/v1", tags=["4. Identity and Access"])


@router.post(
    "/login",
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
        token, _ = create_access_token(
            user_id=user.id,
            tenant_id=user.tenant_id,
            token_version=user.token_version,
            secret=settings.jwt_secret,
            issuer=settings.jwt_issuer,
            audience=settings.jwt_audience,
            lifetime=lifetime,
        )
        response = AccessTokenRead(
            access_token=token,
            expires_in=int(lifetime.total_seconds()),
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


@canonical_router.get(
    "/me",
    response_model=CanonicalPrincipalRead,
    operation_id="getCurrentUser",
)
def canonical_current_user_endpoint(principal: CurrentPrincipal) -> CanonicalPrincipalRead:
    return CanonicalPrincipalRead(
        user_id=principal.user_id,
        tenant_id=principal.tenant_id,
        email=principal.email,
        display_name=principal.display_name,
        roles=list(principal.canonical_roles),
        store_ids=list(principal.assigned_store_ids),
        permissions=sorted(permission.value for permission in principal.permissions),
    )
