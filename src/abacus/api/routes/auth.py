from datetime import timedelta

from fastapi import APIRouter

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.schemas.identity import (
    AccessTokenRead,
    CurrentPrincipalRead,
    LoginRequest,
    RoleAssignmentRead,
)
from abacus.security import CurrentPrincipal, create_access_token
from abacus.services.identity import authenticate_user

router = APIRouter(prefix="/v1/auth", tags=["4. Identity and Access"])


@router.post(
    "/login",
    response_model=AccessTokenRead,
    operation_id="login",
)
def login_endpoint(
    request: LoginRequest,
    db: DatabaseSession,
    settings: SettingsDependency,
) -> AccessTokenRead:
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
    return AccessTokenRead(
        access_token=token,
        expires_in=int(lifetime.total_seconds()),
    )


@router.get(
    "/me",
    response_model=CurrentPrincipalRead,
    operation_id="getCurrentUser",
)
def current_user_endpoint(principal: CurrentPrincipal) -> CurrentPrincipalRead:
    return CurrentPrincipalRead(
        user_id=principal.user_id,
        tenant_id=principal.tenant_id,
        email=principal.email,
        display_name=principal.display_name,
        role_assignments=[
            RoleAssignmentRead(role=scope.role, store_id=scope.store_id)
            for scope in principal.role_scopes
        ],
        permissions=sorted(permission.value for permission in principal.permissions),
    )
