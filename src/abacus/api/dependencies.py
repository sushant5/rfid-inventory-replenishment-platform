import secrets
from typing import Annotated

from fastapi import Depends
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.config import Settings, get_settings
from abacus.db import TenantSession, get_db


def require_tenant_session(db: Annotated[Session, Depends(get_db)]) -> TenantSession:
    """Fail closed when an API override bypasses transaction-local tenant context."""

    if not isinstance(db, TenantSession):
        raise RuntimeError("API database dependency must provide a TenantSession")
    return db


DatabaseSession = Annotated[TenantSession, Depends(require_tenant_session)]
SettingsDependency = Annotated[Settings, Depends(get_settings)]

_platform_key_header = APIKeyHeader(
    name="X-Platform-Key",
    scheme_name="PlatformApiKey",
    description="Trusted platform key for tenant and initial-store onboarding.",
    auto_error=False,
)
PlatformKey = Annotated[str | None, Depends(_platform_key_header)]


def require_platform_key(
    settings: SettingsDependency,
    x_platform_key: PlatformKey,
) -> None:
    if x_platform_key is None or not secrets.compare_digest(
        x_platform_key,
        settings.platform_api_key,
    ):
        raise ApiError(
            401,
            "Unauthorized",
            "A valid platform API key is required.",
            code="invalid_platform_key",
        )


PlatformAccess = Annotated[None, Depends(require_platform_key)]
