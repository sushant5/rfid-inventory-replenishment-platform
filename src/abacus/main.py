import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import perf_counter

import structlog
from fastapi import FastAPI, Request, Response
from starlette.middleware.base import RequestResponseEndpoint
from structlog.contextvars import bind_contextvars, clear_contextvars

from abacus import __version__
from abacus.api.errors import install_error_handlers, install_openapi_error_contract
from abacus.api.router import api_router
from abacus.config import get_settings
from abacus.logging import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)

PUBLIC_DEMO_TENANT = "orange"
PUBLIC_DEMO_EMAIL = "demo-reader@orange.example"
PUBLIC_DEMO_PASSWORD = "Orange-Demo-ReadOnly-2026!"  # noqa: S105 - public demo credential

OPENAPI_TAGS = [
    {"name": "1. Onboarding", "description": "Tenant, store, zone, and device setup."},
    {"name": "2. Product catalog", "description": "Asynchronous catalog import and SKU lookup."},
    {"name": "3. RFID and Inventory", "description": "RFID ingestion and current inventory state."},
    {
        "name": "4. Identity and Access",
        "description": "Authentication, users, roles, and store scope.",
    },
    {"name": "5. Replenishment", "description": "Policies, evaluation, and task lifecycle."},
    {"name": "Operations", "description": "Service health and release metadata."},
]

OPENAPI_DESCRIPTION = f"""
Multi-tenant RFID inventory and replenishment API.

### Public read-only reviewer

Sign in through `POST /v1/auth/login` with:

```json
{{
  "tenant_code": "{PUBLIC_DEMO_TENANT}",
  "email": "{PUBLIC_DEMO_EMAIL}",
  "password": "{PUBLIC_DEMO_PASSWORD}"
}}
```

Paste the returned `access_token` into **Authorize** using `HTTPBearer`, then call:

1. `GET /v1/me`
2. `GET /v1/stores`
3. `GET /v1/skus`
4. `GET /v1/stores/{{store_id}}/inventory`
5. `GET /v1/stores/{{store_id}}/replenishment-tasks`

The public identity is tenant-scoped and read-only. A write operation such as
`POST /v1/replenishment/evaluations` returns `403 Forbidden`.

RFID ingestion uses a separate `DeviceToken`; tenant provisioning and bulk imports use
a `PlatformApiKey`. Platform, tenant-admin, and device credentials remain private.

PostgreSQL RLS is the tenant-isolation boundary. Store-level permissions are resolved
from PostgreSQL on every request and enforced explicitly by the application.
"""


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logger.info("application_started", environment=settings.app_env, build_sha=settings.build_sha)
    try:
        yield
    finally:
        logger.info("application_stopped")


def create_app() -> FastAPI:
    settings = get_settings()
    application = FastAPI(
        title=settings.app_name,
        version=__version__,
        description=OPENAPI_DESCRIPTION,
        openapi_tags=OPENAPI_TAGS,
        lifespan=lifespan,
    )
    install_error_handlers(application)

    @application.middleware("http")
    async def request_context(
        request: Request,
        call_next: RequestResponseEndpoint,
    ) -> Response:
        clear_contextvars()
        supplied_request_id = request.headers.get("X-Request-ID", "").strip()
        request_id = (
            supplied_request_id if 0 < len(supplied_request_id) <= 128 else str(uuid.uuid4())
        )
        bind_contextvars(request_id=request_id, path=request.url.path)
        request.state.request_id = request_id
        started_at = perf_counter()
        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            logger.info(
                "http_request_completed",
                method=request.method,
                status_code=response.status_code,
                duration_ms=round((perf_counter() - started_at) * 1000, 2),
            )
            return response
        except Exception:
            logger.exception(
                "http_request_failed",
                method=request.method,
                duration_ms=round((perf_counter() - started_at) * 1000, 2),
            )
            raise
        finally:
            clear_contextvars()

    application.include_router(api_router)
    install_openapi_error_contract(application)

    @application.get("/", include_in_schema=False)
    def service_discovery() -> dict[str, object]:
        """Give a reviewer useful entry points when opening the hosted base URL."""

        return {
            "service": settings.app_name,
            "version": __version__,
            "docs": "/docs",
            "openapi": "/openapi.json",
            "login": "/v1/auth/login",
            "stores": "/v1/stores",
            "demo_login": {
                "tenant_code": PUBLIC_DEMO_TENANT,
                "email": PUBLIC_DEMO_EMAIL,
                "password": PUBLIC_DEMO_PASSWORD,
            },
            "demo_access": "Read-only Orange tenant access; mutation requests return 403.",
            "reviewer_path": [
                "POST /v1/auth/login",
                "GET /v1/me",
                "GET /v1/stores",
                "GET /v1/skus",
                "GET /v1/stores/{store_id}/inventory",
                "GET /v1/stores/{store_id}/replenishment-tasks",
            ],
            "private_credentials": "Platform, tenant-admin, and device credentials are private.",
            "liveness": "/health/live",
            "readiness": "/health/ready",
        }

    return application


app = create_app()
