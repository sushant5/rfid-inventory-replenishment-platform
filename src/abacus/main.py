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
from abacus.api.router import api_router, legacy_test_router
from abacus.config import get_settings
from abacus.logging import configure_logging

configure_logging()
logger = structlog.get_logger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    logger.info("application_started", environment=settings.app_env, build_sha=settings.build_sha)
    try:
        yield
    finally:
        logger.info("application_stopped")


def create_app(*, include_legacy_test_routes: bool = False) -> FastAPI:
    settings = get_settings()
    application = FastAPI(
        title=settings.app_name,
        version=__version__,
        description="Multi-tenant RFID inventory and replenishment API.",
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
    if include_legacy_test_routes:
        application.include_router(legacy_test_router)
    install_openapi_error_contract(application)

    @application.get("/", include_in_schema=False)
    def service_discovery() -> dict[str, str]:
        """Give a reviewer useful entry points when opening the hosted base URL."""

        return {
            "service": settings.app_name,
            "version": __version__,
            "docs": "/docs",
            "openapi": "/openapi.json",
            "login": "/v1/auth/login",
            "liveness": "/health/live",
            "readiness": "/health/ready",
        }

    return application


app = create_app()
