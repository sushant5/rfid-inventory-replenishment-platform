from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from abacus import __version__
from abacus.api.errors import ApiError
from abacus.config import get_settings
from abacus.db import get_db

router = APIRouter(tags=["Operations"])
EXPECTED_SCHEMA_REVISION = "f0c1d2e3a4b5"


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class VersionResponse(BaseModel):
    version: str
    build_sha: str
    environment: str


@router.get("/health/live", response_model=HealthResponse, operation_id="liveness")
def liveness() -> HealthResponse:
    return HealthResponse()


@router.get("/health/ready", response_model=HealthResponse, operation_id="readiness")
async def readiness(db: Annotated[Session, Depends(get_db)]) -> HealthResponse:
    settings = get_settings()
    try:
        if settings.app_env == "production":
            runtime_role_ready = db.scalar(
                text(
                    "SELECT current_user = :expected_role "
                    "AND NOT rolsuper AND NOT rolbypassrls "
                    "FROM pg_catalog.pg_roles WHERE rolname = current_user"
                ),
                {"expected_role": settings.application_database_role},
            )
            if runtime_role_ready is not True:
                raise ApiError(
                    503,
                    "Service unavailable",
                    "The runtime database credential is not the configured restricted role.",
                    code="database_role_not_ready",
                )
        required_tables_ready = db.scalar(
            text(
                "SELECT to_regclass('public.tenants') IS NOT NULL "
                "AND to_regclass('public.current_item_state') IS NOT NULL "
                "AND to_regclass('public.inventory_transition_outbox') IS NOT NULL "
                "AND to_regclass('public.alembic_version') IS NOT NULL"
            )
        )
        schema_revision = (
            db.scalar(text("SELECT version_num FROM alembic_version"))
            if required_tables_ready
            else None
        )
    except SQLAlchemyError as exc:
        db.rollback()
        raise ApiError(
            503,
            "Service unavailable",
            "The database is unavailable.",
            code="database_not_ready",
        ) from exc
    if schema_revision != EXPECTED_SCHEMA_REVISION:
        raise ApiError(
            503,
            "Service unavailable",
            "The database schema is not at the required application revision.",
            code="schema_not_ready",
        )
    try:
        # The runtime role has no tenant context during a global readiness check.
        # This narrow SECURITY DEFINER function can see every cutover row without
        # granting the application a general RLS bypass.
        cutover_ready = db.scalar(text("SELECT app_cutover_ready()"))
    except SQLAlchemyError as exc:
        db.rollback()
        raise ApiError(
            503,
            "Service unavailable",
            "The database is unavailable.",
            code="database_not_ready",
        ) from exc
    if cutover_ready is not True:
        raise ApiError(
            503,
            "Cutover reconciliation required",
            "Legacy replenishment movement must be reconciled before serving traffic.",
            code="cutover_reconciliation_required",
        )
    return HealthResponse()


@router.get("/version", response_model=VersionResponse, operation_id="version")
def version() -> VersionResponse:
    settings = get_settings()
    return VersionResponse(
        version=__version__,
        build_sha=settings.build_sha,
        environment=settings.app_env,
    )
