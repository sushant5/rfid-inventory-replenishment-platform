from functools import lru_cache
from pathlib import Path
from typing import Annotated, Literal

from alembic.script import ScriptDirectory
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


@lru_cache
def expected_schema_revision() -> str:
    candidates = (Path.cwd() / "alembic", Path(__file__).resolve().parents[4] / "alembic")
    script_location = next((path for path in candidates if path.is_dir()), None)
    if script_location is None:
        raise RuntimeError("Alembic migration directory is unavailable")
    heads = ScriptDirectory(str(script_location)).get_heads()
    if len(heads) != 1:
        raise RuntimeError(f"expected one Alembic head, found {len(heads)}")
    return heads[0]


EXPECTED_SCHEMA_REVISION = expected_schema_revision()


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class ReadinessResponse(HealthResponse):
    database: Literal["ok"] = "ok"
    schema_revision: str
    restricted_database_role: bool
    cutover_ready: bool


class VersionResponse(BaseModel):
    version: str
    build_sha: str
    environment: str


@router.get("/health/live", response_model=HealthResponse, operation_id="liveness")
def liveness() -> HealthResponse:
    return HealthResponse()


@router.get("/health/ready", response_model=ReadinessResponse, operation_id="readiness")
async def readiness(db: Annotated[Session, Depends(get_db)]) -> ReadinessResponse:
    settings = get_settings()
    try:
        runtime_role_ready = db.scalar(
            text(
                "SELECT current_user = :expected_role "
                "AND NOT rolsuper AND NOT rolbypassrls "
                "FROM pg_catalog.pg_roles WHERE rolname = current_user"
            ),
            {"expected_role": settings.application_database_role},
        )
        if settings.app_env == "production" and runtime_role_ready is not True:
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
    return ReadinessResponse(
        schema_revision=schema_revision,
        restricted_database_role=runtime_role_ready,
        cutover_ready=cutover_ready,
    )


@router.get("/version", response_model=VersionResponse, operation_id="version")
def version() -> VersionResponse:
    settings = get_settings()
    return VersionResponse(
        version=__version__,
        build_sha=settings.build_sha,
        environment=settings.app_env,
    )
