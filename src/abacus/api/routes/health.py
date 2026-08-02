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
EXPECTED_SCHEMA_REVISION = "b6e3f19a2d44"


class HealthResponse(BaseModel):
    status: Literal["ok"] = "ok"


class VersionResponse(BaseModel):
    version: str
    build_sha: str


@router.get("/health/live", response_model=HealthResponse, operation_id="liveness")
def liveness() -> HealthResponse:
    return HealthResponse()


@router.get("/health/ready", response_model=HealthResponse, operation_id="readiness")
def readiness(db: Annotated[Session, Depends(get_db)]) -> HealthResponse:
    try:
        required_tables_ready = db.scalar(
            text(
                "SELECT to_regclass('public.tenants') IS NOT NULL "
                "AND to_regclass('public.durable_jobs') IS NOT NULL "
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
        cutover_ready = db.scalar(
            text(
                "SELECT NOT EXISTS ("
                "SELECT 1 FROM replenishment_tasks "
                "WHERE reservation_cutover_reviewed = false)"
            )
        )
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
    return VersionResponse(version=__version__, build_sha=settings.build_sha)
