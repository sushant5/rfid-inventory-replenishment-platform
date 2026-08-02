from typing import Annotated, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.orm import Session

from abacus import __version__
from abacus.api.errors import ApiError
from abacus.config import get_settings
from abacus.db import get_db

router = APIRouter(tags=["Operations"])


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
    schema_ready = db.scalar(
        text(
            "SELECT to_regclass('public.tenants') IS NOT NULL "
            "AND to_regclass('public.durable_jobs') IS NOT NULL"
        )
    )
    if not schema_ready:
        raise ApiError(
            503,
            "Service unavailable",
            "The database schema has not been migrated.",
            code="schema_not_ready",
        )
    return HealthResponse()


@router.get("/version", response_model=VersionResponse, operation_id="version")
def version() -> VersionResponse:
    settings = get_settings()
    return VersionResponse(version=__version__, build_sha=settings.build_sha)
