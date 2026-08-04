import uuid
from datetime import datetime, timedelta
from typing import Any, cast

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.orm import Session

from abacus.enums import JobKind, JobStatus, TenantStatus
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Tenant


def _database_now(db: Session) -> datetime:
    """Use PostgreSQL as the single clock for durable queue decisions."""

    value = cast(datetime | None, db.scalar(select(func.clock_timestamp())))
    if value is None:  # pragma: no cover - PostgreSQL always returns a value
        raise RuntimeError("database clock is unavailable")
    return value


def enqueue_job(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    kind: JobKind,
    payload: dict[str, Any],
    available_at: datetime | None = None,
) -> DurableJob:
    job = DurableJob(
        tenant_id=tenant_id,
        kind=kind,
        payload=payload,
        status=JobStatus.PENDING,
        attempts=0,
        available_at=available_at if available_at is not None else _database_now(db),
    )
    db.add(job)
    db.flush()
    return job


def claim_jobs(
    db: Session,
    *,
    worker_id: str,
    limit: int,
    lease_seconds: int,
    max_attempts: int,
    tenant_id: uuid.UUID | None = None,
    kinds: tuple[JobKind, ...] | None = None,
) -> list[DurableJob]:
    now = _database_now(db)

    # A worker can disappear without reporting its failure, and deployments can
    # lower the configured retry budget while retries are pending. Terminalize
    # due work that already consumed the current budget instead of executing or
    # reclaiming it forever.
    scope_predicates: list[Any] = []
    if tenant_id is not None:
        scope_predicates.append(DurableJob.tenant_id == tenant_id)
    if kinds is not None:
        scope_predicates.append(DurableJob.kind.in_(kinds))
    exhausted_jobs = db.scalars(
        select(DurableJob)
        .where(
            *scope_predicates,
            DurableJob.attempts >= max_attempts,
            or_(
                and_(
                    DurableJob.status == JobStatus.PENDING,
                    DurableJob.available_at <= now,
                ),
                and_(
                    DurableJob.status == JobStatus.PROCESSING,
                    DurableJob.lease_expires_at.is_not(None),
                    DurableJob.lease_expires_at <= now,
                ),
            ),
        )
        .order_by(DurableJob.created_at.asc())
        .with_for_update(of=DurableJob, skip_locked=True)
        .limit(max(limit, 1))
    ).all()
    for job in exhausted_jobs:
        expired_lease = job.status == JobStatus.PROCESSING
        job.status = JobStatus.QUARANTINED
        job.locked_by = None
        job.lease_expires_at = None
        job.last_error = (
            "LeaseExpired: maximum processing attempts exhausted"
            if expired_lease
            else "MaximumAttemptsExceeded: pending job exhausted its attempt budget"
        )
    claimable = or_(
        and_(
            DurableJob.status == JobStatus.PENDING,
            DurableJob.available_at <= now,
            DurableJob.attempts < max_attempts,
        ),
        and_(
            DurableJob.status == JobStatus.PROCESSING,
            DurableJob.lease_expires_at.is_not(None),
            DurableJob.lease_expires_at <= now,
            DurableJob.attempts < max_attempts,
        ),
    )
    jobs = list(
        db.scalars(
            select(DurableJob)
            .join(Tenant, Tenant.id == DurableJob.tenant_id)
            .where(
                *scope_predicates,
                claimable,
                Tenant.status == TenantStatus.ACTIVE,
            )
            .order_by(DurableJob.created_at.asc())
            .with_for_update(of=DurableJob, skip_locked=True)
            .limit(limit)
        ).all()
    )
    for job in jobs:
        job.status = JobStatus.PROCESSING
        job.locked_by = worker_id
        job.lease_expires_at = now + timedelta(seconds=lease_seconds)
        job.attempts += 1
    db.commit()
    return jobs


def renew_lease(
    db: Session,
    job_id: uuid.UUID,
    worker_id: str,
    *,
    lease_seconds: int,
) -> bool:
    """Extend a lease only while this worker still owns it."""

    now = _database_now(db)
    result = cast(
        CursorResult[Any],
        db.execute(
            update(DurableJob)
            .where(
                DurableJob.id == job_id,
                DurableJob.status == JobStatus.PROCESSING,
                DurableJob.locked_by == worker_id,
                DurableJob.lease_expires_at.is_not(None),
                DurableJob.lease_expires_at > now,
            )
            .values(lease_expires_at=now + timedelta(seconds=lease_seconds))
            .execution_options(synchronize_session=False)
        ),
    )
    if result.rowcount != 1:
        db.rollback()
        return False
    db.commit()
    return True


def mark_completed(db: Session, job_id: uuid.UUID, worker_id: str) -> bool:
    """Complete using compare-and-set so a stale worker cannot steal a reclaimed job."""

    result = cast(
        CursorResult[Any],
        db.execute(
            update(DurableJob)
            .where(
                DurableJob.id == job_id,
                DurableJob.status == JobStatus.PROCESSING,
                DurableJob.locked_by == worker_id,
            )
            .values(
                status=JobStatus.COMPLETED,
                locked_by=None,
                lease_expires_at=None,
                last_error=None,
            )
            .execution_options(synchronize_session=False)
        ),
    )
    if result.rowcount != 1:
        # This also discards uncommitted handler changes when ownership was lost.
        db.rollback()
        return False
    db.commit()
    return True


def mark_failed(
    db: Session,
    job_id: uuid.UUID,
    worker_id: str,
    error: Exception,
    *,
    max_attempts: int,
) -> bool:
    job = db.scalar(
        select(DurableJob)
        .where(DurableJob.id == job_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if job is None or job.status != JobStatus.PROCESSING or job.locked_by != worker_id:
        db.rollback()
        return False
    job.last_error = f"{type(error).__name__}: {error}"[:4000]
    job.locked_by = None
    job.lease_expires_at = None
    if job.attempts >= max_attempts:
        job.status = JobStatus.QUARANTINED
    else:
        job.status = JobStatus.PENDING
        delay_seconds = min(300, 2 ** min(job.attempts, 8))
        job.available_at = _database_now(db) + timedelta(seconds=delay_seconds)
    db.commit()
    return True
