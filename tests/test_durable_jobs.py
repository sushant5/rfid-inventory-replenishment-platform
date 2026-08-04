from __future__ import annotations

import uuid
from datetime import datetime, timedelta, tzinfo
from typing import cast

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, sessionmaker

import abacus.services.jobs as jobs_service
from abacus.enums import JobKind, JobStatus, TenantStatus
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Tenant
from abacus.services.jobs import claim_jobs, enqueue_job, mark_completed, mark_failed, renew_lease

pytestmark = pytest.mark.integration


def _database_now(db: Session) -> datetime:
    value = cast(datetime | None, db.scalar(select(func.clock_timestamp())))
    assert value is not None
    return value


def _create_tenant(db: Session, status: TenantStatus) -> Tenant:
    suffix = uuid.uuid4().hex[:16]
    tenant = Tenant(code=f"jobs-{suffix}", name=f"Jobs {suffix}", status=status)
    db.add(tenant)
    db.flush()
    return tenant


def _remove_tenants(db: Session, *tenant_ids: uuid.UUID) -> None:
    db.execute(delete(Tenant).where(Tenant.id.in_(tenant_ids)))
    db.commit()


def test_queue_timestamps_do_not_use_the_worker_process_clock(
    postgres_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ForbiddenApplicationClock(datetime):
        @classmethod
        def now(cls, tz: tzinfo | None = None) -> datetime:
            raise AssertionError("durable queue decisions must use the PostgreSQL clock")

    monkeypatch.setattr(jobs_service, "datetime", ForbiddenApplicationClock)

    with postgres_session_factory() as db:
        tenant = _create_tenant(db, TenantStatus.ACTIVE)
        tenant_id = tenant.id
        before_enqueue = _database_now(db)
        job = enqueue_job(
            db,
            tenant_id=tenant_id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"clock_probe": True},
        )
        after_enqueue = _database_now(db)
        assert before_enqueue <= job.available_at <= after_enqueue
        job_id = job.id
        db.commit()

    with postgres_session_factory() as db:
        claimed = claim_jobs(
            db,
            worker_id="clock-worker",
            limit=1,
            lease_seconds=30,
            max_attempts=3,
        )
        assert [job.id for job in claimed] == [job_id]
        before_renewal = _database_now(db)
        assert renew_lease(db, job_id, "clock-worker", lease_seconds=30)
        after_renewal = _database_now(db)

        db.expire_all()
        renewed = db.get(DurableJob, job_id)
        assert renewed is not None
        assert renewed.lease_expires_at is not None
        assert before_renewal + timedelta(seconds=30) <= renewed.lease_expires_at
        assert renewed.lease_expires_at <= after_renewal + timedelta(seconds=30)

        before_backoff = _database_now(db)
        assert mark_failed(
            db,
            job_id,
            "clock-worker",
            RuntimeError("retry probe"),
            max_attempts=3,
        )
        after_backoff = _database_now(db)
        db.expire_all()
        failed = db.get(DurableJob, job_id)
        assert failed is not None
        assert failed.status == JobStatus.PENDING
        assert before_backoff + timedelta(seconds=2) <= failed.available_at
        assert failed.available_at <= after_backoff + timedelta(seconds=2)

        _remove_tenants(db, tenant_id)


def test_claim_jobs_skips_suspended_tenants(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    with postgres_session_factory() as db:
        active_tenant = _create_tenant(db, TenantStatus.ACTIVE)
        suspended_tenant = _create_tenant(db, TenantStatus.SUSPENDED)
        active_job = enqueue_job(
            db,
            tenant_id=active_tenant.id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"tenant_status_probe": "active"},
        )
        suspended_job = enqueue_job(
            db,
            tenant_id=suspended_tenant.id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"tenant_status_probe": "suspended"},
        )
        active_tenant_id = active_tenant.id
        suspended_tenant_id = suspended_tenant.id
        active_job_id = active_job.id
        suspended_job_id = suspended_job.id
        db.commit()

    with postgres_session_factory() as db:
        claimed = claim_jobs(
            db,
            worker_id="tenant-status-worker",
            limit=10,
            lease_seconds=30,
            max_attempts=3,
        )
        assert [job.id for job in claimed] == [active_job_id]
        assert mark_completed(db, active_job_id, "tenant-status-worker")

        untouched = db.get(DurableJob, suspended_job_id)
        assert untouched is not None
        assert untouched.status == JobStatus.PENDING
        assert untouched.attempts == 0
        assert untouched.locked_by is None

        suspended = db.get(Tenant, suspended_tenant_id)
        assert suspended is not None
        suspended.status = TenantStatus.ACTIVE
        db.commit()

        resumed = claim_jobs(
            db,
            worker_id="tenant-status-worker",
            limit=10,
            lease_seconds=30,
            max_attempts=3,
        )
        assert [job.id for job in resumed] == [suspended_job_id]
        assert mark_completed(db, suspended_job_id, "tenant-status-worker")

        _remove_tenants(db, active_tenant_id, suspended_tenant_id)


def test_expired_lease_at_attempt_limit_is_quarantined(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    with postgres_session_factory() as db:
        tenant = _create_tenant(db, TenantStatus.ACTIVE)
        now = _database_now(db)
        exhausted = DurableJob(
            tenant_id=tenant.id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"lease_probe": "exhausted"},
            status=JobStatus.PROCESSING,
            attempts=3,
            available_at=now - timedelta(minutes=1),
            lease_expires_at=now - timedelta(seconds=1),
            locked_by="dead-worker",
        )
        exhausted_pending = DurableJob(
            tenant_id=tenant.id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"lease_probe": "pending-budget-lowered"},
            status=JobStatus.PENDING,
            attempts=3,
            available_at=now - timedelta(seconds=1),
        )
        reclaimable = DurableJob(
            tenant_id=tenant.id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"lease_probe": "reclaimable"},
            status=JobStatus.PROCESSING,
            attempts=2,
            available_at=now - timedelta(minutes=1),
            lease_expires_at=now - timedelta(seconds=1),
            locked_by="dead-worker",
        )
        db.add_all([exhausted, exhausted_pending, reclaimable])
        db.flush()
        tenant_id = tenant.id
        exhausted_id = exhausted.id
        exhausted_pending_id = exhausted_pending.id
        reclaimable_id = reclaimable.id
        db.commit()

    with postgres_session_factory() as db:
        assert not renew_lease(db, reclaimable_id, "dead-worker", lease_seconds=30)

        claimed = claim_jobs(
            db,
            worker_id="recovery-worker",
            limit=10,
            lease_seconds=30,
            max_attempts=3,
        )
        assert [job.id for job in claimed] == [reclaimable_id]
        assert claimed[0].attempts == 3

        terminal = db.get(DurableJob, exhausted_id)
        assert terminal is not None
        assert terminal.status == JobStatus.QUARANTINED
        assert terminal.locked_by is None
        assert terminal.lease_expires_at is None
        assert terminal.last_error == "LeaseExpired: maximum processing attempts exhausted"

        pending_terminal = db.get(DurableJob, exhausted_pending_id)
        assert pending_terminal is not None
        assert pending_terminal.status == JobStatus.QUARANTINED
        assert pending_terminal.locked_by is None
        assert pending_terminal.lease_expires_at is None
        assert (
            pending_terminal.last_error
            == "MaximumAttemptsExceeded: pending job exhausted its attempt budget"
        )

        assert mark_completed(db, reclaimable_id, "recovery-worker")
        _remove_tenants(db, tenant_id)
