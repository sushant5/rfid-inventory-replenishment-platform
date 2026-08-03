from __future__ import annotations

import uuid
from datetime import datetime, timedelta
from typing import cast
from unittest.mock import Mock

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, sessionmaker

import abacus.processes.catalog_worker as catalog_worker
from abacus.config import get_settings
from abacus.enums import JobKind, JobStatus, TenantStatus
from abacus.models.catalog import CatalogImport, CatalogImportMode, CatalogImportStatus
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Tenant
from abacus.services.catalog import (
    mark_catalog_import_failed_after_retry_exhaustion,
    reconcile_quarantined_catalog_imports,
)
from abacus.services.jobs import claim_jobs


def _database_now(db: Session) -> datetime:
    value = cast(datetime | None, db.scalar(select(func.clock_timestamp())))
    assert value is not None
    return value


def _create_claimed_catalog_job(
    db: Session,
    *,
    attempts: int,
    import_status: CatalogImportStatus = CatalogImportStatus.READY,
) -> tuple[uuid.UUID, CatalogImport, DurableJob, str]:
    suffix = uuid.uuid4().hex[:16]
    tenant = Tenant(
        code=f"catalog-worker-{suffix}",
        name=f"Catalog Worker {suffix}",
        status=TenantStatus.ACTIVE,
    )
    db.add(tenant)
    db.flush()

    catalog_import = CatalogImport(
        tenant_id=tenant.id,
        idempotency_key=f"catalog-worker-{suffix}",
        checksum="a" * 64,
        mode=CatalogImportMode.FULL,
        status=import_status,
        filename="catalog.csv",
        content_type="text/csv",
        size_bytes=1,
        total_rows=1,
        valid_rows=1,
        invalid_rows=0,
        inserted_count=0,
        updated_count=0,
        unchanged_count=0,
        deactivated_count=0,
        reconciliation={},
    )
    db.add(catalog_import)
    db.flush()

    worker_id = f"catalog-test-{suffix}"
    now = _database_now(db)
    job = DurableJob(
        tenant_id=tenant.id,
        kind=JobKind.CATALOG_IMPORT,
        payload={"import_id": str(catalog_import.id)},
        status=JobStatus.PROCESSING,
        attempts=attempts,
        available_at=now,
        lease_expires_at=now + timedelta(minutes=1),
        locked_by=worker_id,
    )
    db.add(job)
    db.commit()
    return tenant.id, catalog_import, job, worker_id


def _force_unexpected_worker_error(monkeypatch: pytest.MonkeyPatch, max_attempts: int) -> None:
    settings = get_settings().model_copy(update={"worker_max_attempts": max_attempts})
    monkeypatch.setattr(catalog_worker, "get_settings", lambda: settings)
    monkeypatch.setattr(catalog_worker, "_lease_heartbeat", lambda *_args: None)

    def fail_processing(_db: Session, _payload: dict[str, object]) -> None:
        raise RuntimeError("catalog parser exploded")

    monkeypatch.setattr(catalog_worker, "process_catalog_import_job", fail_processing)


def test_terminal_failure_marker_sets_a_useful_polling_result() -> None:
    tenant_id = uuid.uuid4()
    catalog_import = CatalogImport(status=CatalogImportStatus.READY)
    db = Mock(spec=Session)
    db.scalar.return_value = catalog_import

    marked = mark_catalog_import_failed_after_retry_exhaustion(
        db,
        tenant_id=tenant_id,
        payload={"import_id": str(uuid.uuid4())},
        error=RuntimeError("catalog parser exploded"),
    )

    assert marked
    assert catalog_import.status is CatalogImportStatus.FAILED
    assert catalog_import.failure_reason is not None
    assert "retry budget exhausted" in catalog_import.failure_reason
    assert "RuntimeError: catalog parser exploded" in catalog_import.failure_reason


@pytest.mark.integration
def test_retry_exhaustion_fails_import_and_quarantines_job(
    postgres_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with postgres_session_factory() as db:
        tenant_id, catalog_import, job, worker_id = _create_claimed_catalog_job(
            db,
            attempts=1,
        )
        import_id = catalog_import.id
        job_id = job.id

    try:
        _force_unexpected_worker_error(monkeypatch, max_attempts=1)
        catalog_worker._process_job(tenant_id, job, worker_id)

        with postgres_session_factory() as db:
            persisted_import = db.get(CatalogImport, import_id)
            persisted_job = db.get(DurableJob, job_id)
            assert persisted_import is not None
            assert persisted_import.status is CatalogImportStatus.FAILED
            assert persisted_import.failure_reason is not None
            assert "retry budget exhausted" in persisted_import.failure_reason
            assert "RuntimeError: catalog parser exploded" in persisted_import.failure_reason
            assert persisted_job is not None
            assert persisted_job.status is JobStatus.QUARANTINED
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


@pytest.mark.integration
def test_retryable_worker_error_leaves_import_pollable_for_next_attempt(
    postgres_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with postgres_session_factory() as db:
        tenant_id, catalog_import, job, worker_id = _create_claimed_catalog_job(
            db,
            attempts=1,
        )
        import_id = catalog_import.id
        job_id = job.id

    try:
        _force_unexpected_worker_error(monkeypatch, max_attempts=2)
        catalog_worker._process_job(tenant_id, job, worker_id)

        with postgres_session_factory() as db:
            persisted_import = db.get(CatalogImport, import_id)
            persisted_job = db.get(DurableJob, job_id)
            assert persisted_import is not None
            assert persisted_import.status is CatalogImportStatus.READY
            assert persisted_import.failure_reason is None
            assert persisted_job is not None
            assert persisted_job.status is JobStatus.PENDING
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


@pytest.mark.integration
def test_expired_exhausted_lease_terminalizes_catalog_import(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    with postgres_session_factory() as db:
        tenant_id, catalog_import, job, _worker_id = _create_claimed_catalog_job(
            db,
            attempts=1,
        )
        import_id = catalog_import.id
        job_id = job.id
        job.lease_expires_at = _database_now(db) - timedelta(seconds=1)
        db.commit()

    try:
        with postgres_session_factory() as db:
            claimed = claim_jobs(
                db,
                worker_id="catalog-recovery-worker",
                limit=1,
                lease_seconds=30,
                max_attempts=1,
                tenant_id=tenant_id,
                kinds=(JobKind.CATALOG_IMPORT,),
            )
            assert claimed == []
            assert reconcile_quarantined_catalog_imports(db, tenant_id=tenant_id) == 1

        with postgres_session_factory() as db:
            persisted_import = db.get(CatalogImport, import_id)
            persisted_job = db.get(DurableJob, job_id)
            assert persisted_import is not None
            assert persisted_import.status is CatalogImportStatus.FAILED
            assert persisted_import.failure_reason is not None
            assert "Catalog worker job quarantined" in persisted_import.failure_reason
            assert "LeaseExpired" in persisted_import.failure_reason
            assert persisted_job is not None
            assert persisted_job.status is JobStatus.QUARANTINED
            assert persisted_job.last_error == (
                "LeaseExpired: maximum processing attempts exhausted"
            )
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()
