import signal
import socket
import threading
import time
import uuid

import structlog
from sqlalchemy import text

from abacus.config import get_settings
from abacus.db import SessionLocal, tenant_session_scope
from abacus.enums import JobKind
from abacus.logging import configure_logging
from abacus.models.jobs import DurableJob
from abacus.services.catalog import process_catalog_import_job
from abacus.services.jobs import claim_jobs, mark_completed, mark_failed, renew_lease

configure_logging()
logger = structlog.get_logger(__name__)
_stop_requested = False


def _request_stop(*_: object) -> None:
    global _stop_requested
    _stop_requested = True


def _active_tenants() -> list[uuid.UUID]:
    with SessionLocal() as db:
        return list(db.scalars(text("SELECT tenant_id FROM app_active_tenants()")))


def _lease_heartbeat(
    tenant_id: uuid.UUID,
    job_id: uuid.UUID,
    worker_id: str,
    lease_seconds: int,
    stop: threading.Event,
) -> None:
    while not stop.wait(max(1.0, lease_seconds / 3)):
        try:
            with tenant_session_scope(tenant_id) as db:
                if not renew_lease(
                    db,
                    job_id,
                    worker_id,
                    lease_seconds=lease_seconds,
                ):
                    return
        except Exception:
            logger.exception("catalog_job_lease_renewal_failed", job_id=str(job_id))
            return


def _process_job(tenant_id: uuid.UUID, job: DurableJob, worker_id: str) -> None:
    settings = get_settings()
    stop = threading.Event()
    heartbeat = threading.Thread(
        target=_lease_heartbeat,
        args=(tenant_id, job.id, worker_id, settings.worker_lease_seconds, stop),
        daemon=True,
    )
    heartbeat.start()
    try:
        with tenant_session_scope(tenant_id) as db:
            current = db.get(DurableJob, job.id)
            if current is None:
                return
            process_catalog_import_job(db, current.payload)
            stop.set()
            heartbeat.join(timeout=2)
            if not mark_completed(db, current.id, worker_id):
                logger.warning("catalog_job_completion_lease_lost", job_id=str(current.id))
                return
            logger.info("catalog_job_completed", job_id=str(current.id))
    except Exception as exc:
        stop.set()
        heartbeat.join(timeout=2)
        with tenant_session_scope(tenant_id) as db:
            mark_failed(
                db,
                job.id,
                worker_id,
                exc,
                max_attempts=settings.worker_max_attempts,
            )
        logger.exception("catalog_job_failed", job_id=str(job.id))
    finally:
        stop.set()
        heartbeat.join(timeout=2)


def run() -> None:
    settings = get_settings()
    worker_id = f"catalog-{socket.gethostname()}-{uuid.uuid4()}"
    logger.info("catalog_worker_started", worker_id=worker_id)
    while not _stop_requested:
        claimed_any = False
        for tenant_id in _active_tenants():
            with tenant_session_scope(tenant_id) as db:
                jobs = claim_jobs(
                    db,
                    worker_id=worker_id,
                    limit=1,
                    lease_seconds=settings.worker_lease_seconds,
                    max_attempts=settings.worker_max_attempts,
                    tenant_id=tenant_id,
                    kinds=(JobKind.CATALOG_IMPORT,),
                )
            for job in jobs:
                claimed_any = True
                _process_job(tenant_id, job, worker_id)
        if not claimed_any:
            time.sleep(settings.worker_poll_interval_ms / 1000)
    logger.info("catalog_worker_stopped", worker_id=worker_id)


def main() -> None:
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    run()


if __name__ == "__main__":
    main()
