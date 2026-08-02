import signal
import socket
import threading
import time
import uuid
from collections.abc import Callable

import structlog
from sqlalchemy.orm import Session

from abacus.config import get_settings
from abacus.db import SessionLocal
from abacus.enums import JobKind
from abacus.logging import configure_logging
from abacus.models.jobs import DurableJob
from abacus.services.jobs import claim_jobs, mark_completed, mark_failed, renew_lease

configure_logging()
logger = structlog.get_logger(__name__)
_stop_requested = False


def _request_stop(*_: object) -> None:
    global _stop_requested
    _stop_requested = True


def _dispatch(db: Session, job: DurableJob) -> None:
    handlers: dict[JobKind, Callable[[Session, dict[str, object]], None]] = {}

    if job.kind == JobKind.CATALOG_IMPORT:
        from abacus.services.catalog import process_catalog_import_job

        handlers[JobKind.CATALOG_IMPORT] = process_catalog_import_job
    elif job.kind == JobKind.RFID_OBSERVATION:
        from abacus.services.rfid import process_rfid_observation_job

        handlers[JobKind.RFID_OBSERVATION] = process_rfid_observation_job
    elif job.kind == JobKind.REPLENISHMENT_RECALC:
        from abacus.services.replenishment import process_replenishment_job

        handlers[JobKind.REPLENISHMENT_RECALC] = process_replenishment_job

    handler = handlers.get(job.kind)
    if handler is None:
        raise ValueError(f"No handler registered for job kind {job.kind}")
    handler(db, job.payload)


def _start_lease_heartbeat(
    job_id: uuid.UUID,
    worker_id: str,
    lease_seconds: int,
) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()
    interval = max(1.0, lease_seconds / 3)

    def heartbeat() -> None:
        while not stop.wait(interval):
            heartbeat_session = SessionLocal()
            try:
                if not renew_lease(
                    heartbeat_session,
                    job_id,
                    worker_id,
                    lease_seconds=lease_seconds,
                ):
                    logger.warning("job_lease_lost", job_id=str(job_id), worker_id=worker_id)
                    return
            except Exception:
                heartbeat_session.rollback()
                logger.exception("job_lease_renewal_failed", job_id=str(job_id))
            finally:
                heartbeat_session.close()

    thread = threading.Thread(target=heartbeat, name=f"lease-{job_id}", daemon=True)
    thread.start()
    return stop, thread


def _stop_lease_heartbeat(stop: threading.Event, thread: threading.Thread) -> None:
    stop.set()
    thread.join(timeout=2)


def run() -> None:
    settings = get_settings()
    worker_id = f"{socket.gethostname()}-{uuid.uuid4()}"
    logger.info("worker_started", worker_id=worker_id)

    while not _stop_requested:
        claim_session = SessionLocal()
        try:
            jobs = claim_jobs(
                claim_session,
                worker_id=worker_id,
                # Claim only what this process can start immediately. This prevents
                # queued work from losing its lease before processing begins.
                limit=1,
                lease_seconds=settings.worker_lease_seconds,
            )
        except Exception:
            claim_session.rollback()
            logger.exception("job_claim_failed", worker_id=worker_id)
            time.sleep(min(5.0, settings.worker_poll_interval_ms / 1000))
            continue
        finally:
            claim_session.close()

        if not jobs:
            time.sleep(settings.worker_poll_interval_ms / 1000)
            continue

        for claimed_job in jobs:
            processing_session = SessionLocal()
            heartbeat_stop, heartbeat_thread = _start_lease_heartbeat(
                claimed_job.id,
                worker_id,
                settings.worker_lease_seconds,
            )
            try:
                current_job = processing_session.get(DurableJob, claimed_job.id)
                if current_job is None:
                    continue
                _dispatch(processing_session, current_job)
                _stop_lease_heartbeat(heartbeat_stop, heartbeat_thread)
                if mark_completed(processing_session, current_job.id, worker_id):
                    logger.info("job_completed", job_id=str(current_job.id), kind=current_job.kind)
                else:
                    logger.warning("job_completion_lease_lost", job_id=str(current_job.id))
            except Exception as exc:
                _stop_lease_heartbeat(heartbeat_stop, heartbeat_thread)
                processing_session.rollback()
                failure_recorded = mark_failed(
                    processing_session,
                    claimed_job.id,
                    worker_id,
                    exc,
                    max_attempts=settings.worker_max_attempts,
                )
                if failure_recorded:
                    logger.exception(
                        "job_failed",
                        job_id=str(claimed_job.id),
                        kind=claimed_job.kind,
                    )
                else:
                    logger.exception("job_failure_lease_lost", job_id=str(claimed_job.id))
            finally:
                _stop_lease_heartbeat(heartbeat_stop, heartbeat_thread)
                processing_session.close()

    logger.info("worker_stopped", worker_id=worker_id)


def main() -> None:
    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)
    run()


if __name__ == "__main__":
    main()
