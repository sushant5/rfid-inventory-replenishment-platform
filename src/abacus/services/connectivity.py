import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from abacus.models.architecture import FreshnessStatus, StoreConnectivity


def lock_store_connectivity_for_receipt(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    received_at: datetime,
    backlog_drained: bool,
    reader_coverage_ok: bool,
) -> tuple[StoreConnectivity, bool]:
    """Lock a connectivity row and apply status from the newest receipt only.

    The conflict-safe insert removes the first-heartbeat race. The subsequent row
    lock serializes API ingress and worker updates, while ``status_received_at``
    prevents a delayed worker from replacing newer gateway status.
    """

    db.execute(
        insert(StoreConnectivity)
        .values(
            tenant_id=tenant_id,
            store_id=store_id,
            gateway_last_heartbeat=received_at,
            status_received_at=received_at,
            backlog_drained=backlog_drained,
            reader_coverage_ok=reader_coverage_ok,
            freshness_status=FreshnessStatus.STALE,
        )
        .on_conflict_do_nothing(constraint="pk_store_connectivity")
    )
    connectivity = db.scalar(
        select(StoreConnectivity)
        .where(
            StoreConnectivity.tenant_id == tenant_id,
            StoreConnectivity.store_id == store_id,
        )
        .with_for_update()
    )
    if connectivity is None:
        raise RuntimeError("Store connectivity row could not be locked")

    if (
        connectivity.gateway_last_heartbeat is None
        or received_at > connectivity.gateway_last_heartbeat
    ):
        connectivity.gateway_last_heartbeat = received_at

    is_current_receipt = received_at >= connectivity.status_received_at
    if is_current_receipt:
        connectivity.status_received_at = received_at
        connectivity.backlog_drained = backlog_drained
        connectivity.reader_coverage_ok = reader_coverage_ok
        if not backlog_drained or not reader_coverage_ok:
            connectivity.freshness_status = FreshnessStatus.STALE

    return connectivity, is_current_receipt
