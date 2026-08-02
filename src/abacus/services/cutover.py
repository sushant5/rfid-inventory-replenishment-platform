import uuid
from dataclasses import dataclass

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.models.replenishment import ReplenishmentTask
from abacus.services.locks import lock_replenishment_store_sku


class ReservationCutoverPending(RuntimeError):
    """Raised when durable work must pause for legacy reservation review."""


@dataclass(frozen=True)
class PendingReservationCutover:
    task_id: uuid.UUID
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    sku_id: uuid.UUID
    moved_quantity: int


def pending_reservation_cutover_count(db: Session) -> int:
    count = db.scalar(
        select(func.count())
        .select_from(ReplenishmentTask)
        .where(ReplenishmentTask.reservation_cutover_reviewed.is_(False))
    )
    return int(count or 0)


def reservation_cutover_ready(db: Session) -> bool:
    return pending_reservation_cutover_count(db) == 0


def ensure_reservation_cutover_ready(db: Session) -> None:
    if not reservation_cutover_ready(db):
        raise ReservationCutoverPending(
            "legacy replenishment movement requires cutover reconciliation"
        )


def require_reservation_cutover_ready(db: Session) -> None:
    try:
        ensure_reservation_cutover_ready(db)
    except ReservationCutoverPending as exc:
        raise ApiError(
            503,
            "Cutover reconciliation required",
            "Legacy replenishment movement must be reconciled before this operation.",
            code="cutover_reconciliation_required",
        ) from exc


def list_pending_reservation_cutovers(db: Session) -> list[PendingReservationCutover]:
    tasks = db.scalars(
        select(ReplenishmentTask)
        .where(ReplenishmentTask.reservation_cutover_reviewed.is_(False))
        .order_by(ReplenishmentTask.created_at, ReplenishmentTask.id)
    ).all()
    return [
        PendingReservationCutover(
            task_id=task.id,
            tenant_id=task.tenant_id,
            store_id=task.store_id,
            sku_id=task.sku_id,
            moved_quantity=task.moved_quantity,
        )
        for task in tasks
    ]


def reconcile_reservation_cutover_task(
    db: Session,
    *,
    task_id: uuid.UUID,
    baseline: int,
    reviewed_by: str,
    note: str,
) -> ReplenishmentTask:
    reviewer = reviewed_by.strip()
    review_note = note.strip()
    if not reviewer or len(reviewer) > 255:
        raise ValueError("reviewed_by must contain 1-255 characters")
    if not review_note or len(review_note) > 1000:
        raise ValueError("note must contain 1-1000 characters")

    initial = db.get(ReplenishmentTask, task_id)
    if initial is None:
        raise ValueError("replenishment task does not exist")
    lock_replenishment_store_sku(db, initial.tenant_id, initial.store_id, initial.sku_id)
    task = db.scalar(
        select(ReplenishmentTask)
        .where(ReplenishmentTask.id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if task is None:  # pragma: no cover - protected by the transaction and row identity
        raise ValueError("replenishment task does not exist")
    if not 0 <= baseline <= task.moved_quantity:
        raise ValueError(f"baseline must be between 0 and moved_quantity ({task.moved_quantity})")
    if task.reservation_cutover_reviewed:
        if task.reconciled_before_tracking_quantity == baseline:
            return task
        raise ValueError("task was already reviewed with a different baseline")

    reviewed_at = db.scalar(select(func.clock_timestamp()))
    if reviewed_at is None:  # pragma: no cover - PostgreSQL always returns a value
        raise RuntimeError("database clock is unavailable")
    task.reconciled_before_tracking_quantity = baseline
    task.reservation_cutover_reviewed = True
    task.reservation_cutover_reviewed_at = reviewed_at
    task.reservation_cutover_reviewed_by = reviewer
    task.reservation_cutover_note = review_note
    db.flush()
    return task
