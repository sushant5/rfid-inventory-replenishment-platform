import uuid

from sqlalchemy import text
from sqlalchemy.orm import Session


def lock_replenishment_store_sku(
    db: Session,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> None:
    """Serialize task reservation and inventory allocation for one store/SKU."""

    lock_key = f"replenishment:{tenant_id}:{store_id}:{sku_id}"
    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": lock_key},
    )
