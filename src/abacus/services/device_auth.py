"""Authentication for store-bound RFID gateways."""

import hashlib
import secrets
import uuid

from sqlalchemy import select, text
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.db import TenantSession, pin_session_to_store_scope, pin_session_to_tenant
from abacus.enums import DeviceStatus, TenantStatus
from abacus.models.tenancy import Device, Tenant


def authenticate_device(db: Session, raw_token: str | None) -> Device:
    """Resolve and verify a device token without trusting request location data."""

    if not raw_token or "." not in raw_token:
        raise ApiError(401, "Unauthorized device", "A valid device token is required.")
    raw_id, secret = raw_token.split(".", 1)
    try:
        device_id = uuid.UUID(raw_id)
    except ValueError as exc:
        raise ApiError(401, "Unauthorized device", "The device token is malformed.") from exc

    if isinstance(db, TenantSession):
        tenant_id = db.scalar(
            text("SELECT abacus_resolve_device_tenant(:device_id)"),
            {"device_id": device_id},
        )
        db.rollback()
        if tenant_id is None:
            raise ApiError(401, "Unauthorized device", "The device token is invalid.")
        pin_session_to_tenant(db, uuid.UUID(str(tenant_id)))
        # Device verification itself precedes assignment lookup. The request remains
        # constrained by the authenticated device ID and effective assignments.
        pin_session_to_store_scope(db, tenant_wide=True)

    device = db.get(Device, device_id)
    candidate = hashlib.sha256(secret.encode()).hexdigest()
    tenant_status = (
        db.scalar(select(Tenant.status).where(Tenant.id == device.tenant_id))
        if device is not None
        else None
    )
    if (
        device is None
        or device.status != DeviceStatus.ACTIVE
        or tenant_status != TenantStatus.ACTIVE
        or device.credential_hash is None
        or not secrets.compare_digest(candidate, device.credential_hash)
    ):
        raise ApiError(401, "Unauthorized device", "The device token is invalid.")
    return device
