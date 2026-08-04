import hashlib
import json
import secrets
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import cast

from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.db import TenantSession, pin_session_to_tenant
from abacus.enums import BatchStatus, StoreStatus, TenantStatus
from abacus.models.tenancy import (
    Device,
    DeviceAssignment,
    OnboardingBatch,
    OrganizationUnit,
    Store,
    Tenant,
    Zone,
)
from abacus.schemas.tenancy import (
    BulkStoreOnboardingRequest,
    StoreDeviceCreate,
    TenantCreate,
    ZoneCreate,
)
from abacus.security import Permission, Principal


@dataclass(frozen=True, slots=True)
class StoreDeviceRegistration:
    device: Device
    assignment: DeviceAssignment
    api_key: str


def _database_now(db: Session) -> datetime:
    """Return the database clock used for effective-dated assignment boundaries."""

    value = db.scalar(select(func.clock_timestamp()))
    if value is None:  # pragma: no cover - PostgreSQL always returns a value.
        raise RuntimeError("database clock is unavailable")
    return cast(datetime, value)


def create_tenant(db: Session, request: TenantCreate) -> Tenant:
    tenant_id: uuid.UUID
    if isinstance(db, TenantSession):
        resolved_tenant_id = db.scalar(
            text("SELECT abacus_resolve_login_tenant(:tenant_code)"),
            {"tenant_code": request.code},
        )
        db.rollback()
        tenant_id = (
            uuid.UUID(str(resolved_tenant_id)) if resolved_tenant_id is not None else uuid.uuid4()
        )
        pin_session_to_tenant(db, tenant_id)
    else:
        tenant_id = uuid.uuid4()
    existing = db.scalar(select(Tenant).where(Tenant.code == request.code))
    if existing is not None:
        if existing.name == request.name:
            return existing
        raise ApiError(
            409,
            "Tenant code conflict",
            f"Tenant code '{request.code}' already belongs to another tenant name.",
            code="tenant_code_conflict",
        )

    tenant = Tenant(
        id=tenant_id,
        code=request.code,
        name=request.name,
        status=TenantStatus.ACTIVE,
    )
    db.add(tenant)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Tenant code conflict",
            f"Tenant code '{request.code}' already exists.",
            code="tenant_code_conflict",
        ) from exc
    db.refresh(tenant)
    return tenant


def _store_in_principal_tenant(
    db: Session,
    principal: Principal,
    store_id: uuid.UUID,
) -> Store:
    store = db.scalar(
        select(Store).where(
            Store.id == store_id,
            Store.tenant_id == principal.tenant_id,
        )
    )
    if store is None:
        # Tenant-scoped lookup avoids confirming another tenant's identifiers.
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    if not principal.can_access_store(Permission.TENANT_CONFIGURE, store.id):
        raise ApiError(
            403,
            "Forbidden",
            "The requested store is outside the current user's access scope.",
            code="store_scope_denied",
        )
    return store


def create_store_zone(
    db: Session,
    principal: Principal,
    store_id: uuid.UUID,
    request: ZoneCreate,
) -> Zone:
    store = _store_in_principal_tenant(db, principal, store_id)
    zone = Zone(
        tenant_id=store.tenant_id,
        store_id=store.id,
        code=request.code,
        name=request.name,
        kind=request.kind,
    )
    db.add(zone)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Zone code conflict",
            f"Zone code '{request.code}' already exists in this store.",
            code="zone_code_conflict",
        ) from exc
    db.refresh(zone)
    return zone


def register_store_device(
    db: Session,
    principal: Principal,
    store_id: uuid.UUID,
    request: StoreDeviceCreate,
) -> StoreDeviceRegistration:
    store = _store_in_principal_tenant(db, principal, store_id)
    zone = db.scalar(
        select(Zone).where(
            Zone.id == request.zone_id,
            Zone.tenant_id == store.tenant_id,
            Zone.store_id == store.id,
        )
    )
    if zone is None:
        raise ApiError(
            422,
            "Invalid assignment location",
            "zone_id must identify a zone in the requested store.",
            code="invalid_device_assignment_location",
        )

    raw_secret = secrets.token_urlsafe(32)
    device = Device(
        tenant_id=store.tenant_id,
        serial_number=request.serial_number,
        display_name=request.display_name,
        credential_hash=hashlib.sha256(raw_secret.encode()).hexdigest(),
    )
    db.add(device)
    try:
        db.flush()
        assignment = DeviceAssignment(
            tenant_id=store.tenant_id,
            device_id=device.id,
            store_id=store.id,
            zone_id=zone.id,
            effective_from=_database_now(db),
        )
        db.add(assignment)
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Device registration conflict",
            "The serial number already exists or the assignment was created concurrently.",
            code="device_registration_conflict",
        ) from exc
    db.refresh(device)
    db.refresh(assignment)
    return StoreDeviceRegistration(
        device=device,
        assignment=assignment,
        api_key=f"{device.id}.{raw_secret}",
    )


def _request_hash(request: BulkStoreOnboardingRequest) -> str:
    canonical = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _get_tenant(db: Session, tenant_id: uuid.UUID) -> Tenant:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise ApiError(404, "Tenant not found", "The requested tenant does not exist.")
    return tenant


def _resolve_organization_path(
    db: Session,
    tenant_id: uuid.UUID,
    path: list[object],
) -> uuid.UUID | None:
    parent_id: uuid.UUID | None = None
    for raw_segment in path:
        segment = raw_segment
        code = segment.code  # type: ignore[attr-defined]
        existing = db.scalar(
            select(OrganizationUnit).where(
                OrganizationUnit.tenant_id == tenant_id,
                OrganizationUnit.code == code,
            )
        )
        if existing is not None:
            if (
                existing.parent_id != parent_id
                or existing.name != segment.name  # type: ignore[attr-defined]
                or existing.unit_type != segment.unit_type  # type: ignore[attr-defined]
            ):
                raise ApiError(
                    409,
                    "Organization hierarchy conflict",
                    f"Organization unit '{code}' conflicts with its existing definition.",
                    code="organization_unit_conflict",
                )
            parent_id = existing.id
            continue

        unit = OrganizationUnit(
            tenant_id=tenant_id,
            parent_id=parent_id,
            code=code,
            name=segment.name,  # type: ignore[attr-defined]
            unit_type=segment.unit_type,  # type: ignore[attr-defined]
        )
        db.add(unit)
        db.flush()
        parent_id = unit.id
    return parent_id


def onboard_stores(
    db: Session,
    tenant_id: uuid.UUID,
    idempotency_key: str,
    request: BulkStoreOnboardingRequest,
) -> OnboardingBatch:
    _get_tenant(db, tenant_id)
    digest = _request_hash(request)
    existing_batch = db.scalar(
        select(OnboardingBatch).where(
            OnboardingBatch.tenant_id == tenant_id,
            OnboardingBatch.idempotency_key == idempotency_key,
        )
    )
    if existing_batch is not None:
        if existing_batch.request_hash != digest:
            raise ApiError(
                409,
                "Idempotency conflict",
                "This idempotency key was already used with a different request.",
                code="idempotency_key_reused",
            )
        return existing_batch

    requested_codes = [store.code for store in request.stores]
    existing_codes = set(
        db.scalars(
            select(Store.code).where(
                Store.tenant_id == tenant_id,
                Store.code.in_(requested_codes),
            )
        ).all()
    )
    if existing_codes:
        raise ApiError(
            409,
            "Store code conflict",
            f"Stores already exist: {sorted(existing_codes)}",
            code="store_code_conflict",
        )

    requested_serials = [
        device.serial_number for store in request.stores for device in store.devices
    ]
    if requested_serials:
        existing_serials = set(
            db.scalars(
                select(Device.serial_number).where(Device.serial_number.in_(requested_serials))
            ).all()
        )
        if existing_serials:
            raise ApiError(
                409,
                "Device serial conflict",
                f"Devices already exist: {sorted(existing_serials)}",
                code="device_serial_conflict",
            )

    batch = OnboardingBatch(
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
        request_hash=digest,
        status=BatchStatus.VALIDATING,
        total_count=len(request.stores),
        succeeded_count=0,
        failed_count=0,
        errors=[],
    )
    db.add(batch)

    effective_from = _database_now(db)
    for store_request in request.stores:
        organization_unit_id = _resolve_organization_path(
            db,
            tenant_id,
            list(store_request.organization_path),
        )
        store = Store(
            tenant_id=tenant_id,
            organization_unit_id=organization_unit_id,
            code=store_request.code,
            name=store_request.name,
            timezone=store_request.timezone,
            status=StoreStatus.ACTIVE,
            configuration=store_request.configuration,
        )
        db.add(store)
        db.flush()

        zones_by_code: dict[str, Zone] = {}
        for zone_request in store_request.zones:
            zone = Zone(
                tenant_id=tenant_id,
                store_id=store.id,
                code=zone_request.code,
                name=zone_request.name,
                kind=zone_request.kind,
            )
            db.add(zone)
            db.flush()
            zones_by_code[zone.code] = zone

        for device_request in store_request.devices:
            device = Device(
                tenant_id=tenant_id,
                serial_number=device_request.serial_number,
                display_name=device_request.display_name,
            )
            db.add(device)
            db.flush()
            db.add(
                DeviceAssignment(
                    tenant_id=tenant_id,
                    device_id=device.id,
                    store_id=store.id,
                    zone_id=zones_by_code[device_request.zone_code].id,
                    effective_from=effective_from,
                )
            )

    batch.status = BatchStatus.COMPLETED
    batch.succeeded_count = len(request.stores)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Onboarding conflict",
            "Onboarding conflicted with concurrently created tenant data.",
            code="onboarding_conflict",
        ) from exc
    db.refresh(batch)
    return batch


def list_stores(db: Session, tenant_id: uuid.UUID) -> list[Store]:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    _get_tenant(db, tenant_id)
    return list(
        db.scalars(
            select(Store).where(Store.tenant_id == tenant_id).order_by(Store.code.asc())
        ).all()
    )


def list_visible_stores(
    db: Session,
    principal: Principal,
    *,
    limit: int,
    offset: int,
) -> tuple[list[Store], int]:
    """List stores authorized for inventory access inside the JWT tenant."""

    filters = [Store.tenant_id == principal.tenant_id]
    if not principal.has_tenant_permission(Permission.INVENTORY_READ):
        store_ids = principal.store_ids_for_permission(Permission.INVENTORY_READ)
        if not store_ids:
            return [], 0
        filters.append(Store.id.in_(store_ids))

    total = db.scalar(select(func.count(Store.id)).where(*filters)) or 0
    stores = list(
        db.scalars(
            select(Store)
            .where(*filters)
            .order_by(Store.code.asc(), Store.id.asc())
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return stores, total


def rotate_device_credential(
    db: Session,
    tenant_id: uuid.UUID,
    device_id: uuid.UUID,
) -> str:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    _get_tenant(db, tenant_id)
    device = db.scalar(select(Device).where(Device.id == device_id, Device.tenant_id == tenant_id))
    if device is None:
        raise ApiError(404, "Device not found", "The requested device does not exist.")
    secret = secrets.token_urlsafe(32)
    device.credential_hash = hashlib.sha256(secret.encode()).hexdigest()
    db.commit()
    return f"{device.id}.{secret}"
