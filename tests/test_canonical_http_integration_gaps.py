from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from abacus.config import get_settings
from abacus.enums import StoreStatus, TenantStatus, ZoneKind
from abacus.models.architecture import (
    CanonicalIdentityRole,
    RfidEventProcessingStatus,
    RfidObservationEventLedger,
    UserRole,
    UserStoreAssignment,
)
from abacus.models.catalog import ProductStyle, Sku
from abacus.models.identity import IdentityAuditAction, User, UserStatus
from abacus.models.tenancy import Store, Tenant, Zone
from abacus.processes import event_worker
from abacus.security import create_access_token
from abacus.services.streaming_inventory import RecentObservationState

pytestmark = pytest.mark.integration


@dataclass(frozen=True, slots=True)
class CanonicalHttpData:
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    zone_id: uuid.UUID
    admin_token: str
    target_user_id: uuid.UUID
    sku_id: uuid.UUID
    sku_code: str


def _token(user: User) -> str:
    settings = get_settings()
    token, _ = create_access_token(
        user_id=user.id,
        tenant_id=user.tenant_id,
        token_version=user.token_version,
        secret=settings.jwt_secret,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        lifetime=timedelta(minutes=15),
    )
    return token


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def canonical_http_data(
    postgres_session_factory: sessionmaker[Session],
) -> Iterator[CanonicalHttpData]:
    suffix = uuid.uuid4().hex[:12]
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    admin = User(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        email=f"admin-{suffix}@orange.example",
        display_name="Canonical HTTP Admin",
        password_hash="not-used-by-this-test",
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    target = User(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        email=f"associate-{suffix}@orange.example",
        display_name="Canonical HTTP Associate",
        password_hash="not-used-by-this-test",
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    style = ProductStyle(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        code=f"STYLE-{suffix.upper()}",
        name="Canonical HTTP Style",
        attributes={"category": "SHIRTS"},
        active=True,
    )
    sku = Sku(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        product_style_id=style.id,
        product_variant_id=None,
        code=f"SKU-{suffix.upper()}-M",
        upc=str(tenant_id.int)[:12],
        color="Orange",
        size="M",
        attributes={},
        active=True,
    )

    with postgres_session_factory() as db:
        db.add(
            Tenant(
                id=tenant_id,
                code=f"canonical-http-{suffix}",
                name="Canonical HTTP Integration",
                status=TenantStatus.ACTIVE,
            )
        )
        db.flush()
        db.add_all(
            [
                Store(
                    id=store_id,
                    tenant_id=tenant_id,
                    code=f"store-{suffix}",
                    name="Canonical HTTP Store",
                    timezone="UTC",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                admin,
                target,
                style,
            ]
        )
        db.flush()
        db.add_all(
            [
                Zone(
                    id=zone_id,
                    tenant_id=tenant_id,
                    store_id=store_id,
                    code="sales-floor",
                    name="Sales Floor",
                    kind=ZoneKind.SALES_FLOOR,
                ),
                sku,
                UserRole(
                    tenant_id=tenant_id,
                    user_id=admin.id,
                    role=CanonicalIdentityRole.TENANT_ADMIN,
                ),
                UserRole(
                    tenant_id=tenant_id,
                    user_id=target.id,
                    role=CanonicalIdentityRole.STORE_ASSOCIATE,
                ),
                UserStoreAssignment(
                    tenant_id=tenant_id,
                    user_id=target.id,
                    store_id=store_id,
                ),
            ]
        )
        db.commit()

    data = CanonicalHttpData(
        tenant_id=tenant_id,
        store_id=store_id,
        zone_id=zone_id,
        admin_token=_token(admin),
        target_user_id=target.id,
        sku_id=sku.id,
        sku_code=sku.code,
    )
    try:
        yield data
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


def _register_device(
    api_client: TestClient,
    data: CanonicalHttpData,
) -> tuple[uuid.UUID, str]:
    response = api_client.post(
        f"/v1/stores/{data.store_id}/devices",
        headers=_bearer(data.admin_token),
        json={
            "serial_number": f"reader-{uuid.uuid4().hex}",
            "display_name": "Canonical HTTP Reader",
            "zone_id": str(data.zone_id),
        },
    )
    assert response.status_code == 201, response.text
    payload = response.json()
    return uuid.UUID(payload["device"]["id"]), str(payload["device_token"])


def _observation_request(
    device_id: uuid.UUID,
    *,
    epc: str,
    event_id: str | None = None,
) -> dict[str, object]:
    return {
        "device_id": str(device_id),
        "observations": [
            {
                "event_id": event_id or str(uuid.uuid4()),
                "epc": epc,
                "observed_at": datetime.now(UTC).isoformat(),
                "rssi": -48.0,
                "antenna_id": "floor-1",
            }
        ],
    }


def test_store_device_and_sku_discovery_cross_the_http_and_database_boundaries(
    api_client: TestClient,
    canonical_http_data: CanonicalHttpData,
) -> None:
    device_id, _device_token = _register_device(api_client, canonical_http_data)

    devices = api_client.get(
        f"/v1/stores/{canonical_http_data.store_id}/devices",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert devices.status_code == 200, devices.text
    assert [uuid.UUID(item["device"]["id"]) for item in devices.json()] == [device_id]
    assert uuid.UUID(devices.json()[0]["assignment"]["zone_id"]) == canonical_http_data.zone_id

    skus = api_client.get(
        "/v1/skus",
        headers=_bearer(canonical_http_data.admin_token),
        params={"active": "ALL", "code": canonical_http_data.sku_code},
    )
    assert skus.status_code == 200, skus.text
    assert skus.json()["total"] == 1
    assert uuid.UUID(skus.json()["items"][0]["id"]) == canonical_http_data.sku_id

    detail = api_client.get(
        f"/v1/skus/{canonical_http_data.sku_id}",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["code"] == canonical_http_data.sku_code
    assert detail.json()["style_code"].startswith("STYLE-")


def test_identity_discovery_and_atomic_access_replacement_cross_http_and_database(
    api_client: TestClient,
    canonical_http_data: CanonicalHttpData,
) -> None:
    users = api_client.get(
        "/v1/users",
        headers=_bearer(canonical_http_data.admin_token),
        params={"limit": 10, "offset": 0},
    )
    assert users.status_code == 200, users.text
    assert users.json()["total"] == 2
    assert canonical_http_data.target_user_id in {
        uuid.UUID(item["id"]) for item in users.json()["items"]
    }

    initial = api_client.get(
        f"/v1/users/{canonical_http_data.target_user_id}",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert initial.status_code == 200, initial.text
    assert initial.json()["roles"] == [CanonicalIdentityRole.STORE_ASSOCIATE]
    assert initial.json()["store_ids"] == [str(canonical_http_data.store_id)]

    replaced = api_client.put(
        f"/v1/users/{canonical_http_data.target_user_id}/access",
        headers=_bearer(canonical_http_data.admin_token),
        json={
            "roles": [CanonicalIdentityRole.STORE_MANAGER],
            "store_ids": [str(canonical_http_data.store_id)],
        },
    )
    assert replaced.status_code == 200, replaced.text
    assert replaced.json() == {
        "user_id": str(canonical_http_data.target_user_id),
        "roles": [CanonicalIdentityRole.STORE_MANAGER],
        "store_ids": [str(canonical_http_data.store_id)],
    }

    detail = api_client.get(
        f"/v1/users/{canonical_http_data.target_user_id}",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert detail.status_code == 200, detail.text
    assert detail.json()["roles"] == [CanonicalIdentityRole.STORE_MANAGER]

    audit = api_client.get(
        "/v1/users/audit-records",
        headers=_bearer(canonical_http_data.admin_token),
        params={"limit": 10, "offset": 0},
    )
    assert audit.status_code == 200, audit.text
    assert audit.json()["total"] == 1
    assert audit.json()["items"][0]["action"] == IdentityAuditAction.USER_ACCESS_CHANGED
    assert uuid.UUID(audit.json()["items"][0]["target_user_id"]) == (
        canonical_http_data.target_user_id
    )


def test_quarantine_replay_and_device_credential_rotation_cross_http_and_database(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
    canonical_http_data: CanonicalHttpData,
) -> None:
    device_id, original_token = _register_device(api_client, canonical_http_data)
    unknown_epc = f"3034{uuid.uuid4().hex[:20].upper()}"
    event_id = str(uuid.uuid4())
    quarantined_event = _observation_request(
        device_id,
        epc=unknown_epc,
        event_id=event_id,
    )
    accepted = api_client.post(
        "/v1/rfid/observation-batches",
        headers={"X-Device-Token": original_token},
        json=quarantined_event,
    )
    assert accepted.status_code == 202, accepted.text

    raw_count, transition_count = event_worker.process_tenant_once(
        canonical_http_data.tenant_id,
        RecentObservationState(),
    )
    assert (raw_count, transition_count) == (1, 0)

    quarantine = api_client.get(
        "/v1/rfid/quarantine",
        headers=_bearer(canonical_http_data.admin_token),
        params={"event_id": event_id},
    )
    assert quarantine.status_code == 200, quarantine.text
    assert quarantine.json()["total"] == 1
    quarantine_record = quarantine.json()["items"][0]
    assert quarantine_record["reason"] == "UNKNOWN_EPC"
    assert quarantine_record["processing_status"] == RfidEventProcessingStatus.REJECTED

    replay = api_client.post(
        f"/v1/rfid/quarantine/{quarantine_record['id']}:replay",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert replay.status_code == 202, replay.text
    assert replay.json()["queued"] is True
    assert replay.json()["processing_status"] == RfidEventProcessingStatus.PENDING
    with postgres_session_factory() as db:
        ledger = db.get(
            RfidObservationEventLedger,
            (canonical_http_data.tenant_id, event_id),
        )
        assert ledger is not None
        assert ledger.processing_status is RfidEventProcessingStatus.PENDING

    rotated = api_client.post(
        f"/v1/devices/{device_id}/credentials:rotate",
        headers=_bearer(canonical_http_data.admin_token),
    )
    assert rotated.status_code == 200, rotated.text
    replacement_token = str(rotated.json()["device_token"])
    assert replacement_token != original_token

    post_rotation_request = _observation_request(
        device_id,
        epc=f"3034{uuid.uuid4().hex[:20].upper()}",
    )
    old_credential = api_client.post(
        "/v1/rfid/observation-batches",
        headers={"X-Device-Token": original_token},
        json=post_rotation_request,
    )
    assert old_credential.status_code == 401, old_credential.text

    new_credential = api_client.post(
        "/v1/rfid/observation-batches",
        headers={"X-Device-Token": replacement_token},
        json=post_rotation_request,
    )
    assert new_credential.status_code == 202, new_credential.text
