from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from scripts.generate_store_batch import build_store_batch
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from abacus.enums import JobKind, JobStatus, ObservationStatus, TenantStatus, ZoneKind
from abacus.models.catalog import CatalogImport, EpcBinding, Sku
from abacus.models.identity import IdentityRole
from abacus.models.jobs import DurableJob
from abacus.models.rfid import InventoryBalance, InventoryItemState, RfidObservation
from abacus.models.tenancy import DeviceAssignment, Tenant, Zone
from abacus.schemas.identity import RoleAssignmentCreate, UserCreate
from abacus.schemas.tenancy import TenantCreate
from abacus.services.identity import bootstrap_corporate_admin
from abacus.services.jobs import claim_jobs, mark_completed, mark_failed
from abacus.worker import _dispatch

pytestmark = pytest.mark.integration

PLATFORM_HEADERS = {"X-Platform-Key": os.environ["PLATFORM_API_KEY"]}
ADMIN_PASSWORD = "Corporate-Admin-123"
MANAGER_PASSWORD = "Store-Manager-1234"
ASSOCIATE_PASSWORD = "Store-Associate-1234"
SECOND_ASSOCIATE_PASSWORD = "Second-Associate-1234"
MIXED_ROLE_PASSWORD = "Mixed-Role-User-1234"

STYLE_CODE = "ST-TRAIL"
SKU_CODE = "SKU-TRAIL-BLUE-M"
UPC = "036000291452"
KNOWN_EPCS = (
    "3074257BF7194E4000001A85",
    "3074257BF7194E4000001A86",
    "3074257BF7194E4000001A87",
    "3074257BF7194E4000001A88",
)
RECOVERED_EPC = "3074257BF7194E4000001A99"


def _expect(response: Response, status_code: int) -> dict[str, object] | list[object]:
    assert response.status_code == status_code, response.text
    if status_code == 204:
        return {}
    payload = response.json()
    assert isinstance(payload, dict | list)
    return payload


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _login(client: TestClient, tenant_code: str, email: str, password: str) -> str:
    payload = _expect(
        client.post(
            "/v1/auth/login",
            json={"tenant_code": tenant_code, "email": email, "password": password},
        ),
        200,
    )
    assert isinstance(payload, dict)
    token = payload["access_token"]
    assert isinstance(token, str)
    return token


def _bootstrap_admin(
    session_factory: sessionmaker[Session],
    *,
    tenant_code: str,
    tenant_name: str,
    email: str,
) -> str:
    with session_factory() as db:
        record = bootstrap_corporate_admin(
            db,
            TenantCreate(code=tenant_code, name=tenant_name),
            UserCreate(
                email=email,
                display_name=f"{tenant_name} Administrator",
                password=ADMIN_PASSWORD,
                role_assignments=[
                    RoleAssignmentCreate(role=IdentityRole.CORPORATE_ADMIN),
                ],
            ),
        )
        return str(record.user.id)


def _drain_jobs(
    session_factory: sessionmaker[Session],
    *,
    maximum_jobs: int = 100,
) -> int:
    """Run the same claim/dispatch/complete path as the worker, without polling."""

    worker_id = f"pytest-{uuid.uuid4()}"
    processed = 0
    while processed < maximum_jobs:
        with session_factory() as claim_session:
            claimed = claim_jobs(
                claim_session,
                worker_id=worker_id,
                limit=1,
                lease_seconds=30,
            )
        if not claimed:
            return processed

        claimed_job = claimed[0]
        with session_factory() as processing_session:
            job = processing_session.get(DurableJob, claimed_job.id)
            assert job is not None
            try:
                _dispatch(processing_session, job)
                assert mark_completed(processing_session, job.id, worker_id)
            except Exception as exc:
                processing_session.rollback()
                mark_failed(
                    processing_session,
                    claimed_job.id,
                    worker_id,
                    exc,
                    max_attempts=1,
                )
                raise
        processed += 1

    pytest.fail(f"worker queue did not drain after {maximum_jobs} jobs")


def _store_payload(
    code: str,
    name: str,
    *,
    serial_prefix: str,
    with_readers: bool = True,
) -> dict[str, object]:
    devices: list[dict[str, str]] = []
    if with_readers:
        devices = [
            {
                "serial_number": f"{serial_prefix}-FLOOR",
                "display_name": f"{name} Floor Reader",
                "zone_code": "floor",
            },
            {
                "serial_number": f"{serial_prefix}-BACK",
                "display_name": f"{name} Backroom Reader",
                "zone_code": "backroom",
            },
        ]
    return {
        "code": code,
        "name": name,
        "timezone": "America/Los_Angeles",
        "organization_path": [
            {"code": "us", "name": "United States", "unit_type": "COUNTRY"},
            {"code": "west", "name": "West", "unit_type": "REGION"},
        ],
        "zones": [
            {"code": "floor", "name": "Sales Floor", "kind": "SALES_FLOOR"},
            {"code": "backroom", "name": "Backroom", "kind": "BACKROOM"},
        ],
        "devices": devices,
        "configuration": {"rfid_enabled": True},
    }


def _catalog_csv(epcs: tuple[str, ...]) -> bytes:
    header = "style_code,style_name,sku,upc,color,size,epc,style_attributes,attributes\n"
    rows = [
        f"{STYLE_CODE},Trail Shirt,{SKU_CODE},{UPC},Blue,M,{epc},"
        '"{""category"":""SHIRTS""}","{""material"":""cotton""}"'
        for epc in epcs
    ]
    return (header + "\n".join(rows) + "\n").encode()


def _stage_catalog(
    client: TestClient,
    tenant_id: str,
    *,
    key: str,
    content: bytes,
) -> dict[str, object]:
    response = client.post(
        f"/v1/tenants/{tenant_id}/catalog/imports",
        headers={**PLATFORM_HEADERS, "Idempotency-Key": key},
        data={"mode": "DELTA"},
        files={"file": ("catalog.csv", content, "text/csv")},
    )
    payload = _expect(response, 202)
    assert isinstance(payload, dict)
    return payload


def _ingest(
    client: TestClient,
    device_key: str,
    *,
    event_id: str,
    epc: str,
    observed_at: datetime,
    batch_id: str,
) -> dict[str, object]:
    payload = _expect(
        client.post(
            "/v1/device/read-batches",
            headers={"X-Device-Key": device_key},
            json={
                "batch_id": batch_id,
                "observations": [
                    {
                        "event_id": event_id,
                        "epc": epc,
                        "observed_at": observed_at.isoformat(),
                        "reader_sequence": 1,
                        "antenna_port": 1,
                        "rssi_dbm": -48.5,
                    }
                ],
            },
        ),
        202,
    )
    assert isinstance(payload, dict)
    return payload


def test_postgres_end_to_end(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
) -> None:
    client = api_client

    assert _expect(client.get("/health/live"), 200) == {"status": "ok"}
    assert _expect(client.get("/health/ready"), 200) == {"status": "ok"}
    version = _expect(client.get("/version"), 200)
    assert isinstance(version, dict)
    assert version["version"] == "0.1.0"
    assert (
        client.get(
            "/v1/platform/tenants/00000000-0000-0000-0000-000000000000/stores",
            headers={"X-Platform-Key": "invalid-platform-key"},
        ).status_code
        == 401
    )
    extra_field = client.post(
        "/v1/platform/tenants",
        headers=PLATFORM_HEADERS,
        json={"code": "extra", "name": "Rejected", "unexpected": True},
    )
    assert extra_field.status_code == 422
    assert extra_field.json()["code"] == "request_validation_failed"
    assert extra_field.json()["errors"][0]["type"] == "extra_forbidden"

    # 1. Trusted platform onboarding is tenant-scoped and idempotent.
    tenant_a = _expect(
        client.post(
            "/v1/platform/tenants",
            headers=PLATFORM_HEADERS,
            json={"code": "orange", "name": "Orange Retail"},
        ),
        201,
    )
    tenant_b = _expect(
        client.post(
            "/v1/platform/tenants",
            headers=PLATFORM_HEADERS,
            json={"code": "blue", "name": "Blue Retail"},
        ),
        201,
    )
    assert isinstance(tenant_a, dict)
    assert isinstance(tenant_b, dict)
    tenant_a_id = str(tenant_a["id"])
    tenant_b_id = str(tenant_b["id"])
    assert tenant_a_id != tenant_b_id

    repeated_tenant = _expect(
        client.post(
            "/v1/platform/tenants",
            headers=PLATFORM_HEADERS,
            json={"code": "orange", "name": "Orange Retail"},
        ),
        201,
    )
    assert isinstance(repeated_tenant, dict)
    assert repeated_tenant["id"] == tenant_a_id
    tenant_conflict = client.post(
        "/v1/platform/tenants",
        headers=PLATFORM_HEADERS,
        json={"code": "orange", "name": "Another Company"},
    )
    assert tenant_conflict.status_code == 409
    assert tenant_conflict.json()["code"] == "tenant_code_conflict"

    orange_stores = {
        "stores": [
            _store_payload("la01", "Los Angeles 01", serial_prefix="ORANGE-LA01"),
            _store_payload(
                "la02",
                "Los Angeles 02",
                serial_prefix="ORANGE-LA02",
                with_readers=False,
            ),
        ]
    }
    onboard_headers = {**PLATFORM_HEADERS, "Idempotency-Key": "orange-stores-v1"}
    onboard = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/stores:bulk-onboard",
            headers=onboard_headers,
            json=orange_stores,
        ),
        201,
    )
    assert isinstance(onboard, dict)
    assert onboard["status"] == "COMPLETED"
    assert onboard["succeeded_count"] == 2
    repeated_onboard = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/stores:bulk-onboard",
            headers=onboard_headers,
            json=orange_stores,
        ),
        201,
    )
    assert isinstance(repeated_onboard, dict)
    assert repeated_onboard["id"] == onboard["id"]
    changed_onboard = {
        "stores": [
            _store_payload("la03", "Los Angeles 03", serial_prefix="ORANGE-LA03"),
        ]
    }
    idempotency_conflict = client.post(
        f"/v1/platform/tenants/{tenant_a_id}/stores:bulk-onboard",
        headers=onboard_headers,
        json=changed_onboard,
    )
    assert idempotency_conflict.status_code == 409
    assert idempotency_conflict.json()["code"] == "idempotency_key_reused"

    blue_onboard = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_b_id}/stores:bulk-onboard",
            headers={**PLATFORM_HEADERS, "Idempotency-Key": "blue-stores-v1"},
            json={
                "stores": [
                    _store_payload(
                        "sf01",
                        "San Francisco 01",
                        serial_prefix="BLUE-SF01",
                        with_readers=False,
                    )
                ]
            },
        ),
        201,
    )
    assert isinstance(blue_onboard, dict)
    assert blue_onboard["succeeded_count"] == 1

    stores_payload = _expect(
        client.get(
            f"/v1/platform/tenants/{tenant_a_id}/stores",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(stores_payload, list)
    stores_by_code = {str(store["code"]): store for store in stores_payload}
    store_1_id = str(stores_by_code["la01"]["id"])
    store_2_id = str(stores_by_code["la02"]["id"])

    devices_payload = _expect(
        client.get(
            f"/v1/platform/tenants/{tenant_a_id}/devices",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(devices_payload, list)
    devices_by_serial = {str(device["serial_number"]): device for device in devices_payload}
    floor_device_id = str(devices_by_serial["ORANGE-LA01-FLOOR"]["id"])
    back_device_id = str(devices_by_serial["ORANGE-LA01-BACK"]["id"])

    floor_credential = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/credentials:rotate",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    back_credential = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{back_device_id}/credentials:rotate",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(floor_credential, dict)
    assert isinstance(back_credential, dict)
    floor_device_key = str(floor_credential["api_key"])
    back_device_key = str(back_credential["api_key"])
    assert floor_device_key.startswith(f"{floor_device_id}.")

    # Device credentials do not bypass a suspended tenant boundary.
    with postgres_session_factory() as db:
        suspended_tenant = db.get(Tenant, uuid.UUID(tenant_a_id))
        assert suspended_tenant is not None
        suspended_tenant.status = TenantStatus.SUSPENDED
        db.commit()
    suspended_device_ingress = client.post(
        "/v1/device/read-batches",
        headers={"X-Device-Key": floor_device_key},
        json={
            "batch_id": "suspended-tenant-probe",
            "observations": [
                {
                    "event_id": str(uuid.uuid4()),
                    "epc": KNOWN_EPCS[0],
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            ],
        },
    )
    assert suspended_device_ingress.status_code == 401
    with postgres_session_factory() as db:
        suspended_tenant = db.get(Tenant, uuid.UUID(tenant_a_id))
        assert suspended_tenant is not None
        suspended_tenant.status = TenantStatus.ACTIVE
        db.commit()

    # 4. Bootstrap, login, and role/store boundaries use real password hashes and JWTs.
    admin_a_id = _bootstrap_admin(
        postgres_session_factory,
        tenant_code="orange",
        tenant_name="Orange Retail",
        email="admin@orange.example",
    )
    _bootstrap_admin(
        postgres_session_factory,
        tenant_code="blue",
        tenant_name="Blue Retail",
        email="admin@blue.example",
    )
    assert (
        client.post(
            "/v1/auth/login",
            json={
                "tenant_code": "orange",
                "email": "admin@orange.example",
                "password": "wrong-password",
            },
        ).status_code
        == 401
    )
    admin_a_token = _login(
        client,
        "orange",
        "admin@orange.example",
        ADMIN_PASSWORD,
    )
    admin_b_token = _login(
        client,
        "blue",
        "admin@blue.example",
        ADMIN_PASSWORD,
    )
    current_admin = _expect(client.get("/v1/auth/me", headers=_bearer(admin_a_token)), 200)
    assert isinstance(current_admin, dict)
    assert current_admin["user_id"] == admin_a_id
    assert "policy:manage" in current_admin["permissions"]

    manager = _expect(
        client.post(
            "/v1/users",
            headers=_bearer(admin_a_token),
            json={
                "email": "manager.la01@orange.example",
                "display_name": "LA01 Manager",
                "password": MANAGER_PASSWORD,
                "role_assignments": [
                    {"role": "STORE_MANAGER", "store_id": store_1_id},
                ],
            },
        ),
        201,
    )
    assert isinstance(manager, dict)
    manager_token = _login(
        client,
        "orange",
        "manager.la01@orange.example",
        MANAGER_PASSWORD,
    )

    forbidden_delegation = client.post(
        "/v1/users",
        headers=_bearer(manager_token),
        json={
            "email": "other.manager@orange.example",
            "display_name": "Other Manager",
            "password": MANAGER_PASSWORD,
            "role_assignments": [
                {"role": "STORE_MANAGER", "store_id": store_1_id},
            ],
        },
    )
    assert forbidden_delegation.status_code == 403
    assert forbidden_delegation.json()["code"] == "role_assignment_forbidden"

    out_of_store_delegation = client.post(
        "/v1/users",
        headers=_bearer(manager_token),
        json={
            "email": "associate.la02@orange.example",
            "display_name": "LA02 Associate",
            "password": ASSOCIATE_PASSWORD,
            "role_assignments": [
                {"role": "STORE_ASSOCIATE", "store_id": store_2_id},
            ],
        },
    )
    assert out_of_store_delegation.status_code == 403
    assert out_of_store_delegation.json()["code"] == "role_assignment_forbidden"

    associate = _expect(
        client.post(
            "/v1/users",
            headers=_bearer(manager_token),
            json={
                "email": "associate.one@orange.example",
                "display_name": "Associate One",
                "password": ASSOCIATE_PASSWORD,
                "role_assignments": [
                    {"role": "STORE_ASSOCIATE", "store_id": store_1_id},
                ],
            },
        ),
        201,
    )
    associate_two = _expect(
        client.post(
            "/v1/users",
            headers=_bearer(manager_token),
            json={
                "email": "associate.two@orange.example",
                "display_name": "Associate Two",
                "password": SECOND_ASSOCIATE_PASSWORD,
                "role_assignments": [
                    {"role": "STORE_ASSOCIATE", "store_id": store_1_id},
                ],
            },
        ),
        201,
    )
    assert isinstance(associate, dict)
    assert isinstance(associate_two, dict)
    associate_token = _login(
        client,
        "orange",
        "associate.one@orange.example",
        ASSOCIATE_PASSWORD,
    )
    associate_two_token = _login(
        client,
        "orange",
        "associate.two@orange.example",
        SECOND_ASSOCIATE_PASSWORD,
    )
    assert client.get("/v1/users", headers=_bearer(associate_token)).status_code == 403
    visible_users = _expect(client.get("/v1/users", headers=_bearer(manager_token)), 200)
    assert isinstance(visible_users, dict)
    assert visible_users["total"] == 3
    assert {item["email"] for item in visible_users["items"]} == {
        "associate.one@orange.example",
        "associate.two@orange.example",
        "manager.la01@orange.example",
    }
    audit = _expect(
        client.get("/v1/users/audit-records", headers=_bearer(admin_a_token)),
        200,
    )
    assert isinstance(audit, dict)
    assert audit["total"] >= 6
    assert client.get("/v1/users/audit-records", headers=_bearer(manager_token)).status_code == 403

    # 2. CSV validation is atomic; a durable job promotes valid staged data.
    invalid_catalog = _stage_catalog(
        client,
        tenant_a_id,
        key="catalog-invalid-v1",
        content=(
            b"style_code,style_name,sku,upc,color,size,epc\n"
            b"ST-BAD,Bad Product,SKU-BAD,036000291453,Red,L,NOT-AN-EPC\n"
        ),
    )
    assert invalid_catalog["status"] == "REJECTED"
    assert invalid_catalog["invalid_rows"] == 1
    invalid_errors = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/imports/{invalid_catalog['id']}/errors",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(invalid_errors, dict)
    assert invalid_errors["total"] == 2

    valid_csv = _catalog_csv(KNOWN_EPCS)
    valid_catalog = _stage_catalog(
        client,
        tenant_a_id,
        key="catalog-orange-v1",
        content=valid_csv,
    )
    assert valid_catalog["status"] == "READY"
    repeated_catalog = _stage_catalog(
        client,
        tenant_a_id,
        key="catalog-orange-v1",
        content=valid_csv,
    )
    assert repeated_catalog["id"] == valid_catalog["id"]
    assert _drain_jobs(postgres_session_factory) == 1

    promoted_catalog = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/imports/{valid_catalog['id']}",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(promoted_catalog, dict)
    assert promoted_catalog["status"] == "COMPLETED"
    assert promoted_catalog["inserted_count"] == 6  # style + SKU + four EPC bindings
    catalog_imports = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/imports",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(catalog_imports, dict)
    assert catalog_imports["total"] == 2

    skus = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/skus",
            headers=_bearer(admin_a_token),
        ),
        200,
    )
    assert isinstance(skus, dict)
    assert skus["total"] == 1
    sku = skus["items"][0]
    sku_id = str(sku["id"])
    assert sku["code"] == SKU_CODE
    single_sku = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/skus/{sku_id}",
            headers=_bearer(admin_a_token),
        ),
        200,
    )
    assert isinstance(single_sku, dict)
    assert single_sku["id"] == sku_id
    assert (
        client.get(
            f"/v1/tenants/{tenant_a_id}/catalog/skus",
            headers=_bearer(admin_b_token),
        ).status_code
        == 403
    )

    with postgres_session_factory() as db:
        first_binding_time = db.scalar(
            select(func.min(EpcBinding.effective_from)).where(
                EpcBinding.tenant_id == uuid.UUID(tenant_a_id)
            )
        )
        zones = list(db.scalars(select(Zone).where(Zone.store_id == uuid.UUID(store_1_id))).all())
        zones_by_kind = {zone.kind: zone for zone in zones}
        floor_zone_id = zones_by_kind[ZoneKind.SALES_FLOOR].id
        backroom_zone_id = zones_by_kind[ZoneKind.BACKROOM].id
    assert first_binding_time is not None

    # 3. RFID ingress deduplicates at the edge, quarantines bad time/mappings,
    # applies event-time ordering, and uses confirmation reads for movement.
    base_time = datetime.now(UTC)
    initial_events: list[tuple[str, str, str]] = []
    for index, epc in enumerate(KNOWN_EPCS):
        event_id = str(uuid.uuid4())
        device_key = back_device_key if index < 3 else floor_device_key
        receipt = _ingest(
            client,
            device_key,
            event_id=event_id,
            epc=epc,
            observed_at=base_time + timedelta(seconds=index),
            batch_id=f"initial-{index}",
        )
        assert receipt["accepted_count"] == 1
        assert receipt["results"][0]["disposition"] == "ACCEPTED"
        initial_events.append((event_id, epc, device_key))

    duplicate_event_id, duplicate_epc, duplicate_device_key = initial_events[1]
    duplicate = _ingest(
        client,
        duplicate_device_key,
        event_id=duplicate_event_id,
        epc=duplicate_epc,
        observed_at=base_time + timedelta(seconds=1),
        batch_id="duplicate-retry",
    )
    assert duplicate["duplicate_count"] == 1
    conflict = _ingest(
        client,
        duplicate_device_key,
        event_id=duplicate_event_id,
        epc=KNOWN_EPCS[2],
        observed_at=base_time + timedelta(seconds=1),
        batch_id="conflicting-retry",
    )
    assert conflict["conflict_count"] == 1

    future_event_id = str(uuid.uuid4())
    future = _ingest(
        client,
        back_device_key,
        event_id=future_event_id,
        epc=KNOWN_EPCS[0],
        observed_at=datetime.now(UTC) + timedelta(hours=1),
        batch_id="future-clock",
    )
    assert future["accepted_count"] == 1
    assert "OBSERVED_AT_TOO_FAR_IN_FUTURE" in str(future["results"][0]["detail"])

    unknown_event_id = str(uuid.uuid4())
    unknown_observed_at = first_binding_time + timedelta(microseconds=1)
    unknown = _ingest(
        client,
        back_device_key,
        event_id=unknown_event_id,
        epc=RECOVERED_EPC,
        observed_at=unknown_observed_at,
        batch_id="unknown-epc",
    )
    unknown_observation_id = str(unknown["results"][0]["observation_id"])

    jobs_processed = _drain_jobs(postgres_session_factory)
    assert jobs_processed >= 9  # five RFID jobs plus their replenishment recalculations
    with postgres_session_factory() as db:
        future_row = db.scalar(
            select(RfidObservation).where(RfidObservation.event_id == future_event_id)
        )
        unknown_row = db.scalar(
            select(RfidObservation).where(RfidObservation.event_id == unknown_event_id)
        )
        assert future_row is not None
        assert future_row.status is ObservationStatus.QUARANTINED
        assert future_row.quarantine_reason == "OBSERVED_AT_TOO_FAR_IN_FUTURE"
        assert unknown_row is not None
        assert unknown_row.status is ObservationStatus.QUARANTINED
        assert unknown_row.quarantine_reason == "UNKNOWN_EPC"
    quarantine_page = _expect(
        client.get(
            f"/v1/platform/tenants/{tenant_a_id}/rfid/observations",
            headers=PLATFORM_HEADERS,
            params={"status": "QUARANTINED"},
        ),
        200,
    )
    assert isinstance(quarantine_page, dict)
    assert quarantine_page["total"] == 2

    # A single cross-zone read is noisy evidence; the second confirms the move.
    moving_epc = KNOWN_EPCS[0]
    first_move_event = str(uuid.uuid4())
    _ingest(
        client,
        floor_device_key,
        event_id=first_move_event,
        epc=moving_epc,
        observed_at=base_time + timedelta(seconds=30),
        batch_id="move-candidate",
    )
    _drain_jobs(postgres_session_factory)
    with postgres_session_factory() as db:
        candidate_state = db.scalar(
            select(InventoryItemState).where(InventoryItemState.epc == moving_epc)
        )
        assert candidate_state is not None
        assert candidate_state.zone_id == backroom_zone_id
        assert candidate_state.candidate_zone_id == floor_zone_id
        assert candidate_state.candidate_count == 1

    second_move_event = str(uuid.uuid4())
    _ingest(
        client,
        floor_device_key,
        event_id=second_move_event,
        epc=moving_epc,
        observed_at=base_time + timedelta(seconds=31),
        batch_id="move-confirmed",
    )
    _drain_jobs(postgres_session_factory)
    with postgres_session_factory() as db:
        moved_state = db.scalar(
            select(InventoryItemState).where(InventoryItemState.epc == moving_epc)
        )
        assert moved_state is not None
        assert moved_state.zone_id == floor_zone_id
        assert moved_state.candidate_zone_id is None
        assert moved_state.candidate_count == 0

    late_event_id = str(uuid.uuid4())
    _ingest(
        client,
        back_device_key,
        event_id=late_event_id,
        epc=moving_epc,
        observed_at=base_time + timedelta(seconds=25),
        batch_id="late-event",
    )
    _drain_jobs(postgres_session_factory)
    with postgres_session_factory() as db:
        late_row = db.scalar(
            select(RfidObservation).where(RfidObservation.event_id == late_event_id)
        )
        state_after_late = db.scalar(
            select(InventoryItemState).where(InventoryItemState.epc == moving_epc)
        )
        assert late_row is not None
        assert late_row.status is ObservationStatus.LATE_IGNORED
        assert state_after_late is not None
        assert state_after_late.zone_id == floor_zone_id

    # Add the previously unknown EPC, then replay the immutable quarantined event.
    recovered_import = _stage_catalog(
        client,
        tenant_a_id,
        key="catalog-recovery-v1",
        content=_catalog_csv((RECOVERED_EPC,)),
    )
    assert recovered_import["status"] == "READY"
    _drain_jobs(postgres_session_factory)
    replayed = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/rfid/observations/{unknown_observation_id}:replay",
            headers=PLATFORM_HEADERS,
        ),
        202,
    )
    assert isinstance(replayed, dict)
    assert replayed["status"] == "RECEIVED"
    _drain_jobs(postgres_session_factory)
    with postgres_session_factory() as db:
        recovered_row = db.get(RfidObservation, uuid.UUID(unknown_observation_id))
        assert recovered_row is not None
        assert recovered_row.status is ObservationStatus.PROCESSED
        assert recovered_row.resolution_strategy == "REPLAY_CURRENT"

    inventory = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/inventory",
            headers=_bearer(admin_a_token),
            params={"store_id": store_1_id},
        ),
        200,
    )
    assert isinstance(inventory, dict)
    assert inventory["total"] == 2
    quantities = {str(item["zone_kind"]): int(item["quantity"]) for item in inventory["items"]}
    assert quantities == {"BACKROOM": 3, "SALES_FLOOR": 2}
    first_inventory_page = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/inventory",
            headers=_bearer(admin_a_token),
            params={"store_id": store_1_id, "limit": 1, "offset": 0},
        ),
        200,
    )
    assert isinstance(first_inventory_page, dict)
    assert first_inventory_page["total"] == 2
    assert len(first_inventory_page["items"]) == 1

    assert (
        client.get(
            f"/v1/tenants/{tenant_a_id}/inventory",
            headers=_bearer(manager_token),
        ).status_code
        == 400
    )
    assert (
        client.get(
            f"/v1/tenants/{tenant_a_id}/inventory",
            headers=_bearer(manager_token),
            params={"store_id": store_2_id},
        ).status_code
        == 403
    )
    assert (
        client.get(
            f"/v1/tenants/{tenant_a_id}/inventory",
            headers=_bearer(admin_b_token),
            params={"store_id": store_1_id},
        ).status_code
        == 403
    )

    # 5. Policy imports are atomic. Store scope outranks tenant scope, and the
    # evaluation explains the calculation while maintaining one active task.
    effective_from = (datetime.now(UTC) - timedelta(days=1)).isoformat()
    policies_request = {
        "policies": [
            {
                "external_key": "tenant-sku-default",
                "store_id": None,
                "selector_type": "SKU",
                "selector_value": SKU_CODE,
                "minimum_floor_quantity": 3,
                "target_floor_quantity": 5,
                "maximum_floor_quantity": 8,
                "priority": 100,
                "effective_from": effective_from,
                "active": True,
            },
            {
                "external_key": "la01-size-override",
                "store_id": store_1_id,
                "selector_type": "SIZE",
                "selector_value": "M",
                "minimum_floor_quantity": 3,
                "target_floor_quantity": 6,
                "maximum_floor_quantity": 8,
                "priority": -100,
                "effective_from": effective_from,
                "active": True,
            },
        ]
    }
    policy_headers = {**_bearer(admin_a_token), "Idempotency-Key": "policy-orange-v1"}
    policy_import = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies:bulk-upsert",
            headers=policy_headers,
            json=policies_request,
        ),
        201,
    )
    assert isinstance(policy_import, dict)
    assert policy_import["status"] == "COMPLETED"
    assert policy_import["created_count"] == 2
    fetched_policy_import = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/policy-imports/{policy_import['id']}",
            headers=_bearer(admin_a_token),
        ),
        200,
    )
    assert isinstance(fetched_policy_import, dict)
    assert fetched_policy_import["request_hash"] == policy_import["request_hash"]
    repeated_policy_import = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies:bulk-upsert",
            headers=policy_headers,
            json=policies_request,
        ),
        201,
    )
    assert isinstance(repeated_policy_import, dict)
    assert repeated_policy_import["id"] == policy_import["id"]

    rejected_policy_import = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies:bulk-upsert",
            headers={
                **_bearer(admin_a_token),
                "Idempotency-Key": "policy-invalid-v1",
            },
            json={
                "policies": [
                    {
                        "external_key": "missing-sku",
                        "selector_type": "SKU",
                        "selector_value": "DOES-NOT-EXIST",
                        "minimum_floor_quantity": 1,
                        "target_floor_quantity": 2,
                        "effective_from": effective_from,
                    }
                ]
            },
        ),
        201,
    )
    assert isinstance(rejected_policy_import, dict)
    assert rejected_policy_import["status"] == "REJECTED"
    assert rejected_policy_import["rejected_count"] == 1

    policies = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies",
            headers=_bearer(admin_a_token),
        ),
        200,
    )
    assert isinstance(policies, dict)
    assert policies["total"] == 2
    size_policy = next(
        policy for policy in policies["items"] if policy["external_key"] == "la01-size-override"
    )
    assert (
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies",
            headers=_bearer(associate_token),
            json={
                "external_key": "associate-forbidden",
                "selector_type": "SKU",
                "selector_value": SKU_CODE,
                "minimum_floor_quantity": 1,
                "target_floor_quantity": 2,
                "effective_from": effective_from,
            },
        ).status_code
        == 403
    )

    evaluation_request = {
        "store_id": store_1_id,
        "sku_ids": [sku_id],
        "generate_tasks": True,
    }
    evaluation_headers = {
        **_bearer(manager_token),
        "Idempotency-Key": "evaluation-la01-v1",
    }
    evaluation = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers=evaluation_headers,
            json=evaluation_request,
        ),
        201,
    )
    assert isinstance(evaluation, dict)
    assert evaluation["line_count"] == 1
    assert evaluation["tasks_created"] == 1
    line = evaluation["lines"][0]
    assert line["policy_id"] == size_policy["id"]
    assert line["selector_type"] == "SIZE"
    assert line["floor_quantity"] == 2
    assert line["backroom_quantity"] == 3
    assert line["recommended_quantity"] == 3
    assert line["reason"] == "REPLENISHMENT_REQUIRED"
    assert "target - floor" in line["formula"]
    fetched_evaluation = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations/{evaluation['id']}",
            headers=_bearer(manager_token),
        ),
        200,
    )
    assert isinstance(fetched_evaluation, dict)
    assert fetched_evaluation["id"] == evaluation["id"]

    repeated_evaluation = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers=evaluation_headers,
            json=evaluation_request,
        ),
        201,
    )
    assert isinstance(repeated_evaluation, dict)
    assert repeated_evaluation["id"] == evaluation["id"]
    assert repeated_evaluation["tasks_created"] == 1

    second_evaluation = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers={
                **_bearer(manager_token),
                "Idempotency-Key": "evaluation-la01-v2",
            },
            json=evaluation_request,
        ),
        201,
    )
    assert isinstance(second_evaluation, dict)
    assert second_evaluation["tasks_created"] == 0
    assert second_evaluation["tasks_updated"] == 0
    assert second_evaluation["lines"][0]["open_task_quantity"] == 3
    assert second_evaluation["lines"][0]["recommended_quantity"] == 0

    assert (
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers={
                **_bearer(manager_token),
                "Idempotency-Key": "evaluation-cross-store",
            },
            json={**evaluation_request, "store_id": store_2_id},
        ).status_code
        == 403
    )
    assert (
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers={
                **_bearer(admin_b_token),
                "Idempotency-Key": "evaluation-cross-tenant",
            },
            json=evaluation_request,
        ).status_code
        == 403
    )

    tasks = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks",
            headers=_bearer(manager_token),
            params={"store_id": store_1_id},
        ),
        200,
    )
    assert isinstance(tasks, dict)
    assert tasks["total"] == 1
    task = tasks["items"][0]
    task_id = str(task["id"])
    assert task["quantity"] == 3
    assert task["version"] == 1
    assert (
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks",
            headers=_bearer(manager_token),
        ).status_code
        == 400
    )

    mixed_role_user = _expect(
        client.post(
            "/v1/users",
            headers=_bearer(admin_a_token),
            json={
                "email": "mixed.scope@orange.example",
                "display_name": "Mixed Scope User",
                "password": MIXED_ROLE_PASSWORD,
                "role_assignments": [
                    {"role": "STORE_MANAGER", "store_id": store_2_id},
                    {"role": "STORE_ASSOCIATE", "store_id": store_1_id},
                ],
            },
        ),
        201,
    )
    assert isinstance(mixed_role_user, dict)
    mixed_role_token = _login(
        client,
        "orange",
        "mixed.scope@orange.example",
        MIXED_ROLE_PASSWORD,
    )
    cross_scope_cancel = client.patch(
        f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
        headers=_bearer(mixed_role_token),
        json={"status": "CANCELLED", "expected_version": 1},
    )
    assert cross_scope_cancel.status_code == 403
    assert cross_scope_cancel.json()["code"] == "task_management_permission_required"

    associate_cancel = client.patch(
        f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
        headers=_bearer(associate_token),
        json={"status": "CANCELLED", "expected_version": 1},
    )
    assert associate_cancel.status_code == 403
    assert associate_cancel.json()["code"] == "task_management_permission_required"

    claimed_task = _expect(
        client.patch(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
            headers=_bearer(associate_token),
            json={"status": "CLAIMED", "expected_version": 1},
        ),
        200,
    )
    assert isinstance(claimed_task, dict)
    assert claimed_task["version"] == 2
    assert claimed_task["claimed_by_subject"] == associate["id"]

    owner_conflict = client.patch(
        f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
        headers=_bearer(associate_two_token),
        json={"status": "IN_PROGRESS", "expected_version": 2},
    )
    assert owner_conflict.status_code == 409
    assert owner_conflict.json()["code"] == "task_claim_owner_conflict"
    stale_update = client.patch(
        f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
        headers=_bearer(associate_token),
        json={"status": "IN_PROGRESS", "expected_version": 1},
    )
    assert stale_update.status_code == 409
    assert stale_update.json()["code"] == "task_version_conflict"

    in_progress = _expect(
        client.patch(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
            headers=_bearer(associate_token),
            json={
                "status": "IN_PROGRESS",
                "expected_version": 2,
                "moved_quantity": 1,
            },
        ),
        200,
    )
    assert isinstance(in_progress, dict)
    awaiting = _expect(
        client.patch(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
            headers=_bearer(associate_token),
            json={
                "status": "AWAITING_VERIFICATION",
                "expected_version": in_progress["version"],
                "moved_quantity": 2,
            },
        ),
        200,
    )
    assert isinstance(awaiting, dict)
    incomplete_verification = client.patch(
        f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
        headers=_bearer(associate_token),
        json={
            "status": "VERIFIED",
            "expected_version": awaiting["version"],
            "moved_quantity": 2,
        },
    )
    assert incomplete_verification.status_code == 422
    assert incomplete_verification.json()["code"] == "task_verification_incomplete"
    verified = _expect(
        client.patch(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks/{task_id}",
            headers=_bearer(associate_token),
            json={
                "status": "VERIFIED",
                "expected_version": awaiting["version"],
                "moved_quantity": 3,
                "note": "Count verified at the sales floor.",
            },
        ),
        200,
    )
    assert isinstance(verified, dict)
    assert verified["status"] == "VERIFIED"
    assert verified["remaining_quantity"] == 0
    assert verified["completed_at"] is not None

    # Human-verified movement remains reserved until a later RFID balance transition,
    # so an immediate reevaluation cannot issue the same physical work again.
    post_verification_run = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/evaluations",
            headers={
                **_bearer(manager_token),
                "Idempotency-Key": "evaluation-after-verification",
            },
            json=evaluation_request,
        ),
        201,
    )
    assert isinstance(post_verification_run, dict)
    post_verification_line = post_verification_run["lines"][0]
    assert post_verification_line["open_task_quantity"] == 3
    assert post_verification_line["recommended_quantity"] == 0
    assert post_verification_line["reason"] == "NO_BACKROOM_STOCK"
    tasks_after_verification = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/tasks",
            headers=_bearer(manager_token),
            params={"store_id": store_1_id},
        ),
        200,
    )
    assert isinstance(tasks_after_verification, dict)
    assert tasks_after_verification["total"] == 1

    # CRUD endpoints remain tenant scoped after the evaluation snapshot has been created.
    created_policy = _expect(
        client.post(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies",
            headers=_bearer(admin_a_token),
            json={
                "external_key": "la01-style-secondary",
                "store_id": store_1_id,
                "selector_type": "STYLE",
                "selector_value": STYLE_CODE,
                "minimum_floor_quantity": 1,
                "target_floor_quantity": 2,
                "effective_from": effective_from,
            },
        ),
        201,
    )
    assert isinstance(created_policy, dict)
    fetched_policy = _expect(
        client.get(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies/{created_policy['id']}",
            headers=_bearer(admin_a_token),
        ),
        200,
    )
    assert isinstance(fetched_policy, dict)
    assert fetched_policy["revision"] == 1
    updated_policy = _expect(
        client.patch(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies/{created_policy['id']}",
            headers=_bearer(admin_a_token),
            json={"priority": 7},
        ),
        200,
    )
    assert isinstance(updated_policy, dict)
    assert updated_policy["priority"] == 7
    assert updated_policy["revision"] == 2
    _expect(
        client.delete(
            f"/v1/tenants/{tenant_a_id}/replenishment/policies/{created_policy['id']}",
            headers=_bearer(admin_a_token),
        ),
        204,
    )

    # Managers can inspect and suspend only associates in their own store.
    fetched_associate = _expect(
        client.get(
            f"/v1/users/{associate_two['id']}",
            headers=_bearer(manager_token),
        ),
        200,
    )
    assert isinstance(fetched_associate, dict)
    assert fetched_associate["email"] == "associate.two@orange.example"
    self_suspend = client.post(
        f"/v1/users/{manager['id']}:suspend",
        headers=_bearer(manager_token),
    )
    assert self_suspend.status_code == 409
    assert self_suspend.json()["code"] == "self_suspension_forbidden"
    suspended_associate = _expect(
        client.post(
            f"/v1/users/{associate_two['id']}:suspend",
            headers=_bearer(manager_token),
        ),
        200,
    )
    assert isinstance(suspended_associate, dict)
    assert suspended_associate["status"] == "SUSPENDED"
    assert client.get("/v1/auth/me", headers=_bearer(associate_two_token)).status_code == 401

    # Effective-dated device history is queryable and rejects cross-tenant locations.
    assignment_history = _expect(
        client.get(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/assignments",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(assignment_history, list)
    assert len(assignment_history) == 1
    original_effective_from = datetime.fromisoformat(str(assignment_history[0]["effective_from"]))
    backfilled_effective_from = original_effective_from - timedelta(minutes=1)
    unchanged_assignment = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/assignments",
            headers=PLATFORM_HEADERS,
            json={
                "store_id": store_1_id,
                "zone_id": str(floor_zone_id),
                "effective_from": backfilled_effective_from.isoformat(),
            },
        ),
        201,
    )
    assert isinstance(unchanged_assignment, dict)
    assert unchanged_assignment["id"] == assignment_history[0]["id"]
    assert datetime.fromisoformat(str(unchanged_assignment["effective_from"])) == (
        backfilled_effective_from
    )
    reassignment_time = datetime.now(UTC) + timedelta(minutes=1)
    reassignment = _expect(
        client.post(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/assignments",
            headers=PLATFORM_HEADERS,
            json={
                "store_id": store_1_id,
                "zone_id": str(backroom_zone_id),
                "effective_from": reassignment_time.isoformat(),
            },
        ),
        201,
    )
    assert isinstance(reassignment, dict)
    assert reassignment["zone_id"] == str(backroom_zone_id)
    assignment_overlap = client.post(
        f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/assignments",
        headers=PLATFORM_HEADERS,
        json={
            "store_id": store_1_id,
            "zone_id": str(floor_zone_id),
            "effective_from": (reassignment_time - timedelta(seconds=1)).isoformat(),
        },
    )
    assert assignment_overlap.status_code == 409
    assert assignment_overlap.json()["code"] == "device_assignment_interval_conflict"
    updated_assignment_history = _expect(
        client.get(
            f"/v1/platform/tenants/{tenant_a_id}/devices/{floor_device_id}/assignments",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(updated_assignment_history, list)
    assert len(updated_assignment_history) == 2
    invalid_assignment = client.post(
        f"/v1/platform/tenants/{tenant_b_id}/devices/{floor_device_id}/assignments",
        headers=PLATFORM_HEADERS,
        json={
            "store_id": store_1_id,
            "zone_id": str(backroom_zone_id),
            "effective_from": datetime.now(UTC).isoformat(),
        },
    )
    assert invalid_assignment.status_code == 404

    with postgres_session_factory() as db:
        quarantined_jobs = db.scalar(
            select(func.count(DurableJob.id)).where(DurableJob.status == JobStatus.QUARANTINED)
        )
        active_task_count = db.scalar(
            select(func.count())
            .select_from(DurableJob)
            .where(DurableJob.status.in_([JobStatus.PENDING, JobStatus.PROCESSING]))
        )
        canonical_sku_count = db.scalar(select(func.count(Sku.id)))
        catalog_import_count = db.scalar(select(func.count(CatalogImport.id)))
        assignment_count = db.scalar(select(func.count(DeviceAssignment.id)))
        balance_count = db.scalar(select(func.count(InventoryBalance.id)))
    assert quarantined_jobs == 0
    assert active_task_count == 0
    assert canonical_sku_count == 1
    assert catalog_import_count == 3
    assert assignment_count == 3
    assert balance_count == 2


def test_assignment_sized_onboarding_and_expired_job_recovery(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant = _expect(
        api_client.post(
            "/v1/platform/tenants",
            headers=PLATFORM_HEADERS,
            json={"code": "scale-rehearsal", "name": "Scale Rehearsal"},
        ),
        201,
    )
    assert isinstance(tenant, dict)
    tenant_id = str(tenant["id"])
    batch = _expect(
        api_client.post(
            f"/v1/platform/tenants/{tenant_id}/stores:bulk-onboard",
            headers={**PLATFORM_HEADERS, "Idempotency-Key": "scale-100-stores-v1"},
            json=build_store_batch(100),
        ),
        201,
    )
    assert isinstance(batch, dict)
    assert batch["status"] == "COMPLETED"
    assert batch["total_count"] == 100
    assert batch["succeeded_count"] == 100
    stores = _expect(
        api_client.get(
            f"/v1/platform/tenants/{tenant_id}/stores",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    devices = _expect(
        api_client.get(
            f"/v1/platform/tenants/{tenant_id}/devices",
            headers=PLATFORM_HEADERS,
        ),
        200,
    )
    assert isinstance(stores, list)
    assert isinstance(devices, list)
    assert len(stores) == 100
    assert len(devices) == 200

    now = datetime.now(UTC)
    with postgres_session_factory() as db:
        job = DurableJob(
            tenant_id=uuid.UUID(tenant_id),
            kind=JobKind.REPLENISHMENT_RECALC,
            payload={"recovery_probe": True},
            status=JobStatus.PENDING,
            attempts=0,
            available_at=now,
        )
        db.add(job)
        db.commit()
        job_id = job.id

    with postgres_session_factory() as db:
        first_claim = claim_jobs(db, worker_id="worker-before-restart", limit=1, lease_seconds=30)
    assert [job.id for job in first_claim] == [job_id]
    assert first_claim[0].attempts == 1

    with postgres_session_factory() as db:
        expired = db.get(DurableJob, job_id)
        assert expired is not None
        expired.lease_expires_at = now - timedelta(seconds=1)
        db.commit()

    with postgres_session_factory() as db:
        reclaimed = claim_jobs(db, worker_id="worker-after-restart", limit=1, lease_seconds=30)
    assert [job.id for job in reclaimed] == [job_id]
    assert reclaimed[0].attempts == 2
    assert reclaimed[0].locked_by == "worker-after-restart"
    with postgres_session_factory() as db:
        assert mark_completed(db, job_id, "worker-after-restart")
