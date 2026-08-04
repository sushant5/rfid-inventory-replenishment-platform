from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session, sessionmaker

from abacus.config import get_settings
from abacus.enums import StoreStatus, TenantStatus
from abacus.main import create_app
from abacus.models.architecture import (
    CanonicalIdentityRole,
    UserRole,
    UserStoreAssignment,
)
from abacus.models.identity import User, UserStatus
from abacus.models.tenancy import Store, Tenant
from abacus.security import create_access_token


@dataclass(frozen=True, slots=True)
class StoreDiscoveryData:
    corporate_token: str
    associate_token: str
    unassigned_token: str
    tenant_store_ids: tuple[uuid.UUID, ...]
    assigned_store_id: uuid.UUID
    other_tenant_store_id: uuid.UUID


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


@pytest.fixture(scope="module")
def store_discovery_data(
    postgres_session_factory: sessionmaker[Session],
) -> StoreDiscoveryData:
    suffix = uuid.uuid4().hex[:10]
    tenant_id = uuid.uuid4()
    other_tenant_id = uuid.uuid4()
    first_store_id = uuid.uuid4()
    second_store_id = uuid.uuid4()
    other_store_id = uuid.uuid4()
    corporate_user = User(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        email=f"corporate-{suffix}@orange.example",
        display_name="Corporate Reader",
        password_hash="not-used-by-this-test",
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    associate_user = User(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        email=f"associate-{suffix}@orange.example",
        display_name="Store Associate",
        password_hash="not-used-by-this-test",
        status=UserStatus.ACTIVE,
        token_version=1,
    )
    unassigned_user = User(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        email=f"unassigned-{suffix}@orange.example",
        display_name="Unassigned Associate",
        password_hash="not-used-by-this-test",
        status=UserStatus.ACTIVE,
        token_version=1,
    )

    with postgres_session_factory() as db:
        db.add_all(
            [
                Tenant(
                    id=tenant_id,
                    code=f"orange-{suffix}",
                    name="Orange Store Discovery",
                    status=TenantStatus.ACTIVE,
                ),
                Tenant(
                    id=other_tenant_id,
                    code=f"blue-{suffix}",
                    name="Blue Store Discovery",
                    status=TenantStatus.ACTIVE,
                ),
            ]
        )
        db.flush()
        db.add_all(
            [
                Store(
                    id=first_store_id,
                    tenant_id=tenant_id,
                    code=f"a-{suffix}",
                    name="Orange First",
                    timezone="America/Los_Angeles",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                Store(
                    id=second_store_id,
                    tenant_id=tenant_id,
                    code=f"b-{suffix}",
                    name="Orange Second",
                    timezone="America/New_York",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                Store(
                    id=other_store_id,
                    tenant_id=other_tenant_id,
                    code=f"other-{suffix}",
                    name="Blue Store",
                    timezone="America/Chicago",
                    status=StoreStatus.ACTIVE,
                    configuration={},
                ),
                corporate_user,
                associate_user,
                unassigned_user,
            ]
        )
        db.flush()
        db.add_all(
            [
                UserRole(
                    tenant_id=tenant_id,
                    user_id=corporate_user.id,
                    role=CanonicalIdentityRole.CORPORATE_USER,
                ),
                UserRole(
                    tenant_id=tenant_id,
                    user_id=associate_user.id,
                    role=CanonicalIdentityRole.STORE_ASSOCIATE,
                ),
                UserRole(
                    tenant_id=tenant_id,
                    user_id=unassigned_user.id,
                    role=CanonicalIdentityRole.STORE_ASSOCIATE,
                ),
                UserStoreAssignment(
                    tenant_id=tenant_id,
                    user_id=associate_user.id,
                    store_id=second_store_id,
                ),
            ]
        )
        db.commit()

    return StoreDiscoveryData(
        corporate_token=_token(corporate_user),
        associate_token=_token(associate_user),
        unassigned_token=_token(unassigned_user),
        tenant_store_ids=(first_store_id, second_store_id),
        assigned_store_id=second_store_id,
        other_tenant_store_id=other_store_id,
    )


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.integration
def test_corporate_user_discovers_only_paginated_tenant_stores(
    api_client: TestClient,
    store_discovery_data: StoreDiscoveryData,
) -> None:
    first_page = api_client.get(
        "/v1/stores",
        headers=_bearer(store_discovery_data.corporate_token),
        params={"limit": 1, "offset": 0},
    )
    second_page = api_client.get(
        "/v1/stores",
        headers=_bearer(store_discovery_data.corporate_token),
        params={"limit": 1, "offset": 1},
    )

    assert first_page.status_code == 200, first_page.text
    assert second_page.status_code == 200, second_page.text
    first_payload = first_page.json()
    second_payload = second_page.json()
    assert first_payload["total"] == 2
    assert second_payload["total"] == 2
    assert first_payload["limit"] == second_payload["limit"] == 1
    assert first_payload["offset"] == 0
    assert second_payload["offset"] == 1
    returned_store_ids = {
        uuid.UUID(first_payload["items"][0]["id"]),
        uuid.UUID(second_payload["items"][0]["id"]),
    }
    assert returned_store_ids == set(store_discovery_data.tenant_store_ids)
    assert store_discovery_data.other_tenant_store_id not in returned_store_ids


@pytest.mark.integration
def test_store_associate_discovers_only_assigned_store(
    api_client: TestClient,
    store_discovery_data: StoreDiscoveryData,
) -> None:
    response = api_client.get(
        "/v1/stores",
        headers=_bearer(store_discovery_data.associate_token),
    )

    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["total"] == 1
    assert payload["limit"] == 50
    assert payload["offset"] == 0
    assert [uuid.UUID(item["id"]) for item in payload["items"]] == [
        store_discovery_data.assigned_store_id
    ]


@pytest.mark.integration
def test_store_role_without_assignments_fails_closed(
    api_client: TestClient,
    store_discovery_data: StoreDiscoveryData,
) -> None:
    response = api_client.get(
        "/v1/stores",
        headers=_bearer(store_discovery_data.unassigned_token),
    )

    assert response.status_code == 403
    assert response.json()["code"] == "permission_denied"


def test_store_discovery_openapi_declares_authentication_and_permission() -> None:
    operation = create_app().openapi()["paths"]["/v1/stores"]["get"]

    assert operation["operationId"] == "listStores"
    assert operation["security"] == [{"HTTPBearer": []}]
    assert "401" in operation["responses"]
    assert "`inventory:read`" in operation["responses"]["403"]["description"]
    assert "500" in operation["responses"]
