import uuid
from collections.abc import Iterator
from dataclasses import dataclass

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from abacus.models.identity import IdentityRole
from abacus.models.tenancy import Tenant
from abacus.schemas.identity import RoleAssignmentCreate, UserCreate
from abacus.schemas.tenancy import TenantCreate
from abacus.services.identity import bootstrap_corporate_admin

pytestmark = pytest.mark.integration


@dataclass(frozen=True, slots=True)
class LoginFixture:
    tenant_id: uuid.UUID
    tenant_code: str
    email: str
    password: str


@pytest.fixture
def login_fixture(
    postgres_session_factory: sessionmaker[Session],
) -> Iterator[LoginFixture]:
    suffix = uuid.uuid4().hex[:12]
    tenant_code = f"auth-{suffix}"
    email = f"admin-{suffix}@orange.example"
    password = f"Auth-Session-{suffix}-Password!"
    with postgres_session_factory() as db:
        record = bootstrap_corporate_admin(
            db,
            TenantCreate(code=tenant_code, name=f"Auth Session {suffix}"),
            UserCreate(
                email=email,
                display_name="Auth Session Administrator",
                password=password,
                role_assignments=[RoleAssignmentCreate(role=IdentityRole.CORPORATE_ADMIN)],
            ),
        )
        tenant_id = record.user.tenant_id

    try:
        yield LoginFixture(
            tenant_id=tenant_id,
            tenant_code=tenant_code,
            email=email,
            password=password,
        )
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


def _login(client: TestClient, fixture: LoginFixture) -> dict[str, object]:
    response = client.post(
        "/v1/auth/login",
        json={
            "tenant_code": fixture.tenant_code,
            "email": fixture.email,
            "password": fixture.password,
        },
    )
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["refresh_token"]
    assert payload["refresh_expires_in"] > payload["expires_in"]
    return payload


def _bearer(token: object) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def test_refresh_rotation_detects_reuse_and_revokes_the_family(
    api_client: TestClient,
    login_fixture: LoginFixture,
) -> None:
    original = _login(api_client, login_fixture)
    refreshed = api_client.post(
        "/v1/auth/refresh",
        json={
            "tenant_code": login_fixture.tenant_code,
            "refresh_token": original["refresh_token"],
        },
    )
    assert refreshed.status_code == 200, refreshed.text
    replacement = refreshed.json()
    assert replacement["refresh_token"] != original["refresh_token"]
    assert api_client.get("/v1/me", headers=_bearer(original["access_token"])).status_code == 401
    assert api_client.get("/v1/me", headers=_bearer(replacement["access_token"])).status_code == 200

    reused = api_client.post(
        "/v1/auth/refresh",
        json={
            "tenant_code": login_fixture.tenant_code,
            "refresh_token": original["refresh_token"],
        },
    )
    assert reused.status_code == 401
    assert reused.json()["code"] == "refresh_token_reused"
    assert api_client.get("/v1/me", headers=_bearer(replacement["access_token"])).status_code == 401


def test_logout_revokes_current_access_and_refresh_tokens(
    api_client: TestClient,
    login_fixture: LoginFixture,
) -> None:
    tokens = _login(api_client, login_fixture)
    headers = _bearer(tokens["access_token"])
    assert api_client.get("/v1/me", headers=headers).status_code == 200
    assert api_client.post("/v1/auth/logout", headers=headers).status_code == 204
    assert api_client.get("/v1/me", headers=headers).status_code == 401

    refresh = api_client.post(
        "/v1/auth/refresh",
        json={
            "tenant_code": login_fixture.tenant_code,
            "refresh_token": tokens["refresh_token"],
        },
    )
    assert refresh.status_code == 401
    assert refresh.json()["code"] == "invalid_refresh_token"
