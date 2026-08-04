from __future__ import annotations

import json
import uuid

from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from abacus.enums import StoreStatus, TenantStatus
from abacus.models.architecture import (
    CanonicalIdentityRole,
    UserRole,
    UserStoreAssignment,
)
from abacus.models.identity import (
    IdentityAuditRecord,
    IdentityRole,
    UserAccessGrant,
    UserStatus,
)
from abacus.models.tenancy import Store, Tenant
from abacus.schemas.identity import CanonicalUserCreate
from abacus.security import verify_password_and_update
from abacus.services.identity import bootstrap_public_reviewer


def _request(*, email: str, password: str) -> CanonicalUserCreate:
    return CanonicalUserCreate(
        email=email,
        display_name="Public API Reviewer",
        password=password,
        roles=[CanonicalIdentityRole.CORPORATE_USER],
        store_ids=[],
    )


def _create_tenant(db: Session, *, code: str) -> Tenant:
    tenant = Tenant(code=code, name=f"Tenant {code}", status=TenantStatus.ACTIVE)
    db.add(tenant)
    db.commit()
    db.refresh(tenant)
    return tenant


def test_public_reviewer_is_exact_and_unchanged_bootstrap_is_write_free(
    postgres_session_factory: sessionmaker[Session],
    api_client: TestClient,
) -> None:
    suffix = uuid.uuid4().hex[:10]
    tenant_code = f"public-{suffix}"
    email = f"viewer-{suffix}@orange.example"
    password = "Orange-Demo-ReadOnly-2026!"
    request = _request(email=email, password=password)

    with postgres_session_factory() as db:
        tenant = _create_tenant(db, code=tenant_code)
        first = bootstrap_public_reviewer(db, tenant_code=tenant_code, request=request)
        user_id = first.user.id
        first_hash = first.user.password_hash
        first_token_version = first.user.token_version
        first_audit_count = db.scalar(
            select(func.count(IdentityAuditRecord.id)).where(
                IdentityAuditRecord.tenant_id == tenant.id,
                IdentityAuditRecord.target_user_id == user_id,
            )
        )

        second = bootstrap_public_reviewer(db, tenant_code=tenant_code, request=request)

        assert second.user.id == user_id
        assert second.user.status is UserStatus.ACTIVE
        assert second.user.password_hash == first_hash
        assert second.user.token_version == first_token_version
        assert verify_password_and_update(password, second.user.password_hash)[0] is True
        assert set(
            db.scalars(
                select(UserRole.role).where(
                    UserRole.tenant_id == tenant.id,
                    UserRole.user_id == user_id,
                )
            ).all()
        ) == {CanonicalIdentityRole.CORPORATE_USER}
        assert (
            db.scalar(
                select(func.count(UserStoreAssignment.user_id)).where(
                    UserStoreAssignment.tenant_id == tenant.id,
                    UserStoreAssignment.user_id == user_id,
                )
            )
            == 0
        )
        assert (
            db.scalar(
                select(func.count(UserAccessGrant.id)).where(
                    UserAccessGrant.tenant_id == tenant.id,
                    UserAccessGrant.user_id == user_id,
                )
            )
            == 0
        )
        assert (
            db.scalar(
                select(func.count(IdentityAuditRecord.id)).where(
                    IdentityAuditRecord.tenant_id == tenant.id,
                    IdentityAuditRecord.target_user_id == user_id,
                )
            )
            == first_audit_count
        )
        audit_details = db.scalars(
            select(IdentityAuditRecord.details).where(
                IdentityAuditRecord.tenant_id == tenant.id,
                IdentityAuditRecord.target_user_id == user_id,
            )
        ).all()
        assert password not in json.dumps(audit_details)

    login = api_client.post(
        "/v1/auth/login",
        json={"tenant_code": tenant_code, "email": email, "password": password},
    )
    assert login.status_code == 200
    headers = {"Authorization": f"Bearer {login.json()['access_token']}"}
    current_user = api_client.get("/v1/me", headers=headers)
    assert current_user.status_code == 200
    current_user_body = current_user.json()
    assert current_user_body["email"] == email
    assert current_user_body["roles"] == [CanonicalIdentityRole.CORPORATE_USER.value]
    assert current_user_body["store_ids"] == []
    assert current_user_body["permissions"] == [
        "catalog:read",
        "inventory:read",
        "policy:read",
        "replenishment:read",
    ]
    assert api_client.get("/v1/stores", headers=headers).status_code == 200
    denied = api_client.post(
        "/v1/replenishment/evaluations",
        headers=headers,
        json={"store_id": str(uuid.uuid4()), "sku_ids": []},
    )
    assert denied.status_code == 403


def test_public_reviewer_bootstrap_removes_non_admin_access_escalation(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    suffix = uuid.uuid4().hex[:10]
    tenant_code = f"reconcile-{suffix}"
    email = f"viewer-{suffix}@orange.example"
    password = "Orange-Reconcile-ReadOnly-1!"
    request = _request(email=email, password=password)

    with postgres_session_factory() as db:
        tenant = _create_tenant(db, code=tenant_code)
        created = bootstrap_public_reviewer(db, tenant_code=tenant_code, request=request)
        user_id = created.user.id
        original_token_version = created.user.token_version
        store = Store(
            tenant_id=tenant.id,
            code=f"store-{suffix}",
            name="Escalation Test Store",
            timezone="America/Los_Angeles",
            status=StoreStatus.ACTIVE,
            configuration={},
        )
        db.add(store)
        db.flush()
        db.add_all(
            [
                UserRole(
                    tenant_id=tenant.id,
                    user_id=user_id,
                    role=CanonicalIdentityRole.STORE_MANAGER,
                ),
                UserStoreAssignment(
                    tenant_id=tenant.id,
                    user_id=user_id,
                    store_id=store.id,
                ),
                UserAccessGrant(
                    tenant_id=tenant.id,
                    user_id=user_id,
                    role=IdentityRole.STORE_ASSOCIATE,
                    store_id=store.id,
                ),
            ]
        )
        db.commit()

        reconciled = bootstrap_public_reviewer(db, tenant_code=tenant_code, request=request)

        assert reconciled.user.token_version == original_token_version + 1
        assert set(
            db.scalars(
                select(UserRole.role).where(
                    UserRole.tenant_id == tenant.id,
                    UserRole.user_id == user_id,
                )
            ).all()
        ) == {CanonicalIdentityRole.CORPORATE_USER}
        assert (
            db.scalar(
                select(func.count(UserStoreAssignment.user_id)).where(
                    UserStoreAssignment.tenant_id == tenant.id,
                    UserStoreAssignment.user_id == user_id,
                )
            )
            == 0
        )
        assert (
            db.scalar(
                select(func.count(UserAccessGrant.id)).where(
                    UserAccessGrant.tenant_id == tenant.id,
                    UserAccessGrant.user_id == user_id,
                )
            )
            == 0
        )


def test_public_reviewer_password_rotation_invalidates_existing_jwt(
    postgres_session_factory: sessionmaker[Session],
    api_client: TestClient,
) -> None:
    suffix = uuid.uuid4().hex[:10]
    tenant_code = f"rotate-{suffix}"
    email = f"viewer-{suffix}@orange.example"
    old_password = "Orange-ReadOnly-Old-1!"
    new_password = "Orange-ReadOnly-New-2!"

    with postgres_session_factory() as db:
        _create_tenant(db, code=tenant_code)
        original = bootstrap_public_reviewer(
            db,
            tenant_code=tenant_code,
            request=_request(email=email, password=old_password),
        )
        original_token_version = original.user.token_version

    login = api_client.post(
        "/v1/auth/login",
        json={"tenant_code": tenant_code, "email": email, "password": old_password},
    )
    assert login.status_code == 200
    access_token = login.json()["access_token"]

    with postgres_session_factory() as db:
        rotated = bootstrap_public_reviewer(
            db,
            tenant_code=tenant_code,
            request=_request(email=email, password=new_password),
        )
        assert rotated.user.token_version == original_token_version + 1
        assert verify_password_and_update(old_password, rotated.user.password_hash)[0] is False
        assert verify_password_and_update(new_password, rotated.user.password_hash)[0] is True

    expired_identity = api_client.get(
        "/v1/me",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    assert expired_identity.status_code == 401

    old_login = api_client.post(
        "/v1/auth/login",
        json={"tenant_code": tenant_code, "email": email, "password": old_password},
    )
    assert old_login.status_code == 401
    new_login = api_client.post(
        "/v1/auth/login",
        json={"tenant_code": tenant_code, "email": email, "password": new_password},
    )
    assert new_login.status_code == 200
