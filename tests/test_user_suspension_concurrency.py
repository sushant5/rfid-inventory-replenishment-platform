from __future__ import annotations

import time
import uuid
from collections.abc import Generator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import timedelta
from typing import cast

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from abacus.config import get_settings
from abacus.enums import TenantStatus
from abacus.models.architecture import CanonicalIdentityRole, UserRole
from abacus.models.identity import IdentityAuditAction, IdentityAuditRecord, User, UserStatus
from abacus.models.tenancy import Tenant
from abacus.security import create_access_token


@dataclass(frozen=True, slots=True)
class AdminPair:
    tenant_id: uuid.UUID
    first_user_id: uuid.UUID
    second_user_id: uuid.UUID
    first_token: str
    second_token: str


def _token(*, user_id: uuid.UUID, tenant_id: uuid.UUID) -> str:
    settings = get_settings()
    token, _ = create_access_token(
        user_id=user_id,
        tenant_id=tenant_id,
        token_version=1,
        secret=settings.jwt_secret,
        issuer=settings.jwt_issuer,
        audience=settings.jwt_audience,
        lifetime=timedelta(minutes=15),
    )
    return token


@pytest.fixture
def admin_pair(postgres_session_factory: sessionmaker[Session]) -> Generator[AdminPair]:
    suffix = uuid.uuid4().hex[:10]
    tenant_id = uuid.uuid4()
    first_user_id = uuid.uuid4()
    second_user_id = uuid.uuid4()

    with postgres_session_factory() as db:
        db.add(
            Tenant(
                id=tenant_id,
                code=f"suspend-{suffix}",
                name="Concurrent Suspension Test",
                status=TenantStatus.ACTIVE,
            )
        )
        db.flush()
        db.add_all(
            [
                User(
                    id=first_user_id,
                    tenant_id=tenant_id,
                    email=f"admin-a-{suffix}@orange.example",
                    display_name="Admin A",
                    password_hash="not-used-by-this-test",
                    status=UserStatus.ACTIVE,
                    token_version=1,
                ),
                User(
                    id=second_user_id,
                    tenant_id=tenant_id,
                    email=f"admin-b-{suffix}@orange.example",
                    display_name="Admin B",
                    password_hash="not-used-by-this-test",
                    status=UserStatus.ACTIVE,
                    token_version=1,
                ),
            ]
        )
        db.flush()
        db.add_all(
            [
                UserRole(
                    tenant_id=tenant_id,
                    user_id=first_user_id,
                    role=CanonicalIdentityRole.TENANT_ADMIN,
                ),
                UserRole(
                    tenant_id=tenant_id,
                    user_id=second_user_id,
                    role=CanonicalIdentityRole.TENANT_ADMIN,
                ),
            ]
        )
        db.commit()

    try:
        yield AdminPair(
            tenant_id=tenant_id,
            first_user_id=first_user_id,
            second_user_id=second_user_id,
            first_token=_token(user_id=first_user_id, tenant_id=tenant_id),
            second_token=_token(user_id=second_user_id, tenant_id=tenant_id),
        )
    finally:
        with postgres_session_factory() as db:
            tenant = db.get(Tenant, tenant_id)
            if tenant is not None:
                db.delete(tenant)
                db.commit()


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.mark.integration
def test_concurrent_cross_suspension_preserves_one_active_tenant_admin(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
    admin_pair: AdminPair,
) -> None:
    def suspend(token: str, target_user_id: uuid.UUID) -> Response:
        return cast(
            Response,
            api_client.post(
                f"/v1/users/{target_user_id}:suspend",
                headers=_bearer(token),
            ),
        )

    with postgres_session_factory() as blocker:
        blocker.scalar(select(Tenant.id).where(Tenant.id == admin_pair.tenant_id).with_for_update())
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = (
                executor.submit(
                    suspend,
                    admin_pair.first_token,
                    admin_pair.second_user_id,
                ),
                executor.submit(
                    suspend,
                    admin_pair.second_token,
                    admin_pair.first_user_id,
                ),
            )
            blocked_requests = 0
            deadline = time.monotonic() + 5
            try:
                observer_engine = cast(Engine, blocker.get_bind())
                with observer_engine.connect().execution_options(
                    isolation_level="AUTOCOMMIT"
                ) as observer:
                    while time.monotonic() < deadline:
                        observer.execute(text("SELECT pg_stat_clear_snapshot()"))
                        blocked_requests = int(
                            observer.scalar(
                                text(
                                    "SELECT count(*) FROM pg_catalog.pg_stat_activity "
                                    "WHERE usename = :role AND wait_event_type = 'Lock' "
                                    "AND query ILIKE '%FOR UPDATE%'"
                                ),
                                {"role": get_settings().application_database_role},
                            )
                            or 0
                        )
                        if blocked_requests >= 2:
                            break
                        time.sleep(0.05)
            finally:
                blocker.commit()

            responses = [future.result(timeout=10) for future in futures]

    assert blocked_requests >= 2
    assert sorted(response.status_code for response in responses) == [200, 409]
    conflict = next(response for response in responses if response.status_code == 409)
    assert conflict.json()["code"] == "last_tenant_admin"

    with postgres_session_factory() as db:
        users = db.scalars(
            select(User).where(
                User.tenant_id == admin_pair.tenant_id,
                User.id.in_([admin_pair.first_user_id, admin_pair.second_user_id]),
            )
        ).all()
        assert sorted(user.status for user in users) == [UserStatus.ACTIVE, UserStatus.SUSPENDED]
        assert sorted(user.token_version for user in users) == [1, 2]
        assert (
            db.scalar(
                select(func.count(IdentityAuditRecord.id)).where(
                    IdentityAuditRecord.tenant_id == admin_pair.tenant_id,
                    IdentityAuditRecord.action == IdentityAuditAction.USER_SUSPENDED,
                )
            )
            == 1
        )
