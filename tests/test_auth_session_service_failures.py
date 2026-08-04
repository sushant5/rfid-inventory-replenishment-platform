from __future__ import annotations

import hashlib
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.enums import TenantStatus
from abacus.models.identity import AuthSession
from abacus.security import Principal
from abacus.services.identity import revoke_auth_session, rotate_auth_session

REFRESH_SECRET = "x" * 48


def _session(
    *,
    revoked: bool = False,
    expired: bool = False,
) -> AuthSession:
    now = datetime.now(UTC)
    return AuthSession(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        user_id=uuid.uuid4(),
        family_id=uuid.uuid4(),
        refresh_token_hash=hashlib.sha256(REFRESH_SECRET.encode()).hexdigest(),
        token_version=1,
        expires_at=now - timedelta(seconds=1) if expired else now + timedelta(days=1),
        revoked_at=now if revoked else None,
    )


def _raw_token(auth_session: AuthSession) -> str:
    return f"{auth_session.id}.{REFRESH_SECRET}"


def test_refresh_rejects_an_unknown_session_without_tenant_lookup() -> None:
    db = MagicMock(spec=Session)
    db.scalar.return_value = None
    raw_token = f"{uuid.uuid4()}.{REFRESH_SECRET}"

    with pytest.raises(ApiError, match="invalid"):
        rotate_auth_session(
            db,
            tenant_code="orange",
            raw_refresh_token=raw_token,
            lifetime=timedelta(days=7),
        )

    db.rollback.assert_not_called()


@pytest.mark.parametrize(("revoked", "expired"), [(True, False), (False, True)])
def test_refresh_rejects_revoked_and_expired_sessions(
    revoked: bool,
    expired: bool,
) -> None:
    auth_session = _session(revoked=revoked, expired=expired)
    db = MagicMock(spec=Session)
    db.scalar.return_value = auth_session

    with pytest.raises(ApiError, match="invalid"):
        rotate_auth_session(
            db,
            tenant_code="orange",
            raw_refresh_token=_raw_token(auth_session),
            lifetime=timedelta(days=7),
        )


def test_refresh_revokes_session_when_user_is_no_longer_available() -> None:
    auth_session = _session()
    db = MagicMock(spec=Session)
    db.scalar.side_effect = [auth_session, None, TenantStatus.ACTIVE]

    with pytest.raises(ApiError, match="invalid"):
        rotate_auth_session(
            db,
            tenant_code="orange",
            raw_refresh_token=_raw_token(auth_session),
            lifetime=timedelta(days=7),
        )

    assert auth_session.revoked_at is not None
    db.commit.assert_called_once_with()


def test_legacy_logout_without_a_user_is_an_idempotent_noop() -> None:
    db = MagicMock(spec=Session)
    db.scalar.return_value = None
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        email="user@example.com",
        display_name="User",
        role_scopes=(),
    )

    revoke_auth_session(db, principal)

    db.commit.assert_not_called()
