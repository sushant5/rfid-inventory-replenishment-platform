import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from abacus.api.errors import ApiError
from abacus.main import create_app
from abacus.security import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password_and_update,
)

SECRET = "unit-test-jwt-secret-that-is-at-least-32-characters"
ISSUER = "test-issuer"
AUDIENCE = "test-audience"


def test_passwords_use_argon2id_and_verify() -> None:
    encoded = hash_password("correct horse battery staple")

    assert encoded.startswith("$argon2id$")
    assert verify_password_and_update("correct horse battery staple", encoded)[0] is True
    assert verify_password_and_update("incorrect password", encoded)[0] is False


def test_access_token_contains_and_validates_required_tenant_claims() -> None:
    user_id = uuid.uuid4()
    tenant_id = uuid.uuid4()
    token, expires_at = create_access_token(
        user_id=user_id,
        tenant_id=tenant_id,
        token_version=7,
        secret=SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
        lifetime=timedelta(minutes=15),
    )

    claims = decode_access_token(
        token,
        secret=SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
    )

    assert claims.user_id == user_id
    assert claims.tenant_id == tenant_id
    assert claims.token_version == 7
    assert abs((claims.expires_at - expires_at).total_seconds()) < 1


@pytest.mark.parametrize(
    ("secret", "issuer", "audience"),
    [
        ("another-unit-test-secret-that-is-long-enough", ISSUER, AUDIENCE),
        (SECRET, "wrong-issuer", AUDIENCE),
        (SECRET, ISSUER, "wrong-audience"),
    ],
)
def test_access_token_rejects_wrong_verification_context(
    secret: str,
    issuer: str,
    audience: str,
) -> None:
    token, _ = create_access_token(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        token_version=1,
        secret=SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
        lifetime=timedelta(minutes=15),
    )

    with pytest.raises(ApiError) as error:
        decode_access_token(token, secret=secret, issuer=issuer, audience=audience)

    assert error.value.status_code == 401
    assert error.value.code == "invalid_access_token"


def test_access_token_rejects_expired_token() -> None:
    token, _ = create_access_token(
        user_id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        token_version=1,
        secret=SECRET,
        issuer=ISSUER,
        audience=AUDIENCE,
        lifetime=timedelta(minutes=1),
        now=datetime.now(UTC) - timedelta(minutes=2),
    )

    with pytest.raises(ApiError) as error:
        decode_access_token(token, secret=SECRET, issuer=ISSUER, audience=AUDIENCE)

    assert error.value.status_code == 401


def test_access_token_rejects_missing_token_version() -> None:
    now = datetime.now(UTC)
    token = jwt.encode(
        {
            "iss": ISSUER,
            "aud": AUDIENCE,
            "iat": now,
            "exp": now + timedelta(minutes=5),
            "sub": str(uuid.uuid4()),
            "tid": str(uuid.uuid4()),
        },
        SECRET,
        algorithm="HS256",
    )

    with pytest.raises(ApiError) as error:
        decode_access_token(token, secret=SECRET, issuer=ISSUER, audience=AUDIENCE)

    assert error.value.status_code == 401


def test_validation_errors_do_not_echo_passwords() -> None:
    password = "sensitive-password-" + ("x" * 128)
    with TestClient(create_app()) as client:
        response = client.post(
            "/v1/auth/login",
            json={
                "tenant_code": "orange",
                "email": "reviewer@orange.example",
                "password": password,
            },
        )

    assert response.status_code == 422
    assert password not in response.text
    assert all("input" not in error for error in response.json()["errors"])
    assert response.json()["request_id"] == response.headers["X-Request-ID"]


def test_root_discovers_the_reviewer_endpoints() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "service": "Abacus RFID Platform",
        "version": "0.3.0",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "liveness": "/health/live",
        "readiness": "/health/ready",
    }
