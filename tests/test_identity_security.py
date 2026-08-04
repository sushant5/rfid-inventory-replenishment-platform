import uuid
from datetime import UTC, datetime, timedelta

import jwt
import pytest
from fastapi.testclient import TestClient

from abacus import __version__
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


@pytest.mark.parametrize(
    "claim_overrides",
    [
        {"sub": 7},
        {"tid": 7},
        {"token_version": True},
        {"token_version": "1"},
        {"token_version": 0},
        {"sid": 7},
        {"sid": "not-a-uuid"},
        {"exp": "9999999999"},
    ],
)
def test_access_token_rejects_malformed_claim_types(
    claim_overrides: dict[str, object],
) -> None:
    now = datetime.now(UTC)
    payload: dict[str, object] = {
        "iss": ISSUER,
        "aud": AUDIENCE,
        "iat": now,
        "exp": now + timedelta(minutes=5),
        "sub": str(uuid.uuid4()),
        "tid": str(uuid.uuid4()),
        "token_version": 1,
    }
    payload.update(claim_overrides)
    token = jwt.encode(payload, SECRET, algorithm="HS256")

    with pytest.raises(ApiError) as error:
        decode_access_token(token, secret=SECRET, issuer=ISSUER, audience=AUDIENCE)

    assert error.value.code == "invalid_access_token"


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
        "version": __version__,
        "docs": "/docs",
        "openapi": "/openapi.json",
        "login": "/v1/auth/login",
        "stores": "/v1/stores",
        "demo_login": {
            "tenant_code": "orange",
            "email": "demo-reader@orange.example",
            "password": "Orange-Demo-ReadOnly-2026!",
        },
        "demo_access": (
            "Demo-only reviewer account with access to synthetic Orange tenant data. "
            "All business-data mutation endpoints are denied."
        ),
        "reviewer_path": [
            "POST /v1/auth/login",
            "GET /v1/me",
            "GET /v1/stores",
            "GET /v1/stores/{store_id}/zones",
            "GET /v1/stores/{store_id}/devices",
            "GET /v1/skus",
            "GET /v1/stores/{store_id}/inventory",
            "GET /v1/replenishment-policies",
            "GET /v1/stores/{store_id}/replenishment-tasks",
            "GET /v1/rfid/quarantine",
        ],
        "private_credentials": (
            "Administrative, device, platform, and infrastructure credentials are not published."
        ),
        "liveness": "/health/live",
        "readiness": "/health/ready",
    }


def test_openapi_publishes_the_read_only_reviewer_path() -> None:
    with TestClient(create_app()) as client:
        response = client.get("/openapi.json")

    assert response.status_code == 200
    description = response.json()["info"]["description"]
    assert "demo-reader@orange.example" in description
    assert "Orange-Demo-ReadOnly-2026!" in description
    assert "GET /v1/stores/{store_id}/inventory" in description
    assert "returns `403 Forbidden`" in description
    assert "Platform, tenant-admin, and device credentials remain private" in description
