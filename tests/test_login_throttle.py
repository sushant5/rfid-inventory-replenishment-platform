import uuid
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from abacus.api.dependencies import require_tenant_session
from abacus.api.errors import ApiError
from abacus.main import create_app
from abacus.security import (
    LoginAttemptOutcome,
    LoginAttemptReservation,
    LoginThrottle,
    LoginThrottleDecision,
    get_login_throttle,
)


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _throttle(
    clock: FakeClock,
    *,
    ip_limit: int = 10,
    account_limit: int = 2,
    max_entries: int = 100,
    enabled: bool = True,
) -> LoginThrottle:
    return LoginThrottle(
        enabled=enabled,
        window_seconds=60,
        ip_limit=ip_limit,
        account_limit=account_limit,
        max_entries=max_entries,
        clock=clock,
    )


def _reserve(
    throttle: LoginThrottle,
    *,
    source_ip: str = "192.0.2.10",
    tenant_code: str = "orange",
    email: str = "user@example.com",
) -> LoginAttemptReservation:
    decision = throttle.begin_attempt(
        source_ip=source_ip,
        tenant_code=tenant_code,
        email=email,
    )
    assert decision.retry_after is None
    assert decision.reservation is not None
    return decision.reservation


def test_failed_attempts_are_retained_and_success_releases_only_its_reservation() -> None:
    clock = FakeClock()
    throttle = _throttle(clock, ip_limit=4, account_limit=2)

    first_failure = _reserve(
        throttle,
        tenant_code=" Orange ",
        email="USER@EXAMPLE.COM",
    )
    successful_attempt = _reserve(throttle)
    throttle.finish_attempt(first_failure, outcome=LoginAttemptOutcome.AUTHENTICATION_FAILED)
    throttle.finish_attempt(successful_attempt, outcome=LoginAttemptOutcome.SUCCESS)

    second_failure = _reserve(throttle)
    throttle.finish_attempt(second_failure, outcome=LoginAttemptOutcome.AUTHENTICATION_FAILED)
    blocked_account = throttle.begin_attempt(
        source_ip="192.0.2.10",
        tenant_code="orange",
        email="user@example.com",
    )
    assert blocked_account.retry_after == 60
    assert blocked_account.reservation is None

    third_failure = _reserve(throttle, email="another@example.com")
    throttle.finish_attempt(third_failure, outcome=LoginAttemptOutcome.AUTHENTICATION_FAILED)
    blocked_ip = throttle.begin_attempt(
        source_ip="192.0.2.10",
        tenant_code="orange",
        email="third@example.com",
    )
    assert blocked_ip.retry_after == 60

    clock.advance(60)
    assert _reserve(throttle) is not None


def test_concurrent_attempts_cannot_exceed_limit() -> None:
    throttle = _throttle(FakeClock(), ip_limit=10, account_limit=10)

    def attempt(_: int) -> LoginThrottleDecision:
        return throttle.begin_attempt(
            source_ip="192.0.2.20",
            tenant_code="orange",
            email="concurrent@example.com",
        )

    with ThreadPoolExecutor(max_workers=20) as executor:
        outcomes = list(executor.map(attempt, range(50)))

    assert sum(outcome.retry_after is None for outcome in outcomes) == 10
    assert sum(outcome.retry_after == 60 for outcome in outcomes) == 40


def test_throttle_memory_is_bounded_and_can_be_disabled() -> None:
    clock = FakeClock()
    throttle = _throttle(clock, ip_limit=100, account_limit=100, max_entries=4)
    for index in range(2):
        reservation = _reserve(
            throttle,
            source_ip=f"192.0.2.{index}",
            email=f"user-{index}@example.com",
        )
        throttle.finish_attempt(reservation, outcome=LoginAttemptOutcome.AUTHENTICATION_FAILED)
        assert throttle.entry_count <= 4
    rejected_at_capacity = throttle.begin_attempt(
        source_ip="192.0.2.3",
        tenant_code="orange",
        email="user-3@example.com",
    )
    assert rejected_at_capacity.retry_after == 60
    assert throttle.entry_count == 4

    clock.advance(60)
    assert _reserve(throttle, source_ip="192.0.2.4", email="user-4@example.com") is not None
    assert throttle.entry_count == 2

    disabled = _throttle(clock, enabled=False)
    for _ in range(20):
        decision = disabled.begin_attempt(
            source_ip="192.0.2.30",
            tenant_code="orange",
            email="disabled@example.com",
        )
        assert decision.retry_after is None
        assert decision.reservation is None
    assert disabled.entry_count == 0


def test_login_endpoint_limits_unknown_account_and_returns_retry_after(
    monkeypatch: MonkeyPatch,
) -> None:
    throttle = _throttle(FakeClock(), ip_limit=100, account_limit=2)
    calls = 0

    def reject_login(
        _db: object,
        *,
        tenant_code: str,
        email: str,
        password: str,
    ) -> SimpleNamespace:
        nonlocal calls
        calls += 1
        raise ApiError(
            401,
            "Unauthorized",
            "The tenant code, email, or password is invalid.",
            code="invalid_credentials",
        )

    monkeypatch.setattr("abacus.api.routes.auth.authenticate_user", reject_login)
    app = create_app()
    app.dependency_overrides[require_tenant_session] = object
    app.dependency_overrides[get_login_throttle] = lambda: throttle
    with TestClient(app) as client:
        request = {
            "tenant_code": "orange",
            "email": "missing@orange.example",
            "password": "wrong-password",
        }
        assert client.post("/v1/auth/login", json=request).status_code == 401
        assert client.post("/v1/auth/login", json=request).status_code == 401
        response = client.post("/v1/auth/login", json=request)

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"
    assert response.json()["code"] == "login_rate_limited"
    assert calls == 2


def test_login_endpoint_ignores_forwarded_for_and_releases_successful_reservation(
    monkeypatch: MonkeyPatch,
) -> None:
    throttle = _throttle(FakeClock(), ip_limit=3, account_limit=2)

    def authenticate(
        _db: object,
        *,
        tenant_code: str,
        email: str,
        password: str,
    ) -> SimpleNamespace:
        if password != "correct-password":
            raise ApiError(401, "Unauthorized", "Invalid credentials", code="invalid_credentials")
        return SimpleNamespace(id=uuid.uuid4(), tenant_id=uuid.uuid4(), token_version=1)

    monkeypatch.setattr("abacus.api.routes.auth.authenticate_user", authenticate)
    app = create_app()
    app.dependency_overrides[require_tenant_session] = object
    app.dependency_overrides[get_login_throttle] = lambda: throttle
    with TestClient(app) as client:
        base = {"tenant_code": "orange", "email": "user@orange.example"}
        assert (
            client.post(
                "/v1/auth/login",
                json={**base, "password": "wrong-password"},
                headers={"X-Forwarded-For": "198.51.100.1"},
            ).status_code
            == 401
        )
        assert (
            client.post(
                "/v1/auth/login",
                json={**base, "password": "correct-password"},
                headers={"X-Forwarded-For": "198.51.100.2"},
            ).status_code
            == 200
        )
        assert (
            client.post(
                "/v1/auth/login",
                json={**base, "password": "wrong-password"},
                headers={"X-Forwarded-For": "198.51.100.3"},
            ).status_code
            == 401
        )
        response = client.post(
            "/v1/auth/login",
            json={
                "tenant_code": "orange",
                "email": "different@orange.example",
                "password": "wrong-password",
            },
            headers={"X-Forwarded-For": "198.51.100.4"},
        )

    assert response.status_code == 429
    assert response.headers["Retry-After"] == "60"


def test_successful_logins_consume_ip_but_not_account_budget(
    monkeypatch: MonkeyPatch,
) -> None:
    throttle = _throttle(FakeClock(), ip_limit=3, account_limit=1)

    def authenticate(
        _db: object,
        *,
        tenant_code: str,
        email: str,
        password: str,
    ) -> SimpleNamespace:
        return SimpleNamespace(id=uuid.uuid4(), tenant_id=uuid.uuid4(), token_version=1)

    monkeypatch.setattr("abacus.api.routes.auth.authenticate_user", authenticate)
    app = create_app()
    app.dependency_overrides[require_tenant_session] = object
    app.dependency_overrides[get_login_throttle] = lambda: throttle
    with TestClient(app) as client:
        for index in range(3):
            response = client.post(
                "/v1/auth/login",
                json={
                    "tenant_code": "orange",
                    "email": f"user-{index}@orange.example",
                    "password": "correct-password",
                },
            )
            assert response.status_code == 200
        response = client.post(
            "/v1/auth/login",
            json={
                "tenant_code": "orange",
                "email": "fourth@orange.example",
                "password": "correct-password",
            },
        )

    assert response.status_code == 429
    # Completed successes retain one shared IP window, while their per-account
    # reservations are released.
    assert throttle.entry_count == 1


def test_non_authentication_errors_release_the_reservation(monkeypatch: MonkeyPatch) -> None:
    throttle = _throttle(FakeClock(), ip_limit=1, account_limit=1)
    calls = 0

    def authenticate(
        _db: object,
        *,
        tenant_code: str,
        email: str,
        password: str,
    ) -> SimpleNamespace:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ApiError(503, "Unavailable", "Try again", code="temporarily_unavailable")
        if calls == 2:
            raise RuntimeError("simulated internal failure")
        return SimpleNamespace(id=uuid.uuid4(), tenant_id=uuid.uuid4(), token_version=1)

    monkeypatch.setattr("abacus.api.routes.auth.authenticate_user", authenticate)
    app = create_app()
    app.dependency_overrides[require_tenant_session] = object
    app.dependency_overrides[get_login_throttle] = lambda: throttle
    request = {
        "tenant_code": "orange",
        "email": "user@orange.example",
        "password": "correct-password",
    }
    with TestClient(app, raise_server_exceptions=False) as client:
        assert client.post("/v1/auth/login", json=request).status_code == 503
        assert client.post("/v1/auth/login", json=request).status_code == 500
        assert client.post("/v1/auth/login", json=request).status_code == 200

    # The two aborted requests were released; the successful authentication remains
    # only in the source-IP budget.
    assert throttle.entry_count == 1
