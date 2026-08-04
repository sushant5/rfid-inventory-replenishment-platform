from collections.abc import Mapping

import pytest
from scripts.public_demo_smoke import HttpResult, run_checks, validate_base_url


def test_public_demo_smoke_exercises_reads_and_proves_writes_are_denied() -> None:
    requests: list[tuple[str, str, dict[str, object] | None]] = []

    def transport(
        method: str,
        url: str,
        headers: Mapping[str, str],
        payload: dict[str, object] | None,
        timeout: float,
    ) -> HttpResult:
        requests.append((method, url, payload))
        assert timeout == 3.0
        if url == "https://abacus.example.test/":
            assert method == "GET"
            assert not headers
            return HttpResult(
                200,
                {
                    "demo_login": {
                        "tenant_code": "orange",
                        "email": "demo-reader@orange.example",
                        "password": "Orange-Demo-ReadOnly-2026!",
                    }
                },
            )
        if url.endswith("/v1/auth/login"):
            assert not headers
            assert payload == {
                "tenant_code": "orange",
                "email": "demo-reader@orange.example",
                "password": "Orange-Demo-ReadOnly-2026!",
            }
            return HttpResult(
                200,
                {"access_token": "original-token", "refresh_token": "test-refresh-token"},
            )
        if url.endswith("/v1/auth/refresh"):
            assert not headers
            assert payload == {"tenant_code": "orange", "refresh_token": "test-refresh-token"}
            return HttpResult(200, {"access_token": "test-token"})
        if headers == {"Authorization": "Bearer original-token"}:
            assert url.endswith("/v1/me")
            return HttpResult(401, {"status": 401})

        assert headers == {"Authorization": "Bearer test-token"}
        if url.endswith("/v1/me"):
            if method == "GET" and any(
                previous_method == "POST" and previous_url.endswith("/v1/auth/logout")
                for previous_method, previous_url, _ in requests[:-1]
            ):
                return HttpResult(401, {"status": 401})
            return HttpResult(200, {"email": "demo-reader@orange.example"})
        if url.endswith("/v1/stores?limit=5"):
            return HttpResult(
                200,
                {"items": [{"id": f"store-{number}"} for number in range(1, 6)], "total": 100},
            )
        if url.endswith("/v1/stores/store-1/zones"):
            if method == "GET":
                return HttpResult(200, [{"id": "zone-1", "code": "floor"}])
            return HttpResult(403, {"status": 403})
        if url.endswith("/v1/stores/store-1/devices"):
            if method == "GET":
                return HttpResult(
                    200,
                    [{"device": {"id": "device-1", "serial_number": "DEMO-DEVICE-1"}}],
                )
            return HttpResult(403, {"status": 403})
        if url.endswith("/v1/skus?limit=5"):
            return HttpResult(200, {"items": [{"id": "sku-1"}], "total": 100})
        if "/inventory?limit=5" in url:
            store_number = int(url.split("/stores/store-", 1)[1].split("/", 1)[0])
            return HttpResult(
                200,
                {"items": [{"quantity": 4}] if store_number <= 3 else [], "total": 1},
            )
        if any(
            suffix in url
            for suffix in (
                "/replenishment-policies?limit=5",
                "/replenishment-tasks?limit=5",
                "/rfid/quarantine?limit=5",
            )
        ):
            return HttpResult(200, {"items": [], "total": 0})
        if method in {"POST", "PATCH"}:
            if url.endswith("/v1/auth/logout"):
                return HttpResult(204, None)
            return HttpResult(403, {"status": 403})
        raise AssertionError(f"unexpected request: {method} {url}")

    assert run_checks("https://abacus.example.test/", timeout=3.0, transport=transport) == [
        "discovery",
        "login",
        "refresh rotation",
        "current user",
        "stores",
        "zones",
        "devices",
        "SKUs",
        "inventory in three stores",
        "policies",
        "tasks",
        "quarantine",
        "eight write categories denied",
        "logout revocation",
    ]
    mutation_requests = [
        request
        for request in requests
        if request[0] in {"POST", "PATCH"}
        and not request[1].endswith(("/v1/auth/login", "/v1/auth/refresh", "/v1/auth/logout"))
    ]
    assert [method for method, _, _ in mutation_requests] == [
        "POST",
        "POST",
        "POST",
        "POST",
        "POST",
        "POST",
        "POST",
        "PATCH",
    ]
    assert [url.rsplit("/v1/", 1)[1] for _, url, _ in mutation_requests] == [
        "users",
        "stores/store-1/zones",
        "stores/store-1/devices",
        "stores/store-1/business-events",
        "replenishment-policy-versions/00000000-0000-0000-0000-000000000000/activate",
        "replenishment/evaluations",
        "devices/00000000-0000-0000-0000-000000000000/credentials:rotate",
        "replenishment-tasks/00000000-0000-0000-0000-000000000000",
    ]


@pytest.mark.parametrize("base_url", ["localhost:8000", "file:///tmp/demo", ""])
def test_public_demo_smoke_rejects_non_http_base_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match=r"http\(s\)"):
        validate_base_url(base_url)
