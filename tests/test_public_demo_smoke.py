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
        if url.endswith("/v1/auth/login"):
            assert not headers
            assert payload == {
                "tenant_code": "orange",
                "email": "demo-reader@orange.example",
                "password": "Orange-Demo-ReadOnly-2026!",
            }
            return HttpResult(200, {"access_token": "test-token"})

        assert headers == {"Authorization": "Bearer test-token"}
        if url.endswith("/v1/me"):
            return HttpResult(200, {"email": "demo-reader@orange.example"})
        if url.endswith("/v1/stores?limit=5"):
            return HttpResult(200, {"items": [{"id": "store-1"}], "total": 1})
        if url.endswith("/v1/stores/store-1/zones"):
            return HttpResult(200, [{"id": "zone-1"}])
        if url.endswith("/v1/stores/store-1/devices"):
            return HttpResult(200, [{"device": {"id": "device-1"}}])
        if url.endswith("/v1/skus?limit=5"):
            return HttpResult(200, {"items": [{"id": "sku-1"}], "total": 1})
        if any(
            suffix in url
            for suffix in (
                "/inventory?limit=5",
                "/replenishment-policies?limit=5",
                "/replenishment-tasks?limit=5",
                "/rfid/quarantine?limit=5",
            )
        ):
            return HttpResult(200, {"items": [], "total": 0})
        if url.endswith("/v1/replenishment/evaluations"):
            assert payload == {"store_id": "store-1", "sku_ids": []}
            return HttpResult(403, {"status": 403})
        raise AssertionError(f"unexpected request: {method} {url}")

    assert run_checks("https://abacus.example.test/", timeout=3.0, transport=transport) == [
        "login",
        "current user",
        "stores",
        "zones",
        "devices",
        "SKUs",
        "inventory",
        "policies",
        "tasks",
        "quarantine",
        "write denied",
    ]
    assert [method for method, _, _ in requests] == [
        "POST",
        "GET",
        "GET",
        "GET",
        "GET",
        "GET",
        "GET",
        "GET",
        "GET",
        "GET",
        "POST",
    ]


@pytest.mark.parametrize("base_url", ["localhost:8000", "file:///tmp/demo", ""])
def test_public_demo_smoke_rejects_non_http_base_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match=r"http\(s\)"):
        validate_base_url(base_url)
