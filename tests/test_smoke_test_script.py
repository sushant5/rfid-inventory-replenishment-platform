from collections.abc import Callable

import pytest
from scripts.smoke_test import JsonObject, run_checks, validate_base_url


def healthy_fetcher(recorder: list[str]) -> Callable[[str, float], JsonObject]:
    def fetch(url: str, timeout: float) -> JsonObject:
        recorder.append(url)
        assert timeout == 3.0
        if url.endswith("/health/live") or url.endswith("/health/ready"):
            return {"status": "ok"}
        if url.endswith("/version"):
            return {"version": "0.5.0", "build_sha": "test", "environment": "test"}
        return {
            "openapi": "3.1.0",
            "paths": {
                "/health/live": {},
                "/health/ready": {},
                "/version": {},
                "/v1/rfid/observation-batches": {},
                "/v1/stores/{store_id}/inventory": {},
            },
        }

    return fetch


def test_smoke_checks_all_public_operational_surfaces_without_network() -> None:
    requested_urls: list[str] = []

    results = run_checks(
        "https://abacus.example.test/",
        timeout=3.0,
        fetcher=healthy_fetcher(requested_urls),
    )

    assert [result.name for result in results] == [
        "liveness",
        "readiness",
        "version",
        "openapi",
    ]
    assert requested_urls == [
        "https://abacus.example.test/health/live",
        "https://abacus.example.test/health/ready",
        "https://abacus.example.test/version",
        "https://abacus.example.test/openapi.json",
    ]


def test_smoke_check_rejects_an_incomplete_openapi_document() -> None:
    def incomplete_fetcher(url: str, _timeout: float) -> JsonObject:
        if url.endswith("/health/live") or url.endswith("/health/ready"):
            return {"status": "ok"}
        if url.endswith("/version"):
            return {"version": "0.5.0", "environment": "test"}
        return {"openapi": "3.1.0", "paths": {}}

    with pytest.raises(RuntimeError, match="missing required paths"):
        run_checks("http://localhost:8000", fetcher=incomplete_fetcher)


@pytest.mark.parametrize("base_url", ["localhost:8000", "file:///tmp/openapi.json", ""])
def test_smoke_check_rejects_non_http_base_urls(base_url: str) -> None:
    with pytest.raises(ValueError, match=r"http\(s\)"):
        validate_base_url(base_url)
