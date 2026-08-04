from __future__ import annotations

import hashlib
import io
import json
import urllib.error
from typing import Any

import pytest
import scripts.run_architecture_demo as architecture_demo


class _ReadinessClient:
    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.attempts = 0

    def request(self, method: str, path: str) -> tuple[int, object]:
        assert (method, path) == ("GET", "/health/ready")
        self.attempts += 1
        if self.attempts <= self.failures:
            raise architecture_demo.DemoFailure("service is waking")
        return 200, {"status": "ok"}


class _JsonResponse:
    status = 200

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self) -> bytes:
        return b'{"status":"ok"}'


def test_readiness_probe_retries_a_sleeping_host(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _ReadinessClient(failures=2)
    monkeypatch.setattr(architecture_demo.time, "sleep", lambda _: None)

    architecture_demo.wait_for_readiness(client, timeout=1)  # type: ignore[arg-type]

    assert client.attempts == 3


def test_readiness_probe_reports_the_last_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    client = _ReadinessClient(failures=10)
    ticks = iter((0.0, 1.0))
    monkeypatch.setattr(architecture_demo.time, "monotonic", lambda: next(ticks))

    with pytest.raises(architecture_demo.DemoFailure, match="service is waking"):
        architecture_demo.wait_for_readiness(client, timeout=0.5)  # type: ignore[arg-type]

    assert client.attempts == 1


def test_readiness_probe_retries_non_json_render_proxy_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses: list[object] = [
        urllib.error.HTTPError(
            "https://demo.example/health/ready",
            502,
            "Bad Gateway",
            {},
            io.BytesIO(b"<html>Render proxy is waking</html>"),
        ),
        _JsonResponse(),
    ]

    def urlopen(*_: object, **__: object) -> object:
        response = responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response

    monkeypatch.setattr(architecture_demo.urllib.request, "urlopen", urlopen)
    monkeypatch.setattr(architecture_demo.time, "sleep", lambda _: None)
    client = architecture_demo.Client("https://demo.example", timeout=1)

    architecture_demo.wait_for_readiness(client, timeout=1)

    assert responses == []


def test_estate_provisioning_adds_only_missing_stores_with_a_stable_key() -> None:
    desired = architecture_demo.build_store_batch(100)["stores"]
    assert isinstance(desired, list)

    class ProvisioningClient:
        def __init__(self) -> None:
            self.created: dict[str, Any] | None = None
            self.idempotency_key = ""
            self.post_calls = 0

        def request(
            self,
            method: str,
            path: str,
            **kwargs: Any,
        ) -> tuple[int, object]:
            if method == "GET":
                stores = desired if self.created is not None else desired[:2]
                return 200, [{"code": store["code"]} for store in stores]
            assert method == "POST"
            assert path == "/v1/tenants/tenant-id/store-imports"
            self.post_calls += 1
            self.created = kwargs["payload"]
            self.idempotency_key = kwargs["headers"]["Idempotency-Key"]
            return 201, {"status": "COMPLETED"}

    client = ProvisioningClient()
    stores = architecture_demo.provision_orange_estate(
        client,  # type: ignore[arg-type]
        tenant_id="tenant-id",
        platform_headers={"X-Platform-Key": "test-key"},
    )

    assert len(stores) == 100
    assert client.created is not None
    assert len(client.created["stores"]) == 98
    assert client.created["stores"][0]["code"] == "orange-003"
    digest = hashlib.sha256(
        json.dumps(client.created, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:16]
    assert client.idempotency_key == f"orange-demo-stores-100-{digest}"

    repeated = architecture_demo.provision_orange_estate(
        client,  # type: ignore[arg-type]
        tenant_id="tenant-id",
        platform_headers={"X-Platform-Key": "test-key"},
    )
    assert len(repeated) == 100
    assert client.post_calls == 1
