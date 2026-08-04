"""Exercise the hosted API with its published read-only reviewer account."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

DEMO_TENANT = "orange"
DEMO_EMAIL = "demo-reader@orange.example"
DEMO_PASSWORD = "Orange-Demo-ReadOnly-2026!"  # noqa: S105 - public demo credential
ZERO_UUID = "00000000-0000-0000-0000-000000000000"

type JsonValue = dict[str, object] | list[object]


@dataclass(frozen=True, slots=True)
class HttpResult:
    status_code: int
    body: JsonValue | None


type Transport = Callable[
    [str, str, Mapping[str, str], dict[str, object] | None, float], HttpResult
]


def validate_base_url(base_url: str) -> str:
    candidate = base_url.rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base URL must be an absolute http(s) URL")
    return candidate


def request_json(
    method: str,
    url: str,
    headers: Mapping[str, str],
    payload: dict[str, object] | None,
    timeout: float,
) -> HttpResult:
    validate_base_url(url)
    request_headers = {"Accept": "application/json", **headers}
    data = None
    if payload is not None:
        request_headers["Content-Type"] = "application/json"
        data = json.dumps(payload).encode("utf-8")
    request = Request(  # noqa: S310 - validate_base_url permits only HTTP(S).
        url,
        data=data,
        headers=request_headers,
        method=method,
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            status_code = response.status
            raw = response.read()
    except HTTPError as exc:
        status_code = exc.code
        raw = exc.read()
    except URLError as exc:
        raise RuntimeError(f"could not reach {url}: {exc.reason}") from exc

    if not raw:
        return HttpResult(status_code=status_code, body=None)
    try:
        body = json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{url} did not return JSON") from exc
    if not isinstance(body, (dict, list)):
        raise RuntimeError(f"{url} returned an unsupported JSON value")
    return HttpResult(status_code=status_code, body=body)


def require_object(
    result: HttpResult, *, operation: str, expected_status: int = 200
) -> dict[str, object]:
    if result.status_code != expected_status:
        raise RuntimeError(f"{operation} returned HTTP {result.status_code}")
    if not isinstance(result.body, dict):
        raise RuntimeError(f"{operation} did not return a JSON object")
    return result.body


def require_page(result: HttpResult, *, operation: str) -> list[object]:
    body = require_object(result, operation=operation)
    items = body.get("items")
    if not isinstance(items, list):
        raise RuntimeError(f"{operation} did not return a page")
    return items


def require_list(result: HttpResult, *, operation: str) -> list[object]:
    if result.status_code != 200:
        raise RuntimeError(f"{operation} returned HTTP {result.status_code}")
    if not isinstance(result.body, list):
        raise RuntimeError(f"{operation} did not return a JSON list")
    return result.body


def discover_public_login(result: HttpResult) -> tuple[str, str, str]:
    discovery = require_object(result, operation="service discovery")
    login = discovery.get("demo_login")
    if not isinstance(login, dict):
        raise RuntimeError("service discovery did not publish a demo login")
    tenant = login.get("tenant_code")
    email = login.get("email")
    password = login.get("password")
    if (
        not isinstance(tenant, str)
        or not tenant
        or not isinstance(email, str)
        or not email
        or not isinstance(password, str)
        or not password
    ):
        raise RuntimeError("service discovery published an incomplete demo login")
    return tenant, email, password


def require_forbidden(result: HttpResult, *, operation: str) -> None:
    if result.status_code != 403:
        raise RuntimeError(f"{operation} returned HTTP {result.status_code}, expected 403")


def run_checks(
    base_url: str,
    *,
    timeout: float = 90.0,
    transport: Transport = request_json,
) -> list[str]:
    root = validate_base_url(base_url)
    demo_tenant, demo_email, demo_password = discover_public_login(
        transport("GET", f"{root}/", {}, None, timeout)
    )
    login = require_object(
        transport(
            "POST",
            f"{root}/v1/auth/login",
            {},
            {"tenant_code": demo_tenant, "email": demo_email, "password": demo_password},
            timeout,
        ),
        operation="login",
    )
    token = login.get("access_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("login response did not include an access token")
    headers = {"Authorization": f"Bearer {token}"}

    me = require_object(
        transport("GET", f"{root}/v1/me", headers, None, timeout), operation="current user"
    )
    if me.get("email") != demo_email:
        raise RuntimeError("current-user response did not identify the public reviewer")

    stores = require_page(
        transport("GET", f"{root}/v1/stores?limit=5", headers, None, timeout),
        operation="store discovery",
    )
    if not stores or not isinstance(stores[0], dict) or not isinstance(stores[0].get("id"), str):
        raise RuntimeError("store discovery returned no usable store")
    store_id = stores[0]["id"]

    zones = require_list(
        transport("GET", f"{root}/v1/stores/{store_id}/zones", headers, None, timeout),
        operation="zones",
    )
    if not zones or not isinstance(zones[0], dict):
        raise RuntimeError("zone discovery returned no seeded zone")
    zone_id = zones[0].get("id")
    zone_code = zones[0].get("code")
    if not isinstance(zone_id, str) or not isinstance(zone_code, str):
        raise RuntimeError("zone discovery returned an unusable zone")

    devices = require_list(
        transport("GET", f"{root}/v1/stores/{store_id}/devices", headers, None, timeout),
        operation="devices",
    )
    if not devices or not isinstance(devices[0], dict):
        raise RuntimeError("device discovery returned no seeded device")
    device = devices[0].get("device")
    if not isinstance(device, dict) or not isinstance(device.get("serial_number"), str):
        raise RuntimeError("device discovery returned an unusable device")
    device_serial = device["serial_number"]

    skus = require_page(
        transport("GET", f"{root}/v1/skus?limit=5", headers, None, timeout),
        operation="SKU discovery",
    )
    if not skus:
        raise RuntimeError("SKU discovery returned no seeded data")

    require_page(
        transport(
            "GET",
            f"{root}/v1/stores/{store_id}/inventory?limit=5",
            headers,
            None,
            timeout,
        ),
        operation="inventory",
    )
    require_page(
        transport(
            "GET",
            f"{root}/v1/replenishment-policies?limit=5",
            headers,
            None,
            timeout,
        ),
        operation="replenishment policies",
    )
    require_page(
        transport(
            "GET",
            f"{root}/v1/stores/{store_id}/replenishment-tasks?limit=5",
            headers,
            None,
            timeout,
        ),
        operation="replenishment tasks",
    )
    require_page(
        transport(
            "GET",
            f"{root}/v1/rfid/quarantine?limit=5",
            headers,
            None,
            timeout,
        ),
        operation="RFID quarantine",
    )

    mutation_checks: tuple[tuple[str, str, str, dict[str, object] | None], ...] = (
        (
            "user creation",
            "POST",
            "/v1/users",
            {
                "email": demo_email,
                "display_name": "Public API Reviewer",
                "password": demo_password,
                "roles": ["CORPORATE_USER"],
                "store_ids": [],
            },
        ),
        (
            "zone creation",
            "POST",
            f"/v1/stores/{store_id}/zones",
            {"code": zone_code, "name": "Existing Demo Zone", "kind": "OTHER"},
        ),
        (
            "device registration",
            "POST",
            f"/v1/stores/{store_id}/devices",
            {
                "serial_number": device_serial,
                "display_name": "Existing Demo Device",
                "zone_id": zone_id,
            },
        ),
        (
            "authoritative inventory removal",
            "POST",
            f"/v1/stores/{store_id}/business-events",
            {
                "source_system": "PUBLIC_REVIEWER",
                "external_event_id": "write-must-be-denied",
                "event_type": "SALE",
                "epc": "303400000000000000000001",
                "occurred_at": "2026-01-01T00:00:00Z",
            },
        ),
        (
            "policy activation",
            "POST",
            f"/v1/replenishment-policy-versions/{ZERO_UUID}/activate",
            None,
        ),
        (
            "replenishment evaluation",
            "POST",
            "/v1/replenishment/evaluations",
            {"store_id": ZERO_UUID, "sku_ids": []},
        ),
        (
            "device credential rotation",
            "POST",
            f"/v1/devices/{ZERO_UUID}/credentials:rotate",
            None,
        ),
        (
            "task mutation",
            "PATCH",
            f"/v1/replenishment-tasks/{ZERO_UUID}",
            {"status": "CLAIMED", "version": 1},
        ),
    )
    for operation, method, path, payload in mutation_checks:
        require_forbidden(
            transport(method, f"{root}{path}", headers, payload, timeout),
            operation=operation,
        )

    return [
        "discovery",
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
        "eight write categories denied",
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="https://abacus-take-home-api.onrender.com",
    )
    parser.add_argument("--timeout", type=float, default=90.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        checks = run_checks(args.base_url, timeout=args.timeout)
    except (RuntimeError, ValueError) as exc:
        print(f"PUBLIC DEMO FAILED: {exc}", file=sys.stderr)
        return 1
    for check in checks:
        print(f"PASS {check}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
