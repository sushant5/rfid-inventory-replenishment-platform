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


def run_checks(
    base_url: str,
    *,
    timeout: float = 90.0,
    transport: Transport = request_json,
) -> list[str]:
    root = validate_base_url(base_url)
    login = require_object(
        transport(
            "POST",
            f"{root}/v1/auth/login",
            {},
            {"tenant_code": DEMO_TENANT, "email": DEMO_EMAIL, "password": DEMO_PASSWORD},
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
    if me.get("email") != DEMO_EMAIL:
        raise RuntimeError("current-user response did not identify the public reviewer")

    stores = require_page(
        transport("GET", f"{root}/v1/stores?limit=5", headers, None, timeout),
        operation="store discovery",
    )
    if not stores or not isinstance(stores[0], dict) or not isinstance(stores[0].get("id"), str):
        raise RuntimeError("store discovery returned no usable store")
    store_id = stores[0]["id"]

    require_list(
        transport("GET", f"{root}/v1/stores/{store_id}/zones", headers, None, timeout),
        operation="zones",
    )
    require_list(
        transport("GET", f"{root}/v1/stores/{store_id}/devices", headers, None, timeout),
        operation="devices",
    )

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

    denied = transport(
        "POST",
        f"{root}/v1/replenishment/evaluations",
        headers,
        {"store_id": store_id, "sku_ids": []},
        timeout,
    )
    if denied.status_code != 403:
        raise RuntimeError(
            f"read-only mutation check returned HTTP {denied.status_code}, expected 403"
        )

    return [
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
