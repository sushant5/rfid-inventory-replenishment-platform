"""Run dependency-free smoke checks against a deployed Abacus API."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from dataclasses import dataclass
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

JsonObject = dict[str, object]
Fetcher = Callable[[str, float], JsonObject]


@dataclass(frozen=True, slots=True)
class CheckResult:
    name: str
    detail: str


def validate_base_url(base_url: str) -> str:
    candidate = base_url.rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("base URL must be an absolute http(s) URL")
    return candidate


def fetch_json(url: str, timeout: float) -> JsonObject:
    validate_base_url(url)
    request = Request(  # noqa: S310 - validate_base_url permits only HTTP(S).
        url, headers={"Accept": "application/json"}
    )
    try:
        with urlopen(request, timeout=timeout) as response:  # noqa: S310
            body = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        raise RuntimeError(f"{url} returned HTTP {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(f"could not reach {url}: {exc.reason}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"{url} did not return JSON") from exc
    if not isinstance(body, dict):
        raise RuntimeError(f"{url} did not return a JSON object")
    return body


def run_checks(
    base_url: str,
    *,
    timeout: float = 10.0,
    fetcher: Fetcher = fetch_json,
) -> list[CheckResult]:
    root = validate_base_url(base_url)
    live = fetcher(f"{root}/health/live", timeout)
    if live.get("status") != "ok":
        raise RuntimeError("liveness response did not report status=ok")

    ready = fetcher(f"{root}/health/ready", timeout)
    if ready.get("status") != "ok":
        raise RuntimeError("readiness response did not report status=ok")

    version = fetcher(f"{root}/version", timeout)
    version_number = version.get("version")
    if not isinstance(version_number, str) or not version_number:
        raise RuntimeError("version response is missing a non-empty version")
    if not isinstance(version.get("environment"), str):
        raise RuntimeError("version response is missing environment")

    specification = fetcher(f"{root}/openapi.json", timeout)
    openapi_version = specification.get("openapi")
    paths = specification.get("paths")
    if not isinstance(openapi_version, str) or not openapi_version.startswith("3."):
        raise RuntimeError("OpenAPI document is missing a 3.x version")
    if not isinstance(paths, dict):
        raise RuntimeError("OpenAPI document is missing paths")
    required_paths = {
        "/health/live",
        "/health/ready",
        "/version",
        "/v1/rfid/observation-batches",
        "/v1/stores/{store_id}/inventory",
    }
    missing = sorted(required_paths - paths.keys())
    if missing:
        raise RuntimeError(f"OpenAPI document is missing required paths: {', '.join(missing)}")

    return [
        CheckResult("liveness", "ok"),
        CheckResult("readiness", "ok"),
        CheckResult("version", version_number),
        CheckResult("openapi", f"{openapi_version}; {len(paths)} paths"),
    ]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--timeout", type=float, default=10.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        results = run_checks(args.base_url, timeout=args.timeout)
    except (RuntimeError, ValueError) as exc:
        print(f"SMOKE TEST FAILED: {exc}", file=sys.stderr)
        return 1
    for result in results:
        print(f"PASS {result.name}: {result.detail}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
