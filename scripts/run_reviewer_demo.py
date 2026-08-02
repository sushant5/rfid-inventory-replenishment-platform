"""Run the frozen reviewer demo against a local or hosted Abacus API.

The script intentionally uses only the Python standard library. Secrets come from
CLI arguments or environment variables and are never printed.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import secrets
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.generate_store_batch import build_store_batch  # noqa: E402,I001


DEFAULT_CATALOG_PATH = ROOT / "examples" / "catalog.csv"
DEFAULT_POLICY_PATH = ROOT / "examples" / "policies.json"

STORE_IDEMPOTENCY_KEY = "reviewer-demo-stores-v1"
CATALOG_IDEMPOTENCY_KEY = "reviewer-demo-catalog-v1"
POLICY_IDEMPOTENCY_KEY = "reviewer-demo-policies-v1"
DEMO_SKU_CODE = "SKU-TRAIL-BLUE-M"
TERMINAL_CATALOG_FAILURES = frozenset({"FAILED", "REJECTED"})
TERMINAL_RFID_FAILURES = frozenset({"LATE_IGNORED", "QUARANTINED"})

JsonValue = None | bool | int | float | str | list["JsonValue"] | dict[str, "JsonValue"]
JsonObject = dict[str, JsonValue]


class DemoError(RuntimeError):
    """A concise, reviewer-actionable demo failure."""


class ApiRequestError(DemoError):
    """An expected-status check failed for an API request."""

    def __init__(
        self,
        method: str,
        path: str,
        status: int,
        *,
        code: str | None = None,
        detail: str | None = None,
    ) -> None:
        self.method = method
        self.path = path
        self.status = status
        self.code = code
        self.detail = detail
        suffix = " ".join(part for part in (code, detail) if part)
        message = f"{method} {path} returned HTTP {status}"
        if suffix:
            message = f"{message}: {suffix}"
        super().__init__(message)


@dataclass(frozen=True, slots=True)
class HttpResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


class HttpTransport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HttpResponse: ...


class UrllibTransport:
    """Small stdlib transport; HTTP errors remain inspectable API responses."""

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HttpResponse:
        request = urllib.request.Request(  # noqa: S310 - ApiClient validates http(s).
            url,
            data=body,
            headers=dict(headers),
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310
                return HttpResponse(
                    status=response.status,
                    body=response.read(),
                    headers=dict(response.headers.items()),
                )
        except urllib.error.HTTPError as exc:
            return HttpResponse(
                status=exc.code,
                body=exc.read(),
                headers=dict(exc.headers.items()) if exc.headers is not None else {},
            )
        except urllib.error.URLError as exc:
            reason = getattr(exc, "reason", "connection failed")
            raise DemoError(
                f"Could not reach {urllib.parse.urlsplit(url).netloc}: {reason}"
            ) from exc


class ApiClient:
    def __init__(
        self,
        base_url: str,
        *,
        transport: HttpTransport,
        timeout_seconds: float,
    ) -> None:
        normalized = base_url.strip().rstrip("/")
        parsed = urllib.parse.urlsplit(normalized)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise DemoError("--base-url must be an absolute http:// or https:// URL")
        self.base_url = normalized
        self.transport = transport
        self.timeout_seconds = timeout_seconds

    def json(
        self,
        method: str,
        path: str,
        *,
        expected: int | set[int] = 200,
        headers: Mapping[str, str] | None = None,
        payload: JsonValue | None = None,
        query: Mapping[str, str | int | bool] | None = None,
    ) -> JsonValue:
        request_headers = {"Accept": "application/json", **(headers or {})}
        body: bytes | None = None
        if payload is not None:
            body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        return self._send(
            method,
            path,
            expected=expected,
            headers=request_headers,
            body=body,
            query=query,
        )

    def multipart(
        self,
        path: str,
        *,
        expected: int | set[int],
        headers: Mapping[str, str],
        fields: Mapping[str, str],
        file_field: str,
        file_path: Path,
        content_type: str,
    ) -> JsonValue:
        if not file_path.is_file():
            raise DemoError(f"Demo fixture is missing: {file_path}")
        boundary = f"abacus-{uuid.uuid4().hex}"
        body = encode_multipart(
            boundary=boundary,
            fields=fields,
            file_field=file_field,
            filename=file_path.name,
            content_type=content_type,
            file_bytes=file_path.read_bytes(),
        )
        return self._send(
            "POST",
            path,
            expected=expected,
            headers={
                "Accept": "application/json",
                "Content-Type": f"multipart/form-data; boundary={boundary}",
                **headers,
            },
            body=body,
        )

    def _send(
        self,
        method: str,
        path: str,
        *,
        expected: int | set[int],
        headers: Mapping[str, str],
        body: bytes | None,
        query: Mapping[str, str | int | bool] | None = None,
    ) -> JsonValue:
        expected_statuses = {expected} if isinstance(expected, int) else expected
        url = f"{self.base_url}/{path.lstrip('/')}"
        if query:
            url = f"{url}?{urllib.parse.urlencode(query)}"
        response = self.transport.request(
            method,
            url,
            headers=headers,
            body=body,
            timeout=self.timeout_seconds,
        )
        parsed = _decode_json(response.body)
        if response.status not in expected_statuses:
            error = parsed if isinstance(parsed, dict) else {}
            raise ApiRequestError(
                method,
                path,
                response.status,
                code=_optional_string(error.get("code")),
                detail=_optional_string(error.get("detail")),
            )
        return parsed


def encode_multipart(
    *,
    boundary: str,
    fields: Mapping[str, str],
    file_field: str,
    filename: str,
    content_type: str,
    file_bytes: bytes,
) -> bytes:
    """Encode the one-file multipart shape used by the catalog endpoint."""

    safe_filename = filename.replace('"', "_").replace("\r", "_").replace("\n", "_")
    parts: list[bytes] = []
    for name, value in fields.items():
        parts.extend(
            (
                f"--{boundary}\r\n".encode(),
                f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode(),
                value.encode(),
                b"\r\n",
            )
        )
    parts.extend(
        (
            f"--{boundary}\r\n".encode(),
            (
                f'Content-Disposition: form-data; name="{file_field}"; '
                f'filename="{safe_filename}"\r\n'
            ).encode(),
            f"Content-Type: {content_type}\r\n\r\n".encode(),
            file_bytes,
            b"\r\n",
            f"--{boundary}--\r\n".encode(),
        )
    )
    return b"".join(parts)


@dataclass(frozen=True, slots=True)
class DemoConfig:
    base_url: str
    platform_key: str
    tenant_code: str
    tenant_name: str
    admin_email: str
    admin_password: str
    manager_email: str
    manager_password: str | None
    associate_email: str
    associate_password: str | None
    catalog_path: Path = DEFAULT_CATALOG_PATH
    policy_path: Path = DEFAULT_POLICY_PATH
    request_timeout_seconds: float = 30.0
    poll_timeout_seconds: float = 90.0
    poll_interval_seconds: float = 0.5


@dataclass(frozen=True, slots=True)
class DemoLocation:
    store_id: str
    floor_device_id: str
    backroom_device_id: str
    floor_zone_id: str
    backroom_zone_id: str


@dataclass(frozen=True, slots=True)
class UserResult:
    user: JsonObject
    login_password: str | None


class ReviewerDemo:
    def __init__(
        self,
        config: DemoConfig,
        *,
        transport: HttpTransport | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        output: Callable[[str], None] = print,
    ) -> None:
        self.config = config
        self.client = ApiClient(
            config.base_url,
            transport=transport or UrllibTransport(),
            timeout_seconds=config.request_timeout_seconds,
        )
        self.sleeper = sleeper
        self.monotonic = monotonic
        self.output = output

    def run(self) -> None:
        self._check_service()
        tenant = self._ensure_tenant()
        tenant_id = _required_string(tenant, "id")
        location = self._ensure_stores_and_devices(tenant_id)
        admin_token, admin_principal = self._login(
            email=self.config.admin_email,
            password=self.config.admin_password,
            expected_tenant_id=tenant_id,
            expected_role="CORPORATE_ADMIN",
            expected_store_id=None,
        )
        self._ensure_scoped_user(
            token=admin_token,
            tenant_id=tenant_id,
            email=self.config.manager_email,
            display_name="Orange Demo Store Manager",
            role="STORE_MANAGER",
            store_id=location.store_id,
            configured_password=self.config.manager_password,
        )
        associate = self._ensure_scoped_user(
            token=admin_token,
            tenant_id=tenant_id,
            email=self.config.associate_email,
            display_name="Orange Demo Store Associate",
            role="STORE_ASSOCIATE",
            store_id=location.store_id,
            configured_password=self.config.associate_password,
        )
        self.output("PASS users: corporate admin plus store-scoped manager and associate are ready")

        self._import_catalog(tenant_id)
        sku = self._get_demo_sku(tenant_id, admin_token)
        sku_id = _required_string(sku, "id")
        epcs = _catalog_epcs(self.config.catalog_path, DEMO_SKU_CODE)
        event_ids = self._ingest_inventory(tenant_id, location, epcs)
        self._wait_for_rfid_projection(tenant_id, event_ids)
        floor_quantity, backroom_quantity = self._wait_for_inventory(
            tenant_id,
            admin_token,
            location.store_id,
            sku_id,
        )
        self.output(f"PASS RFID inventory: floor={floor_quantity}, backroom={backroom_quantity}")

        self._import_policy(tenant_id, admin_token)
        run_line = self._evaluate(tenant_id, admin_token, location.store_id, sku_id)
        task_id = _optional_string(run_line.get("task_id"))
        if task_id is None:
            reason = _optional_string(run_line.get("reason")) or "unknown reason"
            raise DemoError(f"Replenishment evaluation did not produce an active task: {reason}")

        actor_token = admin_token
        actor_principal = admin_principal
        associate_password = associate.login_password
        if associate_password is not None:
            actor_token, actor_principal = self._login(
                email=self.config.associate_email,
                password=associate_password,
                expected_tenant_id=tenant_id,
                expected_role="STORE_ASSOCIATE",
                expected_store_id=location.store_id,
                announce=False,
            )
        task = self._claim_task(
            tenant_id,
            actor_token,
            location.store_id,
            task_id,
            actor_user_id=_required_string(actor_principal, "user_id"),
        )
        self.output(
            "PASS replenishment: "
            f"reason={_required_string(run_line, 'reason')}, "
            f"recommended={_required_int(run_line, 'recommended_quantity')}, "
            f"task={_required_string(task, 'status')}"
        )
        self.output(f"PASS reviewer demo complete: {self.client.base_url}/docs")

    def _check_service(self) -> None:
        health = _as_object(self.client.json("GET", "/health/ready"), "readiness response")
        if health.get("status") != "ok":
            raise DemoError("Readiness endpoint did not report status=ok")
        version = _as_object(self.client.json("GET", "/version"), "version response")
        self.output(
            "PASS service: ready "
            f"(version={_required_string(version, 'version')}, "
            f"build={_required_string(version, 'build_sha')})"
        )

    def _ensure_tenant(self) -> JsonObject:
        tenant = _as_object(
            self.client.json(
                "POST",
                "/v1/platform/tenants",
                expected=201,
                headers=self._platform_headers(),
                payload={"code": self.config.tenant_code, "name": self.config.tenant_name},
            ),
            "tenant response",
        )
        if tenant.get("code") != self.config.tenant_code:
            raise DemoError("Tenant response did not match the configured tenant code")
        return tenant

    def _ensure_stores_and_devices(self, tenant_id: str) -> DemoLocation:
        payload = cast(JsonObject, build_store_batch(100))
        path = f"/v1/platform/tenants/{tenant_id}/stores:bulk-onboard"
        try:
            batch = _as_object(
                self.client.json(
                    "POST",
                    path,
                    expected=201,
                    headers={
                        **self._platform_headers(),
                        "Idempotency-Key": STORE_IDEMPOTENCY_KEY,
                    },
                    payload=payload,
                ),
                "onboarding response",
            )
            if batch.get("status") != "COMPLETED" or batch.get("succeeded_count") != 100:
                raise DemoError("The 100-store onboarding batch did not complete successfully")
        except ApiRequestError as exc:
            if exc.status != 409 or exc.code != "store_code_conflict":
                raise

        stores = _as_object_list(
            self.client.json(
                "GET",
                f"/v1/platform/tenants/{tenant_id}/stores",
                headers=self._platform_headers(),
            ),
            "store list",
        )
        expected_codes = {f"store{number:03d}" for number in range(1, 101)}
        stores_by_code = {_required_string(store, "code"): store for store in stores}
        missing_stores = sorted(expected_codes - stores_by_code.keys())
        if missing_stores:
            raise DemoError(
                f"Onboarding is missing expected stores: {', '.join(missing_stores[:5])}"
            )

        devices = _as_object_list(
            self.client.json(
                "GET",
                f"/v1/platform/tenants/{tenant_id}/devices",
                headers=self._platform_headers(),
            ),
            "device list",
        )
        devices_by_serial = {_required_string(item, "serial_number"): item for item in devices}
        expected_serials = {
            f"ORANGE-{number:04d}-{zone}" for number in range(1, 101) for zone in ("FLOOR", "BACK")
        }
        missing_devices = sorted(expected_serials - devices_by_serial.keys())
        if missing_devices:
            raise DemoError(
                f"Provisioning is missing expected devices: {', '.join(missing_devices[:5])}"
            )

        store_id = _required_string(stores_by_code["store001"], "id")
        floor_device_id = _required_string(devices_by_serial["ORANGE-0001-FLOOR"], "id")
        backroom_device_id = _required_string(devices_by_serial["ORANGE-0001-BACK"], "id")
        floor_assignment = self._active_assignment(tenant_id, floor_device_id)
        backroom_assignment = self._active_assignment(tenant_id, backroom_device_id)
        if (
            floor_assignment.get("store_id") != store_id
            or backroom_assignment.get("store_id") != store_id
        ):
            raise DemoError("Store 001 readers are not assigned to Store 001")
        floor_zone_id = _required_string(floor_assignment, "zone_id")
        backroom_zone_id = _required_string(backroom_assignment, "zone_id")
        if floor_zone_id == backroom_zone_id:
            raise DemoError("Floor and backroom readers unexpectedly share one zone")
        self.output("PASS onboarding: Orange has 100 stores, 200 readers, and zone assignments")
        return DemoLocation(
            store_id=store_id,
            floor_device_id=floor_device_id,
            backroom_device_id=backroom_device_id,
            floor_zone_id=floor_zone_id,
            backroom_zone_id=backroom_zone_id,
        )

    def _active_assignment(self, tenant_id: str, device_id: str) -> JsonObject:
        assignments = _as_object_list(
            self.client.json(
                "GET",
                f"/v1/platform/tenants/{tenant_id}/devices/{device_id}/assignments",
                headers=self._platform_headers(),
            ),
            "device assignment list",
        )
        active = [item for item in assignments if item.get("effective_to") is None]
        if len(active) != 1:
            raise DemoError(f"Device {device_id} must have exactly one active assignment")
        return active[0]

    def _login(
        self,
        *,
        email: str,
        password: str,
        expected_tenant_id: str,
        expected_role: str,
        expected_store_id: str | None,
        announce: bool = True,
    ) -> tuple[str, JsonObject]:
        login = _as_object(
            self.client.json(
                "POST",
                "/v1/auth/login",
                payload={
                    "tenant_code": self.config.tenant_code,
                    "email": email,
                    "password": password,
                },
            ),
            "login response",
        )
        token = _required_string(login, "access_token")
        principal = _as_object(
            self.client.json(
                "GET",
                "/v1/auth/me",
                headers=self._bearer_headers(token),
            ),
            "current principal",
        )
        if principal.get("tenant_id") != expected_tenant_id:
            raise DemoError("Authenticated reviewer belongs to a different tenant")
        assignments = _as_object_list(principal.get("role_assignments"), "role assignments")
        if not any(
            item.get("role") == expected_role and item.get("store_id") == expected_store_id
            for item in assignments
        ):
            raise DemoError(f"Authenticated user does not have expected {expected_role} scope")
        if announce:
            self.output("PASS identity: reviewer login and tenant-scoped JWT verified")
        return token, principal

    def _ensure_scoped_user(
        self,
        *,
        token: str,
        tenant_id: str,
        email: str,
        display_name: str,
        role: str,
        store_id: str,
        configured_password: str | None,
    ) -> UserResult:
        users = _as_object(
            self.client.json(
                "GET",
                "/v1/users",
                headers=self._bearer_headers(token),
                query={"limit": 100, "offset": 0},
            ),
            "user list",
        )
        items = _as_object_list(users.get("items"), "user list items")
        existing = next(
            (item for item in items if _optional_string(item.get("email")) == email.lower()),
            None,
        )
        expected_assignment = {(role, store_id)}
        if existing is not None:
            if existing.get("tenant_id") != tenant_id or existing.get("status") != "ACTIVE":
                raise DemoError(f"Existing demo user {email} is not active in Orange")
            actual_assignments = {
                (_required_string(item, "role"), _optional_string(item.get("store_id")))
                for item in _as_object_list(
                    existing.get("role_assignments"),
                    f"role assignments for {email}",
                )
            }
            if actual_assignments != expected_assignment:
                raise DemoError(
                    f"Existing demo user {email} has different access; refusing to broaden it"
                )
            return UserResult(existing, configured_password)

        password = configured_password or secrets.token_urlsafe(24)
        created = _as_object(
            self.client.json(
                "POST",
                "/v1/users",
                expected=201,
                headers=self._bearer_headers(token),
                payload={
                    "email": email,
                    "display_name": display_name,
                    "password": password,
                    "role_assignments": [{"role": role, "store_id": store_id}],
                },
            ),
            "create user response",
        )
        if created.get("tenant_id") != tenant_id:
            raise DemoError("Created user response belongs to a different tenant")
        return UserResult(created, password)

    def _import_catalog(self, tenant_id: str) -> None:
        path = f"/v1/tenants/{tenant_id}/catalog/imports"
        catalog_import = _as_object(
            self.client.multipart(
                path,
                expected=202,
                headers={
                    **self._platform_headers(),
                    "Idempotency-Key": CATALOG_IDEMPOTENCY_KEY,
                },
                fields={"mode": "DELTA"},
                file_field="file",
                file_path=self.config.catalog_path,
                content_type="text/csv",
            ),
            "catalog import response",
        )
        import_id = _required_string(catalog_import, "id")

        def fetch() -> JsonObject:
            return _as_object(
                self.client.json(
                    "GET",
                    f"{path}/{import_id}",
                    headers=self._platform_headers(),
                ),
                "catalog import status",
            )

        completed = self._poll(
            "catalog import",
            fetch,
            done=lambda item: item.get("status") == "COMPLETED",
            failed=lambda item: item.get("status") in TERMINAL_CATALOG_FAILURES,
        )
        self.output(
            f"PASS catalog: {_required_int(completed, 'total_rows')} rows promoted by the worker"
        )

    def _get_demo_sku(self, tenant_id: str, token: str) -> JsonObject:
        response = _as_object(
            self.client.json(
                "GET",
                f"/v1/tenants/{tenant_id}/catalog/skus",
                headers=self._bearer_headers(token),
                query={"code": DEMO_SKU_CODE, "limit": 10, "offset": 0},
            ),
            "SKU list",
        )
        matches = [
            item
            for item in _as_object_list(response.get("items"), "SKU list items")
            if item.get("code") == DEMO_SKU_CODE
        ]
        if len(matches) != 1:
            raise DemoError(f"Expected exactly one active {DEMO_SKU_CODE} SKU")
        return matches[0]

    def _ingest_inventory(
        self,
        tenant_id: str,
        location: DemoLocation,
        epcs: Sequence[str],
    ) -> set[str]:
        floor_key = self._rotate_device_key(tenant_id, location.floor_device_id)
        backroom_key = self._rotate_device_key(tenant_id, location.backroom_device_id)
        observed_at = datetime.now(UTC).isoformat()
        groups = (
            ("floor", floor_key, epcs[:1]),
            ("backroom", backroom_key, epcs[1:4]),
        )
        all_event_ids: set[str] = set()
        for group_name, device_key, group_epcs in groups:
            observations: list[JsonValue] = []
            for sequence, epc in enumerate(group_epcs, start=1):
                event_id = str(uuid.uuid4())
                all_event_ids.add(event_id)
                observations.append(
                    {
                        "event_id": event_id,
                        "epc": epc,
                        "observed_at": observed_at,
                        "reader_sequence": sequence,
                        "antenna_port": 1,
                        "rssi_dbm": -48.5,
                    }
                )
            receipt = _as_object(
                self.client.json(
                    "POST",
                    "/v1/device/read-batches",
                    expected=202,
                    headers={"X-Device-Key": device_key},
                    payload={
                        "batch_id": f"reviewer-{group_name}-{uuid.uuid4()}",
                        "observations": observations,
                    },
                ),
                "RFID receipt",
            )
            if receipt.get("accepted_count") != len(group_epcs) or receipt.get(
                "conflict_count"
            ) not in {0, None}:
                raise DemoError(f"The {group_name} RFID batch was not fully accepted")
        return all_event_ids

    def _rotate_device_key(self, tenant_id: str, device_id: str) -> str:
        response = _as_object(
            self.client.json(
                "POST",
                f"/v1/platform/tenants/{tenant_id}/devices/{device_id}/credentials:rotate",
                headers=self._platform_headers(),
            ),
            "device credential response",
        )
        return _required_string(response, "api_key")

    def _wait_for_rfid_projection(self, tenant_id: str, event_ids: set[str]) -> None:
        def fetch() -> JsonObject:
            return _as_object(
                self.client.json(
                    "GET",
                    f"/v1/platform/tenants/{tenant_id}/rfid/observations",
                    headers=self._platform_headers(),
                    query={"limit": 100, "offset": 0},
                ),
                "RFID observation list",
            )

        def statuses(response: JsonObject) -> dict[str, str]:
            return {
                _required_string(item, "event_id"): _required_string(item, "status")
                for item in _as_object_list(response.get("items"), "RFID observations")
                if item.get("event_id") in event_ids
            }

        self._poll(
            "RFID projection",
            fetch,
            done=lambda response: (
                statuses(response).keys() == event_ids
                and all(status == "PROCESSED" for status in statuses(response).values())
            ),
            failed=lambda response: any(
                status in TERMINAL_RFID_FAILURES for status in statuses(response).values()
            ),
        )

    def _wait_for_inventory(
        self,
        tenant_id: str,
        token: str,
        store_id: str,
        sku_id: str,
    ) -> tuple[int, int]:
        path = f"/v1/tenants/{tenant_id}/inventory"

        def fetch() -> JsonObject:
            return _as_object(
                self.client.json(
                    "GET",
                    path,
                    headers=self._bearer_headers(token),
                    query={"store_id": store_id, "limit": 100, "offset": 0},
                ),
                "inventory response",
            )

        def quantities(response: JsonObject) -> tuple[int, int]:
            items = [
                item
                for item in _as_object_list(response.get("items"), "inventory items")
                if item.get("sku_id") == sku_id
            ]
            floor = sum(
                _required_int(item, "quantity")
                for item in items
                if item.get("zone_kind") == "SALES_FLOOR"
            )
            backroom = sum(
                _required_int(item, "quantity")
                for item in items
                if item.get("zone_kind") == "BACKROOM"
            )
            return floor, backroom

        inventory = self._poll(
            "inventory projection",
            fetch,
            done=lambda response: quantities(response)[0] >= 1 and quantities(response)[1] >= 3,
            failed=lambda _: False,
        )
        return quantities(inventory)

    def _import_policy(self, tenant_id: str, token: str) -> None:
        if not self.config.policy_path.is_file():
            raise DemoError(f"Demo fixture is missing: {self.config.policy_path}")
        payload = _decode_json(self.config.policy_path.read_bytes())
        policy_import = _as_object(
            self.client.json(
                "POST",
                f"/v1/tenants/{tenant_id}/replenishment/policies:bulk-upsert",
                expected=201,
                headers={
                    **self._bearer_headers(token),
                    "Idempotency-Key": POLICY_IDEMPOTENCY_KEY,
                },
                payload=payload,
            ),
            "policy import response",
        )
        if policy_import.get("status") != "COMPLETED" or policy_import.get("rejected_count") != 0:
            raise DemoError("The replenishment policy import was rejected")
        self.output("PASS policy: SKU replenishment rule is active")

    def _evaluate(
        self,
        tenant_id: str,
        token: str,
        store_id: str,
        sku_id: str,
    ) -> JsonObject:
        path = f"/v1/tenants/{tenant_id}/replenishment/evaluations"
        idempotency_key = f"reviewer-evaluation-{uuid.uuid4()}"
        for attempt in range(3):
            try:
                run = _as_object(
                    self.client.json(
                        "POST",
                        path,
                        expected=201,
                        headers={
                            **self._bearer_headers(token),
                            "Idempotency-Key": idempotency_key,
                        },
                        payload={
                            "store_id": store_id,
                            "sku_ids": [sku_id],
                            "generate_tasks": True,
                        },
                    ),
                    "replenishment evaluation",
                )
                break
            except ApiRequestError as exc:
                if exc.code != "concurrent_replenishment_conflict" or attempt == 2:
                    raise
                self.sleeper(self.config.poll_interval_seconds)
        else:  # pragma: no cover - loop exits by success or exception
            raise AssertionError("unreachable")
        lines = _as_object_list(run.get("lines"), "replenishment evaluation lines")
        matches = [item for item in lines if item.get("sku_id") == sku_id]
        if len(matches) != 1:
            raise DemoError("Evaluation did not return exactly one line for the demo SKU")
        line = matches[0]
        if line.get("policy_id") is None:
            raise DemoError("Evaluation did not resolve the imported policy")
        return line

    def _claim_task(
        self,
        tenant_id: str,
        token: str,
        store_id: str,
        task_id: str,
        *,
        actor_user_id: str,
    ) -> JsonObject:
        response = _as_object(
            self.client.json(
                "GET",
                f"/v1/tenants/{tenant_id}/replenishment/tasks",
                headers=self._bearer_headers(token),
                query={"store_id": store_id, "limit": 100, "offset": 0},
            ),
            "task list",
        )
        matches = [
            item
            for item in _as_object_list(response.get("items"), "task list items")
            if item.get("id") == task_id
        ]
        if len(matches) != 1:
            raise DemoError("The generated replenishment task was not readable by the task actor")
        task = matches[0]
        current_status = _required_string(task, "status")
        if current_status == "CLAIMED":
            owner = _optional_string(task.get("claimed_by_subject"))
            if owner not in {None, actor_user_id}:
                # A rerun must not steal a reviewer-visible task from its owner.
                return task
            return task
        if current_status != "OPEN":
            raise DemoError(f"Expected an OPEN task, but task is {current_status}")
        return _as_object(
            self.client.json(
                "PATCH",
                f"/v1/tenants/{tenant_id}/replenishment/tasks/{task_id}",
                headers=self._bearer_headers(token),
                payload={
                    "status": "CLAIMED",
                    "expected_version": _required_int(task, "version"),
                    "note": "Claimed by reviewer demo",
                },
            ),
            "task update response",
        )

    def _poll(
        self,
        label: str,
        fetch: Callable[[], JsonObject],
        *,
        done: Callable[[JsonObject], bool],
        failed: Callable[[JsonObject], bool],
    ) -> JsonObject:
        deadline = self.monotonic() + self.config.poll_timeout_seconds
        while True:
            value = fetch()
            if done(value):
                return value
            if failed(value):
                status = _optional_string(value.get("status")) or "terminal failure"
                raise DemoError(f"{label} stopped in {status}")
            if self.monotonic() >= deadline:
                raise DemoError(
                    f"Timed out waiting for {label}; verify that the background worker is running"
                )
            self.sleeper(self.config.poll_interval_seconds)

    def _platform_headers(self) -> dict[str, str]:
        return {"X-Platform-Key": self.config.platform_key}

    @staticmethod
    def _bearer_headers(token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}


def _catalog_epcs(path: Path, sku_code: str) -> list[str]:
    if not path.is_file():
        raise DemoError(f"Demo fixture is missing: {path}")
    with path.open(encoding="utf-8", newline="") as handle:
        epcs = [
            (row.get("epc") or "").strip().upper()
            for row in csv.DictReader(handle)
            if (row.get("sku") or "").strip().upper() == sku_code
        ]
    unique_epcs = list(dict.fromkeys(epc for epc in epcs if epc))
    if len(unique_epcs) < 4:
        raise DemoError(f"Catalog fixture needs at least four EPCs for {sku_code}")
    return unique_epcs


def _decode_json(body: bytes) -> JsonValue:
    if not body:
        return None
    try:
        return cast(JsonValue, json.loads(body))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise DemoError("API returned a non-JSON response") from exc


def _as_object(value: JsonValue, label: str) -> JsonObject:
    if not isinstance(value, dict):
        raise DemoError(f"{label} must be a JSON object")
    return value


def _as_object_list(value: JsonValue, label: str) -> list[JsonObject]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise DemoError(f"{label} must be a JSON object list")
    return cast(list[JsonObject], value)


def _required_string(value: JsonObject, key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise DemoError(f"API response is missing string field '{key}'")
    return result


def _optional_string(value: JsonValue) -> str | None:
    return value if isinstance(value, str) and value else None


def _required_int(value: JsonObject, key: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise DemoError(f"API response is missing integer field '{key}'")
    return result


def _environment(*names: str, default: str | None = None) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value is not None and value != "":
            return value
    return default


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the idempotent Orange reviewer demo against an Abacus deployment."
    )
    parser.add_argument(
        "--base-url",
        default=_environment("DEMO_BASE_URL", "ABACUS_DEMO_BASE_URL"),
        help="API origin (env: DEMO_BASE_URL)",
    )
    parser.add_argument(
        "--platform-key",
        default=_environment("PLATFORM_API_KEY"),
        help="Platform integration key (env: PLATFORM_API_KEY; prefer env)",
    )
    parser.add_argument(
        "--tenant-code",
        default=_environment("BOOTSTRAP_TENANT_CODE", default="orange"),
    )
    parser.add_argument(
        "--tenant-name",
        default=_environment("BOOTSTRAP_TENANT_NAME", default="Orange"),
    )
    parser.add_argument(
        "--admin-email",
        default=_environment("BOOTSTRAP_ADMIN_EMAIL"),
        help="Bootstrapped reviewer email (env: BOOTSTRAP_ADMIN_EMAIL)",
    )
    parser.add_argument(
        "--admin-password",
        default=_environment("BOOTSTRAP_ADMIN_PASSWORD"),
        help="Bootstrapped reviewer password (env: BOOTSTRAP_ADMIN_PASSWORD; prefer env)",
    )
    parser.add_argument(
        "--manager-email",
        default=_environment("DEMO_MANAGER_EMAIL", default="manager.demo@orange.example"),
    )
    parser.add_argument(
        "--manager-password",
        default=_environment("DEMO_MANAGER_PASSWORD"),
        help="Optional reusable manager password (env: DEMO_MANAGER_PASSWORD; prefer env)",
    )
    parser.add_argument(
        "--associate-email",
        default=_environment("DEMO_ASSOCIATE_EMAIL", default="associate.demo@orange.example"),
    )
    parser.add_argument(
        "--associate-password",
        default=_environment("DEMO_ASSOCIATE_PASSWORD"),
        help="Optional reusable associate password (env: DEMO_ASSOCIATE_PASSWORD; prefer env)",
    )
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG_PATH)
    parser.add_argument("--policies", type=Path, default=DEFAULT_POLICY_PATH)
    parser.add_argument("--request-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--poll-timeout-seconds", type=float, default=90.0)
    return parser


def _config(arguments: argparse.Namespace, parser: argparse.ArgumentParser) -> DemoConfig:
    required = {
        "--base-url / DEMO_BASE_URL": arguments.base_url,
        "--platform-key / PLATFORM_API_KEY": arguments.platform_key,
        "--admin-email / BOOTSTRAP_ADMIN_EMAIL": arguments.admin_email,
        "--admin-password / BOOTSTRAP_ADMIN_PASSWORD": arguments.admin_password,
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        parser.error(f"missing required configuration: {', '.join(missing)}")
    if arguments.request_timeout_seconds <= 0 or arguments.poll_timeout_seconds <= 0:
        parser.error("timeout values must be positive")
    return DemoConfig(
        base_url=arguments.base_url,
        platform_key=arguments.platform_key,
        tenant_code=arguments.tenant_code,
        tenant_name=arguments.tenant_name,
        admin_email=arguments.admin_email,
        admin_password=arguments.admin_password,
        manager_email=arguments.manager_email,
        manager_password=arguments.manager_password,
        associate_email=arguments.associate_email,
        associate_password=arguments.associate_password,
        catalog_path=arguments.catalog,
        policy_path=arguments.policies,
        request_timeout_seconds=arguments.request_timeout_seconds,
        poll_timeout_seconds=arguments.poll_timeout_seconds,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = _parser()
    arguments = parser.parse_args(argv)
    config = _config(arguments, parser)
    try:
        ReviewerDemo(config).run()
    except DemoError as exc:
        print(f"FAIL reviewer demo: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
