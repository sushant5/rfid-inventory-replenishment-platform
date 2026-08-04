"""Exercise the end-to-end RFID workflow against a running Compose stack."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "examples" / "catalog.csv"
EPCS = (
    "3074257BF7194E4000001A85",
    "3074257BF7194E4000001A86",
    "3074257BF7194E4000001A87",
    "3074257BF7194E4000001A88",
)


class DemoFailure(RuntimeError):
    pass


class Client:
    def __init__(self, base_url: str, *, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        expected: int | set[int] = 200,
        headers: dict[str, str] | None = None,
        payload: object | None = None,
        body: bytes | None = None,
        content_type: str | None = None,
    ) -> tuple[int, Any]:
        request_headers = {"Accept": "application/json", **(headers or {})}
        if payload is not None:
            body = json.dumps(payload).encode()
            content_type = "application/json"
        if content_type is not None:
            request_headers["Content-Type"] = content_type
        request = urllib.request.Request(  # noqa: S310 - base URL is reviewer supplied.
            f"{self.base_url}{path}",
            data=body,
            headers=request_headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:  # noqa: S310
                status_code = response.status
                response_body = response.read()
        except urllib.error.HTTPError as exc:
            status_code = exc.code
            response_body = exc.read()
        except urllib.error.URLError as exc:
            raise DemoFailure(f"Cannot reach {self.base_url}: {exc.reason}") from exc
        expected_codes = {expected} if isinstance(expected, int) else expected
        try:
            decoded = json.loads(response_body) if response_body else None
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            preview = " ".join(response_body.decode("utf-8", errors="replace").split())[:256]
            raise DemoFailure(
                f"{method} {path} returned {status_code} with a non-JSON response: {preview!r}"
            ) from exc
        if status_code not in expected_codes:
            raise DemoFailure(f"{method} {path} returned {status_code}: {decoded}")
        return status_code, decoded

    def multipart_catalog(
        self,
        tenant_id: str,
        *,
        platform_key: str,
    ) -> dict[str, Any]:
        boundary = f"abacus-{uuid.uuid4().hex}"
        content = CATALOG.read_bytes()
        body = b"".join(
            (
                f"--{boundary}\r\n".encode(),
                b'Content-Disposition: form-data; name="mode"\r\n\r\n',
                b"FULL\r\n",
                f"--{boundary}\r\n".encode(),
                b'Content-Disposition: form-data; name="file"; filename="catalog.csv"\r\n',
                b"Content-Type: text/csv\r\n\r\n",
                content,
                b"\r\n",
                f"--{boundary}--\r\n".encode(),
            )
        )
        _, result = self.request(
            "POST",
            f"/v1/tenants/{tenant_id}/catalog-imports",
            expected=202,
            headers={
                "X-Platform-Key": platform_key,
                "Idempotency-Key": "orange-demo-catalog-v2",
            },
            body=body,
            content_type=f"multipart/form-data; boundary={boundary}",
        )
        return object_result(result, "catalog import")


def object_result(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise DemoFailure(f"{label} was not a JSON object")
    return value


def list_result(value: Any, label: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise DemoFailure(f"{label} was not a JSON object list")
    return value


def page_items(value: Any, label: str) -> list[dict[str, Any]]:
    page = object_result(value, label)
    return list_result(required(page, "items"), f"{label} items")


def required(value: dict[str, Any], field: str) -> Any:
    result = value.get(field)
    if result is None:
        raise DemoFailure(f"response is missing {field}")
    return result


def poll(label: str, fetch: Any, done: Any, *, timeout: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    while True:
        result = object_result(fetch(), label)
        if done(result):
            return result
        if time.monotonic() >= deadline:
            raise DemoFailure(f"timed out waiting for {label}: {result}")
        time.sleep(0.25)


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def login(client: Client, email: str, password: str) -> str:
    _, result = client.request(
        "POST",
        "/v1/auth/login",
        payload={"tenant_code": "orange", "email": email, "password": password},
    )
    return str(required(object_result(result, "login"), "access_token"))


def create_scoped_user(
    client: Client,
    admin_token: str,
    *,
    email: str,
    password: str,
    role: str,
    store_ids: list[str],
) -> tuple[str, str]:
    _, created = client.request(
        "POST",
        "/v1/users",
        expected=201,
        headers=bearer(admin_token),
        payload={
            "email": email,
            "display_name": f"Demo {role.replace('_', ' ').title()}",
            "password": password,
            "roles": [role],
            "store_ids": store_ids,
        },
    )
    user_id = str(required(object_result(created, "user"), "id"))
    return user_id, login(client, email, password)


def observation(event_id: str, epc: str, at: datetime, rssi: float) -> dict[str, object]:
    return {
        "event_id": event_id,
        "epc": epc,
        "observed_at": at.isoformat(),
        "rssi": rssi,
        "antenna_id": "demo-antenna-1",
    }


def submit_batch(
    client: Client,
    *,
    device_id: str,
    device_token: str,
    observations: list[dict[str, object]],
) -> str:
    _, result = client.request(
        "POST",
        "/v1/rfid/observation-batches",
        expected=202,
        headers={"X-Device-Token": device_token},
        payload={
            "device_id": device_id,
            "observations": observations,
            "backlog_drained": True,
            "reader_coverage_ok": True,
        },
    )
    return str(required(object_result(result, "RFID acceptance"), "batch_id"))


def wait_for_batch(client: Client, token: str, batch_id: str, timeout: float) -> None:
    result = poll(
        f"RFID batch {batch_id}",
        lambda: client.request(
            "GET",
            f"/v1/rfid/observation-batches/{batch_id}",
            headers=bearer(token),
        )[1],
        lambda item: int(item["pending"]) == 0,
        timeout=timeout,
    )
    if int(result["rejected"]) != 0:
        raise DemoFailure(f"RFID batch rejected records: {result}")


def wait_for_readiness(client: Client, timeout: float) -> None:
    """Wake a sleeping free-tier service using an idempotent readiness probe."""

    deadline = time.monotonic() + timeout
    last_error = "service has not answered"
    while True:
        try:
            client.request("GET", "/health/ready")
            return
        except DemoFailure as exc:
            last_error = str(exc)
        if time.monotonic() >= deadline:
            raise DemoFailure(f"timed out waiting for service readiness: {last_error}")
        time.sleep(1)


def run(args: argparse.Namespace) -> None:
    client = Client(args.base_url, timeout=args.request_timeout)
    platform_headers = {"X-Platform-Key": args.platform_key}
    wait_for_readiness(client, args.startup_timeout)
    client.request("GET", "/health/live")

    _, tenant = client.request(
        "POST",
        "/v1/tenants",
        expected=201,
        headers=platform_headers,
        payload={"code": "orange", "name": "Orange"},
    )
    tenant_id = str(required(object_result(tenant, "tenant"), "id"))
    admin_token = login(client, args.admin_email, args.admin_password)

    stores_payload = {
        "stores": [
            {
                "code": code,
                "name": name,
                "timezone": "America/Los_Angeles",
                "organization_path": [{"code": "west", "name": "West", "unit_type": "REGION"}],
                "zones": [
                    {"code": "floor", "name": "Sales Floor", "kind": "SALES_FLOOR"},
                    {"code": "backroom", "name": "Backroom", "kind": "BACKROOM"},
                ],
                "devices": [],
                "configuration": {},
            }
            for code, name in (("orange-001", "Orange Store 1"), ("orange-002", "Orange Store 2"))
        ]
    }
    client.request(
        "POST",
        f"/v1/tenants/{tenant_id}/store-imports",
        expected=201,
        headers={**platform_headers, "Idempotency-Key": "orange-demo-stores-v2"},
        payload=stores_payload,
    )
    _, stores_value = client.request(
        "GET", f"/v1/tenants/{tenant_id}/stores", headers=platform_headers
    )
    stores = {str(item["code"]): item for item in list_result(stores_value, "stores")}
    store1_id = str(required(stores["orange-001"], "id"))
    store2_id = str(required(stores["orange-002"], "id"))
    run_id = uuid.uuid4().hex[:10].upper()
    client.request(
        "POST",
        f"/v1/stores/{store2_id}/zones",
        expected=201,
        headers=bearer(admin_token),
        payload={
            "code": f"audit-{run_id.lower()}",
            "name": "Demo Audit Zone",
            "kind": "OTHER",
        },
    )
    _, zones_value = client.request(
        "GET", f"/v1/stores/{store1_id}/zones", headers=bearer(admin_token)
    )
    zones = {str(item["code"]): item for item in list_result(zones_value, "zones")}

    devices: dict[str, dict[str, Any]] = {}
    for code in ("floor", "backroom"):
        _, registration = client.request(
            "POST",
            f"/v1/stores/{store1_id}/devices",
            expected=201,
            headers=bearer(admin_token),
            payload={
                "serial_number": f"ORANGE-{code.upper()}-{run_id}",
                "display_name": f"Demo {code} reader",
                "zone_id": required(zones[code], "id"),
            },
        )
        devices[code] = object_result(registration, f"{code} device")

    catalog_import = client.multipart_catalog(tenant_id, platform_key=args.platform_key)
    import_id = str(required(catalog_import, "id"))
    completed_import = poll(
        "catalog promotion",
        lambda: client.request(
            "GET",
            f"/v1/catalog-imports/{import_id}",
            headers=platform_headers,
        )[1],
        lambda item: item["status"] in {"COMPLETED", "FAILED", "REJECTED"},
        timeout=args.poll_timeout,
    )
    if completed_import["status"] != "COMPLETED":
        raise DemoFailure(f"catalog import failed: {completed_import}")
    _, import_errors_value = client.request(
        "GET",
        f"/v1/catalog-imports/{import_id}/errors",
        headers=platform_headers,
    )
    import_errors = object_result(import_errors_value, "catalog import errors")
    if int(required(import_errors, "total")) != 0:
        raise DemoFailure(f"catalog import unexpectedly reported errors: {import_errors}")

    _, policy_value = client.request(
        "POST",
        "/v1/replenishment-policies",
        expected=201,
        headers=bearer(admin_token),
        payload={
            "name": f"Orange demo policy {run_id}",
            "description": "Demo tenant default",
            "rules": [
                {
                    "min_floor_qty": 2,
                    "target_floor_qty": 3,
                    "priority": int(run_id[:5], 16) % 1_000_000,
                }
            ],
        },
    )
    policy = object_result(policy_value, "policy")
    policy_id = str(required(object_result(required(policy, "policy"), "policy definition"), "id"))
    version_id = str(required(object_result(required(policy, "version"), "version"), "id"))
    _, policy_list_value = client.request(
        "GET",
        "/v1/replenishment-policies",
        headers=bearer(admin_token),
    )
    policy_list = object_result(policy_list_value, "policy list")
    policy_items = list_result(required(policy_list, "items"), "policy items")
    if not any(str(item["policy"]["id"]) == policy_id for item in policy_items):
        raise DemoFailure("created replenishment policy was not discoverable")
    client.request(
        "GET",
        f"/v1/replenishment-policies/{policy_id}",
        headers=bearer(admin_token),
    )
    client.request(
        "POST",
        f"/v1/replenishment-policy-versions/{version_id}/activate",
        headers=bearer(admin_token),
    )
    _, draft_value = client.request(
        "POST",
        f"/v1/replenishment-policies/{policy_id}/versions",
        expected=201,
        headers=bearer(admin_token),
    )
    draft = object_result(draft_value, "draft policy version")
    draft_version_id = str(
        required(object_result(required(draft, "version"), "draft version"), "id")
    )
    client.request(
        "PATCH",
        f"/v1/replenishment-policy-versions/{draft_version_id}",
        headers=bearer(admin_token),
        payload={
            "rules": [
                {
                    "min_floor_qty": 2,
                    "target_floor_qty": 4,
                    "priority": int(run_id[:5], 16) % 1_000_000,
                }
            ]
        },
    )

    floor_device = object_result(required(devices["floor"], "device"), "floor device")
    back_device = object_result(required(devices["backroom"], "device"), "backroom device")
    # Device assignments already exist. Slightly future-skewed reads keep every
    # observation inside that effective interval while remaining below the hosted
    # five-minute skew guard.
    base_time = datetime.now(UTC)
    floor_observations = [
        observation(str(uuid.uuid4()), EPCS[0], base_time + timedelta(seconds=index), -40)
        for index in range(3)
    ]
    back_observations = [
        observation(str(uuid.uuid4()), epc, base_time + timedelta(seconds=index), -38)
        for epc in EPCS[1:]
        for index in range(3)
    ]
    floor_batch = submit_batch(
        client,
        device_id=str(required(floor_device, "id")),
        device_token=str(required(devices["floor"], "device_token")),
        observations=floor_observations,
    )
    back_batch = submit_batch(
        client,
        device_id=str(required(back_device, "id")),
        device_token=str(required(devices["backroom"], "device_token")),
        observations=back_observations,
    )
    wait_for_batch(client, admin_token, floor_batch, args.poll_timeout)
    wait_for_batch(client, admin_token, back_batch, args.poll_timeout)

    inventory = poll(
        "inventory projection",
        lambda: {
            "page": client.request(
                "GET", f"/v1/stores/{store1_id}/inventory", headers=bearer(admin_token)
            )[1]
        },
        lambda item: sum(row["quantity"] for row in page_items(item["page"], "inventory")) == 4,
        timeout=args.poll_timeout,
    )
    rows = page_items(inventory["page"], "inventory")
    quantities = {str(row["zone"]): int(row["quantity"]) for row in rows}
    if quantities != {"backroom": 3, "floor": 1}:
        raise DemoFailure(f"unexpected inventory: {quantities}")

    before_item = object_result(
        client.request("GET", f"/v1/items/{EPCS[0]}", headers=bearer(admin_token))[1],
        "item state",
    )
    duplicate_batch = submit_batch(
        client,
        device_id=str(required(floor_device, "id")),
        device_token=str(required(devices["floor"], "device_token")),
        observations=[floor_observations[0]],
    )
    wait_for_batch(client, admin_token, duplicate_batch, args.poll_timeout)
    late_batch = submit_batch(
        client,
        device_id=str(required(floor_device, "id")),
        device_token=str(required(devices["floor"], "device_token")),
        observations=[
            observation(str(uuid.uuid4()), EPCS[0], base_time + timedelta(seconds=1), -30)
        ],
    )
    wait_for_batch(client, admin_token, late_batch, args.poll_timeout)
    after_item = object_result(
        client.request("GET", f"/v1/items/{EPCS[0]}", headers=bearer(admin_token))[1],
        "item state",
    )
    if after_item["state_version"] != before_item["state_version"]:
        raise DemoFailure("duplicate or late event changed item state")

    _, evaluation_value = client.request(
        "POST",
        "/v1/replenishment/evaluations",
        headers=bearer(admin_token),
        payload={"store_id": store1_id},
    )
    evaluation = object_result(evaluation_value, "replenishment evaluation")
    tasks = list_result(required(evaluation, "tasks"), "tasks")
    if len(tasks) != 1 or int(tasks[0]["quantity"]) != 2:
        raise DemoFailure(f"expected one quantity-2 task: {evaluation}")

    associate_password = f"DemoAssociate-{run_id}!"
    associate_email = f"associate-{run_id.lower()}@orange.example"
    associate_user_id, associate_token = create_scoped_user(
        client,
        admin_token,
        email=associate_email,
        password=associate_password,
        role="STORE_ASSOCIATE",
        store_ids=[store1_id],
    )
    client.request(
        "PUT",
        f"/v1/users/{associate_user_id}/roles",
        headers=bearer(admin_token),
        payload={"roles": ["STORE_ASSOCIATE"]},
    )
    client.request(
        "PUT",
        f"/v1/users/{associate_user_id}/store-assignments",
        headers=bearer(admin_token),
        payload={"store_ids": [store1_id]},
    )
    _, me_value = client.request("GET", "/v1/me", headers=bearer(associate_token))
    me = object_result(me_value, "current user")
    if str(required(me, "user_id")) != associate_user_id:
        raise DemoFailure("current-user identity did not match the authenticated user")
    client.request("GET", f"/v1/stores/{store1_id}/inventory", headers=bearer(associate_token))
    denied_status, _ = client.request(
        "GET",
        f"/v1/stores/{store2_id}/inventory",
        expected=403,
        headers=bearer(associate_token),
    )
    if denied_status != 403:
        raise DemoFailure("store-scoped authorization was not enforced")

    task = tasks[0]
    _, task_list_value = client.request(
        "GET",
        f"/v1/stores/{store1_id}/replenishment-tasks",
        headers=bearer(associate_token),
    )
    task_list = page_items(task_list_value, "replenishment task list")
    if not any(str(item["id"]) == str(task["id"]) for item in task_list):
        raise DemoFailure("created replenishment task was not discoverable")
    for next_status in ("CLAIMED", "IN_PROGRESS", "COMPLETED"):
        _, task_value = client.request(
            "PATCH",
            f"/v1/replenishment-tasks/{task['id']}",
            headers=bearer(associate_token),
            payload={"status": next_status, "version": task["version"]},
        )
        task = object_result(task_value, "task transition")

    print("PASS tenant/store/zones/devices")
    print("PASS staged catalog import and atomic promotion")
    print("PASS RFID stable-zone inventory: floor=1 backroom=3")
    print("PASS duplicate and late-event replay protection")
    print("PASS replenishment policy, quantity=2 task, and completed lifecycle")
    print("PASS store-scoped authorization denied Store 2")
    print("DEMO COMPLETE")


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--base-url", default=os.getenv("DEMO_BASE_URL", "http://localhost:8000"))
    result.add_argument("--platform-key", default=os.getenv("PLATFORM_API_KEY"))
    result.add_argument("--admin-email", default=os.getenv("BOOTSTRAP_ADMIN_EMAIL"))
    result.add_argument("--admin-password", default=os.getenv("BOOTSTRAP_ADMIN_PASSWORD"))
    result.add_argument("--request-timeout", type=float, default=15)
    result.add_argument("--startup-timeout", type=float, default=120)
    result.add_argument("--poll-timeout", type=float, default=90)
    return result


def main() -> int:
    args = parser().parse_args()
    missing = [
        name
        for name in ("platform_key", "admin_email", "admin_password")
        if not getattr(args, name)
    ]
    if missing:
        raise SystemExit(f"missing configuration: {', '.join(missing)}")
    try:
        run(args)
    except DemoFailure as exc:
        print(f"DEMO FAILED: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
