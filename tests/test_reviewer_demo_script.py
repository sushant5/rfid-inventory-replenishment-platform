import json
import urllib.parse
import uuid
from collections.abc import Mapping

from scripts.run_reviewer_demo import DemoConfig, HttpResponse, ReviewerDemo, encode_multipart

TENANT_ID = "00000000-0000-0000-0000-000000000001"
STORE_ID = "00000000-0000-0000-0000-000000000101"
FLOOR_DEVICE_ID = "00000000-0000-0000-0000-000000000201"
BACKROOM_DEVICE_ID = "00000000-0000-0000-0000-000000000202"
FLOOR_ZONE_ID = "00000000-0000-0000-0000-000000000301"
BACKROOM_ZONE_ID = "00000000-0000-0000-0000-000000000302"
SKU_ID = "00000000-0000-0000-0000-000000000401"
POLICY_ID = "00000000-0000-0000-0000-000000000501"
TASK_ID = "00000000-0000-0000-0000-000000000601"
ADMIN_ID = "00000000-0000-0000-0000-000000000701"
ASSOCIATE_ID = "00000000-0000-0000-0000-000000000702"


class DemoTransport:
    def __init__(self) -> None:
        self.users: list[dict[str, object]] = []
        self.event_ids: list[str] = []
        self.store_payload: dict[str, object] | None = None
        self.rfid_batch_sizes: list[int] = []
        self.requests: list[tuple[str, str]] = []
        self.task_status = "OPEN"
        self.task_owner: str | None = None
        self.task_moved_quantity = 0
        self.task_patch_count = 0

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> HttpResponse:
        del timeout
        parsed = urllib.parse.urlsplit(url)
        path = parsed.path
        self.requests.append((method, path))
        payload = self._json_body(body, headers)

        if method == "GET" and path == "/health/ready":
            return self._response(200, {"status": "ok"})
        if method == "GET" and path == "/version":
            return self._response(200, {"version": "0.1.0", "build_sha": "test-build"})
        if method == "POST" and path == "/v1/platform/tenants":
            self._platform(headers)
            assert payload == {"code": "orange", "name": "Orange"}
            return self._response(201, {"id": TENANT_ID, "code": "orange"})
        if method == "POST" and path.endswith("/stores:bulk-onboard"):
            self._platform(headers)
            assert headers["Idempotency-Key"] == "reviewer-demo-stores-v1"
            assert isinstance(payload, dict)
            self.store_payload = payload
            stores = payload["stores"]
            assert isinstance(stores, list) and len(stores) == 100
            return self._response(
                201,
                {"status": "COMPLETED", "succeeded_count": 100},
            )
        if method == "GET" and path == f"/v1/platform/tenants/{TENANT_ID}/stores":
            self._platform(headers)
            return self._response(
                200,
                [
                    {
                        "id": STORE_ID if number == 1 else str(uuid.UUID(int=1000 + number)),
                        "code": f"store{number:03d}",
                    }
                    for number in range(1, 101)
                ],
            )
        if method == "GET" and path == f"/v1/platform/tenants/{TENANT_ID}/devices":
            self._platform(headers)
            devices: list[dict[str, str]] = []
            for number in range(1, 101):
                for suffix, offset in (("FLOOR", 0), ("BACK", 1)):
                    device_id = str(uuid.UUID(int=3000 + number * 2 + offset))
                    if number == 1 and suffix == "FLOOR":
                        device_id = FLOOR_DEVICE_ID
                    elif number == 1 and suffix == "BACK":
                        device_id = BACKROOM_DEVICE_ID
                    devices.append(
                        {
                            "id": device_id,
                            "serial_number": f"ORANGE-{number:04d}-{suffix}",
                        }
                    )
            return self._response(200, devices)
        if method == "GET" and path.endswith(f"/devices/{FLOOR_DEVICE_ID}/assignments"):
            self._platform(headers)
            return self._response(
                200,
                [{"store_id": STORE_ID, "zone_id": FLOOR_ZONE_ID, "effective_to": None}],
            )
        if method == "GET" and path.endswith(f"/devices/{BACKROOM_DEVICE_ID}/assignments"):
            self._platform(headers)
            return self._response(
                200,
                [{"store_id": STORE_ID, "zone_id": BACKROOM_ZONE_ID, "effective_to": None}],
            )
        if method == "POST" and path == "/v1/auth/login":
            assert isinstance(payload, dict)
            email = payload["email"]
            return self._response(
                200,
                {"access_token": "associate-token" if "associate" in email else "admin-token"},
            )
        if method == "GET" and path == "/v1/auth/me":
            if headers["Authorization"] == "Bearer associate-token":
                return self._response(
                    200,
                    {
                        "user_id": ASSOCIATE_ID,
                        "tenant_id": TENANT_ID,
                        "role_assignments": [{"role": "STORE_ASSOCIATE", "store_id": STORE_ID}],
                    },
                )
            assert headers["Authorization"] == "Bearer admin-token"
            return self._response(
                200,
                {
                    "user_id": ADMIN_ID,
                    "tenant_id": TENANT_ID,
                    "role_assignments": [{"role": "CORPORATE_ADMIN", "store_id": None}],
                },
            )
        if method == "GET" and path == "/v1/users":
            self._bearer(headers)
            return self._response(
                200,
                {"items": self.users, "total": len(self.users), "limit": 100, "offset": 0},
            )
        if method == "POST" and path == "/v1/users":
            self._bearer(headers)
            assert isinstance(payload, dict)
            user = {
                "id": str(uuid.uuid4()),
                "tenant_id": TENANT_ID,
                "email": payload["email"],
                "status": "ACTIVE",
                "role_assignments": payload["role_assignments"],
            }
            self.users.append(user)
            return self._response(201, user)
        if method == "POST" and path.endswith("/catalog/imports"):
            self._platform(headers)
            assert headers["Idempotency-Key"] == "reviewer-demo-catalog-v1"
            assert body is not None and b'name="mode"' in body and b'name="file"' in body
            return self._response(202, {"id": "catalog-import", "status": "READY"})
        if method == "GET" and path.endswith("/catalog/imports/catalog-import"):
            self._platform(headers)
            return self._response(
                200,
                {"id": "catalog-import", "status": "COMPLETED", "total_rows": 4},
            )
        if method == "GET" and path.endswith("/catalog/skus"):
            self._bearer(headers)
            return self._response(
                200,
                {"items": [{"id": SKU_ID, "code": "SKU-TRAIL-BLUE-M"}], "total": 1},
            )
        if method == "POST" and path.endswith("/credentials:rotate"):
            self._platform(headers)
            device_id = path.split("/devices/")[1].split("/")[0]
            return self._response(200, {"device_id": device_id, "api_key": f"{device_id}.key"})
        if method == "POST" and path == "/v1/device/read-batches":
            assert headers["X-Device-Key"].endswith(".key")
            assert isinstance(payload, dict)
            observations = payload["observations"]
            assert isinstance(observations, list)
            self.rfid_batch_sizes.append(len(observations))
            event_ids = [str(item["event_id"]) for item in observations]
            self.event_ids.extend(event_ids)
            return self._response(
                202,
                {
                    "accepted_count": len(event_ids),
                    "duplicate_count": 0,
                    "conflict_count": 0,
                },
            )
        if method == "GET" and path.endswith("/rfid/observations"):
            self._platform(headers)
            return self._response(
                200,
                {
                    "items": [
                        {"event_id": event_id, "status": "PROCESSED"} for event_id in self.event_ids
                    ],
                    "total": len(self.event_ids),
                },
            )
        if method == "GET" and path.endswith("/inventory"):
            self._bearer(headers)
            return self._response(
                200,
                {
                    "items": [
                        {
                            "sku_id": SKU_ID,
                            "zone_kind": "SALES_FLOOR",
                            "quantity": 1,
                            "projection_updated_at": "2026-08-02T12:00:02Z",
                            "last_relevant_observation_at": "2026-08-02T12:00:01Z",
                        },
                        {
                            "sku_id": SKU_ID,
                            "zone_kind": "BACKROOM",
                            "quantity": 3,
                            "projection_updated_at": "2026-08-02T12:00:02Z",
                            "last_relevant_observation_at": "2026-08-02T12:00:01Z",
                        },
                    ],
                    "total": 2,
                },
            )
        if method == "POST" and path.endswith("/replenishment/policies:bulk-upsert"):
            self._bearer(headers)
            assert headers["Idempotency-Key"] == "reviewer-demo-policies-v1"
            return self._response(
                201,
                {"status": "COMPLETED", "rejected_count": 0},
            )
        if method == "POST" and path.endswith("/replenishment/evaluations"):
            self._bearer(headers)
            assert headers["Idempotency-Key"] == "reviewer-demo-evaluation-v1"
            return self._response(
                201,
                {
                    "lines": [
                        {
                            "sku_id": SKU_ID,
                            "policy_id": POLICY_ID,
                            "task_id": TASK_ID,
                            "reason": "REPLENISHMENT_REQUIRED",
                            "recommended_quantity": 3,
                        }
                    ]
                },
            )
        if method == "GET" and path.endswith("/replenishment/tasks"):
            self._bearer(headers)
            return self._response(
                200,
                {
                    "items": [
                        {
                            "id": TASK_ID,
                            "status": self.task_status,
                            "version": 1 + self.task_patch_count,
                            "quantity": 3,
                            "moved_quantity": self.task_moved_quantity,
                            "claimed_by_subject": self.task_owner,
                        }
                    ],
                    "total": 1,
                },
            )
        if method == "PATCH" and path.endswith(f"/replenishment/tasks/{TASK_ID}"):
            assert headers["Authorization"] == "Bearer associate-token"
            expected_payloads: dict[str, dict[str, object]] = {
                "OPEN": {
                    "status": "CLAIMED",
                    "expected_version": 1,
                    "note": "Claimed by reviewer demo",
                },
                "CLAIMED": {
                    "status": "IN_PROGRESS",
                    "expected_version": 2,
                    "note": "Execution started by reviewer demo",
                },
                "AWAITING_VERIFICATION": {
                    "status": "VERIFIED",
                    "expected_version": 5,
                    "note": "Full movement verified by reviewer demo",
                },
            }
            if self.task_status == "IN_PROGRESS":
                expected_payload = (
                    {
                        "status": "IN_PROGRESS",
                        "expected_version": 3,
                        "moved_quantity": 3,
                        "note": "Physical movement completed by reviewer demo",
                    }
                    if self.task_moved_quantity == 0
                    else {
                        "status": "AWAITING_VERIFICATION",
                        "expected_version": 4,
                        "note": "Full movement submitted for verification",
                    }
                )
            else:
                expected_payload = expected_payloads[self.task_status]
            assert payload == expected_payload
            self.task_status = str(payload["status"])
            if self.task_status == "CLAIMED":
                self.task_owner = ASSOCIATE_ID
            if "moved_quantity" in payload:
                self.task_moved_quantity = int(payload["moved_quantity"])
            self.task_patch_count += 1
            return self._response(
                200,
                {
                    "id": TASK_ID,
                    "status": self.task_status,
                    "version": 1 + self.task_patch_count,
                    "quantity": 3,
                    "moved_quantity": self.task_moved_quantity,
                    "claimed_by_subject": self.task_owner,
                },
            )
        raise AssertionError(f"Unexpected request: {method} {url}")

    @staticmethod
    def _json_body(
        body: bytes | None,
        headers: Mapping[str, str],
    ) -> dict[str, object] | None:
        if body is None or headers.get("Content-Type") != "application/json":
            return None
        value = json.loads(body)
        assert isinstance(value, dict)
        return value

    @staticmethod
    def _response(status: int, payload: object) -> HttpResponse:
        return HttpResponse(
            status, json.dumps(payload).encode(), {"Content-Type": "application/json"}
        )

    @staticmethod
    def _platform(headers: Mapping[str, str]) -> None:
        assert headers["X-Platform-Key"] == "platform-secret-for-test"

    @staticmethod
    def _bearer(headers: Mapping[str, str]) -> None:
        assert headers["Authorization"].startswith("Bearer ")


def test_reviewer_demo_runs_the_frozen_end_to_end_contract() -> None:
    transport = DemoTransport()
    output: list[str] = []
    config = DemoConfig(
        base_url="https://demo.example.test/",
        platform_key="platform-secret-for-test",
        tenant_code="orange",
        tenant_name="Orange",
        admin_email="reviewer@orange.example",
        admin_password="admin-secret-for-test",
        manager_email="manager.demo@orange.example",
        manager_password=None,
        associate_email="associate.demo@orange.example",
        associate_password=None,
        poll_interval_seconds=0,
    )

    ReviewerDemo(
        config,
        transport=transport,
        sleeper=lambda _: None,
        output=output.append,
    ).run()
    # A second run reuses the same stores/users/imports and the frozen evaluation;
    # the immutable VERIFIED task is observed without being rewritten.
    ReviewerDemo(
        config,
        transport=transport,
        sleeper=lambda _: None,
        output=output.append,
    ).run()

    assert transport.store_payload is not None
    assert len(transport.store_payload["stores"]) == 100
    assert transport.rfid_batch_sizes == [1, 3, 1, 3]
    assert transport.task_patch_count == 5
    assert transport.task_status == "VERIFIED"
    assert len(transport.users) == 2
    assert {user["role_assignments"][0]["role"] for user in transport.users} == {
        "STORE_MANAGER",
        "STORE_ASSOCIATE",
    }
    transcript = "\n".join(output)
    assert "task=VERIFIED" in transcript
    assert "PASS reviewer demo complete: https://demo.example.test/docs" in transcript
    assert "platform-secret-for-test" not in transcript
    assert "admin-secret-for-test" not in transcript


def test_multipart_encoder_uses_a_closed_boundary_and_preserves_file_bytes() -> None:
    body = encode_multipart(
        boundary="fixed-boundary",
        fields={"mode": "DELTA"},
        file_field="file",
        filename="catalog.csv",
        content_type="text/csv",
        file_bytes=b"sku,epc\nSKU-1,EPC-1\n",
    )

    assert b'name="mode"\r\n\r\nDELTA' in body
    assert b'filename="catalog.csv"' in body
    assert b"sku,epc\nSKU-1,EPC-1\n" in body
    assert body.endswith(b"--fixed-boundary--\r\n")
