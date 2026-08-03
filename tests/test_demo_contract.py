import json
from pathlib import Path

from scripts.generate_store_batch import build_store_batch

from abacus import API_TITLE, __version__
from abacus.main import create_app
from abacus.schemas.architecture import CanonicalObservationBatchCreate
from abacus.schemas.canonical_replenishment import PolicyCreate
from abacus.schemas.tenancy import BulkStoreOnboardingRequest

ROOT = Path(__file__).resolve().parents[1]


def test_assignment_sized_store_fixture_is_valid_and_deterministic() -> None:
    payload = build_store_batch(100)
    request = BulkStoreOnboardingRequest.model_validate(payload)

    assert len(request.stores) == 100
    assert sum(len(store.devices) for store in request.stores) == 200
    assert build_store_batch(100) == payload


def test_checked_in_examples_match_the_public_request_schemas() -> None:
    PolicyCreate.model_validate(json.loads((ROOT / "examples" / "policies.json").read_text()))
    CanonicalObservationBatchCreate.model_validate(
        json.loads((ROOT / "examples" / "rfid-batch.json").read_text())
    )
    BulkStoreOnboardingRequest.model_validate(
        json.loads((ROOT / "examples" / "stores.json").read_text())
    )


def test_openapi_contains_the_submission_contract() -> None:
    schema = create_app().openapi()
    assert schema["openapi"] == "3.1.0"
    assert schema["info"] == {
        **schema["info"],
        "title": API_TITLE,
        "version": __version__,
    }
    assert __version__ == "0.5.1"

    expected_operations = {
        ("get", "/health/live"): "liveness",
        ("get", "/health/ready"): "readiness",
        ("get", "/version"): "version",
        ("post", "/v1/tenants"): "createTenant",
        ("post", "/v1/tenants/{tenant_id}/store-imports"): "createStoreImport",
        ("post", "/v1/stores/{store_id}/zones"): "createStoreZone",
        ("post", "/v1/stores/{store_id}/devices"): "registerStoreDevice",
        ("post", "/v1/tenants/{tenant_id}/catalog-imports"): "createCatalogImport",
        ("get", "/v1/catalog-imports/{import_id}"): "getCatalogImport",
        (
            "get",
            "/v1/catalog-imports/{import_id}/errors",
        ): "listCatalogImportErrors",
        ("get", "/v1/skus"): "listSkus",
        ("get", "/v1/skus/{sku_id}"): "getSku",
        ("post", "/v1/rfid/observation-batches"): "submitRfidObservationBatch",
        (
            "get",
            "/v1/rfid/observation-batches/{batch_id}",
        ): "getRfidObservationBatch",
        ("get", "/v1/rfid/quarantine"): "listRfidQuarantine",
        ("get", "/v1/stores/{store_id}/inventory"): "getStoreInventory",
        ("get", "/v1/items/{epc}"): "getCurrentItemState",
        ("post", "/v1/users"): "createUser",
        ("put", "/v1/users/{user_id}/roles"): "replaceUserRoles",
        ("put", "/v1/users/{user_id}/access"): "replaceUserAccess",
        (
            "put",
            "/v1/users/{user_id}/store-assignments",
        ): "replaceUserStoreAssignments",
        ("get", "/v1/me"): "getCurrentUser",
        ("post", "/v1/replenishment-policies"): "createReplenishmentPolicy",
        ("get", "/v1/replenishment-policies"): "listReplenishmentPolicies",
        (
            "get",
            "/v1/replenishment-policies/{policy_id}",
        ): "getReplenishmentPolicy",
        (
            "post",
            "/v1/replenishment-policies/{policy_id}/versions",
        ): "createReplenishmentPolicyVersion",
        (
            "patch",
            "/v1/replenishment-policy-versions/{version_id}",
        ): "patchReplenishmentPolicyVersion",
        (
            "post",
            "/v1/replenishment-policy-versions/{version_id}/activate",
        ): "activateReplenishmentPolicyVersion",
        ("post", "/v1/replenishment/evaluations"): "evaluateReplenishment",
        (
            "get",
            "/v1/stores/{store_id}/replenishment-tasks",
        ): "listReplenishmentTasks",
        ("patch", "/v1/replenishment-tasks/{task_id}"): "patchReplenishmentTask",
    }
    optional_operations = {
        ("post", "/v1/auth/login"): "login",
        ("get", "/v1/tenants/{tenant_id}/stores"): "listTenantStores",
        ("get", "/v1/stores/{store_id}/zones"): "listStoreZones",
        ("get", "/v1/stores/{store_id}/devices"): "listStoreDevices",
        (
            "post",
            "/v1/devices/{device_id}/credentials:rotate",
        ): "rotateDeviceCredential",
        ("get", "/v1/users"): "listUsers",
        ("get", "/v1/users/audit-records"): "listIdentityAuditRecords",
        ("get", "/v1/users/{user_id}"): "getUser",
        ("post", "/v1/users/{user_id}:suspend"): "suspendUser",
    }
    http_methods = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
    actual_operations = {
        (method, path): operation["operationId"]
        for path, path_item in schema["paths"].items()
        for method, operation in path_item.items()
        if method in http_methods
    }
    assert actual_operations == expected_operations | optional_operations

    paths = schema["paths"]
    assert not any("/v1/platform/" in path for path in paths)
    assert "/v1/device/read-batches" not in paths
    assert "/v1/tenants/{tenant_id}/replenishment/tasks" not in paths

    security_schemes = schema["components"]["securitySchemes"]
    assert security_schemes["DeviceToken"]["name"] == "X-Device-Token"
    assert security_schemes["PlatformApiKey"]["name"] == "X-Platform-Key"
    assert security_schemes["HTTPBearer"]["scheme"] == "bearer"
    assert paths["/v1/rfid/observation-batches"]["post"]["security"] == [{"DeviceToken": []}]
    for path, method in (
        ("/v1/tenants", "post"),
        ("/v1/tenants/{tenant_id}/store-imports", "post"),
        ("/v1/tenants/{tenant_id}/catalog-imports", "post"),
        ("/v1/catalog-imports/{import_id}", "get"),
        ("/v1/catalog-imports/{import_id}/errors", "get"),
    ):
        assert paths[path][method]["security"] == [{"PlatformApiKey": []}]
    for path, method in (
        ("/v1/me", "get"),
        ("/v1/skus", "get"),
        ("/v1/skus/{sku_id}", "get"),
        ("/v1/stores/{store_id}/devices", "get"),
        ("/v1/stores/{store_id}/inventory", "get"),
        ("/v1/items/{epc}", "get"),
        ("/v1/replenishment-policies", "post"),
        ("/v1/replenishment/evaluations", "post"),
        ("/v1/stores/{store_id}/replenishment-tasks", "get"),
        ("/v1/replenishment-tasks/{task_id}", "patch"),
    ):
        assert paths[path][method]["security"] == [{"HTTPBearer": []}]

    inventory_fields = schema["components"]["schemas"]["InventoryProjectionRead"]["properties"]
    assert {
        "quantity",
        "as_of",
        "oldest_item_observed_at",
        "confidence",
        "freshness_status",
    }.issubset(inventory_fields)
    device_mapping_fields = schema["components"]["schemas"]["StoreDeviceMappingRead"]["properties"]
    assert set(device_mapping_fields) == {"device", "assignment"}
    assignment_fields = schema["components"]["schemas"]["DeviceAssignmentRead"]["properties"]
    assert {"valid_from", "valid_to"}.issubset(assignment_fields)
    assert "effective_from" not in assignment_fields
    device_items = paths["/v1/stores/{store_id}/devices"]["get"]["responses"]["200"]["content"][
        "application/json"
    ]["schema"]["items"]
    assert device_items["$ref"].endswith("/StoreDeviceMappingRead")
    assert schema["components"]["schemas"]["ReplenishmentTaskStatus"]["enum"] == [
        "OPEN",
        "CLAIMED",
        "IN_PROGRESS",
        "COMPLETED",
        "CANCELED",
        "EXPIRED",
    ]
    assert not any(name.startswith("Canonical") for name in schema["components"]["schemas"])
    assert [item["name"] for item in schema["tags"]] == [
        "1. Onboarding",
        "2. Product catalog",
        "3. RFID and Inventory",
        "4. Identity and Access",
        "5. Replenishment",
        "Operations",
    ]
    assert all(
        "Canonical" not in operation["summary"] and not operation["summary"].endswith(" Endpoint")
        for path_item in paths.values()
        for method, operation in path_item.items()
        if method in http_methods
    )

    validation = paths["/v1/items/{epc}"]["get"]["responses"]["422"]
    assert "ProblemDetail" in schema["components"]["schemas"]
    assert set(validation["content"]) == {"application/problem+json"}
    assert validation["content"]["application/problem+json"]["schema"] == {
        "$ref": "#/components/schemas/ProblemDetail"
    }
