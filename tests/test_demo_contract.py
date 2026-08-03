from scripts.generate_store_batch import build_store_batch

from abacus import API_TITLE, __version__
from abacus.main import create_app
from abacus.schemas.tenancy import BulkStoreOnboardingRequest


def test_assignment_sized_store_fixture_is_valid_and_deterministic() -> None:
    payload = build_store_batch(100)
    request = BulkStoreOnboardingRequest.model_validate(payload)

    assert len(request.stores) == 100
    assert sum(len(store.devices) for store in request.stores) == 200
    assert build_store_batch(100) == payload


def test_openapi_contains_the_canonical_submission_contract() -> None:
    schema = create_app().openapi()
    assert schema["openapi"] == "3.1.0"
    assert schema["info"] == {
        **schema["info"],
        "title": API_TITLE,
        "version": __version__,
    }
    assert __version__ == "0.2.0"

    expected_operations = {
        ("get", "/health/live"): "liveness",
        ("get", "/health/ready"): "readiness",
        ("get", "/version"): "version",
        ("post", "/v1/tenants"): "createTenantCanonical",
        ("post", "/v1/tenants/{tenant_id}/store-imports"): "createStoreImport",
        ("post", "/v1/stores/{store_id}/zones"): "createStoreZone",
        ("post", "/v1/stores/{store_id}/devices"): "registerStoreDevice",
        ("post", "/v1/tenants/{tenant_id}/catalog-imports"): "createCatalogImportCanonical",
        ("get", "/v1/catalog-imports/{import_id}"): "getCatalogImportCanonical",
        (
            "get",
            "/v1/catalog-imports/{import_id}/errors",
        ): "listCatalogImportErrorsCanonical",
        ("post", "/v1/rfid/observation-batches"): "submitRfidObservationBatch",
        (
            "get",
            "/v1/rfid/observation-batches/{batch_id}",
        ): "getRfidObservationBatch",
        ("get", "/v1/stores/{store_id}/inventory"): "getStoreInventory",
        ("get", "/v1/items/{epc}"): "getCurrentItemState",
        ("post", "/v1/users"): "createUser",
        ("put", "/v1/users/{user_id}/roles"): "replaceUserRoles",
        (
            "put",
            "/v1/users/{user_id}/store-assignments",
        ): "replaceUserStoreAssignments",
        ("get", "/v1/me"): "getCurrentUserCanonical",
        ("post", "/v1/replenishment-policies"): "createCanonicalReplenishmentPolicy",
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
        ("post", "/v1/replenishment/evaluations"): "evaluateCanonicalReplenishment",
        (
            "get",
            "/v1/stores/{store_id}/replenishment-tasks",
        ): "listCanonicalReplenishmentTasks",
        ("patch", "/v1/replenishment-tasks/{task_id}"): "patchCanonicalReplenishmentTask",
    }
    optional_operations = {
        ("post", "/v1/auth/login"): "login",
        ("get", "/v1/tenants/{tenant_id}/stores"): "listCanonicalTenantStores",
        ("get", "/v1/stores/{store_id}/zones"): "listStoreZones",
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
        ("/v1/stores/{store_id}/inventory", "get"),
        ("/v1/items/{epc}", "get"),
        ("/v1/replenishment-policies", "post"),
        ("/v1/replenishment/evaluations", "post"),
        ("/v1/stores/{store_id}/replenishment-tasks", "get"),
        ("/v1/replenishment-tasks/{task_id}", "patch"),
    ):
        assert paths[path][method]["security"] == [{"HTTPBearer": []}]

    inventory_fields = schema["components"]["schemas"]["InventoryProjectionRead"]["properties"]
    assert {"quantity", "as_of", "confidence", "freshness_status"}.issubset(inventory_fields)
    assert schema["components"]["schemas"]["CanonicalTaskStatus"]["enum"] == [
        "OPEN",
        "CLAIMED",
        "IN_PROGRESS",
        "COMPLETED",
        "CANCELED",
        "EXPIRED",
    ]
