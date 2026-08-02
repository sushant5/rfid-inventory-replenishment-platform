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


def test_openapi_contains_the_frozen_public_contract() -> None:
    schema = create_app().openapi()
    assert schema["openapi"] == "3.1.0"
    assert schema["info"]["title"] == API_TITLE == "Abacus RFID Platform"
    assert schema["info"]["version"] == __version__ == "0.1.0"

    paths = schema["paths"]
    expected_operations = {
        ("get", "/health/live"): "liveness",
        ("get", "/health/ready"): "readiness",
        ("get", "/version"): "version",
        ("post", "/v1/platform/tenants"): "createTenant",
        (
            "get",
            "/v1/platform/tenants/{tenant_id}/stores",
        ): "listTenantStores",
        (
            "post",
            "/v1/platform/tenants/{tenant_id}/stores:bulk-onboard",
        ): "bulkOnboardStores",
        (
            "get",
            "/v1/platform/tenants/{tenant_id}/devices",
        ): "listTenantDevices",
        (
            "post",
            "/v1/platform/tenants/{tenant_id}/devices/{device_id}/assignments",
        ): "assignDevice",
        (
            "get",
            "/v1/platform/tenants/{tenant_id}/devices/{device_id}/assignments",
        ): "listDeviceAssignments",
        (
            "post",
            "/v1/platform/tenants/{tenant_id}/devices/{device_id}/credentials:rotate",
        ): "rotateDeviceCredential",
        ("post", "/v1/auth/login"): "login",
        ("get", "/v1/auth/me"): "getCurrentUser",
        ("post", "/v1/users"): "createUser",
        ("get", "/v1/users"): "listUsers",
        ("get", "/v1/users/audit-records"): "listIdentityAuditRecords",
        ("get", "/v1/users/{user_id}"): "getUser",
        ("post", "/v1/users/{user_id}:suspend"): "suspendUser",
        (
            "post",
            "/v1/tenants/{tenant_id}/catalog/imports",
        ): "createCatalogImport",
        (
            "get",
            "/v1/tenants/{tenant_id}/catalog/imports",
        ): "listCatalogImports",
        (
            "get",
            "/v1/tenants/{tenant_id}/catalog/imports/{import_id}",
        ): "getCatalogImport",
        (
            "get",
            "/v1/tenants/{tenant_id}/catalog/imports/{import_id}/errors",
        ): "listCatalogImportErrors",
        ("get", "/v1/tenants/{tenant_id}/catalog/skus"): "listCatalogSkus",
        (
            "get",
            "/v1/tenants/{tenant_id}/catalog/skus/{sku_id}",
        ): "getCatalogSku",
        ("post", "/v1/device/read-batches"): "ingestRfidReadBatch",
        (
            "get",
            "/v1/platform/tenants/{tenant_id}/rfid/observations",
        ): "listRfidObservations",
        (
            "post",
            "/v1/platform/tenants/{tenant_id}/rfid/observations/{observation_id}:replay",
        ): "replayQuarantinedObservation",
        ("get", "/v1/tenants/{tenant_id}/inventory"): "listInventoryBalances",
        (
            "post",
            "/v1/tenants/{tenant_id}/replenishment/policies",
        ): "createReplenishmentPolicy",
        (
            "get",
            "/v1/tenants/{tenant_id}/replenishment/policies",
        ): "listReplenishmentPolicies",
        (
            "get",
            "/v1/tenants/{tenant_id}/replenishment/policies/{policy_id}",
        ): "getReplenishmentPolicy",
        (
            "patch",
            "/v1/tenants/{tenant_id}/replenishment/policies/{policy_id}",
        ): "updateReplenishmentPolicy",
        (
            "delete",
            "/v1/tenants/{tenant_id}/replenishment/policies/{policy_id}",
        ): "deactivateReplenishmentPolicy",
        (
            "post",
            "/v1/tenants/{tenant_id}/replenishment/policies:bulk-upsert",
        ): "bulkUpsertReplenishmentPolicies",
        (
            "get",
            "/v1/tenants/{tenant_id}/replenishment/policy-imports/{import_id}",
        ): "getReplenishmentPolicyImport",
        (
            "post",
            "/v1/tenants/{tenant_id}/replenishment/evaluations",
        ): "evaluateReplenishment",
        (
            "get",
            "/v1/tenants/{tenant_id}/replenishment/evaluations/{run_id}",
        ): "getReplenishmentEvaluation",
        ("get", "/v1/tenants/{tenant_id}/replenishment/tasks"): "listReplenishmentTasks",
        (
            "patch",
            "/v1/tenants/{tenant_id}/replenishment/tasks/{task_id}",
        ): "updateReplenishmentTask",
    }

    http_methods = {"get", "post", "put", "patch", "delete", "options", "head", "trace"}
    actual_operations = {
        (method, path): operation["operationId"]
        for path, path_item in paths.items()
        for method, operation in path_item.items()
        if method in http_methods
    }
    assert actual_operations == expected_operations

    security_schemes = schema["components"]["securitySchemes"]
    assert security_schemes["PlatformApiKey"] == {
        "type": "apiKey",
        "description": "Trusted platform integration key for onboarding and operational APIs.",
        "in": "header",
        "name": "X-Platform-Key",
    }
    assert security_schemes["DeviceApiKey"] == {
        "type": "apiKey",
        "description": (
            "RFID reader or gateway API key; plaintext is returned once and remains valid "
            "until rotation."
        ),
        "in": "header",
        "name": "X-Device-Key",
    }
    assert security_schemes["HTTPBearer"] == {
        "type": "http",
        "description": "Short-lived Abacus access token",
        "scheme": "bearer",
    }
    assert paths["/v1/platform/tenants"]["post"]["security"] == [{"PlatformApiKey": []}]
    assert paths["/v1/device/read-batches"]["post"]["security"] == [{"DeviceApiKey": []}]

    onboarding_parameters = paths["/v1/platform/tenants/{tenant_id}/stores:bulk-onboard"]["post"][
        "parameters"
    ]
    assert any(
        parameter["name"] == "Idempotency-Key" and parameter["in"] == "header"
        for parameter in onboarding_parameters
    )

    inventory_properties = schema["components"]["schemas"]["InventoryBalanceRead"]["properties"]
    assert "as_of" not in inventory_properties
    assert "projection_updated_at" in inventory_properties
    assert "last_relevant_observation_at" in inventory_properties

    observation_properties = schema["components"]["schemas"]["RfidObservationRead"]["properties"]
    assert observation_properties["acceptance_sequence"]["type"] == "integer"

    task_status = schema["components"]["schemas"]["ReplenishmentTaskStatus"]
    assert task_status["enum"] == [
        "OPEN",
        "CLAIMED",
        "IN_PROGRESS",
        "AWAITING_VERIFICATION",
        "VERIFIED",
        "CANCELLED",
        "EXCEPTION",
    ]
