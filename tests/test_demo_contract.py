from scripts.generate_store_batch import build_store_batch

from abacus.main import create_app
from abacus.schemas.tenancy import BulkStoreOnboardingRequest


def test_assignment_sized_store_fixture_is_valid_and_deterministic() -> None:
    payload = build_store_batch(100)
    request = BulkStoreOnboardingRequest.model_validate(payload)

    assert len(request.stores) == 100
    assert sum(len(store.devices) for store in request.stores) == 200
    assert build_store_batch(100) == payload


def test_openapi_contains_the_frozen_reviewer_demo_path() -> None:
    schema = create_app().openapi()
    paths = schema["paths"]
    expected_operations = {
        ("post", "/v1/platform/tenants"): "createTenant",
        (
            "post",
            "/v1/platform/tenants/{tenant_id}/stores:bulk-onboard",
        ): "bulkOnboardStores",
        ("post", "/v1/auth/login"): "login",
        ("get", "/v1/auth/me"): "getCurrentUser",
        ("post", "/v1/users"): "createUser",
        (
            "post",
            "/v1/tenants/{tenant_id}/catalog/imports",
        ): "createCatalogImport",
        ("get", "/v1/tenants/{tenant_id}/catalog/skus"): "listCatalogSkus",
        ("post", "/v1/device/read-batches"): "ingestRfidReadBatch",
        ("get", "/v1/tenants/{tenant_id}/inventory"): "listInventoryBalances",
        (
            "post",
            "/v1/tenants/{tenant_id}/replenishment/policies:bulk-upsert",
        ): "bulkUpsertReplenishmentPolicies",
        (
            "post",
            "/v1/tenants/{tenant_id}/replenishment/evaluations",
        ): "evaluateReplenishment",
        ("get", "/v1/tenants/{tenant_id}/replenishment/tasks"): "listReplenishmentTasks",
        (
            "patch",
            "/v1/tenants/{tenant_id}/replenishment/tasks/{task_id}",
        ): "updateReplenishmentTask",
    }

    for (method, path), operation_id in expected_operations.items():
        assert paths[path][method]["operationId"] == operation_id

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
    assert paths["/v1/platform/tenants"]["post"]["security"] == [{"PlatformApiKey": []}]
    assert paths["/v1/device/read-batches"]["post"]["security"] == [{"DeviceApiKey": []}]
