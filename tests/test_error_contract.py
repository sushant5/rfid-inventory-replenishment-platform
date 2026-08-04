from fastapi.testclient import TestClient

from abacus.main import create_app

PROBLEM_STATUS_CODES = {"400", "401", "403", "404", "409", "413", "422", "429", "500", "503"}

EXPECTED_PROBLEM_STATUSES = {
    "createTenant": {"401", "409", "422", "500"},
    "createStoreImport": {"400", "401", "404", "409", "422", "500"},
    "createStoreZone": {"401", "403", "404", "409", "422", "500"},
    "listStoreZones": {"401", "403", "404", "422", "500"},
    "registerStoreDevice": {"401", "403", "404", "409", "422", "500"},
    "listStoreDevices": {"401", "403", "404", "422", "500"},
    "rotateDeviceCredential": {"401", "403", "404", "422", "500"},
    "listStores": {"401", "403", "422", "500"},
    "listTenantStores": {"401", "404", "422", "500"},
    "createCatalogImport": {"400", "401", "403", "404", "409", "413", "422", "500"},
    "getCatalogImport": {"401", "403", "404", "422", "500"},
    "listCatalogImportErrors": {"401", "403", "404", "422", "500"},
    "listSkus": {"401", "403", "422", "500"},
    "getSku": {"401", "403", "404", "422", "500"},
    "submitRfidObservationBatch": {"401", "403", "409", "422", "500"},
    "getRfidObservationBatch": {"401", "403", "404", "422", "500"},
    "listRfidQuarantine": {"401", "403", "422", "500"},
    "replayRfidQuarantine": {"401", "403", "404", "409", "422", "500"},
    "getStoreInventory": {"401", "403", "404", "422", "500"},
    "getCurrentItemState": {"401", "403", "404", "422", "500"},
    "createBusinessEvent": {"401", "403", "404", "409", "422", "500"},
    "getBusinessEvent": {"401", "403", "404", "422", "500"},
    "getCurrentUser": {"401", "500"},
    "login": {"401", "422", "429", "500"},
    "createUser": {"401", "403", "409", "422", "500"},
    "listUsers": {"401", "403", "422", "500"},
    "listIdentityAuditRecords": {"401", "403", "422", "500"},
    "getUser": {"401", "403", "404", "422", "500"},
    "replaceUserAccess": {"401", "403", "404", "409", "422", "500"},
    "replaceUserRoles": {"401", "403", "404", "409", "422", "500"},
    "replaceUserStoreAssignments": {"401", "403", "404", "409", "422", "500"},
    "suspendUser": {"401", "403", "404", "409", "422", "500"},
    "listReplenishmentPolicies": {"401", "403", "422", "500"},
    "createReplenishmentPolicy": {"401", "403", "409", "422", "500"},
    "getReplenishmentPolicy": {"401", "403", "404", "422", "500"},
    "createReplenishmentPolicyVersion": {"401", "403", "404", "409", "422", "500"},
    "patchReplenishmentPolicyVersion": {"401", "403", "404", "409", "422", "500"},
    "activateReplenishmentPolicyVersion": {"401", "403", "404", "409", "422", "500"},
    "evaluateReplenishment": {"401", "403", "409", "422", "500"},
    "listReplenishmentTasks": {"401", "403", "404", "422", "500"},
    "patchReplenishmentTask": {"401", "403", "404", "409", "422", "500"},
    "liveness": {"500"},
    "readiness": {"500", "503"},
    "version": {"500"},
}


def _operations(schema: dict[str, object]) -> list[dict[str, object]]:
    paths = schema["paths"]
    assert isinstance(paths, dict)
    return [
        operation
        for path_item in paths.values()
        if isinstance(path_item, dict)
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete"} and isinstance(operation, dict)
    ]


def test_openapi_documents_runtime_error_contract() -> None:
    schema = create_app().openapi()
    operations = _operations(schema)

    assert len(operations) == len(EXPECTED_PROBLEM_STATUSES)
    assert all("500" in operation["responses"] for operation in operations)
    secured = [operation for operation in operations if operation.get("security")]
    assert len(secured) == 40
    assert all("401" in operation["responses"] for operation in secured)
    assert "401" in schema["paths"]["/v1/auth/login"]["post"]["responses"]

    inventory = schema["paths"]["/v1/stores/{store_id}/inventory"]["get"]
    assert "`inventory:read`" in inventory["responses"]["403"]["description"]
    assert "404" in inventory["responses"]
    assert "403" in schema["paths"]["/v1/rfid/observation-batches"]["post"]["responses"]
    assert "503" in schema["paths"]["/health/ready"]["get"]["responses"]
    assert "429" in schema["paths"]["/v1/auth/login"]["post"]["responses"]


def test_every_operation_has_the_reviewed_problem_response_matrix() -> None:
    schema = create_app().openapi()
    operations = {operation["operationId"]: operation for operation in _operations(schema)}

    assert set(operations) == set(EXPECTED_PROBLEM_STATUSES)
    for operation_id, expected_statuses in EXPECTED_PROBLEM_STATUSES.items():
        responses = operations[operation_id]["responses"]
        actual_statuses = set(responses).intersection(PROBLEM_STATUS_CODES)
        assert actual_statuses == expected_statuses, operation_id
        for status_code in expected_statuses:
            response = responses[status_code]
            assert response["description"], (operation_id, status_code)
            assert set(response["content"]) == {"application/problem+json"}, (
                operation_id,
                status_code,
            )
            assert response["content"]["application/problem+json"]["schema"] == {
                "$ref": "#/components/schemas/ProblemDetail"
            }, (operation_id, status_code)


def test_conflict_documentation_is_operation_specific() -> None:
    schema = create_app().openapi()
    operations = {operation["operationId"]: operation for operation in _operations(schema)}
    documented_conflicts = {
        operation_id
        for operation_id, operation in operations.items()
        if "409" in operation["responses"]
    }

    assert "rotateDeviceCredential" not in documented_conflicts
    assert documented_conflicts == {
        operation_id
        for operation_id, statuses in EXPECTED_PROBLEM_STATUSES.items()
        if "409" in statuses
    }


def test_unhandled_error_is_generic_problem_detail_with_request_id() -> None:
    app = create_app()

    @app.get("/_test/unhandled")
    def unhandled() -> None:
        raise RuntimeError("internal value that must not reach the client")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get(
            "/_test/unhandled",
            headers={"X-Request-ID": "unhandled-contract-test"},
        )

    assert response.status_code == 500
    assert response.headers["content-type"] == "application/problem+json"
    assert response.headers["X-Request-ID"] == "unhandled-contract-test"
    assert response.json() == {
        "type": "about:blank",
        "title": "Internal server error",
        "status": 500,
        "detail": "The service could not complete the request.",
        "instance": "/_test/unhandled",
        "code": "internal_server_error",
        "request_id": "unhandled-contract-test",
    }
    assert "internal value" not in response.text
