from fastapi.testclient import TestClient

from abacus.main import create_app


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

    assert len(operations) == 40
    assert all("500" in operation["responses"] for operation in operations)
    secured = [operation for operation in operations if operation.get("security")]
    assert len(secured) == 36
    assert all("401" in operation["responses"] for operation in secured)
    assert "401" in schema["paths"]["/v1/auth/login"]["post"]["responses"]

    inventory = schema["paths"]["/v1/stores/{store_id}/inventory"]["get"]
    assert "`inventory:read`" in inventory["responses"]["403"]["description"]
    assert "404" in inventory["responses"]
    assert "403" in schema["paths"]["/v1/rfid/observation-batches"]["post"]["responses"]
    assert "503" in schema["paths"]["/health/ready"]["get"]["responses"]
    assert "429" in schema["paths"]["/v1/auth/login"]["post"]["responses"]


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
