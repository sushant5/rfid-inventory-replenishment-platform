from collections.abc import Generator

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from abacus.api.errors import install_error_handlers
from abacus.schemas.replenishment import PolicyPatch

app = FastAPI()
install_error_handlers(app)


@app.patch("/policy")
def patch_policy(request: PolicyPatch) -> dict[str, object]:
    return request.model_dump(mode="json", exclude_unset=True)


@pytest.fixture
def client() -> Generator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


@pytest.mark.parametrize(
    "field_name",
    [
        "selector_type",
        "selector_value",
        "minimum_floor_quantity",
        "target_floor_quantity",
        "priority",
        "effective_from",
        "active",
    ],
)
def test_policy_patch_rejects_explicit_null_for_non_nullable_fields(
    client: TestClient,
    field_name: str,
) -> None:
    response = client.patch("/policy", json={field_name: None})

    assert response.status_code == 422
    assert response.headers["content-type"] == "application/problem+json"
    problem = response.json()
    assert problem["code"] == "request_validation_failed"
    assert problem["errors"][0]["loc"] == ["body", field_name]
    assert problem["errors"][0]["msg"] == f"Value error, {field_name} cannot be null"


def test_policy_patch_allows_nullable_fields_to_be_cleared(client: TestClient) -> None:
    request = {
        "store_id": None,
        "maximum_floor_quantity": None,
        "effective_to": None,
    }

    response = client.patch("/policy", json=request)

    assert response.status_code == 200
    assert response.json() == request


def test_policy_patch_openapi_marks_only_clearable_fields_nullable() -> None:
    properties = app.openapi()["components"]["schemas"]["PolicyPatch"]["properties"]
    non_nullable_fields = {
        "selector_type",
        "selector_value",
        "minimum_floor_quantity",
        "target_floor_quantity",
        "priority",
        "effective_from",
        "active",
    }
    nullable_fields = {"store_id", "maximum_floor_quantity", "effective_to"}

    for field_name in non_nullable_fields:
        assert {"type": "null"} not in properties[field_name].get("anyOf", [])
    for field_name in nullable_fields:
        assert {"type": "null"} in properties[field_name]["anyOf"]
    assert properties["selector_value"]["minLength"] == 1
    assert properties["selector_value"]["maxLength"] == 128
    assert properties["minimum_floor_quantity"]["minimum"] == 0
    assert properties["target_floor_quantity"]["minimum"] == 0
    assert properties["priority"]["minimum"] == -1_000_000
    assert properties["priority"]["maximum"] == 1_000_000
