import uuid

import pytest
from fastapi import FastAPI
from pydantic import ValidationError

from abacus.api.routes.canonical_replenishment import router
from abacus.models.architecture import CanonicalTaskStatus
from abacus.schemas.canonical_replenishment import (
    PolicyCreate,
    PolicyRuleWrite,
    ReplenishmentEvaluationCreate,
)
from abacus.services.canonical_replenishment import (
    RuleDescriptor,
    SkuContext,
    calculate_replenishment_quantity,
    rule_precedence,
    rules_overlap,
    select_policy_rule,
)


def _rule(
    *,
    store_id: uuid.UUID | None = None,
    category: str | None = None,
    style_code: str | None = None,
    sku_id: uuid.UUID | None = None,
    size: str | None = None,
    priority: int = 0,
) -> RuleDescriptor:
    return RuleDescriptor(
        id=uuid.uuid4(),
        version_id=uuid.uuid4(),
        store_id=store_id,
        category=category,
        style_code=style_code,
        sku_id=sku_id,
        size=size,
        min_floor_qty=2,
        target_floor_qty=6,
        priority=priority,
    )


@pytest.mark.parametrize(
    ("floor", "backroom", "open_qty", "minimum", "target", "expected"),
    [
        (2, 10, 0, 2, 6, 0),
        (1, 10, 0, 2, 6, 5),
        (1, 3, 0, 2, 6, 3),
        (1, 10, 2, 2, 6, 3),
        (0, 10, 20, 2, 6, 0),
    ],
)
def test_replenishment_formula_is_exact(
    floor: int,
    backroom: int,
    open_qty: int,
    minimum: int,
    target: int,
    expected: int,
) -> None:
    assert (
        calculate_replenishment_quantity(
            floor_qty=floor,
            backroom_qty=backroom,
            open_task_qty=open_qty,
            min_floor_qty=minimum,
            target_floor_qty=target,
        )
        == expected
    )


def test_rule_precedence_matches_the_required_seven_levels() -> None:
    store_id = uuid.uuid4()
    sku_id = uuid.uuid4()
    item = SkuContext(sku_id=sku_id, style_code="STYLE-1", category="SHOES", size="M")
    rules = [
        _rule(store_id=store_id, sku_id=sku_id, size="M"),
        _rule(store_id=store_id, style_code="STYLE-1"),
        _rule(store_id=store_id, category="SHOES"),
        _rule(sku_id=sku_id),
        _rule(style_code="STYLE-1"),
        _rule(category="SHOES"),
        _rule(),
    ]

    assert [rule_precedence(rule, store_id=store_id, item=item) for rule in rules] == [
        7,
        6,
        5,
        4,
        3,
        2,
        1,
    ]
    assert select_policy_rule(rules, store_id=store_id, item=item) == rules[0]


def test_priority_breaks_equal_specificity_and_equal_priority_is_rejected() -> None:
    store_id = uuid.uuid4()
    item = SkuContext(
        sku_id=uuid.uuid4(),
        style_code="STYLE-1",
        category="SHOES",
        size="M",
    )
    lower = _rule(store_id=store_id, style_code="STYLE-1", priority=10)
    higher = _rule(store_id=store_id, style_code="STYLE-1", priority=20)

    assert select_policy_rule([lower, higher], store_id=store_id, item=item) == higher
    with pytest.raises(ValueError, match="equal-priority"):
        select_policy_rule(
            [lower, _rule(store_id=store_id, style_code="STYLE-1", priority=10)],
            store_id=store_id,
            item=item,
        )


def test_sku_size_wildcard_overlap_is_detected() -> None:
    sku_id = uuid.uuid4()

    assert rules_overlap(_rule(sku_id=sku_id), _rule(sku_id=sku_id, size="M"))


def test_rule_schema_rejects_undefined_or_ambiguous_scopes() -> None:
    with pytest.raises(ValidationError, match="store-default"):
        PolicyRuleWrite(
            store_id=uuid.uuid4(),
            min_floor_qty=1,
            target_floor_qty=2,
        )
    with pytest.raises(ValidationError, match="not multiple"):
        PolicyRuleWrite(
            category="SHOES",
            style_code="STYLE-1",
            min_floor_qty=1,
            target_floor_qty=2,
        )


def test_task_statuses_are_the_exact_canonical_state_machine() -> None:
    assert [status.value for status in CanonicalTaskStatus] == [
        "OPEN",
        "CLAIMED",
        "IN_PROGRESS",
        "COMPLETED",
        "CANCELED",
        "EXPIRED",
    ]


def test_canonical_bodies_reject_client_supplied_tenant_id() -> None:
    tenant_id = str(uuid.uuid4())
    rule = {"min_floor_qty": 1, "target_floor_qty": 2}

    with pytest.raises(ValidationError):
        PolicyCreate.model_validate(
            {"tenant_id": tenant_id, "name": "Orange default", "rules": [rule]}
        )
    with pytest.raises(ValidationError):
        ReplenishmentEvaluationCreate.model_validate(
            {
                "tenant_id": tenant_id,
                "store_id": str(uuid.uuid4()),
            }
        )


def test_canonical_replenishment_api_is_versioned_and_jwt_secured() -> None:
    app = FastAPI()
    app.include_router(router)
    paths = app.openapi()["paths"]
    expected = {
        ("post", "/v1/replenishment-policies"),
        ("post", "/v1/replenishment-policies/{policy_id}/versions"),
        ("patch", "/v1/replenishment-policy-versions/{version_id}"),
        ("post", "/v1/replenishment-policy-versions/{version_id}/activate"),
        ("post", "/v1/replenishment/evaluations"),
        ("get", "/v1/stores/{store_id}/replenishment-tasks"),
        ("patch", "/v1/replenishment-tasks/{task_id}"),
    }

    for method, path in expected:
        assert paths[path][method]["security"] == [{"HTTPBearer": []}]
