import uuid
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from abacus.models.replenishment import (
    PolicySelectorType,
    ReplenishmentPolicy,
    ReplenishmentReason,
    ReplenishmentTaskStatus,
)
from abacus.schemas.replenishment import PolicyBulkUpsertRequest, PolicyDefinition
from abacus.services.replenishment import (
    PolicyResolutionConflictError,
    RankedPolicy,
    calculate_replenishment_quantity,
    effective_intervals_overlap,
    select_policy_winner,
    task_movement_allowed,
    task_transition_allowed,
)


def _policy(*, priority: int = 0) -> ReplenishmentPolicy:
    now = datetime.now(UTC)
    return ReplenishmentPolicy(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        external_key=str(uuid.uuid4()),
        selector_type=PolicySelectorType.SKU,
        selector_value="SKU-1",
        minimum_floor_quantity=2,
        target_floor_quantity=5,
        priority=priority,
        effective_from=now,
        active=True,
        revision=1,
    )


@pytest.mark.parametrize(
    ("floor", "minimum", "target", "open_quantity", "backroom", "expected", "reason"),
    [
        (2, 2, 5, 0, 10, 0, ReplenishmentReason.FLOOR_AT_OR_ABOVE_MINIMUM),
        (1, 2, 5, 4, 10, 0, ReplenishmentReason.OPEN_TASK_COVERS_NEED),
        (1, 2, 5, 0, 0, 0, ReplenishmentReason.NO_BACKROOM_STOCK),
        (1, 2, 5, 0, 2, 2, ReplenishmentReason.REPLENISHMENT_REQUIRED),
        (0, 2, 5, 1, 10, 4, ReplenishmentReason.REPLENISHMENT_REQUIRED),
    ],
)
def test_calculate_replenishment_quantity(
    floor: int,
    minimum: int,
    target: int,
    open_quantity: int,
    backroom: int,
    expected: int,
    reason: ReplenishmentReason,
) -> None:
    decision = calculate_replenishment_quantity(
        floor_quantity=floor,
        minimum_floor_quantity=minimum,
        target_floor_quantity=target,
        open_task_quantity=open_quantity,
        available_backroom=backroom,
    )

    assert decision.quantity == expected
    assert decision.reason is reason


def test_calculation_rejects_invalid_quantities() -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        calculate_replenishment_quantity(
            floor_quantity=-1,
            minimum_floor_quantity=2,
            target_floor_quantity=5,
            open_task_quantity=0,
            available_backroom=3,
        )


def test_effective_intervals_are_half_open() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    boundary = start + timedelta(days=1)

    assert not effective_intervals_overlap(start, boundary, boundary, None)
    assert effective_intervals_overlap(start, None, boundary, None)


def test_policy_precedence_is_scope_then_selector_then_priority() -> None:
    tenant_sku = RankedPolicy(_policy(priority=1_000), 0, 4)
    store_size = RankedPolicy(_policy(priority=-1_000), 1, 1)
    store_style = RankedPolicy(_policy(priority=1_000), 1, 3)
    store_sku_low_priority = RankedPolicy(_policy(priority=1), 1, 4)
    store_sku_high_priority = RankedPolicy(_policy(priority=2), 1, 4)

    assert select_policy_winner([tenant_sku, store_size]) is store_size.policy
    assert select_policy_winner([store_style, store_sku_low_priority]) is (
        store_sku_low_priority.policy
    )
    assert select_policy_winner([store_sku_low_priority, store_sku_high_priority]) is (
        store_sku_high_priority.policy
    )


def test_policy_precedence_rejects_unresolved_tie() -> None:
    candidates = [
        RankedPolicy(_policy(priority=10), 1, 4),
        RankedPolicy(_policy(priority=10), 1, 4),
    ]

    with pytest.raises(PolicyResolutionConflictError):
        select_policy_winner(candidates)


def test_policy_definition_validates_threshold_order_and_timezones() -> None:
    now = datetime.now(UTC)
    common = {
        "external_key": "floor-minimum",
        "selector_type": "SKU",
        "selector_value": "sku-1",
        "effective_from": now,
    }
    with pytest.raises(ValidationError, match="target_floor_quantity"):
        PolicyDefinition(
            **common,
            minimum_floor_quantity=5,
            target_floor_quantity=4,
        )
    with pytest.raises(ValidationError, match="maximum_floor_quantity"):
        PolicyDefinition(
            **common,
            minimum_floor_quantity=2,
            target_floor_quantity=5,
            maximum_floor_quantity=4,
        )
    with pytest.raises(ValidationError, match="timezone offset"):
        PolicyDefinition(
            **{**common, "effective_from": datetime(2026, 1, 1)},
            minimum_floor_quantity=2,
            target_floor_quantity=5,
        )


def test_bulk_policy_request_requires_unique_external_keys() -> None:
    definition = PolicyDefinition(
        external_key="duplicate-key",
        selector_type=PolicySelectorType.SKU,
        selector_value="sku-1",
        minimum_floor_quantity=2,
        target_floor_quantity=5,
        effective_from=datetime.now(UTC),
    )

    with pytest.raises(ValidationError, match="external_key values must be unique"):
        PolicyBulkUpsertRequest(policies=[definition, definition])


def test_task_lifecycle_allows_only_declared_transitions() -> None:
    assert task_transition_allowed(
        ReplenishmentTaskStatus.OPEN,
        ReplenishmentTaskStatus.CLAIMED,
    )
    assert task_transition_allowed(
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
        ReplenishmentTaskStatus.VERIFIED,
    )
    assert not task_transition_allowed(
        ReplenishmentTaskStatus.OPEN,
        ReplenishmentTaskStatus.VERIFIED,
    )
    assert not task_transition_allowed(
        ReplenishmentTaskStatus.VERIFIED,
        ReplenishmentTaskStatus.OPEN,
    )
    assert not task_transition_allowed(
        ReplenishmentTaskStatus.EXCEPTION,
        ReplenishmentTaskStatus.OPEN,
    )
    assert not task_transition_allowed(
        ReplenishmentTaskStatus.EXCEPTION,
        ReplenishmentTaskStatus.CANCELLED,
    )

    assert not task_movement_allowed(
        ReplenishmentTaskStatus.CLAIMED,
        ReplenishmentTaskStatus.IN_PROGRESS,
    )
    assert task_movement_allowed(
        ReplenishmentTaskStatus.IN_PROGRESS,
        ReplenishmentTaskStatus.IN_PROGRESS,
    )
    assert task_movement_allowed(
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
        ReplenishmentTaskStatus.IN_PROGRESS,
    )
    assert not task_movement_allowed(
        ReplenishmentTaskStatus.OPEN,
        ReplenishmentTaskStatus.CLAIMED,
    )
    assert not task_movement_allowed(
        ReplenishmentTaskStatus.IN_PROGRESS,
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
    )
    assert not task_movement_allowed(
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
        ReplenishmentTaskStatus.VERIFIED,
    )
