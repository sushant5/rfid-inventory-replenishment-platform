import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi import FastAPI
from pydantic import ValidationError
from sqlalchemy import delete
from sqlalchemy.orm import Session, sessionmaker

from abacus.api.errors import ApiError
from abacus.api.routes.replenishment import router
from abacus.models.architecture import (
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
    ReplenishmentTask,
    ReplenishmentTaskStatus,
    ReplenishmentVerificationStatus,
)
from abacus.models.identity import IdentityRole
from abacus.models.tenancy import Tenant
from abacus.schemas.replenishment import (
    PolicyCreate,
    PolicyRuleWrite,
    ReplenishmentEvaluationCreate,
)
from abacus.security import Principal, RoleScope
from abacus.services.replenishment import (
    PolicyBundle,
    RuleDescriptor,
    SkuContext,
    _visible_bundle,
    calculate_replenishment_quantity,
    get_policy_bundle,
    list_policy_bundles,
    replenishment_verification_status,
    rule_precedence,
    rules_overlap,
    select_policy_rule,
)


@pytest.mark.parametrize(
    ("status", "verified_quantity", "deadline_offset", "expected"),
    [
        (
            ReplenishmentTaskStatus.IN_PROGRESS,
            0,
            60,
            ReplenishmentVerificationStatus.NOT_APPLICABLE,
        ),
        (
            ReplenishmentTaskStatus.COMPLETED,
            1,
            60,
            ReplenishmentVerificationStatus.PENDING,
        ),
        (
            ReplenishmentTaskStatus.COMPLETED,
            2,
            -60,
            ReplenishmentVerificationStatus.VERIFIED,
        ),
        (
            ReplenishmentTaskStatus.COMPLETED,
            1,
            -60,
            ReplenishmentVerificationStatus.UNVERIFIED,
        ),
    ],
)
def test_replenishment_verification_status_is_derived_from_evidence_and_deadline(
    status: ReplenishmentTaskStatus,
    verified_quantity: int,
    deadline_offset: int,
    expected: ReplenishmentVerificationStatus,
) -> None:
    now = datetime.now(UTC)
    task = ReplenishmentTask(
        id=uuid.uuid4(),
        tenant_id=uuid.uuid4(),
        store_id=uuid.uuid4(),
        sku_id=uuid.uuid4(),
        policy_version_id=uuid.uuid4(),
        policy_rule_id=uuid.uuid4(),
        status=status,
        quantity=2,
        verified_quantity=verified_quantity,
        verification_deadline=now + timedelta(seconds=deadline_offset),
        version=1,
    )

    assert replenishment_verification_status(task, evaluated_at=now) is expected


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
    assert [status.value for status in ReplenishmentTaskStatus] == [
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


def test_store_scoped_policy_reader_sees_only_global_and_assigned_store_rules() -> None:
    tenant_id = uuid.uuid4()
    assigned_store_id = uuid.uuid4()
    other_store_id = uuid.uuid4()
    policy_id = uuid.uuid4()
    version_id = uuid.uuid4()
    policy = PolicyDefinition(
        id=policy_id,
        tenant_id=tenant_id,
        name="Scoped policy",
        description=None,
    )
    version = PolicyVersion(
        id=version_id,
        tenant_id=tenant_id,
        policy_id=policy_id,
        version_number=1,
        status=PolicyVersionStatus.ACTIVE,
    )

    def rule(store_id: uuid.UUID | None) -> PolicyRule:
        return PolicyRule(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            version_id=version_id,
            store_id=store_id,
            category="APPAREL",
            min_floor_qty=1,
            target_floor_qty=2,
            priority=0,
        )

    global_rule = rule(None)
    assigned_rule = rule(assigned_store_id)
    hidden_rule = rule(other_store_id)
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="associate@example.com",
        display_name="Associate",
        role_scopes=(RoleScope(IdentityRole.STORE_ASSOCIATE, assigned_store_id),),
    )

    visible = _visible_bundle(
        PolicyBundle(policy, version, (global_rule, assigned_rule, hidden_rule)),
        principal,
    )
    assert visible is not None
    assert visible.rules == (global_rule, assigned_rule)
    assert _visible_bundle(PolicyBundle(policy, version, (hidden_rule,)), principal) is None


@pytest.mark.integration
def test_policy_discovery_prefers_active_and_can_select_latest_draft(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id = uuid.uuid4()
    policy_id = uuid.uuid4()
    active_version_id = uuid.uuid4()
    draft_version_id = uuid.uuid4()
    principal = Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="admin@example.com",
        display_name="Admin",
        role_scopes=(RoleScope(IdentityRole.CORPORATE_ADMIN, None),),
    )

    with postgres_session_factory() as db:
        db.add(Tenant(id=tenant_id, code=f"policy-{tenant_id.hex}", name="Policy test"))
        db.flush()
        db.add(
            PolicyDefinition(
                id=policy_id,
                tenant_id=tenant_id,
                name="Effective version selection",
                description=None,
            )
        )
        db.flush()
        db.add_all(
            [
                PolicyVersion(
                    id=active_version_id,
                    tenant_id=tenant_id,
                    policy_id=policy_id,
                    version_number=1,
                    status=PolicyVersionStatus.DRAFT,
                ),
                PolicyVersion(
                    id=draft_version_id,
                    tenant_id=tenant_id,
                    policy_id=policy_id,
                    version_number=2,
                    status=PolicyVersionStatus.DRAFT,
                ),
            ]
        )
        db.flush()
        for version_id in (active_version_id, draft_version_id):
            db.add(
                PolicyRule(
                    id=uuid.uuid4(),
                    tenant_id=tenant_id,
                    version_id=version_id,
                    min_floor_qty=1,
                    target_floor_qty=2,
                    priority=0,
                )
            )
        db.flush()
        active_version = db.get(PolicyVersion, active_version_id)
        assert active_version is not None
        active_version.status = PolicyVersionStatus.ACTIVE
        db.commit()

        try:
            effective = get_policy_bundle(db, principal, policy_id)
            draft = get_policy_bundle(
                db,
                principal,
                policy_id,
                version_status=PolicyVersionStatus.DRAFT,
            )
            effective_list, total = list_policy_bundles(
                db,
                principal,
                limit=10,
                offset=0,
            )
            draft_list, draft_total = list_policy_bundles(
                db,
                principal,
                limit=10,
                offset=0,
                version_status=PolicyVersionStatus.DRAFT,
            )
            read_only_principal = Principal(
                user_id=uuid.uuid4(),
                tenant_id=tenant_id,
                email="associate@example.com",
                display_name="Associate",
                role_scopes=(RoleScope(IdentityRole.STORE_ASSOCIATE, uuid.uuid4()),),
            )
            read_only_effective = get_policy_bundle(db, read_only_principal, policy_id)

            assert effective.version.id == active_version_id
            assert draft.version.id == draft_version_id
            assert read_only_effective.version.id == active_version_id
            assert total == draft_total == 1
            assert [bundle.version.id for bundle in effective_list] == [active_version_id]
            assert [bundle.version.id for bundle in draft_list] == [draft_version_id]
            with pytest.raises(ApiError) as forbidden:
                get_policy_bundle(
                    db,
                    read_only_principal,
                    policy_id,
                    version_status=PolicyVersionStatus.DRAFT,
                )
            assert forbidden.value.status_code == 403
            assert forbidden.value.code == "policy_version_status_forbidden"
        finally:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


def test_replenishment_api_is_versioned_and_jwt_secured() -> None:
    app = FastAPI()
    app.include_router(router)
    paths = app.openapi()["paths"]
    expected = {
        ("get", "/v1/replenishment-policies"),
        ("post", "/v1/replenishment-policies"),
        ("get", "/v1/replenishment-policies/{policy_id}"),
        ("post", "/v1/replenishment-policies/{policy_id}/versions"),
        ("patch", "/v1/replenishment-policy-versions/{version_id}"),
        ("post", "/v1/replenishment-policy-versions/{version_id}/activate"),
        ("post", "/v1/replenishment/evaluations"),
        ("get", "/v1/stores/{store_id}/replenishment-tasks"),
        ("patch", "/v1/replenishment-tasks/{task_id}"),
    }

    for method, path in expected:
        assert paths[path][method]["security"] == [{"HTTPBearer": []}]
