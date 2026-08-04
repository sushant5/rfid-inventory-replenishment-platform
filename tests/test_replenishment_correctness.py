from __future__ import annotations

import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from abacus.api.errors import ApiError
from abacus.config import Settings
from abacus.enums import StoreStatus, TenantStatus, ZoneKind
from abacus.models.architecture import (
    CurrentItemState,
    FreshnessStatus,
    InventoryProjection,
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
    Product,
    ProductVariant,
    ReplenishmentTask,
    ReplenishmentTaskStatus,
    StoreConnectivity,
)
from abacus.models.catalog import ProductStyle, Sku
from abacus.models.identity import IdentityRole
from abacus.models.tenancy import Store, Tenant, Zone
from abacus.schemas.replenishment import (
    ReplenishmentEvaluationCreate,
    ReplenishmentTaskPatch,
)
from abacus.security import Principal, RoleScope
from abacus.services.replenishment import evaluate_replenishment, patch_replenishment_task

pytestmark = pytest.mark.integration


@dataclass(frozen=True, slots=True)
class ReplenishmentFixture:
    tenant_id: uuid.UUID
    store_id: uuid.UUID
    medium_sku_id: uuid.UUID
    large_sku_id: uuid.UUID
    version_id: uuid.UUID
    medium_rule_id: uuid.UUID
    large_rule_id: uuid.UUID


def _principal(
    fixture: ReplenishmentFixture,
    role: IdentityRole,
) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=fixture.tenant_id,
        email=f"{role.value.lower()}@example.test",
        display_name=role.value,
        role_scopes=(RoleScope(role, fixture.store_id),),
    )


def _add_items(
    db: Session,
    *,
    fixture_tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
    floor_zone_id: uuid.UUID,
    backroom_zone_id: uuid.UUID,
    prefix: str,
    floor_qty: int,
    backroom_qty: int,
    observed_at: datetime,
) -> None:
    for zone_id, zone_label, quantity in (
        (floor_zone_id, "F", floor_qty),
        (backroom_zone_id, "B", backroom_qty),
    ):
        for index in range(quantity):
            db.add(
                CurrentItemState(
                    tenant_id=fixture_tenant_id,
                    epc=f"{prefix}-{zone_label}-{index}",
                    sku_id=sku_id,
                    store_id=store_id,
                    zone_id=zone_id,
                    last_observed_at=observed_at,
                    last_received_at=observed_at,
                    confidence=0.95,
                    state_version=1,
                )
            )
        db.add(
            InventoryProjection(
                tenant_id=fixture_tenant_id,
                store_id=store_id,
                sku_id=sku_id,
                zone_id=zone_id,
                quantity=quantity,
                as_of=observed_at,
                confidence=0.95,
                freshness_status=FreshnessStatus.LIVE,
            )
        )


@pytest.fixture
def replenishment_fixture(
    postgres_session_factory: sessionmaker[Session],
) -> Iterator[ReplenishmentFixture]:
    now = datetime.now(UTC)
    suffix = uuid.uuid4().hex[:12]
    with postgres_session_factory() as db:
        tenant = Tenant(
            code=f"replenishment-{suffix}",
            name="Replenishment correctness",
            status=TenantStatus.ACTIVE,
        )
        db.add(tenant)
        db.flush()
        store = Store(
            tenant_id=tenant.id,
            code="store-1",
            name="Store 1",
            timezone="UTC",
            status=StoreStatus.ACTIVE,
            configuration={},
        )
        db.add(store)
        db.flush()
        floor = Zone(
            tenant_id=tenant.id,
            store_id=store.id,
            code="floor",
            name="Sales floor",
            kind=ZoneKind.SALES_FLOOR,
        )
        backroom = Zone(
            tenant_id=tenant.id,
            store_id=store.id,
            code="backroom",
            name="Backroom",
            kind=ZoneKind.BACKROOM,
        )
        style = ProductStyle(
            tenant_id=tenant.id,
            code="STYLE-1",
            name="Shirt",
            attributes={"category": "SHIRTS"},
            active=True,
        )
        product = Product(
            tenant_id=tenant.id,
            style_code="STYLE-1",
            name="Shirt",
            category="SHIRTS",
            attributes={},
            active=True,
        )
        db.add_all([floor, backroom, style, product])
        db.flush()
        variant = ProductVariant(
            tenant_id=tenant.id,
            product_id=product.id,
            color="Blue",
            attributes={},
            active=True,
        )
        db.add(variant)
        db.flush()
        medium = Sku(
            tenant_id=tenant.id,
            product_style_id=style.id,
            product_variant_id=variant.id,
            code="SKU-BLUE-M",
            upc="036000291452",
            color="Blue",
            size="M",
            attributes={},
            active=True,
        )
        large = Sku(
            tenant_id=tenant.id,
            product_style_id=style.id,
            product_variant_id=variant.id,
            code="SKU-BLUE-L",
            upc="4006381333931",
            color="Blue",
            size="L",
            attributes={},
            active=True,
        )
        db.add_all([medium, large])
        db.flush()
        db.add(
            StoreConnectivity(
                tenant_id=tenant.id,
                store_id=store.id,
                gateway_last_heartbeat=now,
                last_live_event_at=now,
                oldest_buffered_event_at=None,
                backlog_drained=True,
                reader_coverage_ok=True,
                freshness_status=FreshnessStatus.LIVE,
            )
        )
        _add_items(
            db,
            fixture_tenant_id=tenant.id,
            store_id=store.id,
            sku_id=medium.id,
            floor_zone_id=floor.id,
            backroom_zone_id=backroom.id,
            prefix="MEDIUM",
            floor_qty=1,
            backroom_qty=10,
            observed_at=now,
        )
        _add_items(
            db,
            fixture_tenant_id=tenant.id,
            store_id=store.id,
            sku_id=large.id,
            floor_zone_id=floor.id,
            backroom_zone_id=backroom.id,
            prefix="LARGE",
            floor_qty=1,
            backroom_qty=4,
            observed_at=now,
        )

        policy = PolicyDefinition(
            tenant_id=tenant.id,
            name="Size curve",
            description=None,
        )
        db.add(policy)
        db.flush()
        version = PolicyVersion(
            tenant_id=tenant.id,
            policy_id=policy.id,
            version_number=1,
            status=PolicyVersionStatus.DRAFT,
        )
        db.add(version)
        db.flush()
        medium_rule = PolicyRule(
            tenant_id=tenant.id,
            version_id=version.id,
            sku_id=medium.id,
            size="M",
            min_floor_qty=2,
            target_floor_qty=6,
            priority=10,
        )
        large_rule = PolicyRule(
            tenant_id=tenant.id,
            version_id=version.id,
            sku_id=large.id,
            size="L",
            min_floor_qty=2,
            target_floor_qty=3,
            priority=10,
        )
        db.add_all([medium_rule, large_rule])
        db.flush()
        version.status = PolicyVersionStatus.ACTIVE
        db.commit()
        fixture = ReplenishmentFixture(
            tenant_id=tenant.id,
            store_id=store.id,
            medium_sku_id=medium.id,
            large_sku_id=large.id,
            version_id=version.id,
            medium_rule_id=medium_rule.id,
            large_rule_id=large_rule.id,
        )

    try:
        yield fixture
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == fixture.tenant_id))
            db.commit()


def test_evaluation_expands_open_task_once_and_evaluates_each_size(
    postgres_session_factory: sessionmaker[Session],
    replenishment_fixture: ReplenishmentFixture,
) -> None:
    fixture = replenishment_fixture
    with postgres_session_factory() as db:
        db.add(
            ReplenishmentTask(
                tenant_id=fixture.tenant_id,
                store_id=fixture.store_id,
                sku_id=fixture.medium_sku_id,
                policy_version_id=fixture.version_id,
                policy_rule_id=fixture.medium_rule_id,
                status=ReplenishmentTaskStatus.OPEN,
                quantity=2,
                version=1,
            )
        )
        db.commit()

        first = evaluate_replenishment(
            db,
            _principal(fixture, IdentityRole.STORE_MANAGER),
            ReplenishmentEvaluationCreate(store_id=fixture.store_id),
            settings=Settings(),
            minimum_confidence=0.7,
        )

        assert [(task.sku_id, task.quantity) for task in first.tasks] == [(fixture.large_sku_id, 2)]
        assert [(task.sku_id, task.quantity, task.version) for task in first.updated_tasks] == [
            (fixture.medium_sku_id, 5, 2)
        ]
        assert first.deferred_count == 0
        assert first.deferred_quantity == 0

        repeated = evaluate_replenishment(
            db,
            _principal(fixture, IdentityRole.STORE_MANAGER),
            ReplenishmentEvaluationCreate(store_id=fixture.store_id),
            settings=Settings(),
            minimum_confidence=0.7,
        )

        assert repeated.tasks == ()
        assert repeated.updated_tasks == ()
        assert repeated.deferred_count == 0
        assert repeated.deferred_quantity == 0
        active_tasks = list(
            db.scalars(
                select(ReplenishmentTask)
                .where(
                    ReplenishmentTask.tenant_id == fixture.tenant_id,
                    ReplenishmentTask.status.in_(
                        {
                            ReplenishmentTaskStatus.OPEN,
                            ReplenishmentTaskStatus.CLAIMED,
                            ReplenishmentTaskStatus.IN_PROGRESS,
                        }
                    ),
                )
                .order_by(ReplenishmentTask.sku_id)
            ).all()
        )
        assert len(active_tasks) == 2
        assert {task.sku_id: task.quantity for task in active_tasks} == {
            fixture.medium_sku_id: 5,
            fixture.large_sku_id: 2,
        }
        assert (
            db.scalar(
                select(func.count())
                .select_from(ReplenishmentTask)
                .where(ReplenishmentTask.tenant_id == fixture.tenant_id)
            )
            == 2
        )
        db.add(
            ReplenishmentTask(
                tenant_id=fixture.tenant_id,
                store_id=fixture.store_id,
                sku_id=fixture.medium_sku_id,
                policy_version_id=fixture.version_id,
                policy_rule_id=fixture.medium_rule_id,
                status=ReplenishmentTaskStatus.OPEN,
                quantity=1,
                version=1,
            )
        )
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()


@pytest.mark.parametrize("store_status", [StoreStatus.PROVISIONING, StoreStatus.INACTIVE])
def test_evaluation_requires_an_active_store(
    postgres_session_factory: sessionmaker[Session],
    replenishment_fixture: ReplenishmentFixture,
    store_status: StoreStatus,
) -> None:
    fixture = replenishment_fixture
    with postgres_session_factory() as db:
        store = db.get(Store, fixture.store_id)
        assert store is not None
        store.status = store_status
        db.commit()

        with pytest.raises(ApiError) as caught:
            evaluate_replenishment(
                db,
                _principal(fixture, IdentityRole.STORE_MANAGER),
                ReplenishmentEvaluationCreate(store_id=fixture.store_id),
                settings=Settings(),
                minimum_confidence=0.7,
            )

        assert caught.value.status_code == 404
        assert caught.value.title == "Store not found"


def test_claimed_shortage_is_reported_without_mutating_the_task(
    postgres_session_factory: sessionmaker[Session],
    replenishment_fixture: ReplenishmentFixture,
) -> None:
    fixture = replenishment_fixture
    with postgres_session_factory() as db:
        claimed = ReplenishmentTask(
            tenant_id=fixture.tenant_id,
            store_id=fixture.store_id,
            sku_id=fixture.medium_sku_id,
            policy_version_id=fixture.version_id,
            policy_rule_id=fixture.medium_rule_id,
            status=ReplenishmentTaskStatus.CLAIMED,
            quantity=2,
            version=1,
        )
        db.add(claimed)
        db.commit()
        task_id = claimed.id

        result = evaluate_replenishment(
            db,
            _principal(fixture, IdentityRole.STORE_MANAGER),
            ReplenishmentEvaluationCreate(
                store_id=fixture.store_id,
                sku_ids=[fixture.medium_sku_id],
            ),
            settings=Settings(),
            minimum_confidence=0.7,
        )

        assert result.tasks == ()
        assert result.updated_tasks == ()
        assert result.deferred_count == 1
        assert result.deferred_quantity == 3
        db.refresh(claimed)
        assert claimed.id == task_id
        assert claimed.status == ReplenishmentTaskStatus.CLAIMED
        assert claimed.quantity == 2
        assert claimed.version == 1


def test_terminal_cancellation_is_conflict_but_unauthorized_active_cancellation_is_forbidden(
    postgres_session_factory: sessionmaker[Session],
    replenishment_fixture: ReplenishmentFixture,
) -> None:
    fixture = replenishment_fixture
    with postgres_session_factory() as db:
        completed = ReplenishmentTask(
            tenant_id=fixture.tenant_id,
            store_id=fixture.store_id,
            sku_id=fixture.medium_sku_id,
            policy_version_id=fixture.version_id,
            policy_rule_id=fixture.medium_rule_id,
            status=ReplenishmentTaskStatus.COMPLETED,
            quantity=1,
            version=1,
        )
        active = ReplenishmentTask(
            tenant_id=fixture.tenant_id,
            store_id=fixture.store_id,
            sku_id=fixture.large_sku_id,
            policy_version_id=fixture.version_id,
            policy_rule_id=fixture.large_rule_id,
            status=ReplenishmentTaskStatus.OPEN,
            quantity=1,
            version=1,
        )
        db.add_all([completed, active])
        db.commit()

        with pytest.raises(ApiError) as invalid_transition:
            patch_replenishment_task(
                db,
                _principal(fixture, IdentityRole.STORE_MANAGER),
                completed.id,
                ReplenishmentTaskPatch(status=ReplenishmentTaskStatus.CANCELED, version=1),
            )
        assert invalid_transition.value.status_code == 409
        assert invalid_transition.value.code == "invalid_task_transition"

        with pytest.raises(ApiError) as forbidden:
            patch_replenishment_task(
                db,
                _principal(fixture, IdentityRole.STORE_ASSOCIATE),
                active.id,
                ReplenishmentTaskPatch(status=ReplenishmentTaskStatus.CANCELED, version=1),
            )
        assert forbidden.value.status_code == 403
        assert forbidden.value.code == "task_transition_forbidden"
        db.refresh(active)
        assert active.status == ReplenishmentTaskStatus.OPEN
        assert active.version == 1
