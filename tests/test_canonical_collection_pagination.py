import uuid
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.orm import Session, sessionmaker

from abacus.api.errors import ApiError
from abacus.api.routes.canonical_replenishment import list_store_tasks_endpoint
from abacus.api.routes.rfid import get_store_inventory_endpoint
from abacus.config import Settings
from abacus.enums import StoreStatus, TenantStatus, ZoneKind
from abacus.main import create_app
from abacus.models.architecture import (
    CanonicalIdentityRole,
    CanonicalReplenishmentTask,
    CanonicalTaskStatus,
    FreshnessStatus,
    InventoryProjection,
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
)
from abacus.models.catalog import ProductStyle, Sku
from abacus.models.tenancy import Store, Tenant, Zone
from abacus.security import Principal, RoleScope


def _principal(
    tenant_id: uuid.UUID,
    *,
    role: CanonicalIdentityRole = CanonicalIdentityRole.TENANT_ADMIN,
    store_id: uuid.UUID | None = None,
) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="pagination-reviewer@orange.example",
        display_name="Pagination reviewer",
        role_scopes=(RoleScope(role, store_id),),
        canonical_roles=(role,),
        assigned_store_ids=(store_id,) if store_id is not None else (),
    )


def test_collection_openapi_contracts_use_bounded_offset_pages() -> None:
    schema = create_app().openapi()
    cases = (
        ("/v1/stores/{store_id}/inventory", "InventoryProjectionPage", 500),
        (
            "/v1/stores/{store_id}/replenishment-tasks",
            "ReplenishmentTaskPage",
            500,
        ),
    )

    for path, page_schema, maximum_limit in cases:
        operation = schema["paths"][path]["get"]
        parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
        assert parameters["limit"]["schema"] == {
            "type": "integer",
            "maximum": maximum_limit,
            "minimum": 1,
            "default": 100,
            "title": "Limit",
        }
        assert parameters["offset"]["schema"] == {
            "type": "integer",
            "minimum": 0,
            "default": 0,
            "title": "Offset",
        }
        response_schema = operation["responses"]["200"]["content"]["application/json"]["schema"]
        assert response_schema["$ref"] == f"#/components/schemas/{page_schema}"
        assert set(schema["components"]["schemas"][page_schema]["properties"]) == {
            "items",
            "total",
            "limit",
            "offset",
        }


@pytest.mark.integration
def test_inventory_and_task_pages_have_stable_order_totals_and_store_authorization(
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    other_store_id = uuid.uuid4()
    style_id = uuid.uuid4()
    sku_id = uuid.uuid4()
    policy_id = uuid.uuid4()
    version_id = uuid.uuid4()
    rule_id = uuid.uuid4()
    now = datetime.now(UTC)

    with postgres_session_factory() as db:
        tenant = Tenant(
            id=tenant_id,
            code=f"page-{tenant_id.hex}",
            name="Pagination tenant",
            status=TenantStatus.ACTIVE,
        )
        store = Store(
            id=store_id,
            tenant_id=tenant_id,
            code="page-store",
            name="Pagination store",
            timezone="UTC",
            status=StoreStatus.ACTIVE,
            configuration={},
        )
        style = ProductStyle(
            id=style_id,
            tenant_id=tenant_id,
            code="PAGE-STYLE",
            name="Pagination style",
            attributes={"category": "APPAREL"},
            active=True,
        )
        sku = Sku(
            id=sku_id,
            tenant_id=tenant_id,
            product_style_id=style_id,
            code="PAGE-SKU-M",
            upc=f"{tenant_id.int % 10**14:014d}",
            color="Orange",
            size="M",
            attributes={},
            active=True,
        )
        zones = [
            Zone(
                id=uuid.uuid4(),
                tenant_id=tenant_id,
                store_id=store_id,
                code=code,
                name=name,
                kind=kind,
            )
            for code, name, kind in (
                ("backroom", "Backroom", ZoneKind.BACKROOM),
                ("floor", "Sales floor", ZoneKind.SALES_FLOOR),
                ("returns", "Returns", ZoneKind.OTHER),
            )
        ]
        policy = PolicyDefinition(
            id=policy_id,
            tenant_id=tenant_id,
            name="Pagination policy",
            description=None,
        )
        version = PolicyVersion(
            id=version_id,
            tenant_id=tenant_id,
            policy_id=policy_id,
            version_number=1,
            status=PolicyVersionStatus.ACTIVE,
            activated_at=now,
        )
        rule = PolicyRule(
            id=rule_id,
            tenant_id=tenant_id,
            version_id=version_id,
            store_id=None,
            category=None,
            style_code=None,
            sku_id=None,
            size=None,
            min_floor_qty=1,
            target_floor_qty=2,
            priority=0,
        )
        db.add_all([tenant, store, style, sku, *zones, policy, version, rule])
        db.flush()
        db.add_all(
            [
                InventoryProjection(
                    tenant_id=tenant_id,
                    store_id=store_id,
                    sku_id=sku_id,
                    zone_id=zone.id,
                    quantity=index,
                    as_of=now - timedelta(minutes=index),
                    confidence=0.9,
                    freshness_status=FreshnessStatus.LIVE,
                )
                for index, zone in enumerate(zones, start=1)
            ]
        )
        task_specs = (
            (CanonicalTaskStatus.COMPLETED, now - timedelta(minutes=3)),
            (CanonicalTaskStatus.CANCELED, now - timedelta(minutes=2)),
            (CanonicalTaskStatus.EXPIRED, now - timedelta(minutes=1)),
        )
        db.add_all(
            [
                CanonicalReplenishmentTask(
                    id=uuid.uuid4(),
                    tenant_id=tenant_id,
                    store_id=store_id,
                    sku_id=sku_id,
                    policy_version_id=version_id,
                    policy_rule_id=rule_id,
                    status=status,
                    quantity=1,
                    version=1,
                    created_at=created_at,
                    updated_at=created_at,
                )
                for status, created_at in task_specs
            ]
        )
        db.flush()

        tenant_admin = _principal(tenant_id)
        inventory_page = get_store_inventory_endpoint(
            store_id,
            db,
            Settings(),
            tenant_admin,
            limit=1,
            offset=1,
        )
        assert inventory_page.total == 3
        assert inventory_page.limit == 1
        assert inventory_page.offset == 1
        assert [item.zone for item in inventory_page.items] == ["floor"]

        task_page = list_store_tasks_endpoint(
            store_id,
            db,
            tenant_admin,
            task_status=None,
            limit=1,
            offset=1,
        )
        assert task_page.total == 3
        assert task_page.limit == 1
        assert task_page.offset == 1
        assert [item.status for item in task_page.items] == [CanonicalTaskStatus.CANCELED]

        out_of_scope = _principal(
            tenant_id,
            role=CanonicalIdentityRole.STORE_ASSOCIATE,
            store_id=other_store_id,
        )
        with pytest.raises(ApiError) as inventory_forbidden:
            get_store_inventory_endpoint(
                store_id,
                db,
                Settings(),
                out_of_scope,
                limit=1,
                offset=0,
            )
        assert inventory_forbidden.value.status_code == 403

        with pytest.raises(ApiError) as tasks_forbidden:
            list_store_tasks_endpoint(
                store_id,
                db,
                out_of_scope,
                task_status=None,
                limit=1,
                offset=0,
            )
        assert tasks_forbidden.value.status_code == 403

        db.rollback()
