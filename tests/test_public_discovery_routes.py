import uuid
from datetime import UTC, datetime
from unittest.mock import Mock

from sqlalchemy.orm import Session

from abacus.api.routes import catalog as catalog_routes
from abacus.api.routes.onboarding import list_store_devices_endpoint
from abacus.enums import DeviceStatus
from abacus.models.architecture import CanonicalIdentityRole
from abacus.models.catalog import ProductStyle, Sku
from abacus.models.tenancy import Device, DeviceAssignment
from abacus.schemas.catalog import SkuActivityFilter
from abacus.security import Principal, RoleScope


def _principal(tenant_id: uuid.UUID, role: CanonicalIdentityRole) -> Principal:
    return Principal(
        user_id=uuid.uuid4(),
        tenant_id=tenant_id,
        email="reviewer@orange.example",
        display_name="Reviewer",
        role_scopes=(RoleScope(role, None),),
        canonical_roles=(role,),
    )


def _catalog_models(tenant_id: uuid.UUID) -> tuple[Sku, ProductStyle]:
    style_id = uuid.uuid4()
    style = ProductStyle(
        id=style_id,
        tenant_id=tenant_id,
        code="STYLE-1",
        name="Trail Shirt",
        attributes={"category": "SHIRTS"},
        active=True,
    )
    sku = Sku(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        product_style_id=style_id,
        product_variant_id=uuid.uuid4(),
        code="SKU-1-M",
        upc="036000291452",
        color="Blue",
        size="M",
        attributes={},
        active=True,
    )
    return sku, style


def test_canonical_sku_discovery_uses_authenticated_tenant(
    monkeypatch,
) -> None:
    tenant_id = uuid.uuid4()
    principal = _principal(tenant_id, CanonicalIdentityRole.CORPORATE_USER)
    sku, style = _catalog_models(tenant_id)
    observed_tenant_ids: list[uuid.UUID] = []

    def fake_list_skus(
        _db: Session,
        requested_tenant_id: uuid.UUID,
        *,
        active: bool | None,
        code: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[tuple[Sku, ProductStyle]], int]:
        observed_tenant_ids.append(requested_tenant_id)
        assert (active, code, limit, offset) == (True, "SKU-1-M", 25, 0)
        return [(sku, style)], 1

    monkeypatch.setattr(catalog_routes, "list_skus", fake_list_skus)

    result = catalog_routes.list_skus_canonical_endpoint(
        Mock(spec=Session),
        principal,
        active=SkuActivityFilter.ACTIVE,
        code="SKU-1-M",
        limit=25,
        offset=0,
    )

    assert observed_tenant_ids == [tenant_id]
    assert result.total == 1
    assert result.items[0].id == sku.id
    assert result.items[0].style_code == "STYLE-1"


def test_canonical_sku_detail_uses_authenticated_tenant(monkeypatch) -> None:
    tenant_id = uuid.uuid4()
    principal = _principal(tenant_id, CanonicalIdentityRole.CORPORATE_USER)
    sku, style = _catalog_models(tenant_id)

    def fake_get_sku(
        _db: Session,
        requested_tenant_id: uuid.UUID,
        requested_sku_id: uuid.UUID,
    ) -> tuple[Sku, ProductStyle]:
        assert requested_tenant_id == tenant_id
        assert requested_sku_id == sku.id
        return sku, style

    monkeypatch.setattr(catalog_routes, "get_sku", fake_get_sku)

    result = catalog_routes.get_sku_canonical_endpoint(
        sku.id,
        Mock(spec=Session),
        principal,
    )

    assert result.id == sku.id
    assert result.code == "SKU-1-M"


def test_store_device_discovery_returns_current_assignment_mapping() -> None:
    tenant_id = uuid.uuid4()
    store_id = uuid.uuid4()
    zone_id = uuid.uuid4()
    device_id = uuid.uuid4()
    principal = _principal(tenant_id, CanonicalIdentityRole.TENANT_ADMIN)
    device = Device(
        id=device_id,
        tenant_id=tenant_id,
        serial_number="ORANGE-FLOOR-1",
        display_name="Floor Reader",
        status=DeviceStatus.ACTIVE,
    )
    assignment = DeviceAssignment(
        id=uuid.uuid4(),
        tenant_id=tenant_id,
        device_id=device_id,
        store_id=store_id,
        zone_id=zone_id,
        effective_from=datetime.now(UTC),
        effective_to=None,
    )
    db = Mock(spec=Session)
    db.scalar.return_value = store_id
    db.execute.return_value.all.return_value = [(device, assignment)]

    result = list_store_devices_endpoint(store_id, db, principal)

    assert len(result) == 1
    assert result[0].device.id == device_id
    assert result[0].assignment.device_id == device_id
    assert result[0].assignment.store_id == store_id
    assert result[0].assignment.zone_id == zone_id
