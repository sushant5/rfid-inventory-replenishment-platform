import pytest
from pydantic import ValidationError

from abacus.enums import ZoneKind
from abacus.schemas.tenancy import (
    DeviceCreate,
    OrganizationUnitSegment,
    StoreCreate,
    TenantCreate,
)


@pytest.mark.parametrize(
    ("schema", "payload", "field_name"),
    [
        (TenantCreate, {"code": "orange", "name": "   "}, "name"),
        (
            OrganizationUnitSegment,
            {"code": "west", "name": "   ", "unit_type": "REGION"},
            "name",
        ),
        (
            StoreCreate,
            {
                "code": "store-1",
                "name": "   ",
                "timezone": "UTC",
                "zones": [
                    {"code": "floor", "name": "Floor", "kind": ZoneKind.SALES_FLOOR},
                    {"code": "stock", "name": "Stock", "kind": ZoneKind.BACKROOM},
                ],
            },
            "name",
        ),
        (
            DeviceCreate,
            {
                "serial_number": "reader-1",
                "display_name": "   ",
                "zone_code": "floor",
            },
            "display_name",
        ),
    ],
)
def test_tenancy_names_reject_whitespace_only_values(
    schema: type[TenantCreate]
    | type[OrganizationUnitSegment]
    | type[StoreCreate]
    | type[DeviceCreate],
    payload: dict[str, object],
    field_name: str,
) -> None:
    with pytest.raises(ValidationError) as error:
        schema.model_validate(payload)

    assert any(item["loc"] == (field_name,) for item in error.value.errors())


def test_tenancy_names_are_trimmed_before_persistence() -> None:
    tenant = TenantCreate(code="orange", name="  Orange  ")
    organization_unit = OrganizationUnitSegment(
        code="west",
        name="  West Region  ",
        unit_type="REGION",
    )
    store = StoreCreate(
        code="store-1",
        name="  Market Street  ",
        timezone="UTC",
        organization_path=[organization_unit],
        zones=[
            {"code": "floor", "name": "Floor", "kind": ZoneKind.SALES_FLOOR},
            {"code": "stock", "name": "Stock", "kind": ZoneKind.BACKROOM},
        ],
        devices=[
            DeviceCreate(
                serial_number="reader-1",
                display_name="  Floor Reader  ",
                zone_code="floor",
            )
        ],
    )

    assert tenant.name == "Orange"
    assert organization_unit.name == "West Region"
    assert store.name == "Market Street"
    assert store.devices[0].display_name == "Floor Reader"
