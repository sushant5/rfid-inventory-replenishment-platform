from pydantic import TypeAdapter

from abacus.main import create_app
from abacus.schemas.catalog import SkuActivityFilter


def test_sku_activity_filter_has_explicit_three_way_semantics() -> None:
    adapter = TypeAdapter(SkuActivityFilter)

    assert adapter.validate_python("ACTIVE").database_value() is True
    assert adapter.validate_python("INACTIVE").database_value() is False
    assert adapter.validate_python("ALL").database_value() is None


def test_sku_activity_filter_keeps_boolean_query_aliases() -> None:
    adapter = TypeAdapter(SkuActivityFilter)

    assert adapter.validate_python("true") is SkuActivityFilter.ACTIVE
    assert adapter.validate_python("false") is SkuActivityFilter.INACTIVE


def test_sku_activity_filter_is_explicit_in_openapi() -> None:
    schema = create_app().openapi()
    operation = schema["paths"]["/v1/skus"]["get"]
    active_parameter = next(
        parameter for parameter in operation["parameters"] if parameter["name"] == "active"
    )

    assert active_parameter["schema"]["$ref"] == "#/components/schemas/SkuActivityFilter"
    assert schema["components"]["schemas"]["SkuActivityFilter"]["enum"] == [
        "ACTIVE",
        "INACTIVE",
        "ALL",
    ]
    assert "true/false" in active_parameter["description"]
