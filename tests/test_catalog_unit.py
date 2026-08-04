import csv
import hashlib
import io
from datetime import UTC, datetime
from unittest.mock import Mock
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from abacus.api.errors import ApiError
from abacus.models.catalog import CatalogImport, CatalogImportMode
from abacus.schemas.catalog import CatalogRowData, normalize_epc, normalize_upc
from abacus.services.catalog import (
    MAX_CATALOG_FILE_BYTES,
    accept_catalog_import,
    parse_catalog_csv,
    resolve_active_epc,
)

HEADER = "style_code,style_name,sku,upc,color,size,epc,attributes,attr.brand\n"


def csv_bytes(*rows: str) -> bytes:
    return (HEADER + "\n".join(rows) + "\n").encode()


def catalog_json_csv(field_name: str, value: str) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "style_code",
            "style_name",
            "sku",
            "upc",
            "color",
            "size",
            "epc",
            "attributes",
            "style_attributes",
        ],
    )
    writer.writeheader()
    writer.writerow(
        {
            "style_code": "ST-1",
            "style_name": "Trail Shirt",
            "sku": "SKU-1",
            "upc": "036000291452",
            "color": "Blue",
            "size": "M",
            "epc": "3074257BF7194E4000001A85",
            field_name: value,
        }
    )
    return buffer.getvalue().encode()


def test_catalog_row_normalizes_identifiers() -> None:
    row = CatalogRowData(
        style_code=" trail.01 ",
        style_name="  Trail   Shirt ",
        sku=" sku_blue-m ",
        upc="0360-0029-1452",
        color="  Ocean   Blue ",
        size=" M ",
        epc="30-74-25-7b-f7-19-4e-40-00-00-1a-85",
        attributes={"season": "summer"},
    )

    assert row.style_code == "TRAIL.01"
    assert row.style_name == "Trail Shirt"
    assert row.sku == "SKU_BLUE-M"
    assert row.upc == "036000291452"
    assert row.epc == "3074257BF7194E4000001A85"
    assert row.color == "Ocean Blue"


@pytest.mark.parametrize("value", ["036000291453", "ABC123", "1234567"])
def test_invalid_gtin_is_rejected(value: str) -> None:
    with pytest.raises(ValueError, match="upc"):
        normalize_upc(value)


def test_epc_uri_and_hex_have_stable_canonical_forms() -> None:
    assert (
        normalize_epc(" URN:EPC:ID:SGTIN:0614141.112345.400 ")
        == "urn:epc:id:sgtin:0614141.112345.400"
    )
    assert normalize_epc("0x30:74:25:7b:f7:19:4e:40") == "3074257BF7194E40"


def test_csv_parser_normalizes_and_merges_attributes() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "st-1,Trail Shirt,sku-1,036000291452,Blue,M,"
            '3074257BF7194E4000001A85,"{""season"":""summer""}",Orange'
        )
    )

    assert result.issues == []
    assert len(result.rows) == 1
    assert result.rows[0].issues == []
    assert result.rows[0].normalized is not None
    assert result.rows[0].normalized.attributes == {
        "season": "summer",
        "brand": "Orange",
    }


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity", "1e10000"])
@pytest.mark.parametrize(
    ("field_name", "issue_code"),
    [
        ("attributes", "invalid_attributes"),
        ("style_attributes", "invalid_style_attributes"),
    ],
)
def test_csv_parser_rejects_non_finite_json_numbers(
    constant: str,
    field_name: str,
    issue_code: str,
) -> None:
    result = parse_catalog_csv(catalog_json_csv(field_name, f'{{"score":{constant}}}'))

    assert result.issues == []
    assert result.rows[0].normalized is None
    assert result.rows[0].issues[0].field == field_name
    assert result.rows[0].issues[0].code == issue_code
    assert "non-finite JSON number" in result.rows[0].issues[0].message


@pytest.mark.parametrize(
    ("field_name", "issue_code"),
    [
        ("attributes", "invalid_attributes"),
        ("style_attributes", "invalid_style_attributes"),
    ],
)
def test_csv_parser_rejects_deep_json_as_a_row_error(
    field_name: str,
    issue_code: str,
) -> None:
    deeply_nested = '{"value":' + "[" * 1_100 + "0" + "]" * 1_100 + "}"
    content = catalog_json_csv(field_name, deeply_nested)
    assert len(deeply_nested.encode()) < 16_384

    result = parse_catalog_csv(content)

    assert result.issues == []
    assert result.rows[0].normalized is None
    assert result.rows[0].issues[0].field == field_name
    assert result.rows[0].issues[0].code == issue_code
    assert "nested JSON levels" in result.rows[0].issues[0].message


def test_same_sku_may_have_multiple_distinct_epcs() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A85,,",
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A86,,",
        )
    )

    assert result.issues == []
    assert all(not row.issues for row in result.rows)


def test_skus_under_one_style_may_have_different_variant_attributes() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A85,"
            '"{""material"":""cotton""}",',
            "ST-1,Trail Shirt,SKU-2,4006381333931,Red,L,3074257BF7194E4000001A86,"
            '"{""material"":""linen""}",',
        )
    )

    assert result.issues == []
    assert all(not row.issues for row in result.rows)


def test_duplicate_epc_rejects_later_row_with_evidence() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A85,,",
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A85,,",
        )
    )

    assert result.rows[0].issues == []
    assert result.rows[1].issues[0].code == "duplicate_epc"
    assert result.rows[1].issues[0].evidence == {"first_seen_at_row": 2}


def test_conflicting_upc_mapping_is_rejected() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "ST-1,Trail Shirt,SKU-1,036000291452,Blue,M,3074257BF7194E4000001A85,,",
            "ST-2,Hiking Pant,SKU-2,036000291452,Black,L,3074257BF7194E4000001A86,,",
        )
    )

    assert {issue.code for issue in result.rows[1].issues} == {"upc_conflict"}


def test_missing_required_header_is_a_file_level_error() -> None:
    result = parse_catalog_csv(b"style_code,sku\nST-1,SKU-1\n")

    assert result.rows == []
    assert result.issues[0].code == "missing_header"
    assert "epc" in result.issues[0].evidence["missing"]


def test_invalid_row_does_not_prevent_other_rows_being_preserved() -> None:
    result = parse_catalog_csv(
        csv_bytes(
            "ST-1,Trail Shirt,SKU-1,036000291453,Blue,M,NOT-AN-EPC,,",
            "ST-2,Hiking Pant,SKU-2,4006381333931,Black,L,3074257BF7194E4000001A86,,",
        )
    )

    assert len(result.rows) == 2
    assert {issue.field for issue in result.rows[0].issues} == {"upc", "epc"}
    assert result.rows[1].normalized is not None
    assert result.rows[1].issues == []


def test_resolve_active_epc_rejects_naive_timestamp() -> None:
    db = Mock()

    with pytest.raises(ValueError, match="timezone-aware"):
        resolve_active_epc(db, uuid4(), "3074257BF7194E40", datetime(2026, 1, 1))

    db.scalar.assert_not_called()


def test_resolve_active_epc_returns_none_for_invalid_identifier() -> None:
    db = Mock()

    assert resolve_active_epc(db, uuid4(), "bad-epc", datetime.now(UTC)) is None
    db.scalar.assert_not_called()


def test_import_mode_is_explicit() -> None:
    assert {mode.value for mode in CatalogImportMode} == {"DELTA", "FULL"}


def test_catalog_acceptance_rejects_source_larger_than_hosted_limit() -> None:
    db = Mock()
    db.get.return_value = object()

    with pytest.raises(ApiError) as exc_info:
        accept_catalog_import(
            db,
            uuid4(),
            "oversized-import",
            CatalogImportMode.FULL,
            b"x" * (MAX_CATALOG_FILE_BYTES + 1),
            filename="catalog.csv",
            content_type="text/csv",
        )

    assert exc_info.value.status_code == 413
    assert exc_info.value.code == "catalog_file_too_large"
    db.add.assert_not_called()


def test_concurrent_equivalent_catalog_acceptance_returns_committed_winner() -> None:
    content = b"accepted source"
    winner = CatalogImport(
        checksum=hashlib.sha256(content).hexdigest(),
        mode=CatalogImportMode.FULL,
    )
    db = Mock()
    db.get.return_value = object()
    db.scalar.side_effect = [None, winner]
    db.flush.side_effect = IntegrityError("insert", {}, Exception("duplicate"))

    result = accept_catalog_import(
        db,
        uuid4(),
        "concurrent-import",
        CatalogImportMode.FULL,
        content,
        filename="catalog.csv",
        content_type="text/csv",
    )

    assert result is winner
    db.rollback.assert_called_once_with()
