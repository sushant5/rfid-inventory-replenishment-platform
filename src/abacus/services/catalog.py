import csv
import hashlib
import io
import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import UTC, datetime
from pathlib import PurePath
from typing import Any

from pydantic import ValidationError
from sqlalchemy import String, cast, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.db import TenantSession, pin_session_to_tenant
from abacus.enums import JobKind, JobStatus
from abacus.models.architecture import Product, ProductVariant, RfidTag
from abacus.models.catalog import (
    CatalogImport,
    CatalogImportError,
    CatalogImportMode,
    CatalogImportRow,
    CatalogImportSource,
    CatalogImportStatus,
    CatalogRowAction,
    CatalogRowStatus,
    EpcBinding,
    ProductStyle,
    Sku,
)
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Tenant
from abacus.schemas.catalog import CatalogRowData, normalize_epc
from abacus.services.jobs import enqueue_job

MAX_CATALOG_FILE_BYTES = 10 * 1024 * 1024
MAX_CATALOG_ROWS = 100_000
MAX_ATTRIBUTE_BYTES = 16_384
REQUIRED_HEADERS = frozenset({"style_code", "style_name", "sku", "upc", "color", "size", "epc"})
OPTIONAL_HEADERS = frozenset({"attributes", "style_attributes"})


@dataclass(frozen=True, slots=True)
class CatalogIssue:
    row_number: int | None
    field: str | None
    code: str
    message: str
    rejected_value: str | None = None
    evidence: dict[str, Any] = dataclass_field(default_factory=dict)


@dataclass(slots=True)
class ParsedCatalogRow:
    row_number: int
    raw_data: dict[str, Any]
    normalized: CatalogRowData | None = None
    issues: list[CatalogIssue] = dataclass_field(default_factory=list)


@dataclass(slots=True)
class CatalogParseResult:
    rows: list[ParsedCatalogRow] = dataclass_field(default_factory=list)
    issues: list[CatalogIssue] = dataclass_field(default_factory=list)


class PromotionConflictError(Exception):
    pass


def _safe_value(value: object, *, limit: int = 500) -> str | None:
    if value is None:
        return None
    rendered = str(value)
    return rendered if len(rendered) <= limit else f"{rendered[: limit - 3]}..."


def _parse_attributes(value: str, field_name: str) -> dict[str, Any]:
    attributes_value = value.strip()
    attributes: dict[str, Any] = {}
    if attributes_value:
        if len(attributes_value.encode("utf-8")) > MAX_ATTRIBUTE_BYTES:
            raise ValueError(f"{field_name} must be at most {MAX_ATTRIBUTE_BYTES} UTF-8 bytes")
        decoded = json.loads(attributes_value)
        if not isinstance(decoded, dict):
            raise ValueError(f"{field_name} must contain a JSON object")
        attributes = decoded
    return attributes


def _canonical_attributes(raw_row: dict[str, Any], attribute_headers: list[str]) -> dict[str, Any]:
    attributes = _parse_attributes(raw_row.get("attributes", ""), "attributes")

    for header in attribute_headers:
        value = raw_row.get(header, "").strip()
        if not value:
            continue
        attribute_name = header.removeprefix("attr.")
        if attribute_name in attributes and attributes[attribute_name] != value:
            raise ValueError(
                f"attribute '{attribute_name}' is defined differently in attributes and {header}"
            )
        attributes[attribute_name] = value
    return attributes


def _validation_issues(
    row_number: int,
    raw_data: dict[str, Any],
    validation_error: ValidationError,
) -> list[CatalogIssue]:
    issues: list[CatalogIssue] = []
    for error in validation_error.errors(include_url=False):
        location = error.get("loc", ())
        field_name = str(location[0]) if location else None
        issues.append(
            CatalogIssue(
                row_number=row_number,
                field=field_name,
                code="invalid_field",
                message=str(error.get("msg", "Invalid value")),
                rejected_value=_safe_value(raw_data.get(field_name)) if field_name else None,
            )
        )
    return issues


def parse_catalog_csv(content: bytes) -> CatalogParseResult:
    """Decode and normalize a product-master CSV without touching the database."""

    result = CatalogParseResult()
    if len(content) > MAX_CATALOG_FILE_BYTES:
        result.issues.append(
            CatalogIssue(
                row_number=None,
                field="file",
                code="file_too_large",
                message=f"Catalog CSV must not exceed {MAX_CATALOG_FILE_BYTES} bytes.",
                evidence={"received_bytes": len(content)},
            )
        )
        return result
    if not content:
        result.issues.append(
            CatalogIssue(None, "file", "empty_file", "Catalog CSV cannot be empty.")
        )
        return result

    try:
        text_content = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        result.issues.append(
            CatalogIssue(
                None,
                "file",
                "invalid_encoding",
                "Catalog CSV must use UTF-8 encoding.",
                evidence={"byte_offset": exc.start},
            )
        )
        return result
    if "\x00" in text_content:
        result.issues.append(
            CatalogIssue(None, "file", "invalid_character", "Catalog CSV contains a null byte.")
        )
        return result

    reader = csv.reader(io.StringIO(text_content, newline=""), strict=True)
    try:
        raw_headers = next(reader)
    except StopIteration:
        result.issues.append(
            CatalogIssue(None, "file", "empty_file", "Catalog CSV cannot be empty.")
        )
        return result
    except csv.Error as exc:
        result.issues.append(CatalogIssue(None, "file", "malformed_csv", str(exc)))
        return result

    headers = [header.strip().lower() for header in raw_headers]
    if any(not header for header in headers):
        result.issues.append(
            CatalogIssue(None, "header", "blank_header", "CSV headers cannot be blank.")
        )
    duplicates = sorted({header for header in headers if headers.count(header) > 1})
    if duplicates:
        result.issues.append(
            CatalogIssue(
                None,
                "header",
                "duplicate_header",
                f"CSV headers must be unique: {duplicates}",
                evidence={"duplicates": duplicates},
            )
        )
    missing = sorted(REQUIRED_HEADERS - set(headers))
    if missing:
        result.issues.append(
            CatalogIssue(
                None,
                "header",
                "missing_header",
                f"Required CSV headers are missing: {missing}",
                evidence={"missing": missing},
            )
        )
    unexpected = sorted(
        header
        for header in headers
        if header not in REQUIRED_HEADERS
        and header not in OPTIONAL_HEADERS
        and not header.startswith("attr.")
    )
    if unexpected:
        result.issues.append(
            CatalogIssue(
                None,
                "header",
                "unexpected_header",
                f"Unsupported CSV headers: {unexpected}",
                evidence={"unexpected": unexpected},
            )
        )
    attribute_headers = [header for header in headers if header.startswith("attr.")]
    if any(header == "attr." for header in attribute_headers):
        result.issues.append(
            CatalogIssue(
                None,
                "header",
                "invalid_attribute_header",
                "Attribute columns must use attr.<name>.",
            )
        )
    if result.issues:
        return result

    try:
        for values in reader:
            row_number = reader.line_num
            if not any(value.strip() for value in values):
                continue
            if len(result.rows) >= MAX_CATALOG_ROWS:
                result.issues.append(
                    CatalogIssue(
                        None,
                        "file",
                        "too_many_rows",
                        f"Catalog CSV must not exceed {MAX_CATALOG_ROWS} data rows.",
                    )
                )
                break

            raw_data: dict[str, Any] = {
                header: values[index] if index < len(values) else ""
                for index, header in enumerate(headers)
            }
            if len(values) > len(headers):
                raw_data["_extra_columns"] = values[len(headers) :]
            parsed_row = ParsedCatalogRow(row_number=row_number, raw_data=raw_data)
            if len(values) != len(headers):
                parsed_row.issues.append(
                    CatalogIssue(
                        row_number,
                        "row",
                        "column_count_mismatch",
                        f"Expected {len(headers)} columns but received {len(values)}.",
                        evidence={"expected": len(headers), "received": len(values)},
                    )
                )
                result.rows.append(parsed_row)
                continue

            try:
                attributes = _canonical_attributes(raw_data, attribute_headers)
            except (ValueError, json.JSONDecodeError) as exc:
                parsed_row.issues.append(
                    CatalogIssue(
                        row_number,
                        "attributes",
                        "invalid_attributes",
                        str(exc),
                        _safe_value(raw_data.get("attributes")),
                    )
                )
                result.rows.append(parsed_row)
                continue
            try:
                style_attributes = _parse_attributes(
                    raw_data.get("style_attributes", ""),
                    "style_attributes",
                )
            except (ValueError, json.JSONDecodeError) as exc:
                parsed_row.issues.append(
                    CatalogIssue(
                        row_number,
                        "style_attributes",
                        "invalid_style_attributes",
                        str(exc),
                        _safe_value(raw_data.get("style_attributes")),
                    )
                )
                result.rows.append(parsed_row)
                continue

            candidate = {
                "style_code": raw_data["style_code"],
                "style_name": raw_data["style_name"],
                "sku": raw_data["sku"],
                "upc": raw_data["upc"],
                "color": raw_data["color"],
                "size": raw_data["size"],
                "epc": raw_data["epc"],
                "style_attributes": style_attributes,
                "attributes": attributes,
            }
            try:
                parsed_row.normalized = CatalogRowData.model_validate(candidate)
            except ValidationError as exc:
                parsed_row.issues.extend(_validation_issues(row_number, raw_data, exc))
            result.rows.append(parsed_row)
    except csv.Error as exc:
        result.issues.append(CatalogIssue(reader.line_num, "row", "malformed_csv", str(exc)))

    if not result.rows and not result.issues:
        result.issues.append(
            CatalogIssue(None, "file", "no_data_rows", "Catalog CSV has no data rows.")
        )
    _validate_batch_consistency(result)
    return result


def _add_conflict(
    row: ParsedCatalogRow,
    *,
    field_name: str,
    code: str,
    message: str,
    first_row: int,
) -> None:
    row.issues.append(
        CatalogIssue(
            row.row_number,
            field_name,
            code,
            message,
            _safe_value(row.raw_data.get(field_name)),
            {"first_seen_at_row": first_row},
        )
    )


def _validate_batch_consistency(result: CatalogParseResult) -> None:
    styles: dict[str, tuple[int, tuple[object, ...]]] = {}
    skus: dict[str, tuple[int, tuple[object, ...]]] = {}
    upcs: dict[str, tuple[int, str]] = {}
    epcs: dict[str, tuple[int, str]] = {}

    for row in result.rows:
        data = row.normalized
        if data is None:
            continue
        attributes_json = json.dumps(data.attributes, sort_keys=True, separators=(",", ":"))
        style_attributes_json = json.dumps(
            data.style_attributes,
            sort_keys=True,
            separators=(",", ":"),
        )
        style_signature = (data.style_name, style_attributes_json)
        prior_style = styles.get(data.style_code)
        if prior_style is not None and prior_style[1] != style_signature:
            _add_conflict(
                row,
                field_name="style_code",
                code="style_definition_conflict",
                message=f"Style {data.style_code} has conflicting definitions in this import.",
                first_row=prior_style[0],
            )
        else:
            styles.setdefault(data.style_code, (row.row_number, style_signature))

        sku_signature = (
            data.style_code,
            data.upc,
            data.color,
            data.size,
            attributes_json,
        )
        prior_sku = skus.get(data.sku)
        if prior_sku is not None and prior_sku[1] != sku_signature:
            _add_conflict(
                row,
                field_name="sku",
                code="sku_definition_conflict",
                message=f"SKU {data.sku} has conflicting definitions in this import.",
                first_row=prior_sku[0],
            )
        else:
            skus.setdefault(data.sku, (row.row_number, sku_signature))

        prior_upc = upcs.get(data.upc)
        if prior_upc is not None and prior_upc[1] != data.sku:
            _add_conflict(
                row,
                field_name="upc",
                code="upc_conflict",
                message=f"UPC {data.upc} maps to more than one SKU in this import.",
                first_row=prior_upc[0],
            )
        else:
            upcs.setdefault(data.upc, (row.row_number, data.sku))

        prior_epc = epcs.get(data.epc)
        if prior_epc is not None:
            _add_conflict(
                row,
                field_name="epc",
                code="duplicate_epc" if prior_epc[1] == data.sku else "epc_conflict",
                message=(
                    f"EPC {data.epc} is duplicated in this import."
                    if prior_epc[1] == data.sku
                    else f"EPC {data.epc} maps to more than one SKU in this import."
                ),
                first_row=prior_epc[0],
            )
        else:
            epcs[data.epc] = (row.row_number, data.sku)


def _get_tenant(db: Session, tenant_id: uuid.UUID) -> Tenant:
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise ApiError(404, "Tenant not found", "The requested tenant does not exist.")
    return tenant


def _load_catalog_state(
    db: Session,
    tenant_id: uuid.UUID,
) -> tuple[dict[str, ProductStyle], dict[str, Sku], dict[str, EpcBinding]]:
    styles = {
        style.code: style
        for style in db.scalars(
            select(ProductStyle).where(ProductStyle.tenant_id == tenant_id)
        ).all()
    }
    skus = {
        sku.code: sku for sku in db.scalars(select(Sku).where(Sku.tenant_id == tenant_id)).all()
    }
    bindings = {
        binding.epc: binding
        for binding in db.scalars(
            select(EpcBinding).where(
                EpcBinding.tenant_id == tenant_id,
                EpcBinding.effective_to.is_(None),
            )
        ).all()
    }
    return styles, skus, bindings


def _normalized_rows(rows: Iterable[ParsedCatalogRow]) -> list[CatalogRowData]:
    return [row.normalized for row in rows if row.normalized is not None and not row.issues]


def _append_database_conflicts(
    rows: list[ParsedCatalogRow],
    styles: dict[str, ProductStyle],
    skus: dict[str, Sku],
) -> None:
    styles_by_id = {style.id: style for style in styles.values()}
    sku_by_upc = {sku.upc: sku for sku in skus.values()}
    for row in rows:
        data = row.normalized
        if data is None or row.issues:
            continue
        existing_sku = skus.get(data.sku)
        owner = sku_by_upc.get(data.upc)
        if owner is not None and owner.code != data.sku:
            row.issues.append(
                CatalogIssue(
                    row.row_number,
                    "upc",
                    "existing_upc_conflict",
                    f"UPC {data.upc} already belongs to SKU {owner.code}.",
                    data.upc,
                    {"existing_sku": owner.code},
                )
            )
        if existing_sku is not None:
            existing_style = styles_by_id.get(existing_sku.product_style_id)
            if existing_style is not None and existing_style.code != data.style_code:
                row.issues.append(
                    CatalogIssue(
                        row.row_number,
                        "sku",
                        "existing_sku_style_conflict",
                        f"SKU {data.sku} cannot be moved to another style.",
                        data.sku,
                        {"existing_style": existing_style.code},
                    )
                )


def _entity_data(
    data_rows: Iterable[CatalogRowData],
) -> tuple[dict[str, CatalogRowData], dict[str, CatalogRowData], dict[str, CatalogRowData]]:
    styles: dict[str, CatalogRowData] = {}
    skus: dict[str, CatalogRowData] = {}
    epcs: dict[str, CatalogRowData] = {}
    for data in data_rows:
        styles.setdefault(data.style_code, data)
        skus.setdefault(data.sku, data)
        epcs[data.epc] = data
    return styles, skus, epcs


def _style_changed(style: ProductStyle, data: CatalogRowData) -> bool:
    return (
        style.name != data.style_name
        or style.attributes != data.style_attributes
        or not style.active
    )


def _sku_changed(sku: Sku, style: ProductStyle, data: CatalogRowData) -> bool:
    return (
        sku.product_style_id != style.id
        or sku.upc != data.upc
        or sku.color != data.color
        or sku.size != data.size
        or sku.attributes != data.attributes
        or not sku.active
    )


def _build_preview(
    mode: CatalogImportMode,
    data_rows: list[CatalogRowData],
    existing_styles: dict[str, ProductStyle],
    existing_skus: dict[str, Sku],
    existing_bindings: dict[str, EpcBinding],
) -> dict[str, Any]:
    style_data, sku_data, epc_data = _entity_data(data_rows)
    skus_by_id = {sku.id: sku for sku in existing_skus.values()}

    style_counts = {"inserted": 0, "updated": 0, "unchanged": 0, "deactivated": 0}
    for code, data in style_data.items():
        existing_style = existing_styles.get(code)
        if existing_style is None:
            style_counts["inserted"] += 1
        elif _style_changed(existing_style, data):
            style_counts["updated"] += 1
        else:
            style_counts["unchanged"] += 1
    if mode is CatalogImportMode.FULL:
        style_counts["deactivated"] = sum(
            style.active and code not in style_data for code, style in existing_styles.items()
        )

    sku_counts = {"inserted": 0, "updated": 0, "unchanged": 0, "deactivated": 0}
    for code, data in sku_data.items():
        existing_sku = existing_skus.get(code)
        target_style = existing_styles.get(data.style_code)
        if existing_sku is None:
            sku_counts["inserted"] += 1
        elif target_style is None or _sku_changed(existing_sku, target_style, data):
            sku_counts["updated"] += 1
        else:
            sku_counts["unchanged"] += 1
    if mode is CatalogImportMode.FULL:
        sku_counts["deactivated"] = sum(
            sku.active and code not in sku_data for code, sku in existing_skus.items()
        )

    epc_counts = {"inserted": 0, "updated": 0, "unchanged": 0, "deactivated": 0}
    for epc, data in epc_data.items():
        binding = existing_bindings.get(epc)
        bound_sku = skus_by_id.get(binding.sku_id) if binding is not None else None
        if binding is None:
            epc_counts["inserted"] += 1
        elif bound_sku is None or bound_sku.code != data.sku:
            epc_counts["updated"] += 1
        else:
            epc_counts["unchanged"] += 1
    if mode is CatalogImportMode.FULL:
        epc_counts["deactivated"] = sum(epc not in epc_data for epc in existing_bindings)

    return {
        "mode": mode.value,
        "styles": style_counts,
        "skus": sku_counts,
        "epc_bindings": epc_counts,
        "validation": {"rows": len(data_rows), "errors": 0},
    }


def _row_actions(
    data_rows: list[CatalogRowData],
    existing_styles: dict[str, ProductStyle],
    existing_skus: dict[str, Sku],
    existing_bindings: dict[str, EpcBinding],
) -> dict[str, CatalogRowAction]:
    styles_by_id = {style.id: style for style in existing_styles.values()}
    skus_by_id = {sku.id: sku for sku in existing_skus.values()}
    actions: dict[str, CatalogRowAction] = {}
    for data in data_rows:
        style = existing_styles.get(data.style_code)
        sku = existing_skus.get(data.sku)
        binding = existing_bindings.get(data.epc)
        if style is None or sku is None or binding is None:
            actions[data.epc] = CatalogRowAction.INSERT
            continue
        bound_sku = skus_by_id.get(binding.sku_id)
        sku_style = styles_by_id.get(sku.product_style_id)
        if (
            _style_changed(style, data)
            or sku_style is None
            or _sku_changed(sku, style, data)
            or bound_sku is None
            or bound_sku.code != data.sku
        ):
            actions[data.epc] = CatalogRowAction.UPDATE
        else:
            actions[data.epc] = CatalogRowAction.UNCHANGED
    return actions


def _persist_issue(
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
    issue: CatalogIssue,
) -> CatalogImportError:
    return CatalogImportError(
        tenant_id=tenant_id,
        import_id=import_id,
        row_number=issue.row_number,
        field=issue.field,
        code=issue.code,
        message=issue.message[:500],
        rejected_value=issue.rejected_value,
        evidence=issue.evidence,
    )


def accept_catalog_import(
    db: Session,
    tenant_id: uuid.UUID,
    idempotency_key: str,
    mode: CatalogImportMode,
    content: bytes,
    *,
    filename: str,
    content_type: str | None,
) -> CatalogImport:
    """Durably accept immutable source bytes for asynchronous validation."""

    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    _get_tenant(db, tenant_id)
    normalized_key = idempotency_key.strip()
    if len(normalized_key) < 8 or len(normalized_key) > 128:
        raise ApiError(
            400,
            "Invalid idempotency key",
            "Idempotency-Key must contain 8 to 128 non-blank characters.",
            code="invalid_idempotency_key",
        )
    if len(content) > MAX_CATALOG_FILE_BYTES:
        raise ApiError(
            413,
            "Catalog file is too large",
            f"Catalog CSV must not exceed {MAX_CATALOG_FILE_BYTES} bytes.",
            code="catalog_file_too_large",
        )
    checksum = hashlib.sha256(content).hexdigest()
    existing = db.scalar(
        select(CatalogImport).where(
            CatalogImport.tenant_id == tenant_id,
            CatalogImport.idempotency_key == normalized_key,
        )
    )
    if existing is not None:
        if existing.checksum == checksum and existing.mode is mode:
            return existing
        raise ApiError(
            409,
            "Idempotency conflict",
            "This Idempotency-Key was already used for different catalog content or mode.",
            code="idempotency_key_reused",
        )

    safe_filename = PurePath(filename.replace("\\", "/")).name[:255] or "catalog.csv"
    catalog_import = CatalogImport(
        tenant_id=tenant_id,
        idempotency_key=normalized_key,
        checksum=checksum,
        mode=mode,
        status=CatalogImportStatus.VALIDATING,
        filename=safe_filename,
        content_type=(content_type or "text/csv")[:128],
        size_bytes=len(content),
        reconciliation={},
    )
    db.add(catalog_import)
    try:
        db.flush()
        db.add(
            CatalogImportSource(
                tenant_id=tenant_id,
                import_id=catalog_import.id,
                content=content,
            )
        )
        enqueue_job(
            db,
            tenant_id=tenant_id,
            kind=JobKind.CATALOG_IMPORT,
            payload={"import_id": str(catalog_import.id)},
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        # SET LOCAL is cleared by rollback. Re-establish the tenant context before
        # reading the concurrent winner through forced RLS.
        if isinstance(db, TenantSession):
            pin_session_to_tenant(db, tenant_id)
        winner = db.scalar(
            select(CatalogImport).where(
                CatalogImport.tenant_id == tenant_id,
                CatalogImport.idempotency_key == normalized_key,
            )
        )
        if winner is not None:
            if winner.checksum == checksum and winner.mode is mode:
                return winner
            raise ApiError(
                409,
                "Idempotency conflict",
                "This Idempotency-Key was concurrently used for another catalog import.",
                code="idempotency_key_reused",
            ) from exc
        raise
    db.refresh(catalog_import)
    return catalog_import


def validate_and_stage_catalog_import(db: Session, import_id: uuid.UUID) -> CatalogImport:
    """Atomically validate and stage one accepted source in the catalog worker."""

    catalog_import = db.scalar(
        select(CatalogImport).where(CatalogImport.id == import_id).with_for_update()
    )
    if catalog_import is None:
        raise ApiError(404, "Catalog import not found", "The requested import does not exist.")
    if catalog_import.status in {
        CatalogImportStatus.READY,
        CatalogImportStatus.COMPLETED,
        CatalogImportStatus.REJECTED,
        CatalogImportStatus.FAILED,
    }:
        return catalog_import
    if catalog_import.status is not CatalogImportStatus.VALIDATING:
        raise ApiError(
            409,
            "Catalog import is not validatable",
            f"Import status is {catalog_import.status.value}; VALIDATING is required.",
            code="catalog_import_not_validatable",
        )

    source = db.scalar(
        select(CatalogImportSource).where(
            CatalogImportSource.tenant_id == catalog_import.tenant_id,
            CatalogImportSource.import_id == catalog_import.id,
        )
    )
    if source is None:
        raise RuntimeError("catalog import source is missing")
    if hashlib.sha256(source.content).hexdigest() != catalog_import.checksum:
        raise RuntimeError("catalog import source checksum mismatch")

    parsed = parse_catalog_csv(source.content)
    existing_styles, existing_skus, existing_bindings = _load_catalog_state(
        db, catalog_import.tenant_id
    )
    _append_database_conflicts(parsed.rows, existing_styles, existing_skus)
    data_rows = _normalized_rows(parsed.rows)
    all_issues = [*parsed.issues, *(issue for row in parsed.rows for issue in row.issues)]

    actions = _row_actions(data_rows, existing_styles, existing_skus, existing_bindings)
    for row in parsed.rows:
        normalized_data = (
            row.normalized.model_dump(mode="json") if row.normalized is not None else None
        )
        db.add(
            CatalogImportRow(
                tenant_id=catalog_import.tenant_id,
                import_id=catalog_import.id,
                row_number=row.row_number,
                raw_data=row.raw_data,
                normalized_data=normalized_data,
                status=CatalogRowStatus.INVALID if row.issues else CatalogRowStatus.VALID,
                action=(
                    actions.get(row.normalized.epc)
                    if row.normalized is not None and not row.issues
                    else None
                ),
            )
        )
    db.add_all(
        _persist_issue(catalog_import.tenant_id, catalog_import.id, issue) for issue in all_issues
    )

    invalid_row_numbers = {issue.row_number for issue in all_issues if issue.row_number is not None}
    catalog_import.total_rows = len(parsed.rows)
    catalog_import.invalid_rows = len(invalid_row_numbers)
    catalog_import.valid_rows = len(parsed.rows) - len(invalid_row_numbers)
    catalog_import.reconciliation = {
        "validation": {
            "rows_received": len(parsed.rows),
            "valid_rows": catalog_import.valid_rows,
            "invalid_rows": catalog_import.invalid_rows,
            "error_count": len(all_issues),
        },
        "preview": (
            _build_preview(
                catalog_import.mode,
                data_rows,
                existing_styles,
                existing_skus,
                existing_bindings,
            )
            if not all_issues
            else {}
        ),
    }
    catalog_import.status = (
        CatalogImportStatus.REJECTED if all_issues else CatalogImportStatus.READY
    )
    db.commit()
    db.refresh(catalog_import)
    return catalog_import


def get_catalog_import(
    db: Session,
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
) -> CatalogImport:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    catalog_import = db.scalar(
        select(CatalogImport).where(
            CatalogImport.id == import_id,
            CatalogImport.tenant_id == tenant_id,
        )
    )
    if catalog_import is None:
        raise ApiError(404, "Catalog import not found", "The requested import does not exist.")
    return catalog_import


def list_catalog_import_errors(
    db: Session,
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
    *,
    limit: int,
    offset: int,
) -> tuple[list[CatalogImportError], int]:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    get_catalog_import(db, tenant_id, import_id)
    predicate = (
        CatalogImportError.tenant_id == tenant_id,
        CatalogImportError.import_id == import_id,
    )
    total = db.scalar(select(func.count()).select_from(CatalogImportError).where(*predicate))
    errors = list(
        db.scalars(
            select(CatalogImportError)
            .where(*predicate)
            .order_by(
                CatalogImportError.row_number.asc().nullsfirst(),
                CatalogImportError.field.asc().nullsfirst(),
                CatalogImportError.code.asc(),
                CatalogImportError.id.asc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return errors, int(total or 0)


def list_skus(
    db: Session,
    tenant_id: uuid.UUID,
    *,
    active: bool | None,
    code: str | None,
    limit: int,
    offset: int,
) -> tuple[list[tuple[Sku, ProductStyle]], int]:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    _get_tenant(db, tenant_id)
    predicates: list[Any] = [
        Sku.tenant_id == tenant_id,
        ProductStyle.tenant_id == tenant_id,
    ]
    if active is not None:
        predicates.append(Sku.active.is_(active))
    if code is not None:
        predicates.append(Sku.code == code.strip().upper())
    total = db.scalar(
        select(func.count())
        .select_from(Sku)
        .join(ProductStyle, ProductStyle.id == Sku.product_style_id)
        .where(*predicates)
    )
    rows = list(
        db.execute(
            select(Sku, ProductStyle)
            .join(ProductStyle, ProductStyle.id == Sku.product_style_id)
            .where(*predicates)
            .order_by(Sku.code.asc(), Sku.id.asc())
            .limit(limit)
            .offset(offset)
        )
        .tuples()
        .all()
    )
    return rows, int(total or 0)


def get_sku(
    db: Session,
    tenant_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> tuple[Sku, ProductStyle]:
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    row = db.execute(
        select(Sku, ProductStyle)
        .join(ProductStyle, ProductStyle.id == Sku.product_style_id)
        .where(
            Sku.id == sku_id,
            Sku.tenant_id == tenant_id,
            ProductStyle.tenant_id == tenant_id,
        )
    ).first()
    if row is None:
        raise ApiError(404, "SKU not found", "The requested SKU does not exist.")
    return row[0], row[1]


def _counter() -> dict[str, int]:
    return {"inserted": 0, "updated": 0, "unchanged": 0, "deactivated": 0}


def _apply_promotion(
    db: Session,
    catalog_import: CatalogImport,
    rows: list[CatalogRowData],
    effective_at: datetime,
) -> dict[str, Any]:
    tenant_id = catalog_import.tenant_id
    style_data, sku_data, epc_data = _entity_data(rows)
    existing_styles, existing_skus, existing_bindings = _load_catalog_state(db, tenant_id)
    sku_by_upc = {sku.upc: sku for sku in existing_skus.values()}

    for code, data in sku_data.items():
        owner = sku_by_upc.get(data.upc)
        if owner is not None and owner.code != code:
            raise PromotionConflictError(
                f"UPC {data.upc} now belongs to SKU {owner.code}; revalidate the import."
            )
        existing = existing_skus.get(code)
        if existing is not None:
            existing_style = next(
                (
                    style
                    for style in existing_styles.values()
                    if style.id == existing.product_style_id
                ),
                None,
            )
            if existing_style is not None and existing_style.code != data.style_code:
                raise PromotionConflictError(
                    f"SKU {code} now belongs to style {existing_style.code}; revalidate the import."
                )

    style_counts = _counter()
    for code, data in style_data.items():
        style = existing_styles.get(code)
        if style is None:
            style = ProductStyle(
                tenant_id=tenant_id,
                code=code,
                name=data.style_name,
                attributes=data.style_attributes,
                active=True,
            )
            db.add(style)
            db.flush()
            existing_styles[code] = style
            style_counts["inserted"] += 1
        elif _style_changed(style, data):
            style.name = data.style_name
            style.attributes = data.style_attributes
            style.active = True
            style_counts["updated"] += 1
        else:
            style_counts["unchanged"] += 1

    sku_counts = _counter()
    for code, data in sku_data.items():
        style = existing_styles[data.style_code]
        sku = existing_skus.get(code)
        if sku is None:
            sku = Sku(
                tenant_id=tenant_id,
                product_style_id=style.id,
                code=code,
                upc=data.upc,
                color=data.color,
                size=data.size,
                attributes=data.attributes,
                active=True,
            )
            db.add(sku)
            db.flush()
            existing_skus[code] = sku
            sku_counts["inserted"] += 1
        elif _sku_changed(sku, style, data):
            sku.product_style_id = style.id
            sku.upc = data.upc
            sku.color = data.color
            sku.size = data.size
            sku.attributes = data.attributes
            sku.active = True
            sku_counts["updated"] += 1
        else:
            sku_counts["unchanged"] += 1

    epc_counts = _counter()
    for epc, data in epc_data.items():
        sku = existing_skus[data.sku]
        binding = existing_bindings.get(epc)
        if binding is None:
            db.add(
                EpcBinding(
                    tenant_id=tenant_id,
                    sku_id=sku.id,
                    epc=epc,
                    effective_from=effective_at,
                    source_import_id=catalog_import.id,
                )
            )
            epc_counts["inserted"] += 1
        elif binding.sku_id != sku.id:
            binding.effective_to = effective_at
            db.flush()
            db.add(
                EpcBinding(
                    tenant_id=tenant_id,
                    sku_id=sku.id,
                    epc=epc,
                    effective_from=effective_at,
                    source_import_id=catalog_import.id,
                )
            )
            epc_counts["updated"] += 1
        else:
            epc_counts["unchanged"] += 1

    if catalog_import.mode is CatalogImportMode.FULL:
        for code, sku in existing_skus.items():
            if code not in sku_data and sku.active:
                sku.active = False
                sku_counts["deactivated"] += 1
        for code, style in existing_styles.items():
            if code not in style_data and style.active:
                style.active = False
                style_counts["deactivated"] += 1
        for epc, binding in existing_bindings.items():
            if epc not in epc_data and binding.effective_to is None:
                binding.effective_to = effective_at
                epc_counts["deactivated"] += 1

    return {
        "mode": catalog_import.mode.value,
        "effective_at": effective_at.isoformat(),
        "styles": style_counts,
        "skus": sku_counts,
        "epc_bindings": epc_counts,
    }


def _sync_architecture_catalog_projection(
    db: Session,
    catalog_import: CatalogImport,
    rows: list[CatalogRowData],
) -> None:
    """Materialize the assignment-facing product hierarchy in the import transaction.

    ProductStyle and SKU power catalog lookup, while effective-dated EpcBinding rows
    remain the event-time RFID source of truth. Product, ProductVariant, and RfidTag
    expose the requested product hierarchy and its current tag projection. Updating
    both representations in this transaction prevents a partially promoted catalog.
    """

    tenant_id = catalog_import.tenant_id
    products = {
        product.style_code: product
        for product in db.scalars(select(Product).where(Product.tenant_id == tenant_id)).all()
    }
    supplied_products: set[str] = set()
    supplied_variants: set[tuple[str, str]] = set()
    supplied_epcs: set[str] = set()

    first_row_by_style = {row.style_code: row for row in rows}
    for style_code, row in first_row_by_style.items():
        supplied_products.add(style_code)
        category = str(row.style_attributes.get("category") or "UNCATEGORIZED").strip()
        product = products.get(style_code)
        if product is None:
            product = Product(
                tenant_id=tenant_id,
                style_code=style_code,
                name=row.style_name,
                category=category,
                attributes=row.style_attributes,
                active=True,
            )
            db.add(product)
            products[style_code] = product
        else:
            product.name = row.style_name
            product.category = category
            product.attributes = row.style_attributes
            product.active = True
    db.flush()

    variants = {
        (variant.product_id, variant.color): variant
        for variant in db.scalars(
            select(ProductVariant).where(ProductVariant.tenant_id == tenant_id)
        ).all()
    }
    skus = {
        sku.code: sku for sku in db.scalars(select(Sku).where(Sku.tenant_id == tenant_id)).all()
    }
    tags = {
        tag.epc: tag
        for tag in db.scalars(select(RfidTag).where(RfidTag.tenant_id == tenant_id)).all()
    }

    for row in rows:
        supplied_products.add(row.style_code)
        product = products[row.style_code]

        variant_key = (row.style_code, row.color)
        supplied_variants.add(variant_key)
        variant = variants.get((product.id, row.color))
        if variant is None:
            variant = ProductVariant(
                tenant_id=tenant_id,
                product_id=product.id,
                color=row.color,
                attributes={},
                active=True,
            )
            db.add(variant)
            db.flush()
            variants[(product.id, row.color)] = variant
        else:
            variant.active = True

        sku = skus.get(row.sku)
        if sku is None:  # pragma: no cover - primary promotion created it in this transaction
            raise PromotionConflictError(f"SKU {row.sku} was not promoted")
        sku.product_variant_id = variant.id

        supplied_epcs.add(row.epc)
        tag = tags.get(row.epc)
        if tag is None:
            db.add(
                RfidTag(
                    tenant_id=tenant_id,
                    epc=row.epc,
                    sku_id=sku.id,
                    source_import_id=catalog_import.id,
                    active=True,
                )
            )
        else:
            tag.sku_id = sku.id
            tag.source_import_id = catalog_import.id
            tag.active = True

    if catalog_import.mode is CatalogImportMode.FULL:
        for product in products.values():
            if product.style_code not in supplied_products:
                product.active = False
        products_by_id = {product.id: product for product in products.values()}
        for variant in variants.values():
            product = products_by_id[variant.product_id]
            if (product.style_code, variant.color) not in supplied_variants:
                variant.active = False
        for tag in tags.values():
            if tag.epc not in supplied_epcs:
                tag.active = False


def promote_catalog_import(db: Session, import_id: uuid.UUID) -> CatalogImport:
    """Atomically promote a READY import; intended to be called by a durable worker."""

    catalog_import = db.scalar(
        select(CatalogImport).where(CatalogImport.id == import_id).with_for_update()
    )
    if catalog_import is None:
        raise ApiError(404, "Catalog import not found", "The requested import does not exist.")
    if catalog_import.status is CatalogImportStatus.COMPLETED:
        return catalog_import
    if catalog_import.status is not CatalogImportStatus.READY:
        raise ApiError(
            409,
            "Catalog import is not promotable",
            f"Import status is {catalog_import.status.value}; READY is required.",
            code="catalog_import_not_ready",
        )

    tenant = db.scalar(
        select(Tenant).where(Tenant.id == catalog_import.tenant_id).with_for_update()
    )
    if tenant is None:
        raise ApiError(404, "Tenant not found", "The import tenant no longer exists.")

    staged_rows = list(
        db.scalars(
            select(CatalogImportRow)
            .where(
                CatalogImportRow.tenant_id == catalog_import.tenant_id,
                CatalogImportRow.import_id == import_id,
                CatalogImportRow.status == CatalogRowStatus.VALID,
            )
            .order_by(CatalogImportRow.row_number.asc())
        ).all()
    )
    if len(staged_rows) != catalog_import.valid_rows or not staged_rows:
        raise ApiError(
            409,
            "Catalog staging is incomplete",
            "The staged row count does not match the validated import.",
            code="catalog_staging_incomplete",
        )
    rows = [CatalogRowData.model_validate(row.normalized_data) for row in staged_rows]
    catalog_import.status = CatalogImportStatus.PROCESSING
    db.flush()
    effective_at = datetime.now(UTC)

    try:
        with db.begin_nested():
            outcome = _apply_promotion(db, catalog_import, rows, effective_at)
            _sync_architecture_catalog_projection(db, catalog_import, rows)
            db.flush()
    except (IntegrityError, PromotionConflictError) as exc:
        catalog_import.status = CatalogImportStatus.FAILED
        catalog_import.failure_reason = str(exc)[:2000]
        catalog_import.reconciliation = {
            **catalog_import.reconciliation,
            "promotion_error": str(exc)[:500],
        }
        db.commit()
        db.refresh(catalog_import)
        return catalog_import

    counters = [outcome[entity] for entity in ("styles", "skus", "epc_bindings")]
    catalog_import.inserted_count = sum(counter["inserted"] for counter in counters)
    catalog_import.updated_count = sum(counter["updated"] for counter in counters)
    catalog_import.unchanged_count = sum(counter["unchanged"] for counter in counters)
    catalog_import.deactivated_count = sum(counter["deactivated"] for counter in counters)
    catalog_import.reconciliation = {
        **catalog_import.reconciliation,
        "actual": outcome,
    }
    catalog_import.failure_reason = None
    catalog_import.promoted_at = effective_at
    catalog_import.status = CatalogImportStatus.COMPLETED
    db.commit()
    db.refresh(catalog_import)
    return catalog_import


def resolve_active_epc(
    db: Session,
    tenant_id: uuid.UUID,
    epc: str,
    observed_at: datetime,
) -> EpcBinding | None:
    """Resolve the mapping effective when an RFID observation occurred."""

    if observed_at.tzinfo is None or observed_at.utcoffset() is None:
        raise ValueError("observed_at must be timezone-aware")
    if isinstance(db, TenantSession):
        pin_session_to_tenant(db, tenant_id)
    try:
        normalized_epc = normalize_epc(epc)
    except ValueError:
        return None
    return db.scalar(
        select(EpcBinding)
        .where(
            EpcBinding.tenant_id == tenant_id,
            EpcBinding.epc == normalized_epc,
            EpcBinding.effective_from <= observed_at,
            or_(EpcBinding.effective_to.is_(None), EpcBinding.effective_to > observed_at),
        )
        .order_by(EpcBinding.effective_from.desc())
        .limit(1)
    )


def process_catalog_import_job(db: Session, payload: dict[str, object]) -> None:
    """Validate, stage, and promote an accepted import with retry-safe checkpoints."""

    raw_import_id = payload.get("import_id")
    if raw_import_id is None:
        raise ValueError("catalog import job is missing import_id")
    import_id = uuid.UUID(str(raw_import_id))
    catalog_import = validate_and_stage_catalog_import(db, import_id)
    if catalog_import.status is CatalogImportStatus.READY:
        promote_catalog_import(db, import_id)


def mark_catalog_import_failed_after_retry_exhaustion(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    payload: dict[str, object],
    error: Exception,
) -> bool:
    """Stage a terminal import failure for the durable job transaction to commit.

    The caller must invoke ``mark_failed`` in the same session. Its compare-and-set
    commit makes this update atomic with quarantining the job, while a lost lease
    rolls both changes back.
    """

    raw_import_id = payload.get("import_id")
    if raw_import_id is None:
        return False
    try:
        import_id = uuid.UUID(str(raw_import_id))
    except (AttributeError, TypeError, ValueError):
        return False

    catalog_import = db.scalar(
        select(CatalogImport)
        .where(
            CatalogImport.id == import_id,
            CatalogImport.tenant_id == tenant_id,
        )
        .with_for_update()
    )
    if catalog_import is None or catalog_import.status in {
        CatalogImportStatus.COMPLETED,
        CatalogImportStatus.REJECTED,
        CatalogImportStatus.FAILED,
    }:
        return False

    catalog_import.status = CatalogImportStatus.FAILED
    catalog_import.failure_reason = (
        f"Catalog worker retry budget exhausted: {type(error).__name__}: {error}"
    )[:2000]
    return True


def reconcile_quarantined_catalog_imports(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    limit: int = 100,
) -> int:
    """Reconcile terminal imports whose durable jobs were quarantined by a lease race."""

    if limit < 1:
        raise ValueError("limit must be positive")

    stranded = db.execute(
        select(DurableJob, CatalogImport)
        .join(
            CatalogImport,
            (CatalogImport.tenant_id == DurableJob.tenant_id)
            & (DurableJob.payload["import_id"].as_string() == cast(CatalogImport.id, String)),
        )
        .where(
            DurableJob.tenant_id == tenant_id,
            DurableJob.kind == JobKind.CATALOG_IMPORT,
            DurableJob.status == JobStatus.QUARANTINED,
            CatalogImport.status != CatalogImportStatus.FAILED,
        )
        .order_by(DurableJob.created_at.asc())
        .with_for_update(of=(DurableJob, CatalogImport), skip_locked=True)
        .limit(limit)
    ).all()

    for job, catalog_import in stranded:
        if catalog_import.status in {
            CatalogImportStatus.COMPLETED,
            CatalogImportStatus.REJECTED,
        }:
            job.status = JobStatus.COMPLETED
            job.locked_by = None
            job.lease_expires_at = None
            job.last_error = None
            continue
        detail = job.last_error or "Retry budget exhausted without a recorded worker error"
        catalog_import.status = CatalogImportStatus.FAILED
        catalog_import.failure_reason = f"Catalog worker job quarantined: {detail}"[:2000]

    db.commit()
    return len(stranded)
