import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, Query, UploadFile, status
from sqlalchemy import select, text

from abacus.api.dependencies import DatabaseSession, PlatformAccess
from abacus.api.errors import ApiError
from abacus.db import TenantSession, pin_session_to_tenant
from abacus.models.catalog import CatalogImport, CatalogImportMode, ProductStyle, Sku
from abacus.schemas.catalog import (
    CatalogImportErrorListRead,
    CatalogImportErrorRead,
    CatalogImportListRead,
    CatalogImportRead,
    SkuListRead,
    SkuRead,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.catalog import (
    MAX_CATALOG_FILE_BYTES,
    get_catalog_import,
    get_sku,
    list_catalog_import_errors,
    list_catalog_imports,
    list_skus,
    stage_catalog_import,
)

router = APIRouter(prefix="/v1/tenants/{tenant_id}/catalog", tags=["2. Product catalog"])
canonical_router = APIRouter(tags=["2. Product catalog"])
CanReadCatalog = Annotated[Principal, Depends(require_permission(Permission.CATALOG_READ))]


def _sku_read(sku: Sku, style: ProductStyle) -> SkuRead:
    return SkuRead(
        id=sku.id,
        tenant_id=sku.tenant_id,
        product_style_id=sku.product_style_id,
        style_code=style.code,
        style_name=style.name,
        code=sku.code,
        upc=sku.upc,
        color=sku.color,
        size=sku.size,
        attributes=sku.attributes,
        active=sku.active,
    )


def _catalog_import_tenant_id(db: DatabaseSession, import_id: uuid.UUID) -> uuid.UUID:
    """Resolve the owner under the trusted platform boundary for canonical lookups."""

    if isinstance(db, TenantSession):
        tenant_id = db.scalar(
            text("SELECT abacus_resolve_catalog_import_tenant(:import_id)"),
            {"import_id": import_id},
        )
        db.rollback()
        if tenant_id is not None:
            tenant_id = uuid.UUID(str(tenant_id))
            pin_session_to_tenant(db, tenant_id)
    else:
        tenant_id = db.scalar(select(CatalogImport.tenant_id).where(CatalogImport.id == import_id))
    if tenant_id is None:
        raise ApiError(404, "Catalog import not found", "The requested import does not exist.")
    return uuid.UUID(str(tenant_id))


@canonical_router.post(
    "/v1/tenants/{tenant_id}/catalog-imports",
    response_model=CatalogImportRead,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="createCatalogImportCanonical",
    summary="Stage and validate a product-master CSV",
)
@router.post(
    "/imports",
    response_model=CatalogImportRead,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="createCatalogImport",
    summary="Stage and validate a product-master CSV",
)
def create_catalog_import_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
    file: Annotated[UploadFile, File(description="UTF-8 product-master CSV")],
    mode: Annotated[
        CatalogImportMode,
        Form(description="DELTA upserts supplied rows; FULL also deactivates records not supplied"),
    ],
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=8, max_length=128),
    ],
) -> CatalogImportRead:
    """Required columns: style_code, style_name, sku, upc, color, size, and epc."""

    content = file.file.read(MAX_CATALOG_FILE_BYTES + 1)
    catalog_import = stage_catalog_import(
        db,
        tenant_id,
        idempotency_key,
        mode,
        content,
        filename=file.filename or "catalog.csv",
        content_type=file.content_type,
    )
    return CatalogImportRead.model_validate(catalog_import)


@router.get(
    "/imports",
    response_model=CatalogImportListRead,
    operation_id="listCatalogImports",
)
def list_catalog_imports_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CatalogImportListRead:
    imports, total = list_catalog_imports(db, tenant_id, limit=limit, offset=offset)
    return CatalogImportListRead(
        items=[CatalogImportRead.model_validate(item) for item in imports],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/imports/{import_id}",
    response_model=CatalogImportRead,
    operation_id="getCatalogImport",
)
def get_catalog_import_endpoint(
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> CatalogImportRead:
    return CatalogImportRead.model_validate(get_catalog_import(db, tenant_id, import_id))


@canonical_router.get(
    "/v1/catalog-imports/{import_id}",
    response_model=CatalogImportRead,
    operation_id="getCatalogImportCanonical",
)
def get_catalog_import_canonical_endpoint(
    import_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
) -> CatalogImportRead:
    tenant_id = _catalog_import_tenant_id(db, import_id)
    return CatalogImportRead.model_validate(get_catalog_import(db, tenant_id, import_id))


@router.get(
    "/imports/{import_id}/errors",
    response_model=CatalogImportErrorListRead,
    operation_id="listCatalogImportErrors",
)
def list_catalog_import_errors_endpoint(
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CatalogImportErrorListRead:
    errors, total = list_catalog_import_errors(
        db,
        tenant_id,
        import_id,
        limit=limit,
        offset=offset,
    )
    return CatalogImportErrorListRead(
        items=[CatalogImportErrorRead.model_validate(item) for item in errors],
        total=total,
        limit=limit,
        offset=offset,
    )


@canonical_router.get(
    "/v1/catalog-imports/{import_id}/errors",
    response_model=CatalogImportErrorListRead,
    operation_id="listCatalogImportErrorsCanonical",
)
def list_catalog_import_errors_canonical_endpoint(
    import_id: uuid.UUID,
    db: DatabaseSession,
    _: PlatformAccess,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CatalogImportErrorListRead:
    tenant_id = _catalog_import_tenant_id(db, import_id)
    errors, total = list_catalog_import_errors(
        db,
        tenant_id,
        import_id,
        limit=limit,
        offset=offset,
    )
    return CatalogImportErrorListRead(
        items=[CatalogImportErrorRead.model_validate(item) for item in errors],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/skus",
    response_model=SkuListRead,
    operation_id="listCatalogSkus",
)
def list_skus_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadCatalog,
    active: bool | None = True,
    code: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> SkuListRead:
    if principal.tenant_id != tenant_id:
        raise ApiError(
            403,
            "Forbidden",
            "The requested tenant is outside the current user's access scope.",
            code="tenant_scope_denied",
        )
    rows, total = list_skus(
        db,
        tenant_id,
        active=active,
        code=code,
        limit=limit,
        offset=offset,
    )
    return SkuListRead(
        items=[_sku_read(sku, style) for sku, style in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/skus/{sku_id}",
    response_model=SkuRead,
    operation_id="getCatalogSku",
)
def get_sku_endpoint(
    tenant_id: uuid.UUID,
    sku_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadCatalog,
) -> SkuRead:
    if principal.tenant_id != tenant_id:
        raise ApiError(
            403,
            "Forbidden",
            "The requested tenant is outside the current user's access scope.",
            code="tenant_scope_denied",
        )
    sku, style = get_sku(db, tenant_id, sku_id)
    return _sku_read(sku, style)
