import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, Header, Query, UploadFile, status

from abacus.api.dependencies import DatabaseSession
from abacus.api.errors import ApiError
from abacus.models.catalog import CatalogImportMode, ProductStyle, Sku
from abacus.schemas.catalog import (
    CatalogImportErrorListRead,
    CatalogImportErrorRead,
    CatalogImportRead,
    SkuActivityFilter,
    SkuListRead,
    SkuRead,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.catalog import (
    MAX_CATALOG_FILE_BYTES,
    accept_catalog_import,
    get_catalog_import,
    get_sku,
    list_catalog_import_errors,
    list_skus,
)

router = APIRouter(tags=["2. Product catalog"])
CanReadCatalog = Annotated[Principal, Depends(require_permission(Permission.CATALOG_READ))]
CanIngestCatalog = Annotated[Principal, Depends(require_permission(Permission.CATALOG_INGEST))]


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


@router.post(
    "/v1/tenants/{tenant_id}/catalog-imports",
    response_model=CatalogImportRead,
    status_code=status.HTTP_202_ACCEPTED,
    operation_id="createCatalogImport",
    summary="Accept a product-master CSV for asynchronous validation",
)
def create_catalog_import_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanIngestCatalog,
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

    if tenant_id != principal.tenant_id:
        raise ApiError(403, "Forbidden", "Catalog imports are limited to the JWT tenant.")
    content = file.file.read(MAX_CATALOG_FILE_BYTES + 1)
    catalog_import = accept_catalog_import(
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
    "/v1/catalog-imports/{import_id}",
    response_model=CatalogImportRead,
    operation_id="getCatalogImport",
)
def get_catalog_import_endpoint(
    import_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadCatalog,
) -> CatalogImportRead:
    if not principal.has_tenant_permission(Permission.CATALOG_READ):
        raise ApiError(403, "Forbidden", "Catalog import history requires tenant-wide access.")
    return CatalogImportRead.model_validate(get_catalog_import(db, principal.tenant_id, import_id))


@router.get(
    "/v1/catalog-imports/{import_id}/errors",
    response_model=CatalogImportErrorListRead,
    operation_id="listCatalogImportErrors",
)
def list_catalog_import_errors_endpoint(
    import_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadCatalog,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> CatalogImportErrorListRead:
    if not principal.has_tenant_permission(Permission.CATALOG_READ):
        raise ApiError(403, "Forbidden", "Catalog import errors require tenant-wide access.")
    errors, total = list_catalog_import_errors(
        db,
        principal.tenant_id,
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
    "/v1/skus",
    response_model=SkuListRead,
    operation_id="listSkus",
)
def list_skus_endpoint(
    db: DatabaseSession,
    principal: CanReadCatalog,
    active: Annotated[
        SkuActivityFilter,
        Query(
            description=(
                "Activity scope: ACTIVE, INACTIVE, or ALL. The former true/false values "
                "remain accepted as aliases for ACTIVE/INACTIVE."
            )
        ),
    ] = SkuActivityFilter.ACTIVE,
    code: Annotated[str | None, Query(min_length=1, max_length=128)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> SkuListRead:
    rows, total = list_skus(
        db,
        principal.tenant_id,
        active=active.database_value(),
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
    "/v1/skus/{sku_id}",
    response_model=SkuRead,
    operation_id="getSku",
)
def get_sku_endpoint(
    sku_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadCatalog,
) -> SkuRead:
    sku, style = get_sku(db, principal.tenant_id, sku_id)
    return _sku_read(sku, style)
