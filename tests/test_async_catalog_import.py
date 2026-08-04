from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import Session, sessionmaker

import abacus.services.catalog as catalog_service
from abacus.config import get_settings
from abacus.db import tenant_session_scope
from abacus.enums import JobKind, JobStatus
from abacus.models.architecture import (
    CanonicalIdentityRole,
    Product,
    ProductVariant,
    RfidTag,
    UserRole,
)
from abacus.models.catalog import (
    CatalogImport,
    CatalogImportError,
    CatalogImportRow,
    CatalogImportSource,
    CatalogImportStatus,
    EpcBinding,
    ProductStyle,
    Sku,
)
from abacus.models.identity import User, UserStatus
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Tenant
from abacus.processes import catalog_worker
from abacus.security import hash_password
from abacus.services.jobs import claim_jobs

pytestmark = pytest.mark.integration

VALID_CSV = (
    b"style_code,style_name,sku,upc,color,size,epc,style_attributes,attributes\n"
    b"ST-ASYNC,Async Shirt,SKU-ASYNC-BLUE-M,036000291452,Blue,M,"
    b'3074257BF7194E4000001B01,"{""category"":""SHIRTS""}","{}"\n'
)
INVALID_CSV = b"unexpected,columns\nvalue,other\n"


def _create_tenant(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
) -> tuple[uuid.UUID, str]:
    suffix = uuid.uuid4().hex[:16]
    tenant_code = f"async-catalog-{suffix}"
    response = api_client.post(
        "/v1/tenants",
        headers={"X-Platform-Key": get_settings().platform_api_key},
        json={"code": tenant_code, "name": f"Async Catalog {suffix}"},
    )
    assert response.status_code == 201, response.text
    tenant_id = uuid.UUID(response.json()["id"])
    email = f"admin-{suffix}@example.com"
    password = f"Async-Catalog-{suffix}!"
    with postgres_session_factory() as db:
        user = User(
            tenant_id=tenant_id,
            email=email,
            display_name="Async Catalog Administrator",
            password_hash=hash_password(password),
            status=UserStatus.ACTIVE,
            token_version=1,
        )
        db.add(user)
        db.flush()
        db.add(
            UserRole(
                tenant_id=tenant_id,
                user_id=user.id,
                role=CanonicalIdentityRole.TENANT_ADMIN,
            )
        )
        db.commit()
    login = api_client.post(
        "/v1/auth/login",
        json={"tenant_code": tenant_code, "email": email, "password": password},
    )
    assert login.status_code == 200, login.text
    return tenant_id, str(login.json()["access_token"])


def _submit_catalog(
    api_client: TestClient,
    tenant_id: uuid.UUID,
    access_token: str,
    content: bytes,
    *,
    idempotency_key: str | None = None,
) -> dict[str, object]:
    response = api_client.post(
        f"/v1/tenants/{tenant_id}/catalog-imports",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Idempotency-Key": idempotency_key or f"async-{uuid.uuid4().hex}",
        },
        data={"mode": "FULL"},
        files={"file": ("catalog.csv", content, "text/csv")},
    )
    assert response.status_code == 202, response.text
    payload = response.json()
    assert isinstance(payload, dict)
    return payload


def _claim_catalog_job(tenant_id: uuid.UUID, worker_id: str) -> DurableJob:
    settings = get_settings()
    with tenant_session_scope(tenant_id) as db:
        jobs = claim_jobs(
            db,
            worker_id=worker_id,
            limit=1,
            lease_seconds=settings.worker_lease_seconds,
            max_attempts=settings.worker_max_attempts,
            tenant_id=tenant_id,
            kinds=(JobKind.CATALOG_IMPORT,),
        )
    assert len(jobs) == 1
    return jobs[0]


@pytest.mark.integration
def test_catalog_request_only_accepts_source_and_worker_retry_completes_import(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id, access_token = _create_tenant(api_client, postgres_session_factory)
    original_parse = catalog_service.parse_catalog_csv

    def fail_parse(_content: bytes) -> catalog_service.CatalogParseResult:
        raise RuntimeError("simulated parser crash")

    monkeypatch.setattr(catalog_service, "parse_catalog_csv", fail_parse)
    try:
        idempotency_key = f"async-{uuid.uuid4().hex}"
        accepted = _submit_catalog(
            api_client,
            tenant_id,
            access_token,
            VALID_CSV,
            idempotency_key=idempotency_key,
        )
        import_id = uuid.UUID(str(accepted["id"]))
        assert accepted["status"] == CatalogImportStatus.VALIDATING
        assert accepted["total_rows"] == 0
        duplicate = _submit_catalog(
            api_client,
            tenant_id,
            access_token,
            VALID_CSV,
            idempotency_key=idempotency_key,
        )
        assert duplicate["id"] == accepted["id"]

        with postgres_session_factory() as db:
            source = db.get(CatalogImportSource, import_id)
            assert source is not None
            assert source.content == VALID_CSV
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(CatalogImportRow)
                    .where(CatalogImportRow.import_id == import_id)
                )
                == 0
            )
            job = db.scalar(
                select(DurableJob).where(
                    DurableJob.tenant_id == tenant_id,
                    DurableJob.payload["import_id"].as_string() == str(import_id),
                )
            )
            assert job is not None
            assert job.status is JobStatus.PENDING
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(DurableJob)
                    .where(
                        DurableJob.tenant_id == tenant_id,
                        DurableJob.kind == JobKind.CATALOG_IMPORT,
                    )
                )
                == 1
            )

        with tenant_session_scope(tenant_id) as db:
            with pytest.raises(DBAPIError):
                db.execute(
                    update(CatalogImportSource)
                    .where(CatalogImportSource.import_id == import_id)
                    .values(content=b"mutated")
                )
            db.rollback()

        worker_id = f"async-catalog-{uuid.uuid4()}"
        first_attempt = _claim_catalog_job(tenant_id, worker_id)
        catalog_worker._process_job(tenant_id, first_attempt, worker_id)

        with postgres_session_factory() as db:
            catalog_import = db.get(CatalogImport, import_id)
            source = db.get(CatalogImportSource, import_id)
            job = db.get(DurableJob, first_attempt.id)
            assert catalog_import is not None
            assert catalog_import.status is CatalogImportStatus.VALIDATING
            assert source is not None and source.content == VALID_CSV
            assert job is not None and job.status is JobStatus.PENDING
            job.available_at = datetime.now(UTC) - timedelta(seconds=1)
            db.commit()

        monkeypatch.setattr(catalog_service, "parse_catalog_csv", original_parse)
        second_attempt = _claim_catalog_job(tenant_id, worker_id)
        catalog_worker._process_job(tenant_id, second_attempt, worker_id)

        with postgres_session_factory() as db:
            catalog_import = db.get(CatalogImport, import_id)
            source = db.get(CatalogImportSource, import_id)
            job = db.get(DurableJob, second_attempt.id)
            assert catalog_import is not None
            assert catalog_import.status is CatalogImportStatus.COMPLETED
            assert catalog_import.total_rows == 1
            assert catalog_import.valid_rows == 1
            assert source is not None and source.content == VALID_CSV
            assert job is not None and job.status is JobStatus.COMPLETED
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(CatalogImportRow)
                    .where(CatalogImportRow.import_id == import_id)
                )
                == 1
            )

            style = db.scalar(
                select(ProductStyle).where(
                    ProductStyle.tenant_id == tenant_id,
                    ProductStyle.code == "ST-ASYNC",
                )
            )
            product = db.scalar(
                select(Product).where(
                    Product.tenant_id == tenant_id,
                    Product.style_code == "ST-ASYNC",
                )
            )
            sku = db.scalar(
                select(Sku).where(Sku.tenant_id == tenant_id, Sku.code == "SKU-ASYNC-BLUE-M")
            )
            assert style is not None
            assert product is not None and product.category == "SHIRTS"
            assert sku is not None and sku.product_style_id == style.id

            variant = db.scalar(
                select(ProductVariant).where(
                    ProductVariant.tenant_id == tenant_id,
                    ProductVariant.product_id == product.id,
                    ProductVariant.color == "Blue",
                )
            )
            binding = db.scalar(
                select(EpcBinding).where(
                    EpcBinding.tenant_id == tenant_id,
                    EpcBinding.epc == "3074257BF7194E4000001B01",
                    EpcBinding.effective_to.is_(None),
                )
            )
            tag = db.get(RfidTag, (tenant_id, "3074257BF7194E4000001B01"))
            assert variant is not None and sku.product_variant_id == variant.id
            assert binding is not None and binding.sku_id == sku.id
            assert tag is not None and tag.sku_id == sku.id
            assert binding.source_import_id == import_id
            assert tag.source_import_id == import_id

            # A retry after the worker commit is a no-op, not a second promotion.
            catalog_service.process_catalog_import_job(db, {"import_id": str(import_id)})
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(CatalogImportRow)
                    .where(CatalogImportRow.import_id == import_id)
                )
                == 1
            )
    finally:
        monkeypatch.setattr(catalog_service, "parse_catalog_csv", original_parse)
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


@pytest.mark.integration
def test_invalid_catalog_is_rejected_asynchronously_and_source_is_retained(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_id, access_token = _create_tenant(api_client, postgres_session_factory)
    try:
        valid_import = _submit_catalog(api_client, tenant_id, access_token, VALID_CSV)
        valid_worker_id = f"async-catalog-valid-{uuid.uuid4()}"
        valid_job = _claim_catalog_job(tenant_id, valid_worker_id)
        catalog_worker._process_job(tenant_id, valid_job, valid_worker_id)

        accepted = _submit_catalog(api_client, tenant_id, access_token, INVALID_CSV)
        import_id = uuid.UUID(str(accepted["id"]))
        assert accepted["status"] == CatalogImportStatus.VALIDATING

        worker_id = f"async-catalog-{uuid.uuid4()}"
        job = _claim_catalog_job(tenant_id, worker_id)
        catalog_worker._process_job(tenant_id, job, worker_id)

        with postgres_session_factory() as db:
            catalog_import = db.get(CatalogImport, import_id)
            source = db.get(CatalogImportSource, import_id)
            persisted_job = db.get(DurableJob, job.id)
            assert catalog_import is not None
            assert catalog_import.status is CatalogImportStatus.REJECTED
            assert source is not None and source.content == INVALID_CSV
            assert persisted_job is not None and persisted_job.status is JobStatus.COMPLETED
            promoted = db.get(CatalogImport, uuid.UUID(str(valid_import["id"])))
            assert promoted is not None
            assert promoted.status is CatalogImportStatus.COMPLETED
            assert (
                db.scalar(select(func.count()).select_from(Sku).where(Sku.tenant_id == tenant_id))
                == 1
            )
            assert (
                db.scalar(
                    select(func.count()).select_from(RfidTag).where(RfidTag.tenant_id == tenant_id)
                )
                == 1
            )
            assert (
                db.scalar(
                    select(func.count())
                    .select_from(CatalogImportError)
                    .where(CatalogImportError.import_id == import_id)
                )
                or 0
            ) > 0
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id == tenant_id))
            db.commit()


@pytest.mark.integration
def test_catalog_imports_use_jwt_tenant_and_hide_cross_tenant_jobs(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
) -> None:
    tenant_a, token_a = _create_tenant(api_client, postgres_session_factory)
    tenant_b, token_b = _create_tenant(api_client, postgres_session_factory)
    try:
        forbidden = api_client.post(
            f"/v1/tenants/{tenant_b}/catalog-imports",
            headers={
                "Authorization": f"Bearer {token_a}",
                "Idempotency-Key": f"cross-tenant-{uuid.uuid4().hex}",
            },
            data={"mode": "FULL"},
            files={"file": ("catalog.csv", VALID_CSV, "text/csv")},
        )
        assert forbidden.status_code == 403, forbidden.text

        accepted = _submit_catalog(api_client, tenant_a, token_a, VALID_CSV)
        import_id = accepted["id"]
        for path in (
            f"/v1/catalog-imports/{import_id}",
            f"/v1/catalog-imports/{import_id}/errors",
        ):
            hidden = api_client.get(
                path,
                headers={"Authorization": f"Bearer {token_b}"},
            )
            assert hidden.status_code == 404, hidden.text
    finally:
        with postgres_session_factory() as db:
            db.execute(delete(Tenant).where(Tenant.id.in_((tenant_a, tenant_b))))
            db.commit()
