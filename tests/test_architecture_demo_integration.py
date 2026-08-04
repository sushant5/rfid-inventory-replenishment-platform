from __future__ import annotations

import argparse
import uuid
from typing import Any

import pytest
import scripts.run_architecture_demo as architecture_demo
from fastapi.testclient import TestClient
from sqlalchemy import delete, func, select
from sqlalchemy.orm import Session, sessionmaker

from abacus.config import get_settings
from abacus.db import SessionLocal, tenant_session_scope
from abacus.enums import JobKind, JobStatus
from abacus.models.architecture import (
    BusinessEvent,
    CurrentItemState,
    InventoryProjection,
    PolicyDefinition,
    ReplenishmentTask,
    ReplenishmentTaskEvidence,
    ReplenishmentTaskStatus,
    RfidObservationBatch,
)
from abacus.models.catalog import Sku
from abacus.models.identity import IdentityRole, User
from abacus.models.jobs import DurableJob
from abacus.models.tenancy import Device, DeviceAssignment, Store, Tenant, Zone
from abacus.processes import catalog_worker, event_worker
from abacus.schemas.identity import RoleAssignmentCreate, UserCreate
from abacus.schemas.tenancy import TenantCreate
from abacus.services.identity import bootstrap_corporate_admin
from abacus.services.jobs import claim_jobs
from abacus.services.streaming_inventory import RecentObservationState

pytestmark = pytest.mark.integration


def _drain_catalog_jobs() -> int:
    settings = get_settings()
    worker_id = "architecture-demo-catalog-test"
    processed = 0
    for _ in range(20):
        claimed_any = False
        for tenant_id in catalog_worker._active_tenants():
            with tenant_session_scope(tenant_id) as db:
                jobs = claim_jobs(
                    db,
                    worker_id=worker_id,
                    limit=10,
                    lease_seconds=settings.worker_lease_seconds,
                    max_attempts=settings.worker_max_attempts,
                    tenant_id=tenant_id,
                    kinds=(JobKind.CATALOG_IMPORT,),
                )
            for job in jobs:
                claimed_any = True
                catalog_worker._process_job(tenant_id, job, worker_id)
                processed += 1
        if not claimed_any:
            return processed
    raise AssertionError("catalog worker did not drain within 20 passes")


def _drain_events(recent: RecentObservationState) -> tuple[int, int]:
    raw_total = 0
    transition_total = 0
    for _ in range(20):
        processed = 0
        for tenant_id in event_worker._active_tenants():
            raw_count, transition_count = event_worker.process_tenant_once(tenant_id, recent)
            raw_total += raw_count
            transition_total += transition_count
            processed += raw_count + transition_count
        if processed == 0:
            return raw_total, transition_total
    raise AssertionError("event worker did not drain within 20 passes")


def test_canonical_architecture_demo_runs_through_testclient_and_durable_workers(
    api_client: TestClient,
    postgres_session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    admin_email = "architecture-demo-admin@orange.example"
    admin_password = "Architecture-Demo-Admin-2026!"
    settings = get_settings()
    recent = RecentObservationState()
    created_clients: list[InProcessDemoClient] = []

    class InProcessDemoClient(architecture_demo.Client):
        def __init__(self, base_url: str, *, timeout: float) -> None:
            super().__init__(base_url, timeout=timeout)
            self.catalog_jobs = 0
            self.raw_events = 0
            self.inventory_transitions = 0
            created_clients.append(self)

        def request(
            self,
            method: str,
            path: str,
            *,
            expected: int | set[int] = 200,
            headers: dict[str, str] | None = None,
            payload: object | None = None,
            body: bytes | None = None,
            content_type: str | None = None,
        ) -> tuple[int, Any]:
            request_headers = {"Accept": "application/json", **(headers or {})}
            if content_type is not None:
                request_headers["Content-Type"] = content_type
            request_arguments: dict[str, Any] = {"headers": request_headers}
            if payload is not None:
                request_arguments["json"] = payload
            elif body is not None:
                request_arguments["content"] = body
            response = api_client.request(method, path, **request_arguments)
            decoded: Any = response.json() if response.content else None
            expected_codes = {expected} if isinstance(expected, int) else expected
            if response.status_code not in expected_codes:
                raise architecture_demo.DemoFailure(
                    f"{method} {path} returned {response.status_code}: {decoded}"
                )

            if method == "POST" and path.endswith("/catalog-imports"):
                self.catalog_jobs += _drain_catalog_jobs()
            elif method == "POST" and (
                path == "/v1/rfid/observation-batches" or path.endswith("/business-events")
            ):
                raw_count, transition_count = _drain_events(recent)
                self.raw_events += raw_count
                self.inventory_transitions += transition_count
            return response.status_code, decoded

    tenant_id: uuid.UUID | None = None
    try:
        with SessionLocal() as bootstrap_db:
            bootstrap = bootstrap_corporate_admin(
                bootstrap_db,
                TenantCreate(code="orange", name="Orange"),
                UserCreate(
                    email=admin_email,
                    display_name="Architecture Demo Admin",
                    password=admin_password,
                    role_assignments=[RoleAssignmentCreate(role=IdentityRole.CORPORATE_ADMIN)],
                ),
            )
            tenant_id = bootstrap.user.tenant_id

        monkeypatch.setattr(architecture_demo, "Client", InProcessDemoClient)
        architecture_demo.run(
            argparse.Namespace(
                base_url="http://testserver",
                platform_key=settings.platform_api_key,
                admin_email=admin_email,
                admin_password=admin_password,
                request_timeout=2.0,
                startup_timeout=2.0,
                poll_timeout=2.0,
                provision_only=False,
            )
        )

        output = capsys.readouterr().out
        assert "PASS tenant/100-store footprint and Store 1 zones/devices" in output
        assert "PASS RFID stable-zone inventory: floor=1 backroom=3" in output
        assert "PASS quantity=2 replenishment task completed with RFID verification" in output
        assert "PASS reviewer seed: 100 SKUs and inventory/tasks in five stores" in output
        assert "PASS store-scoped authorization denied Store 2" in output
        assert "PASS idempotent authoritative sale removed one physical item" in output
        assert output.rstrip().endswith("DEMO COMPLETE")

        assert len(created_clients) == 1
        in_process_client = created_clients[0]
        assert in_process_client.catalog_jobs == 1
        assert in_process_client.raw_events == 70
        assert in_process_client.inventory_transitions == 23

        with postgres_session_factory() as verify_db:
            assert tenant_id is not None
            assert (
                verify_db.scalar(
                    select(func.count()).select_from(Sku).where(Sku.tenant_id == tenant_id)
                )
                == 100
            )
            assert (
                verify_db.scalar(
                    select(func.count()).select_from(Store).where(Store.tenant_id == tenant_id)
                )
                == 100
            )
            assert (
                verify_db.scalar(
                    select(func.count()).select_from(Zone).where(Zone.tenant_id == tenant_id)
                )
                == 200
            )
            assert (
                verify_db.scalar(
                    select(func.count()).select_from(Device).where(Device.tenant_id == tenant_id)
                )
                == 200
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(DeviceAssignment)
                    .where(DeviceAssignment.tenant_id == tenant_id)
                )
                == 200
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(CurrentItemState)
                    .where(
                        CurrentItemState.tenant_id == tenant_id,
                        CurrentItemState.authoritative_removal_event_id.is_not(None),
                    )
                )
                == 1
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(PolicyDefinition)
                    .where(PolicyDefinition.tenant_id == tenant_id)
                )
                == 1
            )
            assert (
                verify_db.scalar(
                    select(func.count()).select_from(User).where(User.tenant_id == tenant_id)
                )
                == 2
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(ReplenishmentTask)
                    .where(ReplenishmentTask.tenant_id == tenant_id)
                )
                == 5
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(BusinessEvent)
                    .where(BusinessEvent.tenant_id == tenant_id)
                )
                == 1
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(CurrentItemState)
                    .where(CurrentItemState.tenant_id == tenant_id)
                )
                == 20
            )
            assert (
                verify_db.scalar(
                    select(func.sum(InventoryProjection.quantity)).where(
                        InventoryProjection.tenant_id == tenant_id
                    )
                )
                == 19
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(ReplenishmentTask)
                    .where(
                        ReplenishmentTask.tenant_id == tenant_id,
                        ReplenishmentTask.status == ReplenishmentTaskStatus.COMPLETED,
                    )
                )
                == 1
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(ReplenishmentTaskEvidence)
                    .where(ReplenishmentTaskEvidence.tenant_id == tenant_id)
                )
                == 2
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(RfidObservationBatch)
                    .where(
                        RfidObservationBatch.tenant_id == tenant_id,
                        RfidObservationBatch.processed_count + RfidObservationBatch.rejected_count
                        == RfidObservationBatch.accepted_count,
                    )
                )
                == 14
            )
            assert (
                verify_db.scalar(
                    select(func.count())
                    .select_from(DurableJob)
                    .where(
                        DurableJob.tenant_id == tenant_id,
                        DurableJob.status != JobStatus.COMPLETED,
                    )
                )
                == 0
            )
    finally:
        if tenant_id is not None:
            with postgres_session_factory() as cleanup_db:
                cleanup_db.execute(delete(Tenant).where(Tenant.id == tenant_id))
                cleanup_db.commit()
