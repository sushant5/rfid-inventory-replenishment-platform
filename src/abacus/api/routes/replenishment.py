import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from abacus.api.dependencies import DatabaseSession
from abacus.api.errors import ApiError
from abacus.models.catalog import Sku
from abacus.models.replenishment import (
    PolicySelectorType,
    ReplenishmentRun,
    ReplenishmentRunLine,
    ReplenishmentTask,
    ReplenishmentTaskStatus,
    ReplenishmentTrigger,
)
from abacus.schemas.replenishment import (
    PolicyBulkUpsertRequest,
    PolicyCreate,
    PolicyImportRead,
    PolicyListRead,
    PolicyPatch,
    PolicyRead,
    ReplenishmentEvaluationRequest,
    ReplenishmentRunLineRead,
    ReplenishmentRunRead,
    ReplenishmentTaskListRead,
    ReplenishmentTaskRead,
    ReplenishmentTaskUpdate,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.replenishment import (
    bulk_upsert_policies,
    create_policy,
    create_replenishment_run,
    deactivate_policy,
    get_policy,
    get_policy_import,
    get_replenishment_run,
    list_policies,
    list_tasks,
    update_policy,
    update_task,
)

router = APIRouter(
    prefix="/v1/tenants/{tenant_id}/replenishment",
    tags=["5. Replenishment"],
)

CanReadPolicies = Annotated[Principal, Depends(require_permission(Permission.POLICY_READ))]
CanManagePolicies = Annotated[Principal, Depends(require_permission(Permission.POLICY_MANAGE))]
CanReadReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_READ)),
]
CanManageReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_MANAGE)),
]
CanExecuteReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_EXECUTE)),
]


def _ensure_tenant(principal: Principal, tenant_id: uuid.UUID) -> None:
    if principal.tenant_id != tenant_id:
        raise ApiError(
            403,
            "Forbidden",
            "The requested tenant is outside the current user's access scope.",
            code="tenant_scope_denied",
        )


def _ensure_store(
    principal: Principal,
    tenant_id: uuid.UUID,
    permission: Permission,
    store_id: uuid.UUID,
) -> None:
    _ensure_tenant(principal, tenant_id)
    if not principal.can_access_store(permission, store_id):
        raise ApiError(
            403,
            "Forbidden",
            "The requested store is outside the current user's access scope.",
            code="store_scope_denied",
        )


def _ensure_policy_manage_scope(
    principal: Principal,
    tenant_id: uuid.UUID,
    store_ids: list[uuid.UUID | None],
) -> None:
    _ensure_tenant(principal, tenant_id)
    if principal.has_tenant_permission(Permission.POLICY_MANAGE):
        return
    allowed = principal.store_ids_for_permission(Permission.POLICY_MANAGE)
    if any(store_id is None or store_id not in allowed for store_id in store_ids):
        raise ApiError(
            403,
            "Forbidden",
            "A store-scoped user cannot manage tenant-default or out-of-scope policies.",
            code="policy_scope_denied",
        )


def _line_read(line: ReplenishmentRunLine, sku: Sku) -> ReplenishmentRunLineRead:
    return ReplenishmentRunLineRead(
        id=line.id,
        tenant_id=line.tenant_id,
        run_id=line.run_id,
        store_id=line.store_id,
        sku_id=line.sku_id,
        sku_code=sku.code,
        policy_id=line.policy_id,
        task_id=line.task_id,
        selector_type=line.selector_type,
        selector_value=line.selector_value,
        policy_priority=line.policy_priority,
        minimum_floor_quantity=line.minimum_floor_quantity,
        target_floor_quantity=line.target_floor_quantity,
        maximum_floor_quantity=line.maximum_floor_quantity,
        floor_quantity=line.floor_quantity,
        backroom_quantity=line.backroom_quantity,
        open_task_quantity=line.open_task_quantity,
        recommended_quantity=line.recommended_quantity,
        reason=line.reason,
        formula=line.formula,
        inventory_as_of=line.inventory_as_of,
        created_at=line.created_at,
    )


def _run_read(
    run: ReplenishmentRun,
    lines: list[tuple[ReplenishmentRunLine, Sku]],
) -> ReplenishmentRunRead:
    return ReplenishmentRunRead(
        id=run.id,
        tenant_id=run.tenant_id,
        store_id=run.store_id,
        idempotency_key=run.idempotency_key,
        trigger=run.trigger,
        status=run.status,
        evaluated_at=run.evaluated_at,
        requested_by_subject=run.requested_by_subject,
        line_count=run.line_count,
        tasks_created=run.tasks_created,
        tasks_updated=run.tasks_updated,
        created_at=run.created_at,
        lines=[_line_read(line, sku) for line, sku in lines],
    )


def _task_read(task: ReplenishmentTask, sku: Sku) -> ReplenishmentTaskRead:
    return ReplenishmentTaskRead(
        id=task.id,
        tenant_id=task.tenant_id,
        store_id=task.store_id,
        sku_id=task.sku_id,
        sku_code=sku.code,
        source_policy_id=task.source_policy_id,
        status=task.status,
        quantity=task.quantity,
        moved_quantity=task.moved_quantity,
        remaining_quantity=max(0, task.quantity - task.moved_quantity),
        version=task.version,
        claimed_by_subject=task.claimed_by_subject,
        claimed_at=task.claimed_at,
        completed_at=task.completed_at,
        last_note=task.last_note,
        created_at=task.created_at,
        updated_at=task.updated_at,
    )


@router.post(
    "/policies",
    response_model=PolicyRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createReplenishmentPolicy",
)
def create_policy_endpoint(
    tenant_id: uuid.UUID,
    request: PolicyCreate,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyRead:
    _ensure_policy_manage_scope(principal, tenant_id, [request.store_id])
    return PolicyRead.model_validate(create_policy(db, tenant_id, request))


@router.post(
    "/policies:bulk-upsert",
    response_model=PolicyImportRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="bulkUpsertReplenishmentPolicies",
)
def bulk_upsert_policies_endpoint(
    tenant_id: uuid.UUID,
    request: PolicyBulkUpsertRequest,
    db: DatabaseSession,
    principal: CanManagePolicies,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key")],
) -> PolicyImportRead:
    _ensure_policy_manage_scope(
        principal,
        tenant_id,
        [definition.store_id for definition in request.policies],
    )
    result = bulk_upsert_policies(db, tenant_id, idempotency_key, request)
    return PolicyImportRead.model_validate(result)


@router.get(
    "/policy-imports/{import_id}",
    response_model=PolicyImportRead,
    operation_id="getReplenishmentPolicyImport",
)
def get_policy_import_endpoint(
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyImportRead:
    _ensure_tenant(principal, tenant_id)
    if not principal.has_tenant_permission(Permission.POLICY_MANAGE):
        raise ApiError(
            403,
            "Forbidden",
            "Policy import records require tenant-wide policy management access.",
            code="policy_scope_denied",
        )
    return PolicyImportRead.model_validate(get_policy_import(db, tenant_id, import_id))


@router.get(
    "/policies",
    response_model=PolicyListRead,
    operation_id="listReplenishmentPolicies",
)
def list_policies_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadPolicies,
    store_id: uuid.UUID | None = None,
    selector_type: PolicySelectorType | None = None,
    active: bool | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PolicyListRead:
    _ensure_tenant(principal, tenant_id)
    if store_id is None and not principal.has_tenant_permission(Permission.POLICY_READ):
        raise ApiError(
            400,
            "Store filter required",
            "Store-scoped users must specify store_id when listing policies.",
            code="store_filter_required",
        )
    if store_id is not None:
        _ensure_store(principal, tenant_id, Permission.POLICY_READ, store_id)
    policies, total = list_policies(
        db,
        tenant_id,
        store_id=store_id,
        selector_type=selector_type,
        active=active,
        limit=limit,
        offset=offset,
    )
    return PolicyListRead(
        items=[PolicyRead.model_validate(policy) for policy in policies],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/policies/{policy_id}",
    response_model=PolicyRead,
    operation_id="getReplenishmentPolicy",
)
def get_policy_endpoint(
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadPolicies,
) -> PolicyRead:
    _ensure_tenant(principal, tenant_id)
    policy = get_policy(db, tenant_id, policy_id)
    if policy.store_id is not None:
        _ensure_store(principal, tenant_id, Permission.POLICY_READ, policy.store_id)
    return PolicyRead.model_validate(policy)


@router.patch(
    "/policies/{policy_id}",
    response_model=PolicyRead,
    operation_id="updateReplenishmentPolicy",
)
def update_policy_endpoint(
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
    request: PolicyPatch,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyRead:
    _ensure_tenant(principal, tenant_id)
    current = get_policy(db, tenant_id, policy_id)
    target_store_id = (
        request.store_id if "store_id" in request.model_fields_set else current.store_id
    )
    _ensure_policy_manage_scope(principal, tenant_id, [current.store_id, target_store_id])
    return PolicyRead.model_validate(update_policy(db, tenant_id, policy_id, request))


@router.delete(
    "/policies/{policy_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    operation_id="deactivateReplenishmentPolicy",
)
def deactivate_policy_endpoint(
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> Response:
    _ensure_tenant(principal, tenant_id)
    current = get_policy(db, tenant_id, policy_id)
    _ensure_policy_manage_scope(principal, tenant_id, [current.store_id])
    deactivate_policy(db, tenant_id, policy_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/evaluations",
    response_model=ReplenishmentRunRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="evaluateReplenishment",
)
def evaluate_replenishment_endpoint(
    tenant_id: uuid.UUID,
    request: ReplenishmentEvaluationRequest,
    db: DatabaseSession,
    principal: CanManageReplenishment,
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> ReplenishmentRunRead:
    _ensure_store(
        principal,
        tenant_id,
        Permission.REPLENISHMENT_MANAGE,
        request.store_id,
    )
    try:
        run = create_replenishment_run(
            db,
            tenant_id,
            request,
            trigger=ReplenishmentTrigger.API,
            requested_by_subject=str(principal.user_id),
            idempotency_key=idempotency_key,
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Concurrent replenishment conflict",
            "The evaluation conflicted with another active-task or idempotent request.",
            code="concurrent_replenishment_conflict",
        ) from exc
    run, lines = get_replenishment_run(db, tenant_id, run.id)
    return _run_read(run, lines)


@router.get(
    "/evaluations/{run_id}",
    response_model=ReplenishmentRunRead,
    operation_id="getReplenishmentEvaluation",
)
def get_replenishment_run_endpoint(
    tenant_id: uuid.UUID,
    run_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadReplenishment,
) -> ReplenishmentRunRead:
    _ensure_tenant(principal, tenant_id)
    run, lines = get_replenishment_run(db, tenant_id, run_id)
    _ensure_store(
        principal,
        tenant_id,
        Permission.REPLENISHMENT_READ,
        run.store_id,
    )
    return _run_read(run, lines)


@router.get(
    "/tasks",
    response_model=ReplenishmentTaskListRead,
    operation_id="listReplenishmentTasks",
)
def list_tasks_endpoint(
    tenant_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadReplenishment,
    store_id: uuid.UUID | None = None,
    task_status: Annotated[
        ReplenishmentTaskStatus | None,
        Query(alias="status"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> ReplenishmentTaskListRead:
    _ensure_tenant(principal, tenant_id)
    if store_id is None and not principal.has_tenant_permission(Permission.REPLENISHMENT_READ):
        raise ApiError(
            400,
            "Store filter required",
            "Store-scoped users must specify store_id when listing tasks.",
            code="store_filter_required",
        )
    if store_id is not None:
        _ensure_store(principal, tenant_id, Permission.REPLENISHMENT_READ, store_id)
    tasks, total = list_tasks(
        db,
        tenant_id,
        store_id=store_id,
        status=task_status,
        limit=limit,
        offset=offset,
    )
    return ReplenishmentTaskListRead(
        items=[_task_read(task, sku) for task, sku in tasks],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.patch(
    "/tasks/{task_id}",
    response_model=ReplenishmentTaskRead,
    operation_id="updateReplenishmentTask",
)
def update_task_endpoint(
    tenant_id: uuid.UUID,
    task_id: uuid.UUID,
    request: ReplenishmentTaskUpdate,
    db: DatabaseSession,
    principal: CanExecuteReplenishment,
) -> ReplenishmentTaskRead:
    _ensure_tenant(principal, tenant_id)
    target = db.scalar(
        select(ReplenishmentTask).where(
            ReplenishmentTask.id == task_id,
            ReplenishmentTask.tenant_id == tenant_id,
        )
    )
    if target is None:
        raise ApiError(404, "Task not found", "The requested task does not exist.")
    if request.status in {
        ReplenishmentTaskStatus.CANCELLED,
        ReplenishmentTaskStatus.EXCEPTION,
    } and not principal.can_access_store(
        Permission.REPLENISHMENT_MANAGE,
        target.store_id,
    ):
        raise ApiError(
            403,
            "Forbidden",
            "Cancelling a task or placing it in exception requires manager permission "
            "for this store.",
            code="task_management_permission_required",
        )
    _ensure_store(
        principal,
        tenant_id,
        Permission.REPLENISHMENT_EXECUTE,
        target.store_id,
    )
    task, sku = update_task(
        db,
        tenant_id,
        task_id,
        request,
        actor_subject=str(principal.user_id),
    )
    return _task_read(task, sku)
