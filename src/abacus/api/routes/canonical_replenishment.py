import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from abacus.api.dependencies import DatabaseSession, SettingsDependency
from abacus.models.architecture import CanonicalTaskStatus, PolicyVersionStatus
from abacus.schemas.canonical_replenishment import (
    PolicyBundlePage,
    PolicyBundleRead,
    PolicyCreate,
    PolicyDefinitionRead,
    PolicyRuleRead,
    PolicyRulesPatch,
    PolicyVersionRead,
    ReplenishmentEvaluationCreate,
    ReplenishmentEvaluationRead,
    ReplenishmentTaskPatch,
    ReplenishmentTaskRead,
)
from abacus.security import Permission, Principal, require_permission
from abacus.services.canonical_replenishment import (
    PolicyBundle,
    activate_policy_version,
    clone_policy_version,
    create_policy,
    evaluate_replenishment,
    get_policy_bundle,
    list_policy_bundles,
    list_store_tasks,
    patch_policy_version,
    patch_replenishment_task,
)

router = APIRouter(prefix="/v1", tags=["5. Replenishment"])

CanManagePolicies = Annotated[
    Principal,
    Depends(require_permission(Permission.POLICY_MANAGE)),
]
CanReadPolicies = Annotated[
    Principal,
    Depends(require_permission(Permission.POLICY_READ)),
]
CanEvaluateReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_MANAGE)),
]
CanReadReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_READ)),
]
CanExecuteReplenishment = Annotated[
    Principal,
    Depends(require_permission(Permission.REPLENISHMENT_EXECUTE)),
]


def _bundle_read(bundle: PolicyBundle) -> PolicyBundleRead:
    return PolicyBundleRead(
        policy=PolicyDefinitionRead.model_validate(bundle.policy),
        version=PolicyVersionRead.model_validate(bundle.version),
        rules=[PolicyRuleRead.model_validate(rule) for rule in bundle.rules],
    )


@router.get(
    "/replenishment-policies",
    response_model=PolicyBundlePage,
    operation_id="listReplenishmentPolicies",
)
def list_policies_endpoint(
    db: DatabaseSession,
    principal: CanReadPolicies,
    version_status: Annotated[PolicyVersionStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> PolicyBundlePage:
    bundles, total = list_policy_bundles(
        db,
        principal,
        limit=limit,
        offset=offset,
        version_status=version_status,
    )
    return PolicyBundlePage(
        items=[_bundle_read(bundle) for bundle in bundles],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/replenishment-policies/{policy_id}",
    response_model=PolicyBundleRead,
    operation_id="getReplenishmentPolicy",
)
def get_policy_endpoint(
    policy_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadPolicies,
    version_status: Annotated[PolicyVersionStatus | None, Query(alias="status")] = None,
) -> PolicyBundleRead:
    return _bundle_read(
        get_policy_bundle(
            db,
            principal,
            policy_id,
            version_status=version_status,
        )
    )


@router.post(
    "/replenishment-policies",
    response_model=PolicyBundleRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createReplenishmentPolicy",
)
def create_policy_endpoint(
    request: PolicyCreate,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyBundleRead:
    return _bundle_read(create_policy(db, principal, request))


@router.post(
    "/replenishment-policies/{policy_id}/versions",
    response_model=PolicyBundleRead,
    status_code=status.HTTP_201_CREATED,
    operation_id="createReplenishmentPolicyVersion",
)
def clone_policy_version_endpoint(
    policy_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyBundleRead:
    return _bundle_read(clone_policy_version(db, principal, policy_id))


@router.patch(
    "/replenishment-policy-versions/{version_id}",
    response_model=PolicyBundleRead,
    operation_id="patchReplenishmentPolicyVersion",
)
def patch_policy_version_endpoint(
    version_id: uuid.UUID,
    request: PolicyRulesPatch,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyBundleRead:
    return _bundle_read(patch_policy_version(db, principal, version_id, request))


@router.post(
    "/replenishment-policy-versions/{version_id}/activate",
    response_model=PolicyBundleRead,
    operation_id="activateReplenishmentPolicyVersion",
)
def activate_policy_version_endpoint(
    version_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanManagePolicies,
) -> PolicyBundleRead:
    return _bundle_read(activate_policy_version(db, principal, version_id))


@router.post(
    "/replenishment/evaluations",
    response_model=ReplenishmentEvaluationRead,
    operation_id="evaluateReplenishment",
)
def evaluate_replenishment_endpoint(
    request: ReplenishmentEvaluationCreate,
    db: DatabaseSession,
    settings: SettingsDependency,
    principal: CanEvaluateReplenishment,
) -> ReplenishmentEvaluationRead:
    result = evaluate_replenishment(
        db,
        principal,
        request,
        settings=settings,
        minimum_confidence=settings.replenishment_minimum_confidence,
    )
    return ReplenishmentEvaluationRead(
        store_id=result.store_id,
        created_count=len(result.tasks),
        suppressed_connectivity=result.suppressed_connectivity,
        suppressed_low_confidence=result.suppressed_low_confidence,
        tasks=[ReplenishmentTaskRead.model_validate(task) for task in result.tasks],
    )


@router.get(
    "/stores/{store_id}/replenishment-tasks",
    response_model=list[ReplenishmentTaskRead],
    operation_id="listReplenishmentTasks",
)
def list_store_tasks_endpoint(
    store_id: uuid.UUID,
    db: DatabaseSession,
    principal: CanReadReplenishment,
    task_status: Annotated[CanonicalTaskStatus | None, Query(alias="status")] = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
) -> list[ReplenishmentTaskRead]:
    return [
        ReplenishmentTaskRead.model_validate(task)
        for task in list_store_tasks(
            db,
            principal,
            store_id,
            status=task_status,
            limit=limit,
        )
    ]


@router.patch(
    "/replenishment-tasks/{task_id}",
    response_model=ReplenishmentTaskRead,
    operation_id="patchReplenishmentTask",
)
def patch_replenishment_task_endpoint(
    task_id: uuid.UUID,
    request: ReplenishmentTaskPatch,
    db: DatabaseSession,
    principal: CanExecuteReplenishment,
) -> ReplenishmentTaskRead:
    return ReplenishmentTaskRead.model_validate(
        patch_replenishment_task(db, principal, task_id, request)
    )
