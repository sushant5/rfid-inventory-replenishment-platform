import hashlib
import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import case, func, or_, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.enums import ZoneKind
from abacus.models.catalog import ProductStyle, Sku
from abacus.models.replenishment import (
    ACTIVE_TASK_STATUSES,
    PolicyImportStatus,
    PolicySelectorType,
    ReplenishmentPolicy,
    ReplenishmentPolicyImport,
    ReplenishmentReason,
    ReplenishmentRun,
    ReplenishmentRunLine,
    ReplenishmentRunStatus,
    ReplenishmentTask,
    ReplenishmentTaskStatus,
    ReplenishmentTrigger,
)
from abacus.models.rfid import InventoryBalance, InventoryChange
from abacus.models.tenancy import Store, Tenant, Zone
from abacus.schemas.replenishment import (
    PolicyBulkUpsertRequest,
    PolicyCreate,
    PolicyDefinition,
    PolicyPatch,
    ReplenishmentEvaluationRequest,
    ReplenishmentTaskUpdate,
)
from abacus.services.cutover import require_reservation_cutover_ready
from abacus.services.locks import lock_replenishment_store_sku

REPLENISHMENT_FORMULA = (
    "if floor >= minimum: 0; otherwise "
    "max(0, min(target - floor - open_task_quantity, "
    "max(0, backroom_quantity - open_task_quantity)))"
)

SELECTOR_SPECIFICITY: dict[PolicySelectorType, int] = {
    PolicySelectorType.SKU: 4,
    PolicySelectorType.STYLE: 3,
    PolicySelectorType.CATEGORY: 2,
    PolicySelectorType.SIZE: 1,
}

TASK_TRANSITIONS: dict[ReplenishmentTaskStatus, frozenset[ReplenishmentTaskStatus]] = {
    ReplenishmentTaskStatus.OPEN: frozenset(
        {
            ReplenishmentTaskStatus.CLAIMED,
            ReplenishmentTaskStatus.CANCELLED,
            ReplenishmentTaskStatus.EXCEPTION,
        }
    ),
    ReplenishmentTaskStatus.CLAIMED: frozenset(
        {
            ReplenishmentTaskStatus.OPEN,
            ReplenishmentTaskStatus.IN_PROGRESS,
            ReplenishmentTaskStatus.CANCELLED,
            ReplenishmentTaskStatus.EXCEPTION,
        }
    ),
    ReplenishmentTaskStatus.IN_PROGRESS: frozenset(
        {
            ReplenishmentTaskStatus.AWAITING_VERIFICATION,
            ReplenishmentTaskStatus.CANCELLED,
            ReplenishmentTaskStatus.EXCEPTION,
        }
    ),
    ReplenishmentTaskStatus.AWAITING_VERIFICATION: frozenset(
        {
            ReplenishmentTaskStatus.IN_PROGRESS,
            ReplenishmentTaskStatus.VERIFIED,
            ReplenishmentTaskStatus.EXCEPTION,
        }
    ),
    ReplenishmentTaskStatus.EXCEPTION: frozenset(),
    ReplenishmentTaskStatus.VERIFIED: frozenset(),
    ReplenishmentTaskStatus.CANCELLED: frozenset(),
}

OWNED_TASK_STATUSES = frozenset(
    {
        ReplenishmentTaskStatus.CLAIMED,
        ReplenishmentTaskStatus.IN_PROGRESS,
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
    }
)
MANAGED_OUTCOME_STATUSES = frozenset(
    {
        ReplenishmentTaskStatus.CANCELLED,
        ReplenishmentTaskStatus.EXCEPTION,
    }
)
TERMINAL_TASK_STATUSES = frozenset(
    {
        ReplenishmentTaskStatus.VERIFIED,
        ReplenishmentTaskStatus.CANCELLED,
        ReplenishmentTaskStatus.EXCEPTION,
    }
)


class PolicyResolutionConflictError(Exception):
    def __init__(self, policy_ids: list[uuid.UUID]) -> None:
        super().__init__("multiple replenishment policies have the same winning precedence")
        self.policy_ids = policy_ids


@dataclass(frozen=True, slots=True)
class QuantityDecision:
    quantity: int
    reason: ReplenishmentReason


@dataclass(frozen=True, slots=True)
class RankedPolicy:
    policy: ReplenishmentPolicy
    store_specificity: int
    selector_specificity: int

    @property
    def rank(self) -> tuple[int, int, int]:
        return (self.store_specificity, self.selector_specificity, self.policy.priority)


@dataclass(frozen=True, slots=True)
class PolicyWindow:
    external_key: str
    store_id: uuid.UUID | None
    selector_type: PolicySelectorType
    selector_value: str
    priority: int
    effective_from: datetime
    effective_to: datetime | None
    active: bool
    input_index: int | None


@dataclass(frozen=True, slots=True)
class CatalogReferenceState:
    store_ids: frozenset[uuid.UUID]
    sku_codes: frozenset[str]
    style_codes: frozenset[str]
    categories: frozenset[str]
    sizes: frozenset[str]


def calculate_replenishment_quantity(
    *,
    floor_quantity: int,
    minimum_floor_quantity: int,
    target_floor_quantity: int,
    open_task_quantity: int,
    available_backroom: int,
) -> QuantityDecision:
    quantities = {
        "floor_quantity": floor_quantity,
        "minimum_floor_quantity": minimum_floor_quantity,
        "target_floor_quantity": target_floor_quantity,
        "open_task_quantity": open_task_quantity,
        "available_backroom": available_backroom,
    }
    if any(value < 0 for value in quantities.values()):
        raise ValueError("replenishment quantities cannot be negative")
    if target_floor_quantity < minimum_floor_quantity:
        raise ValueError("target_floor_quantity cannot be below minimum_floor_quantity")
    if floor_quantity >= minimum_floor_quantity:
        return QuantityDecision(0, ReplenishmentReason.FLOOR_AT_OR_ABOVE_MINIMUM)

    need = target_floor_quantity - floor_quantity - open_task_quantity
    if need <= 0:
        return QuantityDecision(0, ReplenishmentReason.OPEN_TASK_COVERS_NEED)
    if available_backroom == 0:
        return QuantityDecision(0, ReplenishmentReason.NO_BACKROOM_STOCK)
    return QuantityDecision(
        min(need, available_backroom),
        ReplenishmentReason.REPLENISHMENT_REQUIRED,
    )


def effective_intervals_overlap(
    left_from: datetime,
    left_to: datetime | None,
    right_from: datetime,
    right_to: datetime | None,
) -> bool:
    return (left_to is None or right_from < left_to) and (right_to is None or left_from < right_to)


def select_policy_winner(candidates: list[RankedPolicy]) -> ReplenishmentPolicy | None:
    if not candidates:
        return None
    winning_rank = max(candidate.rank for candidate in candidates)
    winners = [candidate.policy for candidate in candidates if candidate.rank == winning_rank]
    if len(winners) > 1:
        raise PolicyResolutionConflictError([policy.id for policy in winners])
    return winners[0]


def task_transition_allowed(
    current: ReplenishmentTaskStatus,
    requested: ReplenishmentTaskStatus,
) -> bool:
    return current == requested or requested in TASK_TRANSITIONS[current]


def task_movement_allowed(
    current: ReplenishmentTaskStatus,
    requested: ReplenishmentTaskStatus,
) -> bool:
    """Return whether one update may record physical movement.

    A claimant may record movement while starting, continuing, or returning to
    IN_PROGRESS. The full quantity must already be recorded before the task enters
    AWAITING_VERIFICATION, and VERIFIED only confirms that previously recorded work.
    """

    return requested is ReplenishmentTaskStatus.IN_PROGRESS and current in {
        ReplenishmentTaskStatus.IN_PROGRESS,
        ReplenishmentTaskStatus.AWAITING_VERIFICATION,
    }


def _get_tenant(db: Session, tenant_id: uuid.UUID) -> Tenant:
    tenant = db.get(Tenant, tenant_id)
    if tenant is None:
        raise ApiError(404, "Tenant not found", "The requested tenant does not exist.")
    return tenant


def _get_store(db: Session, tenant_id: uuid.UUID, store_id: uuid.UUID) -> Store:
    store = db.scalar(select(Store).where(Store.id == store_id, Store.tenant_id == tenant_id))
    if store is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    return store


def _get_sku(db: Session, tenant_id: uuid.UUID, sku_id: uuid.UUID) -> Sku:
    sku = db.scalar(select(Sku).where(Sku.id == sku_id, Sku.tenant_id == tenant_id))
    if sku is None:
        raise ApiError(404, "SKU not found", "The requested SKU does not exist.")
    return sku


def _get_policy(
    db: Session,
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
    *,
    lock: bool = False,
) -> ReplenishmentPolicy:
    statement = select(ReplenishmentPolicy).where(
        ReplenishmentPolicy.id == policy_id,
        ReplenishmentPolicy.tenant_id == tenant_id,
    )
    if lock:
        statement = statement.with_for_update()
    policy = db.scalar(statement)
    if policy is None:
        raise ApiError(404, "Policy not found", "The requested policy does not exist.")
    return policy


def _lock_policy_writes(db: Session, tenant_id: uuid.UUID) -> None:
    """Serialize validation plus policy mutation for one tenant."""

    db.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"replenishment-policies:{tenant_id}"},
    )


def _lock_policy_snapshot(db: Session, tenant_id: uuid.UUID) -> None:
    """Let evaluations run together while excluding policy writes for the tenant."""

    db.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:lock_key, 0))"),
        {"lock_key": f"replenishment-policies:{tenant_id}"},
    )


def _normalized_attribute(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().split()).upper()
    return normalized or None


def _category_for(sku: Sku, style: ProductStyle) -> str | None:
    sku_category = _normalized_attribute(sku.attributes.get("category"))
    if sku_category is not None:
        return sku_category
    return _normalized_attribute(style.attributes.get("category"))


def _matches_selector(
    policy: ReplenishmentPolicy,
    sku: Sku,
    style: ProductStyle,
) -> bool:
    if policy.selector_type is PolicySelectorType.SKU:
        return policy.selector_value == sku.code.upper()
    if policy.selector_type is PolicySelectorType.STYLE:
        return policy.selector_value == style.code.upper()
    if policy.selector_type is PolicySelectorType.CATEGORY:
        return policy.selector_value == _category_for(sku, style)
    if policy.selector_type is PolicySelectorType.SIZE:
        return policy.selector_value == _normalized_attribute(sku.size)
    return False


def resolve_policy(
    db: Session,
    *,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku: Sku,
    evaluated_at: datetime,
) -> ReplenishmentPolicy | None:
    style = db.scalar(
        select(ProductStyle).where(
            ProductStyle.id == sku.product_style_id,
            ProductStyle.tenant_id == tenant_id,
        )
    )
    if style is None:
        raise ApiError(
            409,
            "Catalog integrity error",
            "The SKU's product style is missing or belongs to another tenant.",
            code="invalid_sku_style",
        )

    policies = list(
        db.scalars(
            select(ReplenishmentPolicy).where(
                ReplenishmentPolicy.tenant_id == tenant_id,
                ReplenishmentPolicy.active.is_(True),
                ReplenishmentPolicy.effective_from <= evaluated_at,
                or_(
                    ReplenishmentPolicy.effective_to.is_(None),
                    ReplenishmentPolicy.effective_to > evaluated_at,
                ),
                or_(
                    ReplenishmentPolicy.store_id.is_(None),
                    ReplenishmentPolicy.store_id == store_id,
                ),
            )
        ).all()
    )
    ranked = [
        RankedPolicy(
            policy=policy,
            store_specificity=1 if policy.store_id == store_id else 0,
            selector_specificity=SELECTOR_SPECIFICITY[policy.selector_type],
        )
        for policy in policies
        if _matches_selector(policy, sku, style)
    ]
    try:
        return select_policy_winner(ranked)
    except PolicyResolutionConflictError as exc:
        raise ApiError(
            409,
            "Policy resolution conflict",
            "More than one policy has the same winning scope, selector specificity, and priority.",
            code="replenishment_policy_tie",
            errors=[{"policy_ids": [str(policy_id) for policy_id in exc.policy_ids]}],
        ) from exc


def _definition_from_model(policy: ReplenishmentPolicy) -> PolicyDefinition:
    return PolicyDefinition(
        external_key=policy.external_key,
        store_id=policy.store_id,
        selector_type=policy.selector_type,
        selector_value=policy.selector_value,
        minimum_floor_quantity=policy.minimum_floor_quantity,
        target_floor_quantity=policy.target_floor_quantity,
        maximum_floor_quantity=policy.maximum_floor_quantity,
        priority=policy.priority,
        effective_from=policy.effective_from,
        effective_to=policy.effective_to,
        active=policy.active,
    )


def _window_from_definition(
    definition: PolicyDefinition,
    *,
    input_index: int | None,
) -> PolicyWindow:
    return PolicyWindow(
        external_key=definition.external_key,
        store_id=definition.store_id,
        selector_type=definition.selector_type,
        selector_value=definition.selector_value,
        priority=definition.priority,
        effective_from=definition.effective_from,
        effective_to=definition.effective_to,
        active=definition.active,
        input_index=input_index,
    )


def _windows_are_ambiguous(left: PolicyWindow, right: PolicyWindow) -> bool:
    return (
        left.active
        and right.active
        and left.store_id == right.store_id
        and left.selector_type is right.selector_type
        and left.selector_value == right.selector_value
        and left.priority == right.priority
        and effective_intervals_overlap(
            left.effective_from,
            left.effective_to,
            right.effective_from,
            right.effective_to,
        )
    )


def _catalog_reference_state(db: Session, tenant_id: uuid.UUID) -> CatalogReferenceState:
    stores = frozenset(db.scalars(select(Store.id).where(Store.tenant_id == tenant_id)).all())
    rows = list(
        db.execute(
            select(Sku, ProductStyle)
            .join(ProductStyle, ProductStyle.id == Sku.product_style_id)
            .where(
                Sku.tenant_id == tenant_id,
                Sku.active.is_(True),
                ProductStyle.tenant_id == tenant_id,
                ProductStyle.active.is_(True),
            )
        )
        .tuples()
        .all()
    )
    return CatalogReferenceState(
        store_ids=stores,
        sku_codes=frozenset(sku.code.upper() for sku, _ in rows),
        style_codes=frozenset(style.code.upper() for _, style in rows),
        categories=frozenset(
            category for sku, style in rows if (category := _category_for(sku, style)) is not None
        ),
        sizes=frozenset(
            size for sku, _ in rows if (size := _normalized_attribute(sku.size)) is not None
        ),
    )


def _reference_issue(
    definition: PolicyDefinition,
    references: CatalogReferenceState,
) -> tuple[str, str] | None:
    if definition.store_id is not None and definition.store_id not in references.store_ids:
        return ("unknown_store", "store_id does not belong to this tenant")
    values_by_type = {
        PolicySelectorType.SKU: references.sku_codes,
        PolicySelectorType.STYLE: references.style_codes,
        PolicySelectorType.CATEGORY: references.categories,
        PolicySelectorType.SIZE: references.sizes,
    }
    if definition.selector_value not in values_by_type[definition.selector_type]:
        return (
            "unknown_selector",
            f"No active {definition.selector_type.value.lower()} matches "
            f"'{definition.selector_value}' in this tenant's catalog",
        )
    return None


def _find_ambiguity(
    db: Session,
    tenant_id: uuid.UUID,
    definition: PolicyDefinition,
    *,
    exclude_policy_id: uuid.UUID | None = None,
) -> ReplenishmentPolicy | None:
    candidate = _window_from_definition(definition, input_index=None)
    policies = list(
        db.scalars(
            select(ReplenishmentPolicy).where(
                ReplenishmentPolicy.tenant_id == tenant_id,
                ReplenishmentPolicy.active.is_(True),
            )
        ).all()
    )
    for policy in policies:
        if exclude_policy_id is not None and policy.id == exclude_policy_id:
            continue
        existing = _window_from_definition(_definition_from_model(policy), input_index=None)
        if _windows_are_ambiguous(candidate, existing):
            return policy
    return None


def _raise_reference_or_ambiguity_error(
    db: Session,
    tenant_id: uuid.UUID,
    definition: PolicyDefinition,
    *,
    exclude_policy_id: uuid.UUID | None = None,
) -> None:
    references = _catalog_reference_state(db, tenant_id)
    issue = _reference_issue(definition, references)
    if issue is not None:
        code, detail = issue
        raise ApiError(422, "Invalid policy reference", detail, code=code)
    conflict = _find_ambiguity(
        db,
        tenant_id,
        definition,
        exclude_policy_id=exclude_policy_id,
    )
    if conflict is not None:
        raise ApiError(
            409,
            "Ambiguous policy interval",
            "The policy overlaps another policy with the same resolution precedence.",
            code="ambiguous_policy_interval",
            errors=[{"conflicting_policy_id": str(conflict.id)}],
        )


def _new_policy(tenant_id: uuid.UUID, definition: PolicyDefinition) -> ReplenishmentPolicy:
    return ReplenishmentPolicy(
        tenant_id=tenant_id,
        external_key=definition.external_key,
        store_id=definition.store_id,
        selector_type=definition.selector_type,
        selector_value=definition.selector_value,
        minimum_floor_quantity=definition.minimum_floor_quantity,
        target_floor_quantity=definition.target_floor_quantity,
        maximum_floor_quantity=definition.maximum_floor_quantity,
        priority=definition.priority,
        effective_from=definition.effective_from,
        effective_to=definition.effective_to,
        active=definition.active,
        revision=1,
    )


def _apply_definition(policy: ReplenishmentPolicy, definition: PolicyDefinition) -> None:
    policy.store_id = definition.store_id
    policy.selector_type = definition.selector_type
    policy.selector_value = definition.selector_value
    policy.minimum_floor_quantity = definition.minimum_floor_quantity
    policy.target_floor_quantity = definition.target_floor_quantity
    policy.maximum_floor_quantity = definition.maximum_floor_quantity
    policy.priority = definition.priority
    policy.effective_from = definition.effective_from
    policy.effective_to = definition.effective_to
    policy.active = definition.active


def _definitions_equal(left: PolicyDefinition, right: PolicyDefinition) -> bool:
    return left.model_dump(mode="python") == right.model_dump(mode="python")


def create_policy(
    db: Session,
    tenant_id: uuid.UUID,
    request: PolicyCreate,
) -> ReplenishmentPolicy:
    _get_tenant(db, tenant_id)
    _lock_policy_writes(db, tenant_id)
    existing = db.scalar(
        select(ReplenishmentPolicy).where(
            ReplenishmentPolicy.tenant_id == tenant_id,
            ReplenishmentPolicy.external_key == request.external_key,
        )
    )
    if existing is not None:
        raise ApiError(
            409,
            "Policy key conflict",
            f"Policy external_key '{request.external_key}' already exists.",
            code="policy_external_key_conflict",
        )
    definition = PolicyDefinition.model_validate(request.model_dump(mode="python"))
    _raise_reference_or_ambiguity_error(db, tenant_id, definition)
    policy = _new_policy(tenant_id, definition)
    db.add(policy)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Policy creation conflict",
            "The policy conflicted with a concurrent change.",
            code="concurrent_policy_conflict",
        ) from exc
    db.refresh(policy)
    return policy


def get_policy(
    db: Session,
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
) -> ReplenishmentPolicy:
    _get_tenant(db, tenant_id)
    return _get_policy(db, tenant_id, policy_id)


def list_policies(
    db: Session,
    tenant_id: uuid.UUID,
    *,
    store_id: uuid.UUID | None,
    selector_type: PolicySelectorType | None,
    active: bool | None,
    limit: int,
    offset: int,
) -> tuple[list[ReplenishmentPolicy], int]:
    _get_tenant(db, tenant_id)
    predicates: list[Any] = [ReplenishmentPolicy.tenant_id == tenant_id]
    if store_id is not None:
        predicates.append(
            or_(
                ReplenishmentPolicy.store_id.is_(None),
                ReplenishmentPolicy.store_id == store_id,
            )
        )
    if selector_type is not None:
        predicates.append(ReplenishmentPolicy.selector_type == selector_type)
    if active is not None:
        predicates.append(ReplenishmentPolicy.active.is_(active))
    total = db.scalar(select(func.count()).select_from(ReplenishmentPolicy).where(*predicates))
    policies = list(
        db.scalars(
            select(ReplenishmentPolicy)
            .where(*predicates)
            .order_by(
                ReplenishmentPolicy.external_key.asc(),
                ReplenishmentPolicy.id.asc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return policies, int(total or 0)


def update_policy(
    db: Session,
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
    request: PolicyPatch,
) -> ReplenishmentPolicy:
    _get_tenant(db, tenant_id)
    _lock_policy_writes(db, tenant_id)
    policy = _get_policy(db, tenant_id, policy_id, lock=True)
    current = _definition_from_model(policy)
    merged = {
        **current.model_dump(mode="python"),
        **request.model_dump(mode="python", exclude_unset=True),
    }
    definition = PolicyDefinition.model_validate(merged)
    if _definitions_equal(current, definition):
        return policy
    _raise_reference_or_ambiguity_error(
        db,
        tenant_id,
        definition,
        exclude_policy_id=policy.id,
    )
    _apply_definition(policy, definition)
    policy.revision += 1
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Policy update conflict",
            "The policy conflicted with a concurrent change.",
            code="concurrent_policy_conflict",
        ) from exc
    db.refresh(policy)
    return policy


def deactivate_policy(
    db: Session,
    tenant_id: uuid.UUID,
    policy_id: uuid.UUID,
) -> None:
    _get_tenant(db, tenant_id)
    _lock_policy_writes(db, tenant_id)
    policy = _get_policy(db, tenant_id, policy_id, lock=True)
    if not policy.active:
        return
    policy.active = False
    policy.revision += 1
    db.commit()


def _policy_request_hash(request: PolicyBulkUpsertRequest) -> str:
    canonical = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def _bulk_validation_errors(
    db: Session,
    tenant_id: uuid.UUID,
    request: PolicyBulkUpsertRequest,
    existing_by_key: dict[str, ReplenishmentPolicy],
) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    references = _catalog_reference_state(db, tenant_id)
    for index, definition in enumerate(request.policies):
        issue = _reference_issue(definition, references)
        if issue is not None:
            code, message = issue
            errors.append(
                {
                    "index": index,
                    "external_key": definition.external_key,
                    "code": code,
                    "message": message,
                }
            )

    requested_keys = {definition.external_key for definition in request.policies}
    windows = [
        _window_from_definition(_definition_from_model(policy), input_index=None)
        for external_key, policy in existing_by_key.items()
        if external_key not in requested_keys
    ]
    windows.extend(
        _window_from_definition(definition, input_index=index)
        for index, definition in enumerate(request.policies)
    )
    reported: set[tuple[int, str]] = set()
    for left_index, left in enumerate(windows):
        for right in windows[left_index + 1 :]:
            if not _windows_are_ambiguous(left, right):
                continue
            for candidate, other in ((left, right), (right, left)):
                if candidate.input_index is None:
                    continue
                marker = (candidate.input_index, other.external_key)
                if marker in reported:
                    continue
                reported.add(marker)
                errors.append(
                    {
                        "index": candidate.input_index,
                        "external_key": candidate.external_key,
                        "code": "ambiguous_policy_interval",
                        "message": (
                            "Policy overlaps another policy with the same resolution "
                            f"precedence: {other.external_key}"
                        ),
                    }
                )
    return errors


def bulk_upsert_policies(
    db: Session,
    tenant_id: uuid.UUID,
    idempotency_key: str,
    request: PolicyBulkUpsertRequest,
) -> ReplenishmentPolicyImport:
    _get_tenant(db, tenant_id)
    _lock_policy_writes(db, tenant_id)
    normalized_key = idempotency_key.strip()
    if len(normalized_key) < 8 or len(normalized_key) > 128:
        raise ApiError(
            400,
            "Invalid idempotency key",
            "Idempotency-Key must contain 8 to 128 non-blank characters.",
            code="invalid_idempotency_key",
        )
    digest = _policy_request_hash(request)
    existing_import = db.scalar(
        select(ReplenishmentPolicyImport).where(
            ReplenishmentPolicyImport.tenant_id == tenant_id,
            ReplenishmentPolicyImport.idempotency_key == normalized_key,
        )
    )
    if existing_import is not None:
        if existing_import.request_hash == digest:
            return existing_import
        raise ApiError(
            409,
            "Idempotency conflict",
            "This Idempotency-Key was already used with different policy content.",
            code="idempotency_key_reused",
        )

    existing_by_key = {
        policy.external_key: policy
        for policy in db.scalars(
            select(ReplenishmentPolicy).where(ReplenishmentPolicy.tenant_id == tenant_id)
        ).all()
    }
    errors = _bulk_validation_errors(db, tenant_id, request, existing_by_key)
    policy_import = ReplenishmentPolicyImport(
        tenant_id=tenant_id,
        idempotency_key=normalized_key,
        request_hash=digest,
        status=PolicyImportStatus.REJECTED if errors else PolicyImportStatus.COMPLETED,
        total_count=len(request.policies),
        created_count=0,
        updated_count=0,
        unchanged_count=0,
        rejected_count=len({int(error["index"]) for error in errors}),
        reconciliation={},
        errors=errors,
    )
    db.add(policy_import)

    if not errors:
        for definition in request.policies:
            existing = existing_by_key.get(definition.external_key)
            if existing is None:
                db.add(_new_policy(tenant_id, definition))
                policy_import.created_count += 1
                continue
            current = _definition_from_model(existing)
            if _definitions_equal(current, definition):
                policy_import.unchanged_count += 1
                continue
            _apply_definition(existing, definition)
            existing.revision += 1
            policy_import.updated_count += 1

    policy_import.reconciliation = {
        "received": policy_import.total_count,
        "created": policy_import.created_count,
        "updated": policy_import.updated_count,
        "unchanged": policy_import.unchanged_count,
        "rejected": policy_import.rejected_count,
        "atomic": True,
    }
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        winner = db.scalar(
            select(ReplenishmentPolicyImport).where(
                ReplenishmentPolicyImport.tenant_id == tenant_id,
                ReplenishmentPolicyImport.idempotency_key == normalized_key,
            )
        )
        if winner is not None and winner.request_hash == digest:
            return winner
        if winner is not None:
            raise ApiError(
                409,
                "Idempotency conflict",
                "This Idempotency-Key was concurrently used with different policy content.",
                code="idempotency_key_reused",
            ) from exc
        raise ApiError(
            409,
            "Policy import conflict",
            "The policy import conflicted with a concurrent catalog or policy change.",
            code="concurrent_policy_conflict",
        ) from exc
    db.refresh(policy_import)
    return policy_import


def get_policy_import(
    db: Session,
    tenant_id: uuid.UUID,
    import_id: uuid.UUID,
) -> ReplenishmentPolicyImport:
    _get_tenant(db, tenant_id)
    policy_import = db.scalar(
        select(ReplenishmentPolicyImport).where(
            ReplenishmentPolicyImport.id == import_id,
            ReplenishmentPolicyImport.tenant_id == tenant_id,
        )
    )
    if policy_import is None:
        raise ApiError(
            404,
            "Policy import not found",
            "The requested policy import does not exist.",
        )
    return policy_import


def _inventory_quantities(
    db: Session,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> tuple[int, int, datetime | None]:
    row = db.execute(
        select(
            func.coalesce(
                func.sum(
                    case(
                        (Zone.kind == ZoneKind.SALES_FLOOR, InventoryBalance.quantity),
                        else_=0,
                    )
                ),
                0,
            ),
            func.coalesce(
                func.sum(
                    case(
                        (Zone.kind == ZoneKind.BACKROOM, InventoryBalance.quantity),
                        else_=0,
                    )
                ),
                0,
            ),
            # Only a quantity transition can show that verified physical work has
            # reached the RFID aggregate. A same-location read refreshes evidence
            # and ``updated_at`` but must not release the verified-work reservation.
            func.max(InventoryBalance.quantity_changed_at),
        )
        .select_from(InventoryBalance)
        .join(Zone, Zone.id == InventoryBalance.zone_id)
        .where(
            InventoryBalance.tenant_id == tenant_id,
            InventoryBalance.store_id == store_id,
            InventoryBalance.sku_id == sku_id,
            Zone.tenant_id == tenant_id,
            Zone.store_id == store_id,
        )
    ).one()
    return int(row[0] or 0), int(row[1] or 0), row[2]


def _active_task(
    db: Session,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> ReplenishmentTask | None:
    return db.scalar(
        select(ReplenishmentTask)
        .where(
            ReplenishmentTask.tenant_id == tenant_id,
            ReplenishmentTask.store_id == store_id,
            ReplenishmentTask.sku_id == sku_id,
            ReplenishmentTask.status.in_(ACTIVE_TASK_STATUSES),
        )
        .with_for_update()
    )


def _unreflected_terminal_moved_quantity(
    db: Session,
    tenant_id: uuid.UUID,
    store_id: uuid.UUID,
    sku_id: uuid.UUID,
) -> int:
    """Return terminal moved units not yet matched to a confirmed RFID move.

    VERIFIED work is fully moved. CANCELLED/EXCEPTION work may be partially moved
    before a manager closes the remainder. Each confirmed same-store
    BACKROOM-to-SALES_FLOOR inventory change can consume at most one reserved unit.
    The durable foreign-key allocation avoids releasing all units on one transition.
    """

    remaining_by_task = (
        select(
            func.greatest(
                ReplenishmentTask.moved_quantity
                - ReplenishmentTask.reconciled_before_tracking_quantity
                - func.count(InventoryChange.id),
                0,
            ).label("remaining_quantity")
        )
        .select_from(ReplenishmentTask)
        .outerjoin(
            InventoryChange,
            InventoryChange.replenishment_task_id == ReplenishmentTask.id,
        )
        .where(
            ReplenishmentTask.tenant_id == tenant_id,
            ReplenishmentTask.store_id == store_id,
            ReplenishmentTask.sku_id == sku_id,
            ReplenishmentTask.status.in_(TERMINAL_TASK_STATUSES),
            ReplenishmentTask.completed_at.is_not(None),
            ReplenishmentTask.moved_quantity > 0,
        )
        .group_by(ReplenishmentTask.id)
        .subquery()
    )
    quantity = db.scalar(select(func.coalesce(func.sum(remaining_by_task.c.remaining_quantity), 0)))
    return int(quantity or 0)


def _linked_task_movement_count(db: Session, task_id: uuid.UUID) -> int:
    quantity = db.scalar(
        select(func.count(InventoryChange.id)).where(
            InventoryChange.replenishment_task_id == task_id
        )
    )
    return int(quantity or 0)


def _unreflected_active_task_quantity(db: Session, task: ReplenishmentTask | None) -> int:
    if task is None:
        return 0
    return max(
        0,
        task.quantity
        - task.reconciled_before_tracking_quantity
        - _linked_task_movement_count(db, task.id),
    )


def _cancel_unstarted_task_if_obsolete(
    task: ReplenishmentTask | None,
    reason: ReplenishmentReason,
) -> bool:
    obsolete_reasons = {
        ReplenishmentReason.NO_MATCHING_POLICY,
        ReplenishmentReason.FLOOR_AT_OR_ABOVE_MINIMUM,
    }
    if (
        task is None
        or task.status is not ReplenishmentTaskStatus.OPEN
        or task.moved_quantity != 0
        or reason not in obsolete_reasons
    ):
        return False
    task.status = ReplenishmentTaskStatus.CANCELLED
    task.completed_at = datetime.now(UTC)
    task.last_note = f"Automatically cancelled after reevaluation: {reason.value}"
    task.version += 1
    return True


def _evaluate_sku(
    db: Session,
    *,
    run: ReplenishmentRun,
    sku: Sku,
    generate_tasks: bool,
) -> tuple[ReplenishmentRunLine, bool, bool]:
    lock_replenishment_store_sku(db, run.tenant_id, run.store_id, sku.id)
    task = _active_task(db, run.tenant_id, run.store_id, sku.id)
    floor_quantity, backroom_quantity, inventory_as_of = _inventory_quantities(
        db,
        run.tenant_id,
        run.store_id,
        sku.id,
    )
    # Active and terminal work reserves only units not yet FIFO-linked to confirmed
    # backroom-to-floor EPC transitions. This keeps the RFID projection plus work
    # reservation from counting the same physical unit twice.
    open_task_quantity = _unreflected_active_task_quantity(db, task)
    open_task_quantity += _unreflected_terminal_moved_quantity(
        db,
        run.tenant_id,
        run.store_id,
        sku.id,
    )
    policy = resolve_policy(
        db,
        tenant_id=run.tenant_id,
        store_id=run.store_id,
        sku=sku,
        evaluated_at=run.evaluated_at,
    )
    if policy is None:
        line = ReplenishmentRunLine(
            tenant_id=run.tenant_id,
            run_id=run.id,
            store_id=run.store_id,
            sku_id=sku.id,
            floor_quantity=floor_quantity,
            backroom_quantity=backroom_quantity,
            open_task_quantity=open_task_quantity,
            recommended_quantity=0,
            reason=ReplenishmentReason.NO_MATCHING_POLICY,
            formula="No calculation was performed because no effective policy matched.",
            inventory_as_of=inventory_as_of,
        )
        db.add(line)
        if task is not None:
            line.task_id = task.id
        return (
            line,
            False,
            (
                generate_tasks
                and _cancel_unstarted_task_if_obsolete(
                    task,
                    ReplenishmentReason.NO_MATCHING_POLICY,
                )
            ),
        )

    decision = calculate_replenishment_quantity(
        floor_quantity=floor_quantity,
        minimum_floor_quantity=policy.minimum_floor_quantity,
        target_floor_quantity=policy.target_floor_quantity,
        open_task_quantity=open_task_quantity,
        available_backroom=max(0, backroom_quantity - open_task_quantity),
    )
    line = ReplenishmentRunLine(
        tenant_id=run.tenant_id,
        run_id=run.id,
        store_id=run.store_id,
        sku_id=sku.id,
        policy_id=policy.id,
        selector_type=policy.selector_type,
        selector_value=policy.selector_value,
        policy_priority=policy.priority,
        minimum_floor_quantity=policy.minimum_floor_quantity,
        target_floor_quantity=policy.target_floor_quantity,
        maximum_floor_quantity=policy.maximum_floor_quantity,
        floor_quantity=floor_quantity,
        backroom_quantity=backroom_quantity,
        open_task_quantity=open_task_quantity,
        recommended_quantity=decision.quantity,
        reason=decision.reason,
        formula=REPLENISHMENT_FORMULA,
        inventory_as_of=inventory_as_of,
    )
    db.add(line)
    db.flush()
    if not generate_tasks or decision.quantity == 0:
        if task is not None:
            line.task_id = task.id
        updated = (
            generate_tasks
            and decision.quantity == 0
            and _cancel_unstarted_task_if_obsolete(task, decision.reason)
        )
        return line, False, updated

    if task is None:
        task = ReplenishmentTask(
            tenant_id=run.tenant_id,
            store_id=run.store_id,
            sku_id=sku.id,
            source_policy_id=policy.id,
            status=ReplenishmentTaskStatus.OPEN,
            quantity=decision.quantity,
            moved_quantity=0,
            version=1,
        )
        db.add(task)
        db.flush()
        line.task_id = task.id
        return line, True, False

    task.quantity += decision.quantity
    task.source_policy_id = policy.id
    task.version += 1
    line.task_id = task.id
    return line, False, True


def _evaluation_hash(
    tenant_id: uuid.UUID,
    request: ReplenishmentEvaluationRequest,
) -> str:
    canonical = json.dumps(
        {
            "tenant_id": str(tenant_id),
            "store_id": str(request.store_id),
            "sku_ids": sorted(str(sku_id) for sku_id in request.sku_ids),
            "generate_tasks": request.generate_tasks,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(canonical).hexdigest()


def create_replenishment_run(
    db: Session,
    tenant_id: uuid.UUID,
    request: ReplenishmentEvaluationRequest,
    *,
    trigger: ReplenishmentTrigger,
    requested_by_subject: str | None,
    idempotency_key: str | None = None,
    evaluated_at: datetime | None = None,
) -> ReplenishmentRun:
    require_reservation_cutover_ready(db)
    _get_tenant(db, tenant_id)
    _get_store(db, tenant_id, request.store_id)
    normalized_key = idempotency_key.strip() if idempotency_key is not None else None
    if normalized_key is not None and not 8 <= len(normalized_key) <= 128:
        raise ApiError(
            400,
            "Invalid idempotency key",
            "Idempotency-Key must contain 8 to 128 non-blank characters.",
            code="invalid_idempotency_key",
        )
    digest = _evaluation_hash(tenant_id, request)
    if normalized_key is not None:
        existing = db.scalar(
            select(ReplenishmentRun).where(
                ReplenishmentRun.tenant_id == tenant_id,
                ReplenishmentRun.idempotency_key == normalized_key,
            )
        )
        if existing is not None:
            if existing.request_hash == digest:
                return existing
            raise ApiError(
                409,
                "Idempotency conflict",
                "This Idempotency-Key was already used for another evaluation request.",
                code="idempotency_key_reused",
            )

    _lock_policy_snapshot(db, tenant_id)

    skus_by_id = {
        sku.id: sku
        for sku in db.scalars(
            select(Sku).where(
                Sku.tenant_id == tenant_id,
                Sku.id.in_(request.sku_ids),
                Sku.active.is_(True),
            )
        ).all()
    }
    missing = sorted(str(sku_id) for sku_id in request.sku_ids if sku_id not in skus_by_id)
    if missing:
        raise ApiError(
            422,
            "Invalid SKU selection",
            "One or more SKUs are missing, inactive, or belong to another tenant.",
            code="invalid_sku_selection",
            errors=[{"sku_ids": missing}],
        )

    timestamp = evaluated_at or datetime.now(UTC)
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("evaluated_at must be timezone-aware")
    run = ReplenishmentRun(
        tenant_id=tenant_id,
        store_id=request.store_id,
        idempotency_key=normalized_key,
        request_hash=digest,
        trigger=trigger,
        status=ReplenishmentRunStatus.PROCESSING,
        evaluated_at=timestamp,
        requested_by_subject=requested_by_subject,
        line_count=0,
        tasks_created=0,
        tasks_updated=0,
    )
    db.add(run)
    db.flush()
    for sku_id in sorted(request.sku_ids, key=str):
        _, created, updated = _evaluate_sku(
            db,
            run=run,
            sku=skus_by_id[sku_id],
            generate_tasks=request.generate_tasks,
        )
        run.line_count += 1
        run.tasks_created += int(created)
        run.tasks_updated += int(updated)
    run.status = ReplenishmentRunStatus.COMPLETED
    db.flush()
    return run


def get_replenishment_run(
    db: Session,
    tenant_id: uuid.UUID,
    run_id: uuid.UUID,
) -> tuple[ReplenishmentRun, list[tuple[ReplenishmentRunLine, Sku]]]:
    _get_tenant(db, tenant_id)
    run = db.scalar(
        select(ReplenishmentRun).where(
            ReplenishmentRun.id == run_id,
            ReplenishmentRun.tenant_id == tenant_id,
        )
    )
    if run is None:
        raise ApiError(404, "Evaluation not found", "The requested evaluation does not exist.")
    lines = list(
        db.execute(
            select(ReplenishmentRunLine, Sku)
            .join(Sku, Sku.id == ReplenishmentRunLine.sku_id)
            .where(
                ReplenishmentRunLine.tenant_id == tenant_id,
                ReplenishmentRunLine.run_id == run.id,
            )
            .order_by(Sku.code.asc(), ReplenishmentRunLine.id.asc())
        )
        .tuples()
        .all()
    )
    return run, lines


def process_replenishment_job(db: Session, payload: dict[str, object]) -> None:
    try:
        store_id = uuid.UUID(str(payload["store_id"]))
        sku_id = uuid.UUID(str(payload["sku_id"]))
    except (KeyError, ValueError) as exc:
        raise ValueError("replenishment job requires valid store_id and sku_id values") from exc

    store = db.get(Store, store_id)
    sku = db.get(Sku, sku_id)
    if store is None or sku is None or store.tenant_id != sku.tenant_id:
        raise ValueError("replenishment job references missing or cross-tenant data")
    request = ReplenishmentEvaluationRequest(
        store_id=store.id,
        sku_ids=[sku.id],
        generate_tasks=True,
    )
    create_replenishment_run(
        db,
        store.tenant_id,
        request,
        trigger=ReplenishmentTrigger.RFID,
        requested_by_subject=None,
    )


def list_tasks(
    db: Session,
    tenant_id: uuid.UUID,
    *,
    store_id: uuid.UUID | None,
    status: ReplenishmentTaskStatus | None,
    limit: int,
    offset: int,
) -> tuple[list[tuple[ReplenishmentTask, Sku]], int]:
    _get_tenant(db, tenant_id)
    predicates: list[Any] = [ReplenishmentTask.tenant_id == tenant_id]
    if store_id is not None:
        predicates.append(ReplenishmentTask.store_id == store_id)
    if status is not None:
        predicates.append(ReplenishmentTask.status == status)
    total = db.scalar(select(func.count()).select_from(ReplenishmentTask).where(*predicates))
    tasks = list(
        db.execute(
            select(ReplenishmentTask, Sku)
            .join(Sku, Sku.id == ReplenishmentTask.sku_id)
            .where(*predicates)
            .order_by(ReplenishmentTask.created_at.desc(), ReplenishmentTask.id.desc())
            .limit(limit)
            .offset(offset)
        )
        .tuples()
        .all()
    )
    return tasks, int(total or 0)


def update_task(
    db: Session,
    tenant_id: uuid.UUID,
    task_id: uuid.UUID,
    request: ReplenishmentTaskUpdate,
    *,
    actor_subject: str,
    can_manage_task: bool,
) -> tuple[ReplenishmentTask, Sku]:
    require_reservation_cutover_ready(db)
    _get_tenant(db, tenant_id)
    initial = db.scalar(
        select(ReplenishmentTask).where(
            ReplenishmentTask.id == task_id,
            ReplenishmentTask.tenant_id == tenant_id,
        )
    )
    if initial is None:
        raise ApiError(404, "Task not found", "The requested task does not exist.")
    lock_replenishment_store_sku(db, tenant_id, initial.store_id, initial.sku_id)
    task = db.scalar(
        select(ReplenishmentTask)
        .where(
            ReplenishmentTask.id == task_id,
            ReplenishmentTask.tenant_id == tenant_id,
        )
        .with_for_update()
    )
    if task is None:
        raise ApiError(404, "Task not found", "The requested task does not exist.")
    if task.version != request.expected_version:
        raise ApiError(
            409,
            "Task version conflict",
            f"Expected version {request.expected_version}, but the task is at version "
            f"{task.version}.",
            code="task_version_conflict",
        )
    management_involved = (
        task.status in MANAGED_OUTCOME_STATUSES or request.status in MANAGED_OUTCOME_STATUSES
    )
    if management_involved and not can_manage_task:
        raise ApiError(
            403,
            "Task management permission required",
            "Cancelling a task, placing it in exception, or updating a managed outcome "
            "requires manager permission for this store.",
            code="task_management_permission_required",
        )
    if task.status in TERMINAL_TASK_STATUSES:
        exact_no_op = (
            request.status is task.status
            and request.moved_quantity is None
            and request.note is None
        )
        if exact_no_op:
            sku = _get_sku(db, tenant_id, task.sku_id)
            return task, sku
        raise ApiError(
            409,
            "Terminal task is immutable",
            "Verified, cancelled, and exception task records cannot be changed; "
            "create or evaluate new work instead.",
            code="terminal_task_immutable",
        )
    if not task_transition_allowed(task.status, request.status):
        raise ApiError(
            409,
            "Invalid task transition",
            f"Task cannot transition from {task.status.value} to {request.status.value}.",
            code="invalid_task_transition",
        )
    management_override = can_manage_task and request.status in MANAGED_OUTCOME_STATUSES
    if task.status in OWNED_TASK_STATUSES and not management_override:
        if task.claimed_by_subject is None:
            raise ApiError(
                409,
                "Task is not claimed",
                "The task must have a claimant before execution can continue.",
                code="task_claim_required",
            )
        if task.claimed_by_subject != actor_subject:
            raise ApiError(
                409,
                "Task owned by another user",
                "Only the user who claimed this active task may update it.",
                code="task_claim_owner_conflict",
            )
    if request.moved_quantity is not None:
        if task.claimed_by_subject is None:
            raise ApiError(
                409,
                "Task is not claimed",
                "moved_quantity can be recorded only after a user claims the task.",
                code="task_claim_required",
            )
        if task.claimed_by_subject != actor_subject:
            raise ApiError(
                409,
                "Task owned by another user",
                "Only the task claimant may change moved_quantity.",
                code="task_claim_owner_conflict",
            )
        if not task_movement_allowed(task.status, request.status):
            raise ApiError(
                409,
                "Movement not allowed in task state",
                "moved_quantity may change only while the claimant is executing the task; "
                "first start or return the task to IN_PROGRESS, then record movement.",
                code="task_movement_not_allowed",
            )
        if request.moved_quantity < task.moved_quantity:
            raise ApiError(
                422,
                "Invalid moved quantity",
                "moved_quantity cannot decrease.",
                code="moved_quantity_decreased",
            )
        if request.moved_quantity > task.quantity:
            raise ApiError(
                422,
                "Invalid moved quantity",
                "moved_quantity cannot exceed the requested task quantity.",
                code="moved_quantity_exceeds_task",
            )
    resulting_moved_quantity = (
        request.moved_quantity if request.moved_quantity is not None else task.moved_quantity
    )
    linked_movement_count = _linked_task_movement_count(db, task.id)
    if request.status in MANAGED_OUTCOME_STATUSES:
        # A confirmed floorward RFID transition is stronger evidence of physical
        # movement than a stale client counter. Preserve that observed minimum when a
        # manager closes partially executed work.
        observed_movement_floor = min(
            task.quantity,
            task.reconciled_before_tracking_quantity + linked_movement_count,
        )
        resulting_moved_quantity = max(
            resulting_moved_quantity,
            observed_movement_floor,
        )
    if (
        request.status is ReplenishmentTaskStatus.AWAITING_VERIFICATION
        and resulting_moved_quantity != task.quantity
    ):
        raise ApiError(
            422,
            "Task movement is incomplete",
            "A task may await verification only after moved_quantity equals quantity.",
            code="task_awaiting_verification_incomplete",
        )
    if (
        request.status is ReplenishmentTaskStatus.VERIFIED
        and resulting_moved_quantity != task.quantity
    ):
        raise ApiError(
            422,
            "Task is not fully moved",
            "A task can be verified only after moved_quantity equals quantity.",
            code="task_verification_incomplete",
        )
    if request.status is ReplenishmentTaskStatus.OPEN and resulting_moved_quantity != 0:
        raise ApiError(
            409,
            "Task cannot be released after movement",
            "A claimed task may return to OPEN only before any movement is recorded.",
            code="task_release_after_movement",
        )

    changed = (
        task.status != request.status
        or (request.moved_quantity is not None and task.moved_quantity != request.moved_quantity)
        or (request.note is not None and task.last_note != request.note)
    )
    if not changed:
        sku = _get_sku(db, tenant_id, task.sku_id)
        return task, sku

    now = datetime.now(UTC)
    previous_status = task.status
    task.status = request.status
    if request.moved_quantity is not None:
        task.moved_quantity = request.moved_quantity
    if request.status in MANAGED_OUTCOME_STATUSES:
        task.moved_quantity = resulting_moved_quantity
    if request.note is not None:
        task.last_note = request.note
    if (
        request.status is ReplenishmentTaskStatus.CLAIMED
        and previous_status is ReplenishmentTaskStatus.OPEN
    ):
        task.claimed_by_subject = actor_subject
        task.claimed_at = now
    elif request.status is ReplenishmentTaskStatus.OPEN:
        task.claimed_by_subject = None
        task.claimed_at = None
    if request.status in TERMINAL_TASK_STATUSES and previous_status not in TERMINAL_TASK_STATUSES:
        task.completed_at = now
    task.version += 1
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Active task conflict",
            "Another active task already exists for this store and SKU.",
            code="active_task_conflict",
        ) from exc
    db.refresh(task)
    sku = _get_sku(db, tenant_id, task.sku_id)
    return task, sku
