import uuid
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import case, delete, exists, func, or_, select, update
from sqlalchemy.engine import CursorResult
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from abacus.api.errors import ApiError
from abacus.config import Settings
from abacus.enums import StoreStatus, ZoneKind
from abacus.models.architecture import (
    FreshnessStatus,
    InventoryProjection,
    PolicyDefinition,
    PolicyRule,
    PolicyVersion,
    PolicyVersionStatus,
    Product,
    ProductVariant,
    ReplenishmentTask,
    ReplenishmentTaskStatus,
    StoreConnectivity,
)
from abacus.models.catalog import Sku
from abacus.models.tenancy import Store, Tenant, Zone
from abacus.schemas.replenishment import (
    PolicyCreate,
    PolicyRulesPatch,
    PolicyRuleWrite,
    ReplenishmentEvaluationCreate,
    ReplenishmentTaskPatch,
)
from abacus.security import Permission, Principal
from abacus.services.streaming_inventory import (
    current_inventory_bucket_metadata,
    effective_bucket_confidence,
    effective_freshness,
)


@dataclass(frozen=True, slots=True)
class PolicyBundle:
    policy: PolicyDefinition
    version: PolicyVersion
    rules: tuple[PolicyRule, ...]


@dataclass(frozen=True, slots=True)
class RuleDescriptor:
    id: uuid.UUID | None
    version_id: uuid.UUID | None
    store_id: uuid.UUID | None
    category: str | None
    style_code: str | None
    sku_id: uuid.UUID | None
    size: str | None
    min_floor_qty: int
    target_floor_qty: int
    priority: int


@dataclass(frozen=True, slots=True)
class SkuContext:
    sku_id: uuid.UUID
    style_code: str
    category: str
    size: str


@dataclass(slots=True)
class InventorySnapshot:
    item: SkuContext
    floor_qty: int = 0
    backroom_qty: int = 0
    confidence: float = 1.0


@dataclass(frozen=True, slots=True)
class EvaluationResult:
    store_id: uuid.UUID
    tasks: tuple[ReplenishmentTask, ...]
    suppressed_connectivity: bool
    suppressed_low_confidence: int


def calculate_replenishment_quantity(
    *,
    floor_qty: int,
    backroom_qty: int,
    open_task_qty: int,
    min_floor_qty: int,
    target_floor_qty: int,
) -> int:
    """Apply the assignment formula exactly, without hidden safety-stock behavior."""

    if floor_qty >= min_floor_qty:
        return 0
    return min(backroom_qty, max(0, target_floor_qty - floor_qty - open_task_qty))


def _descriptor(rule: PolicyRule | PolicyRuleWrite) -> RuleDescriptor:
    return RuleDescriptor(
        id=getattr(rule, "id", None),
        version_id=getattr(rule, "version_id", None),
        store_id=rule.store_id,
        category=rule.category,
        style_code=rule.style_code,
        sku_id=rule.sku_id,
        size=rule.size,
        min_floor_qty=rule.min_floor_qty,
        target_floor_qty=rule.target_floor_qty,
        priority=rule.priority,
    )


def _selector_kind(rule: RuleDescriptor) -> str:
    if rule.sku_id is not None:
        return "SKU"
    if rule.style_code is not None:
        return "STYLE"
    if rule.category is not None:
        return "CATEGORY"
    return "DEFAULT"


def rule_precedence(
    rule: RuleDescriptor,
    *,
    store_id: uuid.UUID,
    item: SkuContext,
) -> int | None:
    if rule.store_id is not None and rule.store_id != store_id:
        return None
    tenant_scope = rule.store_id is None
    kind = _selector_kind(rule)

    if kind == "SKU":
        if rule.sku_id != item.sku_id:
            return None
        if rule.size is not None and rule.size.casefold() != item.size.casefold():
            return None
        return 4 if tenant_scope else 7
    if kind == "STYLE":
        if rule.style_code is None or rule.style_code.casefold() != item.style_code.casefold():
            return None
        return 3 if tenant_scope else 6
    if kind == "CATEGORY":
        if rule.category is None or rule.category.casefold() != item.category.casefold():
            return None
        return 2 if tenant_scope else 5
    if tenant_scope:
        return 1
    return None


def select_policy_rule(
    rules: Iterable[RuleDescriptor],
    *,
    store_id: uuid.UUID,
    item: SkuContext,
) -> RuleDescriptor | None:
    candidates = [
        (precedence, rule.priority, rule)
        for rule in rules
        if (precedence := rule_precedence(rule, store_id=store_id, item=item)) is not None
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda candidate: (candidate[0], candidate[1]), reverse=True)
    best = candidates[0]
    if len(candidates) > 1 and candidates[1][:2] == best[:2]:
        raise ValueError("equal-priority rules overlap at the same specificity")
    return best[2]


def rules_overlap(left: RuleDescriptor, right: RuleDescriptor) -> bool:
    if left.priority != right.priority or left.store_id != right.store_id:
        return False
    left_kind = _selector_kind(left)
    if left_kind != _selector_kind(right):
        return False
    if left_kind == "DEFAULT":
        return True
    if left_kind == "CATEGORY":
        assert left.category is not None and right.category is not None
        return left.category.casefold() == right.category.casefold()
    if left_kind == "STYLE":
        assert left.style_code is not None and right.style_code is not None
        return left.style_code.casefold() == right.style_code.casefold()
    if left.sku_id != right.sku_id:
        return False
    return left.size is None or right.size is None or left.size.casefold() == right.size.casefold()


def _first_overlap(
    rules: Sequence[RuleDescriptor],
) -> tuple[RuleDescriptor, RuleDescriptor] | None:
    for index, left in enumerate(rules):
        for right in rules[index + 1 :]:
            if rules_overlap(left, right):
                return left, right
    return None


def _ensure_manage_rule_scopes(
    principal: Principal,
    rules: Iterable[PolicyRuleWrite | PolicyRule],
) -> None:
    if not principal.has_permission(Permission.POLICY_MANAGE):
        raise ApiError(403, "Forbidden", "Policy management permission is required.")
    for rule in rules:
        if rule.store_id is None:
            allowed = principal.has_tenant_permission(Permission.POLICY_MANAGE)
        else:
            allowed = principal.can_access_store(Permission.POLICY_MANAGE, rule.store_id)
        if not allowed:
            raise ApiError(
                403,
                "Forbidden",
                "A policy rule is outside the current user's store scope.",
                code="policy_scope_denied",
            )


def _ensure_store_scope(
    principal: Principal,
    permission: Permission,
    store_id: uuid.UUID,
) -> None:
    if not principal.can_access_store(permission, store_id):
        raise ApiError(
            403,
            "Forbidden",
            "The requested store is outside the current user's access scope.",
            code="store_scope_denied",
        )


def _validate_rules(
    db: Session,
    tenant_id: uuid.UUID,
    rules: Sequence[PolicyRuleWrite | PolicyRule],
) -> None:
    descriptors = [_descriptor(rule) for rule in rules]
    if any(
        descriptor.store_id is not None and _selector_kind(descriptor) == "DEFAULT"
        for descriptor in descriptors
    ):
        raise ApiError(
            422,
            "Undefined rule scope",
            "Store-default rules are outside the defined precedence.",
            code="invalid_policy_scope",
        )
    if _first_overlap(descriptors) is not None:
        raise ApiError(
            422,
            "Ambiguous policy rules",
            "Equal-priority rules overlap at the same specificity.",
            code="policy_rule_overlap",
        )

    store_ids = {rule.store_id for rule in rules if rule.store_id is not None}
    if store_ids:
        valid_store_ids = set(
            db.scalars(
                select(Store.id).where(
                    Store.tenant_id == tenant_id,
                    Store.id.in_(store_ids),
                    Store.status != StoreStatus.INACTIVE,
                )
            ).all()
        )
        if valid_store_ids != store_ids:
            raise ApiError(
                422,
                "Invalid rule store",
                "Every rule store must be a non-inactive store in the current tenant.",
                code="invalid_policy_store",
            )

    sku_rules = [rule for rule in rules if rule.sku_id is not None]
    sku_ids = {rule.sku_id for rule in sku_rules if rule.sku_id is not None}
    if sku_ids:
        sku_sizes: dict[uuid.UUID, str] = {
            sku_id: size
            for sku_id, size in db.execute(
                select(Sku.id, Sku.size).where(
                    Sku.tenant_id == tenant_id,
                    Sku.id.in_(sku_ids),
                    Sku.active.is_(True),
                )
            ).all()
        }
        if set(sku_sizes) != sku_ids or any(
            rule.size is not None
            and rule.sku_id is not None
            and sku_sizes[rule.sku_id].casefold() != rule.size.casefold()
            for rule in sku_rules
            if rule.sku_id in sku_sizes
        ):
            raise ApiError(
                422,
                "Invalid rule SKU",
                "Every SKU selector must reference an active tenant SKU and matching size.",
                code="invalid_policy_sku",
            )

    styles = {rule.style_code for rule in rules if rule.style_code is not None}
    if styles:
        valid_styles = {
            value.upper()
            for value in db.scalars(
                select(Product.style_code).where(
                    Product.tenant_id == tenant_id,
                    func.upper(Product.style_code).in_(styles),
                    Product.active.is_(True),
                )
            ).all()
        }
        if valid_styles != styles:
            raise ApiError(
                422,
                "Invalid rule style",
                "Every style selector must reference an active tenant product.",
                code="invalid_policy_style",
            )

    categories = {rule.category for rule in rules if rule.category is not None}
    if categories:
        valid_categories = {
            value.upper()
            for value in db.scalars(
                select(Product.category).where(
                    Product.tenant_id == tenant_id,
                    func.upper(Product.category).in_(categories),
                    Product.active.is_(True),
                )
            ).all()
        }
        if valid_categories != categories:
            raise ApiError(
                422,
                "Invalid rule category",
                "Every category selector must exist in the active tenant catalog.",
                code="invalid_policy_category",
            )


def _new_rule(
    *,
    tenant_id: uuid.UUID,
    version_id: uuid.UUID,
    request: PolicyRuleWrite,
) -> PolicyRule:
    return PolicyRule(
        tenant_id=tenant_id,
        version_id=version_id,
        store_id=request.store_id,
        category=request.category,
        style_code=request.style_code,
        sku_id=request.sku_id,
        size=request.size,
        min_floor_qty=request.min_floor_qty,
        target_floor_qty=request.target_floor_qty,
        priority=request.priority,
    )


def _load_bundle(
    db: Session,
    tenant_id: uuid.UUID,
    version_id: uuid.UUID,
) -> PolicyBundle:
    version = db.scalar(
        select(PolicyVersion).where(
            PolicyVersion.tenant_id == tenant_id,
            PolicyVersion.id == version_id,
        )
    )
    if version is None:
        raise ApiError(404, "Policy version not found", "The policy version does not exist.")
    policy = db.scalar(
        select(PolicyDefinition).where(
            PolicyDefinition.tenant_id == tenant_id,
            PolicyDefinition.id == version.policy_id,
        )
    )
    if policy is None:  # pragma: no cover - protected by the foreign key.
        raise ApiError(404, "Policy not found", "The policy does not exist.")
    rules = tuple(
        db.scalars(
            select(PolicyRule)
            .where(
                PolicyRule.tenant_id == tenant_id,
                PolicyRule.version_id == version.id,
            )
            .order_by(PolicyRule.priority.desc(), PolicyRule.id.asc())
        ).all()
    )
    return PolicyBundle(policy=policy, version=version, rules=rules)


def _visible_bundle(bundle: PolicyBundle, principal: Principal) -> PolicyBundle | None:
    if principal.has_tenant_permission(Permission.POLICY_READ):
        return bundle
    allowed_store_ids = principal.store_ids_for_permission(Permission.POLICY_READ)
    visible_rules = tuple(
        rule for rule in bundle.rules if rule.store_id is None or rule.store_id in allowed_store_ids
    )
    if not visible_rules:
        return None
    return PolicyBundle(policy=bundle.policy, version=bundle.version, rules=visible_rules)


def _selected_policy_versions(
    tenant_id: uuid.UUID,
    version_status: PolicyVersionStatus | None,
) -> Any:
    """Select one discoverable version per policy.

    The default is the effective version: ACTIVE first, then the latest DRAFT for a
    policy that has not gone live. Callers may request a specific lifecycle status
    when inspecting drafts or retired history.
    """

    if version_status is None:
        lifecycle_order = case(
            (PolicyVersion.status == PolicyVersionStatus.ACTIVE, 0),
            (PolicyVersion.status == PolicyVersionStatus.DRAFT, 1),
            else_=2,
        )
        version_order: tuple[Any, ...] = (
            lifecycle_order.asc(),
            PolicyVersion.version_number.desc(),
        )
    else:
        version_order = (PolicyVersion.version_number.desc(),)

    ranked = select(
        PolicyVersion.policy_id.label("policy_id"),
        PolicyVersion.id.label("version_id"),
        func.row_number()
        .over(
            partition_by=PolicyVersion.policy_id,
            order_by=version_order,
        )
        .label("selection_rank"),
    ).where(PolicyVersion.tenant_id == tenant_id)
    if version_status is not None:
        ranked = ranked.where(PolicyVersion.status == version_status)
    return ranked.subquery("selected_policy_versions")


def _authorized_policy_version_status(
    principal: Principal,
    requested_status: PolicyVersionStatus | None,
) -> PolicyVersionStatus | None:
    """Keep draft and retired policy configuration out of read-only roles."""

    if principal.has_permission(Permission.POLICY_MANAGE):
        return requested_status
    if requested_status not in (None, PolicyVersionStatus.ACTIVE):
        raise ApiError(
            403,
            "Forbidden",
            "Draft and retired policy versions require policy-management permission.",
            code="policy_version_status_forbidden",
        )
    return PolicyVersionStatus.ACTIVE


def get_policy_bundle(
    db: Session,
    principal: Principal,
    policy_id: uuid.UUID,
    *,
    version_status: PolicyVersionStatus | None = None,
) -> PolicyBundle:
    version_status = _authorized_policy_version_status(principal, version_status)
    selected_versions = _selected_policy_versions(principal.tenant_id, version_status)
    version_id = db.scalar(
        select(selected_versions.c.version_id)
        .where(
            selected_versions.c.policy_id == policy_id,
            selected_versions.c.selection_rank == 1,
        )
        .limit(1)
    )
    if version_id is None:
        raise ApiError(404, "Policy not found", "The policy does not exist.")
    visible = _visible_bundle(_load_bundle(db, principal.tenant_id, version_id), principal)
    if visible is None:
        raise ApiError(404, "Policy not found", "The policy does not exist.")
    return visible


def list_policy_bundles(
    db: Session,
    principal: Principal,
    *,
    limit: int,
    offset: int,
    version_status: PolicyVersionStatus | None = None,
) -> tuple[list[PolicyBundle], int]:
    version_status = _authorized_policy_version_status(principal, version_status)
    selected_versions = _selected_policy_versions(principal.tenant_id, version_status)
    query = (
        select(PolicyDefinition.id, selected_versions.c.version_id)
        .join(
            selected_versions,
            (selected_versions.c.policy_id == PolicyDefinition.id)
            & (selected_versions.c.selection_rank == 1),
        )
        .where(PolicyDefinition.tenant_id == principal.tenant_id)
    )
    if not principal.has_tenant_permission(Permission.POLICY_READ):
        allowed_store_ids = principal.store_ids_for_permission(Permission.POLICY_READ)
        query = query.where(
            exists(
                select(PolicyRule.id).where(
                    PolicyRule.tenant_id == principal.tenant_id,
                    PolicyRule.version_id == selected_versions.c.version_id,
                    or_(
                        PolicyRule.store_id.is_(None),
                        PolicyRule.store_id.in_(allowed_store_ids),
                    ),
                )
            )
        )
    total = db.scalar(select(func.count()).select_from(query.subquery())) or 0
    rows = db.execute(
        query.order_by(PolicyDefinition.updated_at.desc(), PolicyDefinition.id.desc())
        .limit(limit)
        .offset(offset)
    ).all()
    bundles = [
        visible
        for _policy_id, version_id in rows
        if (
            visible := _visible_bundle(
                _load_bundle(db, principal.tenant_id, version_id),
                principal,
            )
        )
        is not None
    ]
    return bundles, total


def create_policy(
    db: Session,
    principal: Principal,
    request: PolicyCreate,
) -> PolicyBundle:
    _ensure_manage_rule_scopes(principal, request.rules)
    _validate_rules(db, principal.tenant_id, request.rules)
    policy = PolicyDefinition(
        tenant_id=principal.tenant_id,
        name=request.name,
        description=request.description,
    )
    db.add(policy)
    try:
        db.flush()
        version = PolicyVersion(
            tenant_id=principal.tenant_id,
            policy_id=policy.id,
            version_number=1,
            status=PolicyVersionStatus.DRAFT,
        )
        db.add(version)
        db.flush()
        db.add_all(
            _new_rule(tenant_id=principal.tenant_id, version_id=version.id, request=rule)
            for rule in request.rules
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Policy creation conflict",
            "The policy name or a rule conflicts with existing data.",
            code="policy_creation_conflict",
        ) from exc
    return _load_bundle(db, principal.tenant_id, version.id)


def clone_policy_version(
    db: Session,
    principal: Principal,
    policy_id: uuid.UUID,
) -> PolicyBundle:
    policy = db.scalar(
        select(PolicyDefinition)
        .where(
            PolicyDefinition.tenant_id == principal.tenant_id,
            PolicyDefinition.id == policy_id,
        )
        .with_for_update()
    )
    if policy is None:
        raise ApiError(404, "Policy not found", "The policy does not exist.")
    source = db.scalar(
        select(PolicyVersion)
        .where(
            PolicyVersion.tenant_id == principal.tenant_id,
            PolicyVersion.policy_id == policy.id,
            PolicyVersion.status == PolicyVersionStatus.ACTIVE,
        )
        .limit(1)
    )
    if source is None:
        source = db.scalar(
            select(PolicyVersion)
            .where(
                PolicyVersion.tenant_id == principal.tenant_id,
                PolicyVersion.policy_id == policy.id,
            )
            .order_by(PolicyVersion.version_number.desc())
            .limit(1)
        )
    if source is None:  # pragma: no cover - policy creation is atomic.
        raise ApiError(409, "Policy has no version", "The policy cannot be cloned.")
    source_rules = tuple(
        db.scalars(
            select(PolicyRule).where(
                PolicyRule.tenant_id == principal.tenant_id,
                PolicyRule.version_id == source.id,
            )
        ).all()
    )
    _ensure_manage_rule_scopes(principal, source_rules)
    latest_number = (
        db.scalar(
            select(func.max(PolicyVersion.version_number)).where(
                PolicyVersion.policy_id == policy.id
            )
        )
        or 0
    )
    version = PolicyVersion(
        tenant_id=principal.tenant_id,
        policy_id=policy.id,
        version_number=latest_number + 1,
        status=PolicyVersionStatus.DRAFT,
    )
    db.add(version)
    try:
        db.flush()
        db.add_all(
            PolicyRule(
                tenant_id=principal.tenant_id,
                version_id=version.id,
                store_id=rule.store_id,
                category=rule.category,
                style_code=rule.style_code,
                sku_id=rule.sku_id,
                size=rule.size,
                min_floor_qty=rule.min_floor_qty,
                target_floor_qty=rule.target_floor_qty,
                priority=rule.priority,
            )
            for rule in source_rules
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Version creation conflict",
            "A draft or version number was created concurrently.",
            code="policy_version_conflict",
        ) from exc
    return _load_bundle(db, principal.tenant_id, version.id)


def patch_policy_version(
    db: Session,
    principal: Principal,
    version_id: uuid.UUID,
    request: PolicyRulesPatch,
) -> PolicyBundle:
    version = db.scalar(
        select(PolicyVersion)
        .where(
            PolicyVersion.tenant_id == principal.tenant_id,
            PolicyVersion.id == version_id,
        )
        .with_for_update()
    )
    if version is None:
        raise ApiError(404, "Policy version not found", "The policy version does not exist.")
    existing_rules = tuple(
        db.scalars(
            select(PolicyRule).where(
                PolicyRule.tenant_id == principal.tenant_id,
                PolicyRule.version_id == version.id,
            )
        ).all()
    )
    _ensure_manage_rule_scopes(principal, existing_rules)
    if version.status != PolicyVersionStatus.DRAFT:
        raise ApiError(
            409,
            "Immutable policy version",
            "Only DRAFT policy versions may be changed.",
            code="policy_version_immutable",
        )
    _ensure_manage_rule_scopes(principal, request.rules)
    _validate_rules(db, principal.tenant_id, request.rules)
    try:
        db.execute(delete(PolicyRule).where(PolicyRule.version_id == version.id))
        db.add_all(
            _new_rule(tenant_id=principal.tenant_id, version_id=version.id, request=rule)
            for rule in request.rules
        )
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Policy update conflict",
            "The draft rules conflict or changed concurrently.",
            code="policy_rule_update_conflict",
        ) from exc
    return _load_bundle(db, principal.tenant_id, version.id)


def activate_policy_version(
    db: Session,
    principal: Principal,
    version_id: uuid.UUID,
) -> PolicyBundle:
    db.scalar(select(Tenant.id).where(Tenant.id == principal.tenant_id).with_for_update())
    bundle = _load_bundle(db, principal.tenant_id, version_id)
    version = db.scalar(
        select(PolicyVersion).where(PolicyVersion.id == version_id).with_for_update()
    )
    assert version is not None
    bundle = PolicyBundle(policy=bundle.policy, version=version, rules=bundle.rules)
    _ensure_manage_rule_scopes(principal, bundle.rules)
    if version.status == PolicyVersionStatus.ACTIVE:
        return bundle
    if version.status != PolicyVersionStatus.DRAFT:
        raise ApiError(
            409,
            "Retired policy version",
            "A RETIRED version cannot be activated again; clone it instead.",
            code="policy_version_retired",
        )
    if not bundle.rules:
        raise ApiError(422, "Empty policy", "A policy version requires at least one rule.")
    _validate_rules(db, principal.tenant_id, bundle.rules)

    other_active_rules = tuple(
        db.scalars(
            select(PolicyRule)
            .join(PolicyVersion, PolicyVersion.id == PolicyRule.version_id)
            .where(
                PolicyRule.tenant_id == principal.tenant_id,
                PolicyVersion.status == PolicyVersionStatus.ACTIVE,
                PolicyVersion.policy_id != bundle.policy.id,
            )
        ).all()
    )
    for draft_rule in map(_descriptor, bundle.rules):
        if any(rules_overlap(draft_rule, _descriptor(active)) for active in other_active_rules):
            raise ApiError(
                409,
                "Ambiguous active policies",
                "An active policy has an equal-priority overlapping rule.",
                code="active_policy_overlap",
            )

    now = datetime.now(UTC)
    try:
        db.execute(
            update(PolicyVersion)
            .where(
                PolicyVersion.policy_id == bundle.policy.id,
                PolicyVersion.status == PolicyVersionStatus.ACTIVE,
                PolicyVersion.id != version.id,
            )
            .values(status=PolicyVersionStatus.RETIRED)
        )
        version.status = PolicyVersionStatus.ACTIVE
        version.activated_at = now
        version.activated_by_user_id = principal.user_id
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Policy activation conflict",
            "Another policy version was activated concurrently.",
            code="policy_activation_conflict",
        ) from exc
    return _load_bundle(db, principal.tenant_id, version.id)


def evaluate_replenishment(
    db: Session,
    principal: Principal,
    request: ReplenishmentEvaluationCreate,
    *,
    settings: Settings,
    minimum_confidence: float,
) -> EvaluationResult:
    store = db.scalar(
        select(Store)
        .where(
            Store.tenant_id == principal.tenant_id,
            Store.id == request.store_id,
            Store.status != StoreStatus.INACTIVE,
        )
        .with_for_update()
    )
    if store is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    _ensure_store_scope(principal, Permission.REPLENISHMENT_MANAGE, store.id)
    connectivity = db.scalar(
        select(StoreConnectivity).where(
            StoreConnectivity.tenant_id == principal.tenant_id,
            StoreConnectivity.store_id == store.id,
        )
    )
    evaluated_at = datetime.now(UTC)
    if effective_freshness(connectivity, settings, now=evaluated_at) != FreshnessStatus.LIVE:
        return EvaluationResult(store.id, (), True, 0)

    rules = tuple(
        db.scalars(
            select(PolicyRule)
            .join(PolicyVersion, PolicyVersion.id == PolicyRule.version_id)
            .where(
                PolicyRule.tenant_id == principal.tenant_id,
                PolicyVersion.status == PolicyVersionStatus.ACTIVE,
            )
        ).all()
    )
    descriptors = tuple(map(_descriptor, rules))
    if not descriptors:
        return EvaluationResult(store.id, (), False, 0)

    current_metadata = current_inventory_bucket_metadata(
        tenant_id=principal.tenant_id,
        store_id=store.id,
        evaluated_at=evaluated_at,
        confidence_half_life_seconds=settings.rfid_confidence_half_life_seconds,
    )
    projection_query = (
        select(
            InventoryProjection,
            Zone.kind,
            Sku.id,
            Sku.size,
            Product.style_code,
            Product.category,
            current_metadata.c.item_count,
            current_metadata.c.confidence,
        )
        .join(
            Zone,
            (Zone.id == InventoryProjection.zone_id)
            & (Zone.tenant_id == InventoryProjection.tenant_id),
        )
        .join(
            Sku,
            (Sku.id == InventoryProjection.sku_id)
            & (Sku.tenant_id == InventoryProjection.tenant_id),
        )
        .join(
            ProductVariant,
            (ProductVariant.id == Sku.product_variant_id)
            & (ProductVariant.tenant_id == Sku.tenant_id),
        )
        .join(
            Product,
            (Product.id == ProductVariant.product_id)
            & (Product.tenant_id == ProductVariant.tenant_id),
        )
        .outerjoin(
            current_metadata,
            (current_metadata.c.tenant_id == InventoryProjection.tenant_id)
            & (current_metadata.c.store_id == InventoryProjection.store_id)
            & (current_metadata.c.sku_id == InventoryProjection.sku_id)
            & (current_metadata.c.zone_id == InventoryProjection.zone_id),
        )
        .where(
            InventoryProjection.tenant_id == principal.tenant_id,
            InventoryProjection.store_id == store.id,
            Sku.active.is_(True),
            Product.active.is_(True),
        )
    )
    if request.sku_ids:
        projection_query = projection_query.where(InventoryProjection.sku_id.in_(request.sku_ids))
    snapshots: dict[uuid.UUID, InventorySnapshot] = {}
    for (
        projection,
        kind,
        sku_id,
        size,
        style_code,
        category,
        current_item_count,
        current_confidence,
    ) in db.execute(projection_query).all():
        if kind not in {ZoneKind.SALES_FLOOR, ZoneKind.BACKROOM}:
            continue
        snapshot = snapshots.setdefault(
            sku_id,
            InventorySnapshot(
                item=SkuContext(
                    sku_id=sku_id,
                    style_code=style_code,
                    category=category,
                    size=size,
                )
            ),
        )
        if kind == ZoneKind.SALES_FLOOR:
            snapshot.floor_qty += projection.quantity
        else:
            snapshot.backroom_qty += projection.quantity
        snapshot.confidence = min(
            snapshot.confidence,
            effective_bucket_confidence(
                projected_quantity=projection.quantity,
                current_item_count=current_item_count,
                current_confidence=current_confidence,
            ),
        )

    active_statuses = {
        ReplenishmentTaskStatus.OPEN,
        ReplenishmentTaskStatus.CLAIMED,
        ReplenishmentTaskStatus.IN_PROGRESS,
    }
    active_tasks = {
        task.sku_id: task
        for task in db.scalars(
            select(ReplenishmentTask).where(
                ReplenishmentTask.tenant_id == principal.tenant_id,
                ReplenishmentTask.store_id == store.id,
                ReplenishmentTask.status.in_(active_statuses),
            )
        ).all()
    }
    rules_by_id = {rule.id: rule for rule in rules}
    created: list[ReplenishmentTask] = []
    suppressed_low_confidence = 0
    for snapshot in snapshots.values():
        if snapshot.confidence < minimum_confidence:
            suppressed_low_confidence += 1
            continue
        try:
            selected = select_policy_rule(
                descriptors,
                store_id=store.id,
                item=snapshot.item,
            )
        except ValueError as exc:
            raise ApiError(
                409,
                "Ambiguous active policy",
                "Equal-priority active rules match this SKU.",
                code="active_policy_overlap",
            ) from exc
        if selected is None or selected.id is None or selected.version_id is None:
            continue
        active_task = active_tasks.get(snapshot.item.sku_id)
        quantity = calculate_replenishment_quantity(
            floor_qty=snapshot.floor_qty,
            backroom_qty=snapshot.backroom_qty,
            open_task_qty=active_task.quantity if active_task is not None else 0,
            min_floor_qty=selected.min_floor_qty,
            target_floor_qty=selected.target_floor_qty,
        )
        if quantity <= 0 or active_task is not None:
            continue
        selected_model = rules_by_id[selected.id]
        task = ReplenishmentTask(
            tenant_id=principal.tenant_id,
            store_id=store.id,
            sku_id=snapshot.item.sku_id,
            policy_version_id=selected_model.version_id,
            policy_rule_id=selected_model.id,
            status=ReplenishmentTaskStatus.OPEN,
            quantity=quantity,
            version=1,
        )
        db.add(task)
        created.append(task)
    try:
        db.commit()
    except IntegrityError as exc:
        db.rollback()
        raise ApiError(
            409,
            "Evaluation conflict",
            "An active task was created concurrently; retry the evaluation.",
            code="replenishment_evaluation_conflict",
        ) from exc
    for task in created:
        db.refresh(task)
    return EvaluationResult(store.id, tuple(created), False, suppressed_low_confidence)


def list_store_tasks(
    db: Session,
    principal: Principal,
    store_id: uuid.UUID,
    *,
    status: ReplenishmentTaskStatus | None,
    limit: int,
    offset: int,
) -> tuple[list[ReplenishmentTask], int]:
    store_exists = db.scalar(
        select(Store.id).where(
            Store.tenant_id == principal.tenant_id,
            Store.id == store_id,
        )
    )
    if store_exists is None:
        raise ApiError(404, "Store not found", "The requested store does not exist.")
    _ensure_store_scope(principal, Permission.REPLENISHMENT_READ, store_id)
    predicates = [
        ReplenishmentTask.tenant_id == principal.tenant_id,
        ReplenishmentTask.store_id == store_id,
    ]
    if status is not None:
        predicates.append(ReplenishmentTask.status == status)
    total = db.scalar(select(func.count()).select_from(ReplenishmentTask).where(*predicates)) or 0
    tasks = list(
        db.scalars(
            select(ReplenishmentTask)
            .where(*predicates)
            .order_by(
                ReplenishmentTask.created_at.desc(),
                ReplenishmentTask.id.desc(),
            )
            .limit(limit)
            .offset(offset)
        ).all()
    )
    return tasks, int(total)


def patch_replenishment_task(
    db: Session,
    principal: Principal,
    task_id: uuid.UUID,
    request: ReplenishmentTaskPatch,
) -> ReplenishmentTask:
    task = db.scalar(
        select(ReplenishmentTask).where(
            ReplenishmentTask.tenant_id == principal.tenant_id,
            ReplenishmentTask.id == task_id,
        )
    )
    if task is None:
        raise ApiError(404, "Task not found", "The replenishment task does not exist.")
    _ensure_store_scope(principal, Permission.REPLENISHMENT_EXECUTE, task.store_id)
    manager = principal.can_access_store(Permission.REPLENISHMENT_MANAGE, task.store_id)
    allowed: dict[ReplenishmentTaskStatus, frozenset[ReplenishmentTaskStatus]] = {
        ReplenishmentTaskStatus.OPEN: frozenset({ReplenishmentTaskStatus.CLAIMED}),
        ReplenishmentTaskStatus.CLAIMED: frozenset({ReplenishmentTaskStatus.IN_PROGRESS}),
        ReplenishmentTaskStatus.IN_PROGRESS: frozenset({ReplenishmentTaskStatus.COMPLETED}),
        ReplenishmentTaskStatus.COMPLETED: frozenset(),
        ReplenishmentTaskStatus.CANCELED: frozenset(),
        ReplenishmentTaskStatus.EXPIRED: frozenset(),
    }
    if request.status in {ReplenishmentTaskStatus.CANCELED, ReplenishmentTaskStatus.EXPIRED}:
        if (
            task.status
            not in {
                ReplenishmentTaskStatus.OPEN,
                ReplenishmentTaskStatus.CLAIMED,
                ReplenishmentTaskStatus.IN_PROGRESS,
            }
            or not manager
        ):
            raise ApiError(
                403,
                "Forbidden",
                "Only a store manager may cancel or expire an active task.",
                code="task_transition_forbidden",
            )
    elif request.status not in allowed[task.status]:
        raise ApiError(
            409,
            "Invalid task transition",
            f"{task.status.value} cannot transition to {request.status.value}.",
            code="invalid_task_transition",
        )
    if (
        task.status in {ReplenishmentTaskStatus.CLAIMED, ReplenishmentTaskStatus.IN_PROGRESS}
        and task.claimed_by_user_id != principal.user_id
        and not manager
    ):
        raise ApiError(
            403,
            "Forbidden",
            "Only the claimant or a store manager may advance this task.",
            code="task_claim_denied",
        )

    now = datetime.now(UTC)
    values: dict[str, object] = {
        "status": request.status,
        "version": request.version + 1,
        "updated_at": now,
    }
    if request.status == ReplenishmentTaskStatus.CLAIMED:
        values.update(claimed_by_user_id=principal.user_id, claimed_at=now)
    elif request.status == ReplenishmentTaskStatus.IN_PROGRESS:
        values["started_at"] = now
    elif request.status == ReplenishmentTaskStatus.COMPLETED:
        values["completed_at"] = now
    elif request.status == ReplenishmentTaskStatus.EXPIRED:
        values["expires_at"] = now
    if "note" in request.model_fields_set:
        values["note"] = request.note

    result = cast(
        CursorResult[Any],
        db.execute(
            update(ReplenishmentTask)
            .where(
                ReplenishmentTask.id == task.id,
                ReplenishmentTask.tenant_id == principal.tenant_id,
                ReplenishmentTask.version == request.version,
            )
            .values(**values)
        ),
    )
    if result.rowcount != 1:
        db.rollback()
        raise ApiError(
            409,
            "Task version conflict",
            "The task changed; reload it and retry with the latest version.",
            code="task_version_conflict",
        )
    db.commit()
    updated_task = db.scalar(
        select(ReplenishmentTask).where(
            ReplenishmentTask.tenant_id == principal.tenant_id,
            ReplenishmentTask.id == task.id,
        )
    )
    assert updated_task is not None
    return updated_task
