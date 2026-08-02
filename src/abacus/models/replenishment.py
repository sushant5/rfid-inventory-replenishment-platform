import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class PolicySelectorType(StrEnum):
    SKU = "SKU"
    STYLE = "STYLE"
    CATEGORY = "CATEGORY"
    SIZE = "SIZE"


class PolicyImportStatus(StrEnum):
    COMPLETED = "COMPLETED"
    REJECTED = "REJECTED"


class ReplenishmentRunStatus(StrEnum):
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"


class ReplenishmentReason(StrEnum):
    NO_MATCHING_POLICY = "NO_MATCHING_POLICY"
    FLOOR_AT_OR_ABOVE_MINIMUM = "FLOOR_AT_OR_ABOVE_MINIMUM"
    OPEN_TASK_COVERS_NEED = "OPEN_TASK_COVERS_NEED"
    NO_BACKROOM_STOCK = "NO_BACKROOM_STOCK"
    REPLENISHMENT_REQUIRED = "REPLENISHMENT_REQUIRED"


class ReplenishmentTrigger(StrEnum):
    API = "API"
    RFID = "RFID"
    POLICY_CHANGE = "POLICY_CHANGE"


class ReplenishmentTaskStatus(StrEnum):
    OPEN = "OPEN"
    CLAIMED = "CLAIMED"
    IN_PROGRESS = "IN_PROGRESS"
    AWAITING_VERIFICATION = "AWAITING_VERIFICATION"
    VERIFIED = "VERIFIED"
    CANCELLED = "CANCELLED"
    EXCEPTION = "EXCEPTION"


ACTIVE_TASK_STATUSES = (
    ReplenishmentTaskStatus.OPEN,
    ReplenishmentTaskStatus.CLAIMED,
    ReplenishmentTaskStatus.IN_PROGRESS,
    ReplenishmentTaskStatus.AWAITING_VERIFICATION,
)


class ReplenishmentPolicy(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_policies"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "external_key",
            name="uq_replenishment_policies_tenant_external_key",
        ),
        CheckConstraint(
            "effective_to IS NULL OR effective_to > effective_from",
            name="ck_replenishment_policies_valid_interval",
        ),
        CheckConstraint(
            "minimum_floor_quantity >= 0",
            name="ck_replenishment_policies_nonnegative_minimum",
        ),
        CheckConstraint(
            "target_floor_quantity >= minimum_floor_quantity",
            name="ck_replenishment_policies_target_at_least_minimum",
        ),
        CheckConstraint(
            "maximum_floor_quantity IS NULL OR maximum_floor_quantity >= target_floor_quantity",
            name="ck_replenishment_policies_maximum_at_least_target",
        ),
        Index(
            "ix_replenishment_policies_resolution",
            "tenant_id",
            "store_id",
            "active",
            "effective_from",
            "effective_to",
        ),
        Index(
            "ix_replenishment_policies_selector",
            "tenant_id",
            "selector_type",
            "selector_value",
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    external_key: Mapped[str] = mapped_column(String(128), nullable=False)
    store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=True,
    )
    selector_type: Mapped[PolicySelectorType] = mapped_column(
        Enum(
            PolicySelectorType,
            name="policy_selector_type",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    selector_value: Mapped[str] = mapped_column(String(128), nullable=False)
    minimum_floor_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    target_floor_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    maximum_floor_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    effective_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    effective_to: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    revision: Mapped[int] = mapped_column(Integer, nullable=False, default=1)


class ReplenishmentPolicyImport(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_policy_imports"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uq_replenishment_policy_imports_tenant_idempotency",
        ),
        CheckConstraint("total_count >= 0", name="ck_policy_imports_nonnegative_total"),
        CheckConstraint("created_count >= 0", name="ck_policy_imports_nonnegative_created"),
        CheckConstraint("updated_count >= 0", name="ck_policy_imports_nonnegative_updated"),
        CheckConstraint(
            "unchanged_count >= 0",
            name="ck_policy_imports_nonnegative_unchanged",
        ),
        CheckConstraint("rejected_count >= 0", name="ck_policy_imports_nonnegative_rejected"),
        Index("ix_replenishment_policy_imports_tenant_created", "tenant_id", "created_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    idempotency_key: Mapped[str] = mapped_column(String(128), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[PolicyImportStatus] = mapped_column(
        Enum(
            PolicyImportStatus,
            name="policy_import_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    total_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    updated_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unchanged_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rejected_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    reconciliation: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    errors: Mapped[list[dict[str, Any]]] = mapped_column(JSONB, nullable=False, default=list)


class ReplenishmentRun(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_runs"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "idempotency_key",
            name="uq_replenishment_runs_tenant_idempotency",
        ),
        CheckConstraint("line_count >= 0", name="ck_replenishment_runs_nonnegative_lines"),
        CheckConstraint("tasks_created >= 0", name="ck_replenishment_runs_nonnegative_created"),
        CheckConstraint("tasks_updated >= 0", name="ck_replenishment_runs_nonnegative_updated"),
        Index("ix_replenishment_runs_tenant_store_created", "tenant_id", "store_id", "created_at"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=False,
    )
    idempotency_key: Mapped[str | None] = mapped_column(String(128), nullable=True)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    trigger: Mapped[ReplenishmentTrigger] = mapped_column(
        Enum(
            ReplenishmentTrigger,
            name="replenishment_trigger",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    status: Mapped[ReplenishmentRunStatus] = mapped_column(
        Enum(
            ReplenishmentRunStatus,
            name="replenishment_run_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=ReplenishmentRunStatus.PROCESSING,
    )
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    requested_by_subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    line_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tasks_created: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tasks_updated: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class ReplenishmentTask(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_tasks"
    __table_args__ = (
        CheckConstraint("quantity > 0", name="ck_replenishment_tasks_positive_quantity"),
        CheckConstraint(
            "moved_quantity >= 0 AND moved_quantity <= quantity",
            name="ck_replenishment_tasks_valid_moved_quantity",
        ),
        CheckConstraint(
            "reconciled_before_tracking_quantity >= 0 "
            "AND reconciled_before_tracking_quantity <= moved_quantity",
            name="ck_replenishment_tasks_reconciled_baseline",
        ),
        CheckConstraint(
            "(reservation_cutover_reviewed_at IS NULL "
            "AND reservation_cutover_reviewed_by IS NULL "
            "AND reservation_cutover_note IS NULL) OR "
            "(reservation_cutover_reviewed = true "
            "AND reservation_cutover_reviewed_at IS NOT NULL "
            "AND reservation_cutover_reviewed_by IS NOT NULL "
            "AND reservation_cutover_note IS NOT NULL)",
            name="ck_replenishment_tasks_cutover_audit",
        ),
        CheckConstraint(
            "(status IN ('VERIFIED', 'CANCELLED', 'EXCEPTION') "
            "AND completed_at IS NOT NULL) OR "
            "(status IN ('OPEN', 'CLAIMED', 'IN_PROGRESS', 'AWAITING_VERIFICATION') "
            "AND completed_at IS NULL)",
            name="ck_replenishment_tasks_terminal_completion",
        ),
        CheckConstraint("version >= 1", name="ck_replenishment_tasks_positive_version"),
        Index(
            "uq_replenishment_tasks_active_store_sku",
            "tenant_id",
            "store_id",
            "sku_id",
            unique=True,
            postgresql_where=text(
                "status IN ('OPEN', 'CLAIMED', 'IN_PROGRESS', 'AWAITING_VERIFICATION')"
            ),
        ),
        Index("ix_replenishment_tasks_tenant_store_status", "tenant_id", "store_id", "status"),
        Index(
            "ix_replenishment_tasks_unreviewed_cutover",
            "id",
            postgresql_where=text("reservation_cutover_reviewed = false"),
        ),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=False,
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_policy_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_policies.id", ondelete="RESTRICT"),
        nullable=False,
    )
    status: Mapped[ReplenishmentTaskStatus] = mapped_column(
        Enum(
            ReplenishmentTaskStatus,
            name="replenishment_task_status",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
        default=ReplenishmentTaskStatus.OPEN,
    )
    quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    moved_quantity: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    # Cutover baseline for terminal tasks completed before per-EPC reservation
    # allocation existed. New tasks always start at zero.
    reconciled_before_tracking_quantity: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        server_default=text("0"),
    )
    # Legacy rows with recorded movement require a one-time operator decision: RFID
    # evidence may or may not already include those units. New tasks are born after
    # durable allocation exists and therefore need no cutover review.
    reservation_cutover_reviewed: Mapped[bool] = mapped_column(
        Boolean,
        nullable=False,
        default=True,
        # A legacy binary or direct SQL insert does not know how to create durable
        # movement links. Fail such rows closed; current ORM code sends True.
        server_default=text("false"),
    )
    reservation_cutover_reviewed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    reservation_cutover_reviewed_by: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    reservation_cutover_note: Mapped[str | None] = mapped_column(
        String(1000),
        nullable=True,
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    claimed_by_subject: Mapped[str | None] = mapped_column(String(255), nullable=True)
    claimed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_note: Mapped[str | None] = mapped_column(String(1000), nullable=True)


class ReplenishmentRunLine(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "replenishment_run_lines"
    __table_args__ = (
        UniqueConstraint("run_id", "sku_id", name="uq_replenishment_run_lines_run_sku"),
        CheckConstraint(
            "floor_quantity >= 0 AND backroom_quantity >= 0 "
            "AND open_task_quantity >= 0 AND recommended_quantity >= 0",
            name="ck_replenishment_run_lines_nonnegative_quantities",
        ),
        Index("ix_replenishment_run_lines_tenant_store_sku", "tenant_id", "store_id", "sku_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    run_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("replenishment_runs.id", ondelete="CASCADE"),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=False,
    )
    sku_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("skus.id", ondelete="RESTRICT"),
        nullable=False,
    )
    policy_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("replenishment_policies.id", ondelete="RESTRICT"),
        nullable=True,
    )
    task_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("replenishment_tasks.id", ondelete="SET NULL"),
        nullable=True,
    )
    selector_type: Mapped[PolicySelectorType | None] = mapped_column(
        Enum(
            PolicySelectorType,
            name="run_line_policy_selector_type",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=True,
    )
    selector_value: Mapped[str | None] = mapped_column(String(128), nullable=True)
    policy_priority: Mapped[int | None] = mapped_column(Integer, nullable=True)
    minimum_floor_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    target_floor_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    maximum_floor_quantity: Mapped[int | None] = mapped_column(Integer, nullable=True)
    floor_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    backroom_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    open_task_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    recommended_quantity: Mapped[int] = mapped_column(Integer, nullable=False)
    reason: Mapped[ReplenishmentReason] = mapped_column(
        Enum(
            ReplenishmentReason,
            name="replenishment_reason",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    formula: Mapped[str] = mapped_column(String(500), nullable=False)
    inventory_as_of: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
