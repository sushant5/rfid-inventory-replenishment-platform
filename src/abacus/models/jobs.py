import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import CheckConstraint, DateTime, Enum, ForeignKey, Index, Integer, String, Text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from abacus.enums import JobKind, JobStatus
from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class DurableJob(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "durable_jobs"
    __table_args__ = (
        CheckConstraint("attempts >= 0", name="ck_durable_jobs_nonnegative_attempts"),
        CheckConstraint(
            "(status = 'PROCESSING' AND locked_by IS NOT NULL "
            "AND lease_expires_at IS NOT NULL) OR status <> 'PROCESSING'",
            name="ck_durable_jobs_processing_lease",
        ),
        Index("ix_durable_jobs_claim", "status", "available_at", "created_at"),
        Index("ix_durable_jobs_tenant_kind", "tenant_id", "kind"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    kind: Mapped[JobKind] = mapped_column(
        Enum(JobKind, name="job_kind", native_enum=False, create_constraint=True),
        nullable=False,
    )
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, name="job_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=JobStatus.PENDING,
    )
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    locked_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
