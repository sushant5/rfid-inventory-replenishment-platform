import uuid
from datetime import datetime
from enum import StrEnum
from typing import Any

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column

from abacus.models.base import Base, TimestampMixin, UUIDPrimaryKeyMixin


class UserStatus(StrEnum):
    ACTIVE = "ACTIVE"
    SUSPENDED = "SUSPENDED"


class IdentityRole(StrEnum):
    CORPORATE_ADMIN = "CORPORATE_ADMIN"
    STORE_MANAGER = "STORE_MANAGER"
    STORE_ASSOCIATE = "STORE_ASSOCIATE"


class IdentityAuditAction(StrEnum):
    LOGIN_SUCCEEDED = "LOGIN_SUCCEEDED"
    LOGIN_FAILED = "LOGIN_FAILED"
    USER_CREATED = "USER_CREATED"
    USER_SUSPENDED = "USER_SUSPENDED"
    USER_ACCESS_CHANGED = "USER_ACCESS_CHANGED"


class User(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "users"
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_users_tenant_email"),
        CheckConstraint("email = lower(email)", name="ck_users_email_lowercase"),
        CheckConstraint("token_version >= 1", name="ck_users_token_version_positive"),
        Index("ix_users_tenant_status", "tenant_id", "status"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    email: Mapped[str] = mapped_column(String(320), nullable=False)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[UserStatus] = mapped_column(
        Enum(UserStatus, name="user_status", native_enum=False, create_constraint=True),
        nullable=False,
        default=UserStatus.ACTIVE,
    )
    token_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    last_login_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class UserAccessGrant(UUIDPrimaryKeyMixin, TimestampMixin, Base):
    __tablename__ = "user_access_grants"
    __table_args__ = (
        CheckConstraint(
            "(role = 'CORPORATE_ADMIN' AND store_id IS NULL) OR "
            "(role IN ('STORE_MANAGER', 'STORE_ASSOCIATE') AND store_id IS NOT NULL)",
            name="ck_user_access_grants_role_scope",
        ),
        Index(
            "uq_user_access_grants_tenant_role",
            "user_id",
            "role",
            unique=True,
            postgresql_where=text("store_id IS NULL"),
        ),
        Index(
            "uq_user_access_grants_store_role",
            "user_id",
            "role",
            "store_id",
            unique=True,
            postgresql_where=text("store_id IS NOT NULL"),
        ),
        Index("ix_user_access_grants_tenant_store", "tenant_id", "store_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    user_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )
    role: Mapped[IdentityRole] = mapped_column(
        Enum(IdentityRole, name="identity_role", native_enum=False, create_constraint=True),
        nullable=False,
    )
    store_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("stores.id", ondelete="CASCADE"),
        nullable=True,
    )


class IdentityAuditRecord(UUIDPrimaryKeyMixin, Base):
    __tablename__ = "identity_audit_records"
    __table_args__ = (
        Index("ix_identity_audit_tenant_occurred", "tenant_id", "occurred_at"),
        Index("ix_identity_audit_actor", "tenant_id", "actor_user_id"),
    )

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
    )
    actor_user_id: Mapped[uuid.UUID | None] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
    )
    action: Mapped[IdentityAuditAction] = mapped_column(
        Enum(
            IdentityAuditAction,
            name="identity_audit_action",
            native_enum=False,
            create_constraint=True,
        ),
        nullable=False,
    )
    target_user_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    occurred_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
