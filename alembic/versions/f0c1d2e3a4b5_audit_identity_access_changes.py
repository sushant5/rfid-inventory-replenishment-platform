"""Audit canonical role and store-scope changes.

Revision ID: f0c1d2e3a4b5
Revises: e9b7c1a4d205
"""

import sqlalchemy as sa
from alembic import op

revision: str = "f0c1d2e3a4b5"
down_revision: str | None = "e9b7c1a4d205"
branch_labels: str | None = None
depends_on: str | None = None

_CONSTRAINT = "identity_audit_action"
_OLD_ACTIONS = (
    "LOGIN_SUCCEEDED",
    "LOGIN_FAILED",
    "USER_CREATED",
    "USER_SUSPENDED",
)
_NEW_ACTION = "USER_ACCESS_CHANGED"


def _action_constraint(actions: tuple[str, ...]) -> str:
    values = ", ".join(f"'{action}'" for action in actions)
    return f"action IN ({values})"


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT, "identity_audit_records", type_="check")
    op.alter_column(
        "identity_audit_records",
        "action",
        existing_type=sa.String(length=15),
        type_=sa.String(length=len(_NEW_ACTION)),
        existing_nullable=False,
    )
    op.create_check_constraint(
        _CONSTRAINT,
        "identity_audit_records",
        _action_constraint((*_OLD_ACTIONS, _NEW_ACTION)),
    )


def downgrade() -> None:
    has_new_records = op.get_bind().scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM identity_audit_records WHERE action = :action)"),
        {"action": _NEW_ACTION},
    )
    if has_new_records:
        raise RuntimeError(
            "Cannot downgrade while USER_ACCESS_CHANGED audit records exist; "
            "archive them before retrying."
        )
    op.drop_constraint(_CONSTRAINT, "identity_audit_records", type_="check")
    op.alter_column(
        "identity_audit_records",
        "action",
        existing_type=sa.String(length=len(_NEW_ACTION)),
        type_=sa.String(length=15),
        existing_nullable=False,
    )
    op.create_check_constraint(
        _CONSTRAINT,
        "identity_audit_records",
        _action_constraint(_OLD_ACTIONS),
    )
