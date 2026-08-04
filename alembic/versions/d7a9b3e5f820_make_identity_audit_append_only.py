"""make identity audit records append-only for the runtime role

Revision ID: d7a9b3e5f820
Revises: c6f8a2d4e719
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d7a9b3e5f820"
down_revision: str | None = "c6f8a2d4e719"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def _application_role() -> str:
    role = os.environ.get("APPLICATION_DATABASE_ROLE", "abacus_app").strip()
    if not ROLE_NAME_PATTERN.fullmatch(role):
        raise RuntimeError("APPLICATION_DATABASE_ROLE must be a simple PostgreSQL role name")
    return role


def _quoted_identifier(name: str) -> str:
    return op.get_bind().dialect.identifier_preparer.quote(name)


def _role_exists(role: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :role)"),
            {"role": role},
        )
    )


def upgrade() -> None:
    application_role = _application_role()
    if not _role_exists(application_role):
        return

    migration_owner = str(op.get_bind().scalar(sa.text("SELECT current_user")))
    if application_role == migration_owner:
        raise RuntimeError("Migration owner and application database role must be different roles")

    quoted_role = _quoted_identifier(application_role)
    op.execute(f"REVOKE UPDATE, DELETE ON TABLE public.identity_audit_records FROM {quoted_role}")
    op.execute(f"GRANT SELECT, INSERT ON TABLE public.identity_audit_records TO {quoted_role}")


def downgrade() -> None:
    application_role = _application_role()
    if not _role_exists(application_role):
        return

    quoted_role = _quoted_identifier(application_role)
    op.execute(f"GRANT UPDATE, DELETE ON TABLE public.identity_audit_records TO {quoted_role}")
