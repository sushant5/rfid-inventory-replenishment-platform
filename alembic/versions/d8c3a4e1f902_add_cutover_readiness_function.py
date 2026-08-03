"""add RLS-safe cutover readiness function

Revision ID: d8c3a4e1f902
Revises: a6f0c4d1e537
Create Date: 2026-08-02
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d8c3a4e1f902"
down_revision: str | None = "a6f0c4d1e537"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
FUNCTION_SIGNATURE = "public.app_cutover_ready()"


def _application_role() -> str:
    role = os.environ.get("APPLICATION_DATABASE_ROLE", "abacus_app").strip()
    if not ROLE_NAME_PATTERN.fullmatch(role):
        raise RuntimeError("APPLICATION_DATABASE_ROLE must be a simple PostgreSQL role name")
    return role


def _role_exists(role: str) -> bool:
    return bool(
        op.get_bind().scalar(
            sa.text("SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = :role)"),
            {"role": role},
        )
    )


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION public.app_cutover_ready()
        RETURNS boolean
        LANGUAGE sql
        STABLE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
          SELECT NOT EXISTS (
            SELECT 1
            FROM public.legacy_replenishment_tasks
            WHERE reservation_cutover_reviewed = false
          )
        $function$
        """
    )
    op.execute(f"REVOKE ALL ON FUNCTION {FUNCTION_SIGNATURE} FROM PUBLIC")
    role = _application_role()
    if _role_exists(role):
        quoted_role = op.get_bind().dialect.identifier_preparer.quote(role)
        op.execute(f"GRANT EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} TO {quoted_role}")


def downgrade() -> None:
    role = _application_role()
    if _role_exists(role):
        quoted_role = op.get_bind().dialect.identifier_preparer.quote(role)
        op.execute(f"REVOKE EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} FROM {quoted_role}")
    op.execute(f"DROP FUNCTION IF EXISTS {FUNCTION_SIGNATURE}")
