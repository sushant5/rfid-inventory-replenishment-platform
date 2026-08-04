"""retire the compatibility RFID and replenishment schema

Revision ID: c9e8d4f2a715
Revises: b4f7a9c2d610
"""

# DDL interpolation is limited to module-owned identifiers and an application role
# validated by ROLE_NAME_PATTERN.
# ruff: noqa: S608

import os
import re
from collections.abc import Sequence

from alembic import op

revision: str = "c9e8d4f2a715"
down_revision: str | None = "b4f7a9c2d610"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

RETIRED_SCHEMA = "retired_compatibility"
RETIRED_TABLES: tuple[str, ...] = (
    "replenishment_run_lines",
    "inventory_changes",
    "legacy_replenishment_tasks",
    "replenishment_runs",
    "replenishment_policy_imports",
    "legacy_replenishment_rules",
    "inventory_item_states",
    "inventory_balances",
    "rfid_observations",
)
FUNCTION_SIGNATURE = "public.app_cutover_ready()"
ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
RETIRED_QUALIFIED_TABLES = ", ".join(f"public.{table}" for table in RETIRED_TABLES)


def _application_role() -> str:
    role = os.environ.get("APPLICATION_DATABASE_ROLE", "abacus_app").strip()
    if not ROLE_NAME_PATTERN.fullmatch(role):
        raise RuntimeError("APPLICATION_DATABASE_ROLE must be a simple PostgreSQL role name")
    return role


def _execute_for_application_role(command: str) -> None:
    """Emit role-conditional DDL that also works in Alembic offline mode."""

    role = _application_role()
    op.execute(
        f"""
        DO $block$
        DECLARE target_role text := '{role}';
        BEGIN
          IF EXISTS (
            SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = target_role
          ) THEN
            EXECUTE pg_catalog.format('{command}', target_role);
          END IF;
        END
        $block$
        """
    )


def _assert_legacy_work_is_quiescent() -> None:
    # Keep these locks through the migration transaction so a legacy writer cannot
    # pass the checks and enqueue more work before the tables move.
    op.execute(
        "LOCK TABLE public.legacy_replenishment_tasks, public.rfid_observations, "
        "public.replenishment_runs, public.durable_jobs "
        "IN SHARE ROW EXCLUSIVE MODE"
    )
    op.execute(
        """
        DO $block$
        BEGIN
          IF EXISTS (
            SELECT 1
            FROM public.legacy_replenishment_tasks
            WHERE reservation_cutover_reviewed IS NOT TRUE
          ) THEN
            RAISE EXCEPTION
              'legacy replenishment tasks must be reconciled before retirement';
          END IF;
          IF EXISTS (
            SELECT 1 FROM public.rfid_observations WHERE status = 'RECEIVED'
          ) THEN
            RAISE EXCEPTION
              'received legacy RFID observations must be drained before retirement';
          END IF;
          IF EXISTS (
            SELECT 1 FROM public.replenishment_runs WHERE status = 'PROCESSING'
          ) THEN
            RAISE EXCEPTION
              'processing legacy replenishment runs must finish before retirement';
          END IF;
          IF EXISTS (
            SELECT 1
            FROM public.durable_jobs
            WHERE kind IN ('RFID_OBSERVATION', 'REPLENISHMENT_RECALC')
              AND status IN ('PENDING', 'PROCESSING')
          ) THEN
            RAISE EXCEPTION
              'legacy durable jobs must reach a terminal state before retirement';
          END IF;
        END
        $block$
        """
    )


def _create_cutover_function() -> None:
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
    _execute_for_application_role(f"GRANT EXECUTE ON FUNCTION {FUNCTION_SIGNATURE} TO %I")


def upgrade() -> None:
    # Preserve retired data and a reversible rollback path without keeping its ORM,
    # handlers, or worker in the running application. Runtime roles have no USAGE on
    # this schema, so archived compatibility tables are outside the application surface.
    _assert_legacy_work_is_quiescent()
    op.execute(f"DROP FUNCTION IF EXISTS {FUNCTION_SIGNATURE}")
    # A pre-existing schema could carry an unexpected owner, ACLs, or objects. Treat
    # that as a collision and abort instead of adopting it as the archive boundary.
    op.execute(f"CREATE SCHEMA {RETIRED_SCHEMA}")
    op.execute(f"REVOKE ALL ON SCHEMA {RETIRED_SCHEMA} FROM PUBLIC")
    _execute_for_application_role(f"REVOKE ALL PRIVILEGES ON SCHEMA {RETIRED_SCHEMA} FROM %I")
    # The canonical RFID outbox deliberately shares this monotonic acceptance
    # sequence. Detach it before moving its original owner table so the sequence
    # remains in ``public`` for the active ingestion pipeline.
    op.execute("ALTER SEQUENCE public.rfid_observation_acceptance_seq OWNED BY NONE")
    for table in RETIRED_TABLES:
        op.execute(f"ALTER TABLE public.{table} SET SCHEMA {RETIRED_SCHEMA}")
    op.execute(f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {RETIRED_SCHEMA} FROM PUBLIC")
    # ALTER TABLE SET SCHEMA preserves table ACLs. Revoke them explicitly so an
    # existing prepared statement cannot retain access through a pre-resolved OID.
    _execute_for_application_role(
        f"REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA {RETIRED_SCHEMA} FROM %I"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT canonical_task_status TO replenishment_task_status"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT ck_canonical_tasks_positive_quantity "
        "TO ck_replenishment_tasks_positive_quantity"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT ck_canonical_tasks_positive_version "
        "TO ck_replenishment_tasks_positive_version"
    )
    op.execute(
        "ALTER INDEX public.ix_canonical_tasks_store_status "
        "RENAME TO ix_replenishment_tasks_store_status"
    )


def downgrade() -> None:
    op.execute(
        "ALTER INDEX public.ix_replenishment_tasks_store_status "
        "RENAME TO ix_canonical_tasks_store_status"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT ck_replenishment_tasks_positive_version "
        "TO ck_canonical_tasks_positive_version"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT ck_replenishment_tasks_positive_quantity "
        "TO ck_canonical_tasks_positive_quantity"
    )
    op.execute(
        "ALTER TABLE public.replenishment_tasks "
        "RENAME CONSTRAINT replenishment_task_status TO canonical_task_status"
    )
    for table in reversed(RETIRED_TABLES):
        op.execute(f"ALTER TABLE {RETIRED_SCHEMA}.{table} SET SCHEMA public")
    op.execute(
        "ALTER SEQUENCE public.rfid_observation_acceptance_seq "
        "OWNED BY public.rfid_observations.acceptance_sequence"
    )
    # a6 granted exactly these table privileges before the retirement migration.
    # Restore them so a full downgrade recreates the previous runtime ACL surface.
    _execute_for_application_role(
        f"GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE {RETIRED_QUALIFIED_TABLES} TO %I"
    )
    op.execute(f"DROP SCHEMA {RETIRED_SCHEMA}")
    _create_cutover_function()
