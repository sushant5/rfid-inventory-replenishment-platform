"""enforce authenticated store scope in PostgreSQL

Revision ID: a1c5e7f9b042
Revises: e9f2a4c6b831
"""

# DDL interpolation is limited to module-owned identifiers and a database-derived
# role name escaped as a SQL literal.
# ruff: noqa: S608

from collections.abc import Sequence

from alembic import op

revision: str = "a1c5e7f9b042"
down_revision: str | None = "e9f2a4c6b831"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DIRECT_STORE_TABLES: dict[str, tuple[str, bool]] = {
    "stores": ("id", False),
    "zones": ("store_id", False),
    "device_assignments": ("store_id", False),
    "rfid_observation_batches": ("store_id", False),
    "rfid_observation_events": ("store_id", False),
    "current_item_state": ("store_id", False),
    "inventory_projection": ("store_id", False),
    "applied_inventory_deltas": ("store_id", False),
    "store_connectivity": ("store_id", False),
    "replenishment_policy_rules": ("store_id", True),
    "replenishment_tasks": ("store_id", False),
    "business_events": ("store_id", False),
}
INDIRECT_STORE_TABLES: tuple[str, ...] = (
    "devices",
    "replenishment_task_evidence",
)
POLICY_NAME = "store_scope_isolation"


def _role_literal(role: str) -> str:
    return "'" + role.replace("'", "''") + "'"


def _scope_match(column: str) -> str:
    return (
        "current_setting('app.store_scope', true) = '*' OR "
        f"{column}::text = ANY(string_to_array("
        "current_setting('app.store_scope', true), ','))"
    )


def _direct_predicate(*, owner: str, column: str, allow_shared: bool) -> str:
    scope = _scope_match(column)
    if allow_shared:
        scope = (
            "NULLIF(current_setting('app.store_scope', true), '') IS NOT NULL AND "
            f"({column} IS NULL OR {scope})"
        )
    return f"current_user = {_role_literal(owner)} OR ({scope})"


def _devices_predicate(owner: str) -> str:
    assignment_scope = _scope_match("assignment.store_id")
    return (
        f"current_user = {_role_literal(owner)} OR "
        "current_setting('app.store_scope', true) = '*' OR EXISTS ("
        "SELECT 1 FROM public.device_assignments AS assignment "
        "WHERE assignment.tenant_id = devices.tenant_id "
        "AND assignment.device_id = devices.id "
        f"AND ({assignment_scope}))"
    )


def _evidence_predicate(owner: str) -> str:
    task_scope = _scope_match("task.store_id")
    return (
        f"current_user = {_role_literal(owner)} OR "
        "current_setting('app.store_scope', true) = '*' OR EXISTS ("
        "SELECT 1 FROM public.replenishment_tasks AS task "
        "WHERE task.tenant_id = replenishment_task_evidence.tenant_id "
        "AND task.id = replenishment_task_evidence.task_id "
        f"AND ({task_scope}))"
    )


def _create_restrictive_policy(table: str, predicate: str) -> None:
    op.execute(
        f"CREATE POLICY {POLICY_NAME} ON public.{table} AS RESTRICTIVE "
        f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
    )


def upgrade() -> None:
    migration_owner = str(op.get_bind().exec_driver_sql("SELECT current_user").scalar_one())
    for table, (column, allow_shared) in DIRECT_STORE_TABLES.items():
        _create_restrictive_policy(
            table,
            _direct_predicate(
                owner=migration_owner,
                column=column,
                allow_shared=allow_shared,
            ),
        )
    _create_restrictive_policy("devices", _devices_predicate(migration_owner))
    _create_restrictive_policy(
        "replenishment_task_evidence",
        _evidence_predicate(migration_owner),
    )


def downgrade() -> None:
    for table in (*DIRECT_STORE_TABLES, *INDIRECT_STORE_TABLES):
        op.execute(f"DROP POLICY IF EXISTS {POLICY_NAME} ON public.{table}")
