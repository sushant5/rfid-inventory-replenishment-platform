"""authoritative business events and database hardening

Revision ID: f8c1d2e3a4b6
Revises: e2f6a1b3c904
"""

import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f8c1d2e3a4b6"
down_revision: str | None = "e2f6a1b3c904"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

ADDED_TENANT_TABLES: tuple[str, ...] = ("business_events",)
ROLE_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")
TENANT_CONTEXT_SQL = "NULLIF(current_setting('app.tenant_id', true), '')::uuid"


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


def _install_hardened_policy_triggers() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_activated_policy_rule_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE old_status text;
        DECLARE new_status text;
        BEGIN
          IF TG_OP = 'INSERT' THEN
            SELECT status INTO new_status
            FROM public.replenishment_policy_versions
            WHERE id = NEW.version_id
            FOR SHARE;
            IF new_status IS DISTINCT FROM 'DRAFT' THEN
              RAISE EXCEPTION 'activated replenishment policy versions are immutable';
            END IF;
            RETURN NEW;
          ELSIF TG_OP = 'DELETE' THEN
            IF NOT EXISTS (
              SELECT 1 FROM public.tenants WHERE id = OLD.tenant_id
            ) THEN
              RETURN OLD;
            END IF;
            SELECT status INTO old_status
            FROM public.replenishment_policy_versions
            WHERE id = OLD.version_id
            FOR SHARE;
            IF old_status IS DISTINCT FROM 'DRAFT' THEN
              RAISE EXCEPTION 'activated replenishment policy versions are immutable';
            END IF;
            RETURN OLD;
          END IF;

          IF NEW.version_id IS DISTINCT FROM OLD.version_id THEN
            RAISE EXCEPTION 'policy rules cannot move between versions';
          END IF;
          SELECT status INTO old_status
          FROM public.replenishment_policy_versions
          WHERE id = OLD.version_id
          FOR SHARE;
          SELECT status INTO new_status
          FROM public.replenishment_policy_versions
          WHERE id = NEW.version_id
          FOR SHARE;
          IF old_status IS DISTINCT FROM 'DRAFT' OR new_status IS DISTINCT FROM 'DRAFT' THEN
            RAISE EXCEPTION 'activated replenishment policy versions are immutable';
          END IF;
          RETURN NEW;
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_activated_policy_version_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
          IF TG_OP = 'DELETE' THEN
            IF NOT EXISTS (
              SELECT 1 FROM public.tenants WHERE id = OLD.tenant_id
            ) THEN
              RETURN OLD;
            END IF;
            IF OLD.status IS DISTINCT FROM 'DRAFT' THEN
              RAISE EXCEPTION 'activated replenishment policy versions are immutable';
            END IF;
            RETURN OLD;
          END IF;

          IF OLD.status = 'DRAFT' AND NEW.status IN ('DRAFT', 'ACTIVE') THEN
            RETURN NEW;
          END IF;
          IF OLD.status = 'ACTIVE' AND NEW.status = 'RETIRED' AND
             ROW(NEW.id, NEW.tenant_id, NEW.policy_id, NEW.version_number,
                 NEW.created_at, NEW.activated_at, NEW.activated_by_user_id)
             IS NOT DISTINCT FROM
             ROW(OLD.id, OLD.tenant_id, OLD.policy_id, OLD.version_number,
                 OLD.created_at, OLD.activated_at, OLD.activated_by_user_id) THEN
            RETURN NEW;
          END IF;
          RAISE EXCEPTION 'activated replenishment policy versions are immutable';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER trg_policy_versions_immutable_after_activation
        BEFORE UPDATE OR DELETE ON replenishment_policy_versions
        FOR EACH ROW EXECUTE FUNCTION prevent_activated_policy_version_mutation()
        """
    )


def _restore_original_rule_trigger() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION prevent_activated_policy_rule_mutation()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        DECLARE target_version uuid;
        DECLARE target_status text;
        BEGIN
          target_version := CASE WHEN TG_OP = 'DELETE' THEN OLD.version_id ELSE NEW.version_id END;
          SELECT status INTO target_status
          FROM public.replenishment_policy_versions
          WHERE id = target_version;
          IF target_status IS DISTINCT FROM 'DRAFT' THEN
            RAISE EXCEPTION 'activated replenishment policy versions are immutable';
          END IF;
          RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
        END;
        $$
        """
    )


def upgrade() -> None:
    application_role = _application_role()
    migration_owner = str(op.get_bind().scalar(sa.text("SELECT current_user")))
    application_role_exists = _role_exists(application_role)
    if application_role_exists and application_role == migration_owner:
        raise RuntimeError("Migration owner and application database role must be different roles")

    op.drop_index("ix_current_item_state_removal_sweep", table_name="current_item_state")
    op.create_index(
        "ix_durable_jobs_tenant_claim",
        "durable_jobs",
        ["tenant_id", "kind", "status", "available_at", "created_at"],
    )
    op.create_index(
        "ix_rfid_observation_outbox_tenant_pending",
        "rfid_observation_outbox",
        ["tenant_id", "acceptance_sequence"],
        postgresql_where=sa.text("published_at IS NULL"),
    )
    op.create_index(
        "ix_rfid_quarantine_tenant_created",
        "rfid_quarantine",
        ["tenant_id", sa.text("quarantined_at DESC"), sa.text("id DESC")],
    )

    op.create_table(
        "business_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("store_id", sa.Uuid(), nullable=False),
        sa.Column("source_system", sa.String(length=64), nullable=False),
        sa.Column("external_event_id", sa.String(length=128), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column(
            "event_type",
            sa.Enum(
                "SALE",
                "TRANSFER_OUT",
                "ADJUSTMENT_REMOVE",
                name="business_event_type",
                native_enum=False,
                create_constraint=True,
            ),
            nullable=False,
        ),
        sa.Column("epc", sa.String(length=128), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("transition_id", sa.Uuid(), nullable=False),
        sa.Column("state_version", sa.Integer(), nullable=False),
        sa.Column("note", sa.String(length=500), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            name="fk_business_events_tenant_id_tenants",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["store_id"],
            ["stores.id"],
            name="fk_business_events_store_id_stores",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["transition_id"],
            ["inventory_transition_outbox.transition_id"],
            name="fk_business_events_transition_id_inventory_outbox",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_business_events"),
        sa.UniqueConstraint(
            "tenant_id",
            "source_system",
            "external_event_id",
            name="uq_business_events_source_event",
        ),
    )
    op.create_index(
        "ix_business_events_store_created",
        "business_events",
        ["tenant_id", "store_id", sa.text("created_at DESC")],
    )
    op.add_column(
        "current_item_state",
        sa.Column("authoritative_removal_event_id", sa.Uuid(), nullable=True),
    )
    op.add_column(
        "current_item_state",
        sa.Column("authoritative_removed_at", sa.DateTime(timezone=True), nullable=True),
    )

    predicate = f"tenant_id = {TENANT_CONTEXT_SQL}"
    quoted_owner = _quoted_identifier(migration_owner)
    op.execute("ALTER TABLE public.business_events ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.business_events FORCE ROW LEVEL SECURITY")
    op.execute(
        "CREATE POLICY tenant_isolation ON public.business_events "
        f"FOR ALL USING ({predicate}) WITH CHECK ({predicate})"
    )
    op.execute(
        "CREATE POLICY migration_owner_access ON public.business_events "
        f"FOR ALL TO {quoted_owner} USING (true) WITH CHECK (true)"
    )
    if application_role_exists:
        quoted_role = _quoted_identifier(application_role)
        op.execute(f"GRANT SELECT, INSERT ON TABLE public.business_events TO {quoted_role}")

    op.execute(
        "DROP TRIGGER IF EXISTS trg_policy_versions_immutable_after_activation "
        "ON replenishment_policy_versions"
    )
    _install_hardened_policy_triggers()


def downgrade() -> None:
    op.execute(
        "DROP TRIGGER IF EXISTS trg_policy_versions_immutable_after_activation "
        "ON replenishment_policy_versions"
    )
    op.execute("DROP FUNCTION IF EXISTS prevent_activated_policy_version_mutation()")
    _restore_original_rule_trigger()

    application_role = _application_role()
    if _role_exists(application_role):
        quoted_role = _quoted_identifier(application_role)
        op.execute(f"REVOKE SELECT, INSERT ON TABLE public.business_events FROM {quoted_role}")
    op.execute("DROP POLICY IF EXISTS migration_owner_access ON public.business_events")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON public.business_events")
    op.execute("ALTER TABLE public.business_events NO FORCE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE public.business_events DISABLE ROW LEVEL SECURITY")
    op.drop_column("current_item_state", "authoritative_removed_at")
    op.drop_column("current_item_state", "authoritative_removal_event_id")
    op.drop_index("ix_business_events_store_created", table_name="business_events")
    op.drop_table("business_events")

    op.drop_index("ix_rfid_quarantine_tenant_created", table_name="rfid_quarantine")
    op.drop_index(
        "ix_rfid_observation_outbox_tenant_pending",
        table_name="rfid_observation_outbox",
    )
    op.drop_index("ix_durable_jobs_tenant_claim", table_name="durable_jobs")
    op.create_index(
        "ix_current_item_state_removal_sweep",
        "current_item_state",
        ["tenant_id", "last_observed_at"],
        postgresql_where=sa.text("zone_id IS NOT NULL"),
    )
