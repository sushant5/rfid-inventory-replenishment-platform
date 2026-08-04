"""enforce tenant consistency across core relationships

Revision ID: a9d4e6f2b713
Revises: f8c1d2e3a4b6
"""

from collections.abc import Sequence

from alembic import op

revision: str = "a9d4e6f2b713"
down_revision: str | None = "f8c1d2e3a4b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


PARENT_UNIQUE_CONSTRAINTS: tuple[tuple[str, str, list[str]], ...] = (
    ("organization_units", "uq_organization_units_tenant_id", ["tenant_id", "id"]),
    ("stores", "uq_stores_tenant_id", ["tenant_id", "id"]),
    ("zones", "uq_zones_tenant_store_id", ["tenant_id", "store_id", "id"]),
    ("devices", "uq_devices_tenant_id", ["tenant_id", "id"]),
    ("users", "uq_users_tenant_id", ["tenant_id", "id"]),
    ("products", "uq_products_tenant_id", ["tenant_id", "id"]),
    ("product_variants", "uq_product_variants_tenant_id", ["tenant_id", "id"]),
    ("product_styles", "uq_product_styles_tenant_id", ["tenant_id", "id"]),
    ("skus", "uq_skus_tenant_id", ["tenant_id", "id"]),
    (
        "rfid_observation_batches",
        "uq_rfid_observation_batches_tenant_id",
        ["tenant_id", "id"],
    ),
    (
        "inventory_transition_outbox",
        "uq_inventory_transition_outbox_tenant_transition",
        ["tenant_id", "transition_id"],
    ),
    ("business_events", "uq_business_events_tenant_id", ["tenant_id", "id"]),
    (
        "replenishment_policies",
        "uq_replenishment_policies_tenant_id",
        ["tenant_id", "id"],
    ),
    (
        "replenishment_policy_versions",
        "uq_replenishment_policy_versions_tenant_id",
        ["tenant_id", "id"],
    ),
    (
        "replenishment_policy_rules",
        "uq_replenishment_policy_rules_tenant_version_id",
        ["tenant_id", "version_id", "id"],
    ),
)


# name, child table/columns, parent table/columns, delete action
TENANT_FOREIGN_KEYS: tuple[tuple[str, str, list[str], str, list[str], str | None], ...] = (
    (
        "fk_organization_units_tenant_parent",
        "organization_units",
        ["tenant_id", "parent_id"],
        "organization_units",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_stores_tenant_organization_unit",
        "stores",
        ["tenant_id", "organization_unit_id"],
        "organization_units",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_zones_tenant_store",
        "zones",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_device_assignments_tenant_device",
        "device_assignments",
        ["tenant_id", "device_id"],
        "devices",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_device_assignments_tenant_store",
        "device_assignments",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_device_assignments_tenant_store_zone",
        "device_assignments",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_user_access_grants_tenant_user",
        "user_access_grants",
        ["tenant_id", "user_id"],
        "users",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_user_access_grants_tenant_store",
        "user_access_grants",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_identity_audit_tenant_actor",
        "identity_audit_records",
        ["tenant_id", "actor_user_id"],
        "users",
        ["tenant_id", "id"],
        None,
    ),
    (
        "fk_product_variants_tenant_product",
        "product_variants",
        ["tenant_id", "product_id"],
        "products",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_skus_tenant_product_style",
        "skus",
        ["tenant_id", "product_style_id"],
        "product_styles",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_skus_tenant_product_variant",
        "skus",
        ["tenant_id", "product_variant_id"],
        "product_variants",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_catalog_import_rows_tenant_import",
        "catalog_import_rows",
        ["tenant_id", "import_id"],
        "catalog_imports",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_catalog_import_errors_tenant_import",
        "catalog_import_errors",
        ["tenant_id", "import_id"],
        "catalog_imports",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_epc_bindings_tenant_sku",
        "epc_bindings",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_epc_bindings_tenant_source_import",
        "epc_bindings",
        ["tenant_id", "source_import_id"],
        "catalog_imports",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_tags_tenant_sku",
        "rfid_tags",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_tags_tenant_source_import",
        "rfid_tags",
        ["tenant_id", "source_import_id"],
        "catalog_imports",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_user_roles_tenant_user",
        "user_roles",
        ["tenant_id", "user_id"],
        "users",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_user_store_assignments_tenant_user",
        "user_store_assignments",
        ["tenant_id", "user_id"],
        "users",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_user_store_assignments_tenant_store",
        "user_store_assignments",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_rfid_batches_tenant_device",
        "rfid_observation_batches",
        ["tenant_id", "device_id"],
        "devices",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_batches_tenant_store",
        "rfid_observation_batches",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_batches_tenant_store_zone",
        "rfid_observation_batches",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_events_tenant_device",
        "rfid_observation_events",
        ["tenant_id", "device_id"],
        "devices",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_events_tenant_store",
        "rfid_observation_events",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_events_tenant_store_zone",
        "rfid_observation_events",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_rfid_batch_events_tenant_batch",
        "rfid_observation_batch_events",
        ["tenant_id", "batch_id"],
        "rfid_observation_batches",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_rfid_quarantine_tenant_batch",
        "rfid_quarantine",
        ["tenant_id", "batch_id"],
        "rfid_observation_batches",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_current_item_state_tenant_sku",
        "current_item_state",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_current_item_state_tenant_store",
        "current_item_state",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_current_item_state_tenant_store_zone",
        "current_item_state",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_current_item_state_tenant_authoritative_removal_event",
        "current_item_state",
        ["tenant_id", "authoritative_removal_event_id"],
        "business_events",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_business_events_tenant_store",
        "business_events",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_business_events_tenant_transition",
        "business_events",
        ["tenant_id", "transition_id"],
        "inventory_transition_outbox",
        ["tenant_id", "transition_id"],
        "RESTRICT",
    ),
    (
        "fk_inventory_projection_tenant_store",
        "inventory_projection",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_inventory_projection_tenant_sku",
        "inventory_projection",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_inventory_projection_tenant_store_zone",
        "inventory_projection",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "CASCADE",
    ),
    (
        "fk_applied_inventory_deltas_tenant_store",
        "applied_inventory_deltas",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_applied_inventory_deltas_tenant_sku",
        "applied_inventory_deltas",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_applied_inventory_deltas_tenant_store_zone",
        "applied_inventory_deltas",
        ["tenant_id", "store_id", "zone_id"],
        "zones",
        ["tenant_id", "store_id", "id"],
        "CASCADE",
    ),
    (
        "fk_store_connectivity_tenant_store",
        "store_connectivity",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_policy_versions_tenant_policy",
        "replenishment_policy_versions",
        ["tenant_id", "policy_id"],
        "replenishment_policies",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_policy_versions_tenant_activated_by",
        "replenishment_policy_versions",
        ["tenant_id", "activated_by_user_id"],
        "users",
        ["tenant_id", "id"],
        None,
    ),
    (
        "fk_policy_rules_tenant_version",
        "replenishment_policy_rules",
        ["tenant_id", "version_id"],
        "replenishment_policy_versions",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_policy_rules_tenant_store",
        "replenishment_policy_rules",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_policy_rules_tenant_sku",
        "replenishment_policy_rules",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_replenishment_tasks_tenant_store",
        "replenishment_tasks",
        ["tenant_id", "store_id"],
        "stores",
        ["tenant_id", "id"],
        "CASCADE",
    ),
    (
        "fk_replenishment_tasks_tenant_sku",
        "replenishment_tasks",
        ["tenant_id", "sku_id"],
        "skus",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_replenishment_tasks_tenant_policy_version",
        "replenishment_tasks",
        ["tenant_id", "policy_version_id"],
        "replenishment_policy_versions",
        ["tenant_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_replenishment_tasks_tenant_version_rule",
        "replenishment_tasks",
        ["tenant_id", "policy_version_id", "policy_rule_id"],
        "replenishment_policy_rules",
        ["tenant_id", "version_id", "id"],
        "RESTRICT",
    ),
    (
        "fk_replenishment_tasks_tenant_claimed_by",
        "replenishment_tasks",
        ["tenant_id", "claimed_by_user_id"],
        "users",
        ["tenant_id", "id"],
        None,
    ),
)


def upgrade() -> None:
    for table_name, constraint_name, columns in PARENT_UNIQUE_CONSTRAINTS:
        op.create_unique_constraint(constraint_name, table_name, columns)

    for (
        constraint_name,
        child_table,
        child_columns,
        parent_table,
        parent_columns,
        ondelete,
    ) in TENANT_FOREIGN_KEYS:
        op.create_foreign_key(
            constraint_name,
            child_table,
            parent_table,
            child_columns,
            parent_columns,
            ondelete=ondelete,
        )

    op.create_check_constraint(
        "ck_current_item_state_location_pair",
        "current_item_state",
        "(store_id IS NULL) = (zone_id IS NULL)",
    )
    op.create_check_constraint(
        "ck_current_item_state_authoritative_removal_location",
        "current_item_state",
        "authoritative_removal_event_id IS NULL OR (store_id IS NULL AND zone_id IS NULL)",
    )
    op.create_check_constraint(
        "ck_current_item_state_authoritative_removal_pair",
        "current_item_state",
        "(authoritative_removal_event_id IS NULL) = (authoritative_removed_at IS NULL)",
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_current_item_state_authoritative_removal_pair",
        "current_item_state",
        type_="check",
    )
    op.drop_constraint(
        "ck_current_item_state_authoritative_removal_location",
        "current_item_state",
        type_="check",
    )
    op.drop_constraint("ck_current_item_state_location_pair", "current_item_state", type_="check")

    for constraint_name, child_table, *_rest in reversed(TENANT_FOREIGN_KEYS):
        op.drop_constraint(constraint_name, child_table, type_="foreignkey")

    for table_name, constraint_name, _columns in reversed(PARENT_UNIQUE_CONSTRAINTS):
        op.drop_constraint(constraint_name, table_name, type_="unique")
