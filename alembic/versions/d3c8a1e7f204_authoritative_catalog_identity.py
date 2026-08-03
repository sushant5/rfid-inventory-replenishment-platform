"""add authoritative catalog and identity structures

Revision ID: d3c8a1e7f204
Revises: b6e3f19a2d44
Create Date: 2026-08-02
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d3c8a1e7f204"
down_revision: str | None = "b6e3f19a2d44"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("catalog_imports", sa.Column("object_key", sa.String(1024), nullable=True))
    op.add_column("catalog_imports", sa.Column("source_version", sa.String(255), nullable=True))

    op.create_table(
        "products",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("style_code", sa.String(64), nullable=False),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("category", sa.String(128), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("tenant_id", "style_code", name="uq_products_tenant_style"),
    )
    op.create_index("ix_products_tenant_category", "products", ["tenant_id", "category"])
    op.create_table(
        "product_variants",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("product_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("color", sa.String(128), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["product_id"], ["products.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "tenant_id", "product_id", "color", name="uq_product_variants_product_color"
        ),
    )
    op.create_index(
        "ix_product_variants_tenant_product", "product_variants", ["tenant_id", "product_id"]
    )
    op.add_column(
        "skus", sa.Column("product_variant_id", postgresql.UUID(as_uuid=True), nullable=True)
    )
    op.create_foreign_key(
        "fk_skus_product_variant",
        "skus",
        "product_variants",
        ["product_variant_id"],
        ["id"],
        ondelete="RESTRICT",
    )

    canonical_role = sa.Enum(
        "STORE_ASSOCIATE",
        "STORE_MANAGER",
        "CORPORATE_USER",
        "TENANT_ADMIN",
        name="canonical_identity_role",
        native_enum=False,
        create_constraint=True,
    )
    op.create_table(
        "user_roles",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("role", canonical_role, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("tenant_id", "user_id", "role", name="pk_user_roles"),
    )
    op.create_index("ix_user_roles_tenant_role", "user_roles", ["tenant_id", "role"])
    op.create_table(
        "user_store_assignments",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("user_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("store_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["store_id"], ["stores.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint(
            "tenant_id", "user_id", "store_id", name="pk_user_store_assignments"
        ),
    )
    op.create_index(
        "ix_user_store_assignments_store",
        "user_store_assignments",
        ["tenant_id", "store_id"],
    )
    op.create_table(
        "rfid_tags",
        sa.Column("tenant_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("epc", sa.String(128), nullable=False),
        sa.Column("sku_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_import_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("active", sa.Boolean(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.ForeignKeyConstraint(["source_import_id"], ["catalog_imports.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["sku_id"], ["skus.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenants.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("tenant_id", "epc", name="pk_rfid_tags"),
    )
    op.create_index("ix_rfid_tags_tenant_sku", "rfid_tags", ["tenant_id", "sku_id"])

    op.execute(
        """
        INSERT INTO products
            (id, tenant_id, style_code, name, category, attributes, active, created_at, updated_at)
        SELECT id, tenant_id, code, name,
               COALESCE(NULLIF(attributes->>'category', ''), 'UNCATEGORIZED'),
               attributes, active, created_at, updated_at
        FROM product_styles
        ON CONFLICT (tenant_id, style_code) DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO product_variants
            (id, tenant_id, product_id, color, attributes, active, created_at, updated_at)
        SELECT gen_random_uuid(), sku.tenant_id, sku.product_style_id, sku.color,
               '{}'::jsonb, bool_or(sku.active), min(sku.created_at), max(sku.updated_at)
        FROM skus AS sku
        GROUP BY sku.tenant_id, sku.product_style_id, sku.color
        ON CONFLICT (tenant_id, product_id, color) DO NOTHING
        """
    )
    op.execute(
        """
        UPDATE skus AS sku
        SET product_variant_id = variant.id
        FROM product_variants AS variant
        WHERE variant.tenant_id = sku.tenant_id
          AND variant.product_id = sku.product_style_id
          AND variant.color = sku.color
        """
    )
    op.execute(
        """
        INSERT INTO rfid_tags
            (tenant_id, epc, sku_id, source_import_id, active, created_at, updated_at)
        SELECT DISTINCT ON (tenant_id, epc)
               tenant_id, epc, sku_id, source_import_id, true, created_at, updated_at
        FROM epc_bindings
        WHERE effective_to IS NULL
        ORDER BY tenant_id, epc, effective_from DESC
        """
    )
    op.execute(
        """
        INSERT INTO user_roles (tenant_id, user_id, role)
        SELECT DISTINCT tenant_id, user_id,
               CASE role
                 WHEN 'CORPORATE_ADMIN' THEN 'TENANT_ADMIN'
                 WHEN 'STORE_MANAGER' THEN 'STORE_MANAGER'
                 ELSE 'STORE_ASSOCIATE'
               END
        FROM user_access_grants
        ON CONFLICT DO NOTHING
        """
    )
    op.execute(
        """
        INSERT INTO user_store_assignments (tenant_id, user_id, store_id)
        SELECT DISTINCT tenant_id, user_id, store_id
        FROM user_access_grants
        WHERE store_id IS NOT NULL
        ON CONFLICT DO NOTHING
        """
    )


def downgrade() -> None:
    op.drop_index("ix_rfid_tags_tenant_sku", table_name="rfid_tags")
    op.drop_table("rfid_tags")
    op.drop_index("ix_user_store_assignments_store", table_name="user_store_assignments")
    op.drop_table("user_store_assignments")
    op.drop_index("ix_user_roles_tenant_role", table_name="user_roles")
    op.drop_table("user_roles")
    op.drop_constraint("fk_skus_product_variant", "skus", type_="foreignkey")
    op.drop_column("skus", "product_variant_id")
    op.drop_index("ix_product_variants_tenant_product", table_name="product_variants")
    op.drop_table("product_variants")
    op.drop_index("ix_products_tenant_category", table_name="products")
    op.drop_table("products")
    op.drop_column("catalog_imports", "source_version")
    op.drop_column("catalog_imports", "object_key")
