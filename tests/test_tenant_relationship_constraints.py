import uuid
from collections.abc import Mapping

import pytest
from sqlalchemy import Connection, Engine, text
from sqlalchemy.exc import IntegrityError


def _must_reject(connection: Connection, statement: str, parameters: Mapping[str, object]) -> None:
    with pytest.raises(IntegrityError), connection.begin_nested():
        connection.execute(text(statement), parameters)


@pytest.mark.integration
def test_postgres_rejects_cross_tenant_and_wrong_store_zone_links(
    postgres_engine: Engine,
) -> None:
    tenant_a = uuid.uuid4()
    tenant_b = uuid.uuid4()
    store_a1 = uuid.uuid4()
    store_a2 = uuid.uuid4()
    store_b = uuid.uuid4()
    zone_a1 = uuid.uuid4()
    zone_a2 = uuid.uuid4()
    zone_b = uuid.uuid4()
    device_a = uuid.uuid4()
    device_b = uuid.uuid4()
    style_a = uuid.uuid4()
    style_b = uuid.uuid4()
    sku_a = uuid.uuid4()
    user_b = uuid.uuid4()
    policy_b = uuid.uuid4()
    transition_b = uuid.uuid4()
    suffix = uuid.uuid4().hex[:12]

    with postgres_engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(
                text(
                    "INSERT INTO tenants (id, code, name, status) VALUES "
                    "(:tenant_a, :code_a, 'Constraint Tenant A', 'ACTIVE'), "
                    "(:tenant_b, :code_b, 'Constraint Tenant B', 'ACTIVE')"
                ),
                {
                    "tenant_a": tenant_a,
                    "tenant_b": tenant_b,
                    "code_a": f"constraint-a-{suffix}",
                    "code_b": f"constraint-b-{suffix}",
                },
            )
            connection.execute(
                text(
                    "INSERT INTO stores "
                    "(id, tenant_id, code, name, timezone, status, configuration) VALUES "
                    "(:store_a1, :tenant_a, 'a-1', 'A1', 'UTC', 'ACTIVE', '{}'::jsonb), "
                    "(:store_a2, :tenant_a, 'a-2', 'A2', 'UTC', 'ACTIVE', '{}'::jsonb), "
                    "(:store_b, :tenant_b, 'b-1', 'B1', 'UTC', 'ACTIVE', '{}'::jsonb)"
                ),
                {
                    "tenant_a": tenant_a,
                    "tenant_b": tenant_b,
                    "store_a1": store_a1,
                    "store_a2": store_a2,
                    "store_b": store_b,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO zones (id, tenant_id, store_id, code, name, kind) VALUES "
                    "(:zone_a1, :tenant_a, :store_a1, 'floor', 'A1 Floor', 'SALES_FLOOR'), "
                    "(:zone_a2, :tenant_a, :store_a2, 'floor', 'A2 Floor', 'SALES_FLOOR'), "
                    "(:zone_b, :tenant_b, :store_b, 'floor', 'B Floor', 'SALES_FLOOR')"
                ),
                {
                    "tenant_a": tenant_a,
                    "tenant_b": tenant_b,
                    "store_a1": store_a1,
                    "store_a2": store_a2,
                    "store_b": store_b,
                    "zone_a1": zone_a1,
                    "zone_a2": zone_a2,
                    "zone_b": zone_b,
                },
            )
            connection.execute(
                text(
                    "INSERT INTO devices "
                    "(id, tenant_id, serial_number, display_name, status) VALUES "
                    "(:device_a, :tenant_a, :serial_a, 'Device A', 'ACTIVE'), "
                    "(:device_b, :tenant_b, :serial_b, 'Device B', 'ACTIVE')"
                ),
                {
                    "tenant_a": tenant_a,
                    "tenant_b": tenant_b,
                    "device_a": device_a,
                    "device_b": device_b,
                    "serial_a": f"constraint-a-{suffix}",
                    "serial_b": f"constraint-b-{suffix}",
                },
            )
            connection.execute(
                text(
                    "INSERT INTO product_styles "
                    "(id, tenant_id, code, name, attributes, active) VALUES "
                    "(:style_a, :tenant_a, :code_a, 'Style A', '{}'::jsonb, true), "
                    "(:style_b, :tenant_b, :code_b, 'Style B', '{}'::jsonb, true)"
                ),
                {
                    "style_a": style_a,
                    "style_b": style_b,
                    "tenant_a": tenant_a,
                    "tenant_b": tenant_b,
                    "code_a": f"STYLE-A-{suffix.upper()}",
                    "code_b": f"STYLE-B-{suffix.upper()}",
                },
            )
            connection.execute(
                text(
                    "INSERT INTO skus "
                    "(id, tenant_id, product_style_id, code, upc, color, size, attributes, active) "
                    "VALUES (:sku_id, :tenant_id, :style_id, :code, :upc, 'BLACK', 'M', "
                    "'{}'::jsonb, true)"
                ),
                {
                    "sku_id": sku_a,
                    "tenant_id": tenant_a,
                    "style_id": style_a,
                    "code": f"SKU-{suffix.upper()}",
                    "upc": str(int(suffix[:10], 16)).zfill(14)[-14:],
                },
            )
            connection.execute(
                text(
                    "INSERT INTO users "
                    "(id, tenant_id, email, display_name, password_hash, status, token_version) "
                    "VALUES (:id, :tenant_id, :email, 'Tenant B User', 'not-a-real-hash', "
                    "'ACTIVE', 1)"
                ),
                {
                    "id": user_b,
                    "tenant_id": tenant_b,
                    "email": f"constraint-{suffix}@tenant-b.example",
                },
            )
            connection.execute(
                text(
                    "INSERT INTO replenishment_policies (id, tenant_id, name) "
                    "VALUES (:id, :tenant_id, :name)"
                ),
                {"id": policy_b, "tenant_id": tenant_b, "name": f"Policy B {suffix}"},
            )
            connection.execute(
                text(
                    "INSERT INTO inventory_transition_outbox "
                    "(transition_id, tenant_id, epc, state_version, deltas, publish_attempts) "
                    "VALUES (:transition_id, :tenant_id, 'EPC-TENANT-B', 1, '[]'::jsonb, 0)"
                ),
                {"transition_id": transition_b, "tenant_id": tenant_b},
            )

            _must_reject(
                connection,
                "INSERT INTO skus "
                "(id, tenant_id, product_style_id, code, upc, color, size, attributes, active) "
                "VALUES (:id, :tenant_id, :style_id, :code, :upc, 'BLACK', 'L', "
                "'{}'::jsonb, true)",
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_a,
                    "style_id": style_b,
                    "code": f"SKU-CROSS-{suffix.upper()}",
                    "upc": str(int(suffix[-10:], 16) + 1).zfill(14)[-14:],
                },
            )
            _must_reject(
                connection,
                "INSERT INTO user_store_assignments (tenant_id, user_id, store_id) "
                "VALUES (:tenant_id, :user_id, :store_id)",
                {"tenant_id": tenant_a, "user_id": user_b, "store_id": store_a1},
            )
            _must_reject(
                connection,
                "INSERT INTO replenishment_policy_versions "
                "(id, tenant_id, policy_id, version_number, status) "
                "VALUES (:id, :tenant_id, :policy_id, 1, 'DRAFT')",
                {"id": uuid.uuid4(), "tenant_id": tenant_a, "policy_id": policy_b},
            )
            _must_reject(
                connection,
                "INSERT INTO business_events "
                "(id, tenant_id, store_id, source_system, external_event_id, "
                "request_fingerprint, event_type, epc, occurred_at, transition_id, "
                "state_version) VALUES "
                "(:id, :tenant_id, :store_id, 'TEST', :external_id, :fingerprint, "
                "'SALE', 'EPC-TENANT-B', now(), :transition_id, 1)",
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_a,
                    "store_id": store_a1,
                    "external_id": f"cross-transition-{suffix}",
                    "fingerprint": "b" * 64,
                    "transition_id": transition_b,
                },
            )

            # Global UUID foreign keys alone would allow this device from tenant B.
            _must_reject(
                connection,
                "INSERT INTO device_assignments "
                "(id, tenant_id, device_id, store_id, zone_id, effective_from) "
                "VALUES (:id, :tenant_id, :device_id, :store_id, :zone_id, now())",
                {
                    "id": uuid.uuid4(),
                    "tenant_id": tenant_a,
                    "device_id": device_b,
                    "store_id": store_a1,
                    "zone_id": zone_a1,
                },
            )

            wrong_location = {
                "tenant_id": tenant_a,
                "store_id": store_a1,
                "zone_id": zone_a2,
                "device_id": device_a,
                "sku_id": sku_a,
            }
            _must_reject(
                connection,
                "INSERT INTO device_assignments "
                "(id, tenant_id, device_id, store_id, zone_id, effective_from) "
                "VALUES (:id, :tenant_id, :device_id, :store_id, :zone_id, now())",
                {**wrong_location, "id": uuid.uuid4()},
            )
            _must_reject(
                connection,
                "INSERT INTO rfid_observation_batches "
                "(id, tenant_id, device_id, store_id, zone_id, status, accepted_count, "
                "processed_count, rejected_count, received_at) VALUES "
                "(:id, :tenant_id, :device_id, :store_id, :zone_id, 'ACCEPTED', 1, 0, 0, now())",
                {**wrong_location, "id": uuid.uuid4()},
            )
            _must_reject(
                connection,
                "INSERT INTO rfid_observation_events "
                "(tenant_id, event_id, payload_fingerprint, device_id, store_id, zone_id, epc, "
                "observed_at, first_received_at, rssi, reader_health, is_buffered, "
                "backlog_drained, reader_coverage_ok, processing_status) VALUES "
                "(:tenant_id, :event_id, :fingerprint, :device_id, :store_id, :zone_id, "
                "'EPC-WRONG-ZONE', now(), now(), -42, 1, false, true, true, 'PENDING')",
                {
                    **wrong_location,
                    "event_id": f"event-{suffix}",
                    "fingerprint": "f" * 64,
                },
            )
            _must_reject(
                connection,
                "INSERT INTO current_item_state "
                "(tenant_id, epc, sku_id, store_id, zone_id, last_observed_at, "
                "last_received_at, confidence, state_version) VALUES "
                "(:tenant_id, 'EPC-STATE-WRONG-ZONE', :sku_id, :store_id, :zone_id, "
                "now(), now(), 0.9, 1)",
                wrong_location,
            )
            _must_reject(
                connection,
                "INSERT INTO inventory_projection "
                "(tenant_id, store_id, sku_id, zone_id, quantity, as_of, confidence, "
                "freshness_status) VALUES "
                "(:tenant_id, :store_id, :sku_id, :zone_id, 1, now(), 0.9, 'LIVE')",
                wrong_location,
            )
            _must_reject(
                connection,
                "INSERT INTO applied_inventory_deltas "
                "(delta_id, tenant_id, store_id, sku_id, zone_id, quantity_delta) VALUES "
                "(:delta_id, :tenant_id, :store_id, :sku_id, :zone_id, 1)",
                {**wrong_location, "delta_id": f"delta-{suffix}"},
            )

            # Item state must be fully located or fully removed, never half-located.
            _must_reject(
                connection,
                "INSERT INTO current_item_state "
                "(tenant_id, epc, sku_id, store_id, zone_id, last_observed_at, "
                "last_received_at, confidence, state_version) VALUES "
                "(:tenant_id, 'EPC-HALF-LOCATED', :sku_id, :store_id, NULL, "
                "now(), now(), 0.9, 1)",
                wrong_location,
            )
        finally:
            transaction.rollback()
