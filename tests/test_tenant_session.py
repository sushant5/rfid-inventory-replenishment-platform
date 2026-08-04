import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import Connection, event
from sqlalchemy.orm import Session, SessionTransaction

from abacus.api.dependencies import require_tenant_session
from abacus.db import (
    STORE_SCOPE_CONTEXT_KEY,
    TENANT_CONTEXT_KEY,
    TENANT_WIDE_STORE_SCOPE,
    TenantSession,
    _apply_transaction_tenant_context,
    get_db,
    pin_session_to_store_scope,
    pin_session_to_tenant,
    tenant_session_scope,
)


def _event_arguments() -> tuple[MagicMock, MagicMock]:
    transaction = MagicMock(spec=SessionTransaction)
    connection = MagicMock(spec=Connection)
    return transaction, connection


def test_tenant_listener_is_registered_for_tenant_sessions() -> None:
    assert event.contains(TenantSession, "after_begin", _apply_transaction_tenant_context)


def test_unpinned_session_preserves_existing_database_behavior() -> None:
    session = TenantSession()
    transaction, connection = _event_arguments()

    _apply_transaction_tenant_context(session, transaction, connection)

    connection.execute.assert_not_called()


def test_pinned_session_applies_parameterized_transaction_local_context() -> None:
    tenant_id = uuid.uuid4()
    session = pin_session_to_tenant(TenantSession(), tenant_id)
    transaction, connection = _event_arguments()

    _apply_transaction_tenant_context(session, transaction, connection)

    statement, parameters = connection.execute.call_args.args
    assert str(statement) == "SELECT set_config('app.tenant_id', :tenant_id, true)"
    assert parameters == {"tenant_id": str(tenant_id)}


def test_listener_reapplies_context_for_each_new_transaction() -> None:
    tenant_id = uuid.uuid4()
    session = pin_session_to_tenant(TenantSession(), tenant_id)
    first_transaction, connection = _event_arguments()
    second_transaction = MagicMock(spec=SessionTransaction)

    _apply_transaction_tenant_context(session, first_transaction, connection)
    _apply_transaction_tenant_context(session, second_transaction, connection)

    assert connection.execute.call_count == 2


def test_listener_applies_database_store_scope_after_tenant_context() -> None:
    tenant_id = uuid.uuid4()
    store_ids = (uuid.uuid4(), uuid.uuid4())
    session = pin_session_to_tenant(TenantSession(), tenant_id)
    pin_session_to_store_scope(session, store_ids)
    transaction, connection = _event_arguments()

    _apply_transaction_tenant_context(session, transaction, connection)

    assert connection.execute.call_count == 2
    statement, parameters = connection.execute.call_args_list[1].args
    assert str(statement) == "SELECT set_config('app.store_scope', :store_scope, true)"
    assert set(parameters["store_scope"].split(",")) == {str(store_id) for store_id in store_ids}


def test_store_scope_requires_tenant_and_is_immutable() -> None:
    store_id = uuid.uuid4()
    with pytest.raises(RuntimeError, match="tenant context"):
        pin_session_to_store_scope(TenantSession(), [store_id])

    session = pin_session_to_tenant(TenantSession(), uuid.uuid4())
    assert pin_session_to_store_scope(session, [store_id]) is session
    assert pin_session_to_store_scope(session, [store_id]) is session
    with pytest.raises(RuntimeError, match="cannot be rebound"):
        pin_session_to_store_scope(session, [uuid.uuid4()])


def test_empty_store_scope_is_valid_and_fails_closed() -> None:
    session = pin_session_to_tenant(TenantSession(), uuid.uuid4())
    pin_session_to_store_scope(session)
    transaction, connection = _event_arguments()

    _apply_transaction_tenant_context(session, transaction, connection)

    assert session.info[STORE_SCOPE_CONTEXT_KEY] == ""
    _, parameters = connection.execute.call_args_list[1].args
    assert parameters == {"store_scope": ""}


def test_tenant_binding_is_idempotent_but_cannot_be_changed() -> None:
    first_tenant_id = uuid.uuid4()
    session = pin_session_to_tenant(TenantSession(), first_tenant_id)

    assert pin_session_to_tenant(session, first_tenant_id) is session
    with pytest.raises(RuntimeError, match="cannot be rebound"):
        pin_session_to_tenant(session, uuid.uuid4())


def test_tenant_must_be_bound_before_the_first_transaction() -> None:
    session = TenantSession()
    session.begin()

    with pytest.raises(RuntimeError, match="before the first database transaction"):
        pin_session_to_tenant(session, uuid.uuid4())

    session.close()


def test_tenant_binding_rejects_unvalidated_string_ids() -> None:
    with pytest.raises(TypeError, match="must be a UUID"):
        pin_session_to_tenant(TenantSession(), "not-a-uuid")  # type: ignore[arg-type]


def test_tenant_binding_rejects_plain_sqlalchemy_sessions() -> None:
    with pytest.raises(TypeError, match="requires a TenantSession"):
        pin_session_to_tenant(Session(), uuid.uuid4())


def test_api_dependency_rejects_plain_sqlalchemy_sessions() -> None:
    with pytest.raises(RuntimeError, match="must provide a TenantSession"):
        require_tenant_session(Session())


def test_listener_fails_closed_for_corrupted_session_context() -> None:
    session = TenantSession(info={TENANT_CONTEXT_KEY: "not-a-uuid"})
    transaction, connection = _event_arguments()

    with pytest.raises(RuntimeError, match="invalid tenant context"):
        _apply_transaction_tenant_context(session, transaction, connection)

    connection.execute.assert_not_called()


def test_existing_dependency_yields_an_unpinned_tenant_session() -> None:
    dependency = get_db()
    session = next(dependency)

    assert isinstance(session, TenantSession)
    assert TENANT_CONTEXT_KEY not in session.info

    dependency.close()


def test_tenant_scope_yields_a_pinned_session() -> None:
    tenant_id = uuid.uuid4()

    with tenant_session_scope(tenant_id) as session:
        assert isinstance(session, TenantSession)
        assert session.info[TENANT_CONTEXT_KEY] == tenant_id
        assert session.info[STORE_SCOPE_CONTEXT_KEY] == TENANT_WIDE_STORE_SCOPE
