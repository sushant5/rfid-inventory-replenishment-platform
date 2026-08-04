import uuid
from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import Connection, Engine, create_engine, event, text
from sqlalchemy.orm import Session, SessionTransaction, sessionmaker

from abacus.config import Settings, get_settings

settings = get_settings()


def create_database_engine(runtime_settings: Settings) -> Engine:
    """Create a bounded, stale-connection-safe application engine."""

    return create_engine(
        runtime_settings.database_url,
        pool_pre_ping=True,
        pool_size=runtime_settings.database_pool_size,
        max_overflow=runtime_settings.database_pool_max_overflow,
        pool_timeout=runtime_settings.database_pool_timeout_seconds,
        pool_recycle=runtime_settings.database_pool_recycle_seconds,
        hide_parameters=True,
        future=True,
        connect_args={
            "options": (
                f"-c statement_timeout={runtime_settings.database_statement_timeout_ms} "
                f"-c lock_timeout={runtime_settings.database_lock_timeout_ms} "
                "-c idle_in_transaction_session_timeout="
                f"{runtime_settings.database_idle_transaction_timeout_ms}"
            )
        },
    )


engine = create_database_engine(settings)

TENANT_CONTEXT_KEY = "tenant_id"


class TenantSession(Session):
    """Session that can be permanently pinned to one tenant for its lifetime."""


@event.listens_for(TenantSession, "after_begin")
def _apply_transaction_tenant_context(
    session: TenantSession,
    _transaction: SessionTransaction,
    connection: Connection,
) -> None:
    """Apply tenant context to every transaction opened by a pinned session.

    PostgreSQL's ``set_config(..., true)`` is the parameter-safe equivalent of
    ``SET LOCAL``. The value therefore disappears at commit or rollback, while the
    session remains pinned so this listener can restore it for the next transaction.
    """

    tenant_id = session.info.get(TENANT_CONTEXT_KEY)
    if tenant_id is None:
        return
    if not isinstance(tenant_id, uuid.UUID):
        raise RuntimeError("TenantSession contains an invalid tenant context")
    connection.execute(
        text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
        {"tenant_id": str(tenant_id)},
    )


SessionLocal: sessionmaker[TenantSession] = sessionmaker(
    bind=engine,
    class_=TenantSession,
    expire_on_commit=False,
)


def pin_session_to_tenant(session: Session, tenant_id: uuid.UUID) -> TenantSession:
    """Bind a new session to one verified tenant before its first transaction.

    The binding is immutable. Callers must derive ``tenant_id`` from authenticated
    claims or another trusted registry lookup, never directly from an untrusted body.
    """

    if not isinstance(session, TenantSession):
        raise TypeError("tenant context requires a TenantSession")
    if not isinstance(tenant_id, uuid.UUID):
        raise TypeError("tenant_id must be a UUID")

    existing_tenant_id = session.info.get(TENANT_CONTEXT_KEY)
    if existing_tenant_id is not None:
        if existing_tenant_id != tenant_id:
            raise RuntimeError("A TenantSession cannot be rebound to another tenant")
        return session

    if session.in_transaction():
        raise RuntimeError("Tenant context must be set before the first database transaction")

    session.info[TENANT_CONTEXT_KEY] = tenant_id
    return session


def get_db() -> Generator[TenantSession]:
    """Yield a session that authentication can pin to one trusted tenant.

    Authentication and device lookup resolve tenant identity through restricted
    database functions, then pin this ``TenantSession`` before tenant-owned SQL runs.
    Platform operations use their separately authenticated control-plane paths.
    """

    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def tenant_session_scope(tenant_id: uuid.UUID) -> Generator[TenantSession]:
    """Open a tenant-pinned session for trusted application and worker code."""

    session = pin_session_to_tenant(SessionLocal(), tenant_id)
    try:
        yield session
    finally:
        session.close()
