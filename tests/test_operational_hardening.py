import inspect

import pytest
from pydantic import ValidationError
from sqlalchemy.pool import QueuePool

from abacus.api.routes.health import readiness
from abacus.config import Settings
from abacus.db import create_database_engine


def test_readiness_uses_fastapi_thread_pool_for_synchronous_database_work() -> None:
    assert inspect.iscoroutinefunction(readiness) is False


def test_database_engine_applies_configured_pool_limits_and_redacts_parameters() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://test:test@localhost/test",
        database_pool_size=4,
        database_pool_max_overflow=1,
        database_pool_timeout_seconds=7,
        database_pool_recycle_seconds=90,
    )

    database_engine = create_database_engine(settings)
    try:
        pool = database_engine.pool
        assert isinstance(pool, QueuePool)
        assert pool.size() == 4
        assert pool.timeout() == 7
        assert pool._max_overflow == 1
        assert pool._recycle == 90
        assert database_engine.hide_parameters is True
    finally:
        database_engine.dispose()


def test_database_pool_defaults_bound_each_hosted_process() -> None:
    settings = Settings()

    assert settings.database_pool_size == 3
    assert settings.database_pool_max_overflow == 2
    assert settings.database_pool_timeout_seconds == 10
    assert settings.database_pool_recycle_seconds == 300


def test_unobserved_threshold_must_exceed_last_seen_flush_interval() -> None:
    with pytest.raises(ValidationError, match="must exceed RFID_LAST_SEEN_FLUSH_SECONDS"):
        Settings(
            rfid_unobserved_after_seconds=60,
            rfid_last_seen_flush_seconds=60,
        )
