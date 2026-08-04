import pytest
from pydantic import ValidationError

from abacus.config import Settings


def test_postgresql_urls_use_the_installed_psycopg_driver() -> None:
    settings = Settings(
        database_url="postgresql://runtime@example.test/abacus",
        migration_database_url="postgresql://owner@example.test/abacus",
    )

    assert settings.database_url == "postgresql+psycopg://runtime@example.test/abacus"
    assert settings.migration_database_url == "postgresql+psycopg://owner@example.test/abacus"
    assert settings.alembic_database_url == settings.migration_database_url


def test_optional_migration_url_and_render_build_sha_are_normalized() -> None:
    settings = Settings(
        database_url="postgresql+psycopg://runtime@example.test/abacus",
        migration_database_url=None,
        build_sha="local",
        RENDER_GIT_COMMIT="0123456789abcdef",
    )

    assert settings.migration_database_url is None
    assert settings.alembic_database_url == settings.database_url
    assert settings.build_sha == "0123456789abcdef"


def test_connectivity_windows_must_be_ordered() -> None:
    with pytest.raises(ValidationError, match="STALE_WINDOW_SECONDS must exceed"):
        Settings(
            connectivity_live_window_seconds=120,
            connectivity_stale_window_seconds=120,
        )


@pytest.mark.parametrize(
    ("jwt_secret", "platform_key", "message"),
    [
        (
            "change-before-deploy-" + "x" * 20,
            "secure-platform-key-" + "x" * 20,
            "JWT_SECRET",
        ),
        (
            "secure-jwt-secret-" + "x" * 20,
            "replace-with-secure-platform-key-" + "x" * 10,
            "PLATFORM_API_KEY",
        ),
    ],
)
def test_production_rejects_placeholder_secrets(
    jwt_secret: str,
    platform_key: str,
    message: str,
) -> None:
    with pytest.raises(ValidationError, match=message):
        Settings(
            app_env="production",
            jwt_secret=jwt_secret,
            platform_api_key=platform_key,
        )
