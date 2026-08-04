from abacus.config import Settings


def test_postgresql_urls_use_the_installed_psycopg_driver() -> None:
    settings = Settings(
        database_url="postgresql://runtime@example.test/abacus",
        migration_database_url="postgresql://owner@example.test/abacus",
    )

    assert settings.database_url == "postgresql+psycopg://runtime@example.test/abacus"
    assert settings.migration_database_url == "postgresql+psycopg://owner@example.test/abacus"
    assert settings.alembic_database_url == settings.migration_database_url
