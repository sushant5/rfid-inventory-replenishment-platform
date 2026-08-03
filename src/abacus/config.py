from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from abacus import API_TITLE


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = API_TITLE
    app_env: Literal["development", "test", "production"] = "development"
    database_url: str = "postgresql+psycopg://abacus:abacus@localhost:5432/abacus"
    migration_database_url: str | None = None
    application_database_role: str = "abacus_app"
    jwt_secret: str = Field(
        default="local-development-secret-change-before-deploy",
        min_length=32,
    )
    jwt_issuer: str = "abacus-assignment"
    jwt_audience: str = "abacus-api"
    access_token_minutes: int = Field(default=15, ge=1, le=1440)
    login_throttle_enabled: bool = True
    login_throttle_window_seconds: int = Field(default=60, ge=1, le=3600)
    login_throttle_ip_limit: int = Field(default=30, ge=1, le=10_000)
    login_throttle_account_limit: int = Field(default=5, ge=1, le=1000)
    login_throttle_max_entries: int = Field(default=10_000, ge=2, le=1_000_000)
    platform_api_key: str = Field(
        default="local-platform-key-change-before-deploy",
        min_length=24,
    )
    worker_poll_interval_ms: int = Field(default=500, ge=50, le=60_000)
    worker_lease_seconds: int = Field(default=30, ge=5, le=3600)
    worker_max_attempts: int = Field(default=5, ge=1, le=100)
    rfid_move_confirmation_reads: int = Field(default=3, ge=1, le=10)
    rfid_move_confirmation_window_seconds: int = Field(default=10, ge=1, le=300)
    rfid_removal_timeout_seconds: int = Field(default=1800, ge=60, le=604_800)
    rfid_removal_sweep_interval_seconds: int = Field(default=60, ge=1, le=3600)
    rfid_last_seen_flush_seconds: int = Field(default=30, ge=1, le=3600)
    connectivity_live_window_seconds: int = Field(default=120, ge=10, le=3600)
    connectivity_stale_window_seconds: int = Field(default=600, ge=30, le=86_400)
    replenishment_minimum_confidence: float = Field(default=0.70, ge=0.0, le=1.0)
    rfid_max_future_skew_seconds: int = Field(default=300, ge=0, le=86_400)
    build_sha: str = "local"
    render_git_commit: str | None = Field(
        default=None,
        validation_alias="RENDER_GIT_COMMIT",
        exclude=True,
    )

    @field_validator("database_url")
    @classmethod
    def select_psycopg_driver(cls, value: str) -> str:
        if value.startswith("postgresql://"):
            return value.replace("postgresql://", "postgresql+psycopg://", 1)
        return value

    @model_validator(mode="after")
    def reject_demo_secrets_in_production(self) -> "Settings":
        if self.connectivity_stale_window_seconds <= self.connectivity_live_window_seconds:
            raise ValueError(
                "CONNECTIVITY_STALE_WINDOW_SECONDS must exceed CONNECTIVITY_LIVE_WINDOW_SECONDS"
            )
        if self.build_sha == "local" and self.render_git_commit:
            self.build_sha = self.render_git_commit
        if self.app_env == "production":
            insecure_markers = ("local-", "change-before-deploy", "replace-with")
            if any(marker in self.jwt_secret for marker in insecure_markers):
                raise ValueError("JWT_SECRET must be replaced in production")
            if any(marker in self.platform_api_key for marker in insecure_markers):
                raise ValueError("PLATFORM_API_KEY must be replaced in production")
        return self

    @property
    def alembic_database_url(self) -> str:
        """Return the owner connection used only for schema migrations."""

        return self.migration_database_url or self.database_url


@lru_cache
def get_settings() -> Settings:
    return Settings()
