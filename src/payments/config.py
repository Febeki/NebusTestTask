"""Validated configuration loaded from environment variables or a local .env file."""

from functools import lru_cache

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True
    )

    database_url: SecretStr

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
        except ArgumentError as exc:
            raise ValueError("DATABASE_URL must be a valid SQLAlchemy URL") from exc
        if url.drivername != "postgresql+asyncpg" or not url.database:
            raise ValueError("DATABASE_URL must use postgresql+asyncpg and specify a database")
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()


class ApiSettings(Settings):
    api_key: SecretStr

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("API_KEY must not be empty")
        return value


class WorkerSettings(Settings):
    rabbitmq_url: SecretStr
    outbox_poll_interval: float = 1.0
    webhook_timeout: float = Field(default=5, gt=0, le=60)
    retry_base_delay: float = Field(default=2, gt=0, le=60)

    @field_validator("outbox_poll_interval")
    @classmethod
    def validate_poll_interval(cls, value: float) -> float:
        if not 0 < value <= 60:
            raise ValueError("OUTBOX_POLL_INTERVAL must be between 0 and 60 seconds")
        return value
