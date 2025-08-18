from functools import lru_cache

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # Database Credentials
    postgres_host: str
    postgres_port: str
    db_user: str
    db_password: str
    db_name: str

    # Connection Pooling
    postgres_pool_size: int = Field(default=5)
    postgres_max_overflow: int = Field(default=10)


class REIASettings(Settings):
    """Application settings with environment variable support."""

    # OpenQuake API Configuration
    oq_host: str = Field(...)
    oq_user: str = Field(...)
    oq_password: str = Field(...)

    # Database Superuser
    postgres_user: str = Field(...)
    postgres_password: str = Field(...)

    # Application Configuration
    max_processes: int = Field(default=2)

    @computed_field
    @property
    def oq_api_auth(self) -> dict[str, str]:
        """OpenQuake API authentication dictionary."""
        return {
            "username": self.oq_user,
            "password": self.oq_password
        }

    @computed_field
    @property
    def db_connection_string(self) -> str:
        """Database connection string."""
        return (
            f"postgresql+psycopg2://{self.db_user}:"
            f"{self.db_password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.db_name}"
        )


class WebserviceSettings(Settings):

    root_path: str

    allow_origins: list
    allow_origin_regex: str

    @computed_field
    @property
    def db_connection_string(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:" \
            f"{self.db_password}@" \
            f"{self.postgres_host}:" \
            f"{self.postgres_port}/{self.db_name}"


@lru_cache()
def get_webservice_settings():
    return WebserviceSettings()


@lru_cache()
def get_settings() -> REIASettings:
    """Get cached settings instance."""
    return REIASettings()


class TestSettings(REIASettings):
    """Test-specific settings that override database configuration."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_name = f"{self.db_name}_test"

    @computed_field
    @property
    def db_connection_string(self) -> str:
        """Test database connection string."""
        return (
            f"postgresql+psycopg2://{self.postgres_user}:"
            f"{self.postgres_password}@{self.postgres_host}:"
            f"{self.postgres_port}/{self.db_name}"
        )
