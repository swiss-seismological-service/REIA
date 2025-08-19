import os
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
    oq_host: str = Field(default='http://localhost:8800')
    oq_user: str = Field(default='user')
    oq_password: str = Field(default='password')

    # Database Superuser
    postgres_user: str = Field(default='postgres')
    postgres_password: str = Field(default='postgres')

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

    root_path: str = Field(default='')

    allow_origins: list = Field(default=[])
    allow_origin_regex: str = Field(default='')

    @computed_field
    @property
    def db_connection_string(self) -> str:
        return f"postgresql+asyncpg://{self.db_user}:" \
            f"{self.db_password}@" \
            f"{self.postgres_host}:" \
            f"{self.postgres_port}/{self.db_name}"


class TestWebserviceSettings(WebserviceSettings):
    """Test-specific settings for webservice."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # create test database
        self.db_name = f"{self.db_name}_test"

    # use superuser to be able to create/delete test database
    db_user: str = Field(..., alias='postgres_user')
    db_password: str = Field(..., alias='postgres_password')


class TestSettings(REIASettings):
    """Test-specific settings that override database configuration."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        # create test database
        self.db_name = f"{self.db_name}_test"

        # use superuser to be able to create/delete test database
        self.db_user = self.postgres_user
        self.db_password = self.postgres_password


@lru_cache()
def get_webservice_settings():
    """Get cached webservice settings instance."""
    if os.getenv('TESTING') == '1':
        return TestWebserviceSettings()
    return WebserviceSettings()


@lru_cache()
def get_settings() -> REIASettings:
    """Get cached settings instance."""
    # Automatically use TestSettings when testing
    # set in pyproject.toml tools section
    if os.getenv('TESTING') == '1':
        return TestSettings()
    return REIASettings()
