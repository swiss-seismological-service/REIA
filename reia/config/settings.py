from functools import lru_cache
from pathlib import Path

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings with environment variable support."""

    model_config = SettingsConfigDict(
        env_file=Path(__file__).parent.parent.parent / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    # OpenQuake API Configuration
    oq_host: str = Field(..., description="OpenQuake API server URL")
    oq_user: str = Field(..., description="OpenQuake API username")
    oq_password: str = Field(..., description="OpenQuake API password")

    # Database Configuration
    db_user: str = Field(..., description="Database username")
    db_password: str = Field(..., description="Database password")
    db_name: str = Field(..., description="Database name")
    postgres_host: str = Field(
        default="localhost",
        description="PostgreSQL host")
    postgres_port: int = Field(default=5432, description="PostgreSQL port")
    postgres_pool_size: int = Field(
        default=5, description="PostgreSQL connection pool size")
    postgres_max_overflow: int = Field(
        default=10, description="PostgreSQL max overflow connections")

    # Application Configuration
    max_processes: int = Field(
        default=2,
        description="Maximum number of processes for multiprocessing")

    @computed_field
    @property
    def oq_api_server(self) -> str:
        """OpenQuake API server URL."""
        return self.oq_host

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


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
