from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional
import os


class Settings(BaseSettings):
    """Application configuration settings loaded from environment variables"""

    # Environment configuration
    environment: str = "local"  # local or production

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )

    def __init__(self, **kwargs):
        # Determine environment from ENV variable
        env = os.getenv("ENV", "local")
        kwargs.setdefault("environment", env)

        # Try to load environment-specific .env file
        env_file = f".env.{env}"
        if os.path.exists(env_file):
            # Override model_config to use environment-specific file
            self.model_config = SettingsConfigDict(
                env_file=env_file,
                env_file_encoding="utf-8",
                case_sensitive=False,
                extra="ignore"
            )

        super().__init__(**kwargs)

    # PostgreSQL Configuration
    # For local dev: see docker-compose.yml for credentials
    postgres_user: str = "postgres"
    postgres_password: str = ""  # Set via environment variable or .env.local
    postgres_host: str = "localhost"
    postgres_port: int = 5433
    postgres_db: str = "mydb"

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"

    # MinIO Configuration (S3-compatible storage)
    # For local dev: see docker-compose.yml for credentials
    minio_endpoint: str = "localhost:9000"
    minio_access_key: str = ""  # Set via environment variable or .env.local
    minio_secret_key: str = ""  # Set via environment variable or .env.local
    minio_secure: bool = False  # Use HTTPS
    minio_bucket: str = "datalake"  # Default bucket for parquet files

    # MongoDB Configuration
    # For local dev: see docker-compose.yml for credentials
    mongodb_host: str = "localhost"
    mongodb_port: int = 27017
    mongodb_user: str = ""  # Set via environment variable or .env.local
    mongodb_password: str = ""  # Set via environment variable or .env.local
    mongodb_db: str = "xflow"  # Default database

    # Trino Configuration
    trino_host: str = "localhost"
    trino_port: int = 8085
    trino_user: str = "trino"
    trino_catalog: str = "hive"  # Default catalog
    trino_schema: str = "default"  # Default schema

    @property
    def trino_url(self) -> str:
        """Construct Trino connection URL"""
        return f"trino://{self.trino_user}@{self.trino_host}:{self.trino_port}/{self.trino_catalog}/{self.trino_schema}"

    # Neo4j Configuration
    # For local dev: see docker-compose.yml for credentials
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = ""  # Set via environment variable or .env.local

    # Application Settings
    app_name: str = "XFlow"
    debug: bool = True

    # Event Processing Settings
    event_queue_url: Optional[str] = None  # For MinIO event notifications (e.g., Redis, RabbitMQ)
    metadata_file_name: str = "_metadata.json"  # Name of metadata files in MinIO

    @property
    def is_local(self) -> bool:
        """Check if running in local environment"""
        return self.environment == "local"

    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment == "production"

    def model_post_init(self, __context):
        """Adjust settings based on environment after initialization"""
        if self.is_production:
            # Override debug mode in production
            self.debug = False


# Global settings instance
settings = Settings()
