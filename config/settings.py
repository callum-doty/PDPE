"""
Configuration settings for the PPM (Psychographic Prediction Machine) system.
Contains database connections, API keys, and other runtime settings.
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database configuration settings."""

    host: str = "localhost"
    port: int = 5432
    database: str = "ppm"
    username: str = "postgres"
    password: str = ""

    @property
    def connection_string(self) -> str:
        """Generate PostgreSQL connection string."""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass
class APIConfig:
    """API configuration settings."""

    google_places_api_key: Optional[str] = None
    predicthq_api_key: Optional[str] = None
    openweather_api_key: Optional[str] = None
    mapbox_api_key: Optional[str] = None

    def __post_init__(self):
        """Load API keys from environment variables."""
        self.google_places_api_key = os.getenv("GOOGLE_PLACES_API_KEY")
        self.predicthq_api_key = os.getenv("PREDICTHQ_API_KEY")
        self.openweather_api_key = os.getenv("OPENWEATHER_API_KEY")
        self.mapbox_api_key = os.getenv("MAPBOX_API_KEY")


@dataclass
class MLConfig:
    """Machine learning configuration settings."""

    model_path: str = "models/"
    feature_cache_hours: int = 1
    prediction_cache_minutes: int = 15
    batch_size: int = 1000
    max_training_samples: int = 100000
    validation_split: float = 0.2
    random_seed: int = 42


@dataclass
class AppConfig:
    """Application configuration settings."""

    debug: bool = False
    log_level: str = "INFO"
    host: str = "0.0.0.0"
    port: int = 8000
    cors_origins: list = None

    def __post_init__(self):
        """Set default CORS origins if not provided."""
        if self.cors_origins is None:
            self.cors_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

        # Load from environment
        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.host = os.getenv("HOST", "0.0.0.0")
        self.port = int(os.getenv("PORT", "8000"))


class Settings:
    """Main settings class that combines all configuration."""

    def __init__(self):
        self.database = DatabaseConfig()
        self.api = APIConfig()
        self.ml = MLConfig()
        self.app = AppConfig()

        # Load environment-specific overrides
        self._load_environment_overrides()

    def _load_environment_overrides(self):
        """Load configuration overrides from environment variables."""
        # Database overrides
        if os.getenv("DATABASE_URL"):
            # Parse DATABASE_URL if provided (common in cloud deployments)
            db_url = os.getenv("DATABASE_URL")
            # Simple parsing - in production would use urllib.parse
            if "postgresql://" in db_url:
                self.database.host = os.getenv("DB_HOST", self.database.host)
                self.database.port = int(os.getenv("DB_PORT", str(self.database.port)))
                self.database.database = os.getenv("DB_NAME", self.database.database)
                self.database.username = os.getenv("DB_USER", self.database.username)
                self.database.password = os.getenv(
                    "DB_PASSWORD", self.database.password
                )

        # ML overrides
        self.ml.model_path = os.getenv("MODEL_PATH", self.ml.model_path)
        self.ml.batch_size = int(os.getenv("ML_BATCH_SIZE", str(self.ml.batch_size)))

    @property
    def is_development(self) -> bool:
        """Check if running in development mode."""
        return (
            self.app.debug or os.getenv("ENVIRONMENT", "development") == "development"
        )

    @property
    def is_production(self) -> bool:
        """Check if running in production mode."""
        return os.getenv("ENVIRONMENT") == "production"

    def get_database_url(self) -> str:
        """Get database connection URL."""
        return self.database.connection_string

    def has_api_key(self, service: str) -> bool:
        """Check if API key is available for a service."""
        api_key = getattr(self.api, f"{service}_api_key", None)
        return api_key is not None and api_key.strip() != ""


# Global settings instance
settings = Settings()


# Convenience functions for backward compatibility
def get_database_url() -> str:
    """Get database connection URL."""
    return settings.get_database_url()


def get_api_key(service: str) -> Optional[str]:
    """Get API key for a service."""
    return getattr(settings.api, f"{service}_api_key", None)


def is_development() -> bool:
    """Check if running in development mode."""
    return settings.is_development


def is_production() -> bool:
    """Check if running in production mode."""
    return settings.is_production
