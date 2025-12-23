"""Application configuration using environment variables."""

import os
from functools import lru_cache
from typing import Optional

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Database
    DATABASE_URL: str = "postgresql://kaspero:kaspero@db:5432/kaspero"

    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_KEY: str = ""  # Provided API key for external services

    # ETL Configuration
    ETL_BATCH_SIZE: int = 1000
    ETL_SCHEDULE_MINUTES: int = 5

    # Rate Limiting
    RATE_LIMIT_REQUESTS_PER_MINUTE: int = 60
    RATE_LIMIT_RETRY_MAX: int = 5
    RATE_LIMIT_BACKOFF_BASE: float = 2.0

    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "json"  # json or text

    # Data Sources
    CSV_SOURCE_PATH: str = "/app/data/source.csv"
    CSV_SOURCE_2_PATH: str = "/app/data/source2.csv"
    API_SOURCE_URL: str = "https://api.coinpaprika.com/v1"
    COINGECKO_API_URL: str = "https://api.coingecko.com/api/v3"
    RSS_SOURCE_URL: str = "https://api.coingecko.com/api/v3"  # Using CoinGecko as second source

    # Schema Drift
    SCHEMA_DRIFT_CONFIDENCE_THRESHOLD: float = 0.8

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


@lru_cache()
def get_settings() -> Settings:
    """Get cached settings instance."""
    return Settings()
