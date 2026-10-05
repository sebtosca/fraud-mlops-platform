"""Runtime settings, read from environment variables (12-factor).

Every environment (Compose, kind, GKE) differs only in these values, never in code.
"""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All runtime configuration. Field names are the env var names."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    ENV: Literal["local", "staging", "prod"]

    # Postgres
    DATABASE_URL: str

    # Kafka
    KAFKA_BOOTSTRAP: str
    TOPIC_PREFIX: str

    # Object store (S3 API)
    S3_ENDPOINT_URL: str
    AWS_ACCESS_KEY_ID: str
    AWS_SECRET_ACCESS_KEY: SecretStr
    LAKE_BUCKET: str

    # MLflow
    MLFLOW_TRACKING_URI: str
    MODEL_NAME: str

    # HMAC key for card_hash
    CARD_HASH_KEY: SecretStr

    # Decision policy and rollout
    EXPLORATION_RATE: float = Field(ge=0.0, le=1.0)
    CANARY_PCT: int = Field(ge=0, le=100)

    # Simulated clock: sim seconds per wall second
    SPEEDUP: float = Field(gt=0.0)

    # Case-summary LLM (OpenAI-compatible API)
    LLM_BASE_URL: str

    REGION: str = Field(default="us-east-1", min_length=1, max_length=20)


@lru_cache
def get_settings() -> Settings:
    """Load settings once per process. Call `get_settings.cache_clear()` in tests."""
    return Settings()  # type: ignore[call-arg]  # values come from the environment
