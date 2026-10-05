from collections.abc import Iterator
from pathlib import Path

import pytest
from pydantic import ValidationError

from fraud.config import Settings, get_settings

VALID_ENV = {
    "ENV": "local",
    "DATABASE_URL": "postgresql://fraud:fraud@postgres:5432/fraud_local",
    "KAFKA_BOOTSTRAP": "fraud-kafka-kafka-bootstrap:9092",
    "TOPIC_PREFIX": "local",
    "S3_ENDPOINT_URL": "http://s3:8333",
    "AWS_ACCESS_KEY_ID": "fraud",
    "AWS_SECRET_ACCESS_KEY": "s3-secret-value",
    "LAKE_BUCKET": "lake",
    "MLFLOW_TRACKING_URI": "http://mlflow:5000",
    "MODEL_NAME": "fraud-lgbm",
    "CARD_HASH_KEY": "hmac-secret-value",
    "EXPLORATION_RATE": "0.02",
    "CANARY_PCT": "10",
    "SPEEDUP": "2880",
    "LLM_BASE_URL": "http://vllm:8000/v1",
}


@pytest.fixture
def env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[pytest.MonkeyPatch]:
    """Set a full valid environment, isolated from the real shell and any real `.env`."""
    monkeypatch.chdir(tmp_path)  # no .env here, so only the variables below count
    for name in Settings.model_fields:
        monkeypatch.delenv(name, raising=False)
    for name, value in VALID_ENV.items():
        monkeypatch.setenv(name, value)
    get_settings.cache_clear()
    yield monkeypatch
    get_settings.cache_clear()


def test_loads_and_converts_types(env: pytest.MonkeyPatch) -> None:
    settings = Settings()  # type: ignore[call-arg]

    assert settings.ENV == "local"
    assert settings.EXPLORATION_RATE == 0.02
    assert settings.CANARY_PCT == 10
    assert isinstance(settings.CANARY_PCT, int)
    assert settings.SPEEDUP == 2880.0
    assert settings.REGION == "us-east-1"  # default


def test_secrets_are_hidden_but_readable(env: pytest.MonkeyPatch) -> None:
    settings = Settings()  # type: ignore[call-arg]

    assert "s3-secret-value" not in repr(settings)
    assert "hmac-secret-value" not in repr(settings)
    assert settings.AWS_SECRET_ACCESS_KEY.get_secret_value() == "s3-secret-value"
    assert settings.CARD_HASH_KEY.get_secret_value() == "hmac-secret-value"


def test_missing_required_variable_raises(env: pytest.MonkeyPatch) -> None:
    env.delenv("DATABASE_URL")

    with pytest.raises(ValidationError, match="DATABASE_URL"):
        Settings()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ENV", "production"),
        ("EXPLORATION_RATE", "1.5"),
        ("EXPLORATION_RATE", "-0.1"),
        ("CANARY_PCT", "101"),
        ("CANARY_PCT", "ten"),
        ("SPEEDUP", "0"),
    ],
)
def test_invalid_value_raises(env: pytest.MonkeyPatch, name: str, value: str) -> None:
    env.setenv(name, value)

    with pytest.raises(ValidationError, match=name):
        Settings()  # type: ignore[call-arg]


def test_reads_dotenv_file(env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    env.delenv("MODEL_NAME")
    (tmp_path / ".env").write_text("MODEL_NAME=from-dotenv\n")

    assert Settings().MODEL_NAME == "from-dotenv"  # type: ignore[call-arg]


def test_env_var_overrides_dotenv(env: pytest.MonkeyPatch, tmp_path: Path) -> None:
    (tmp_path / ".env").write_text("MODEL_NAME=from-dotenv\n")

    assert Settings().MODEL_NAME == "fraud-lgbm"  # type: ignore[call-arg]


def test_get_settings_is_cached(env: pytest.MonkeyPatch) -> None:
    first = get_settings()
    env.setenv("MODEL_NAME", "changed")

    assert get_settings() is first
    get_settings.cache_clear()
    assert get_settings().MODEL_NAME == "changed"
