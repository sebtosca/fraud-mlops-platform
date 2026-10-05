"""Ports: the interfaces business code depends on instead of concrete infrastructure.

Adapters in `fraud.adapters` (and `fraud.simclock`, `fraud.trigger`) implement these.
Swapping an adapter is a config change; code never branches on where it runs.
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, Protocol

TriggerKind = Literal["drift", "retrain_due", "sim_week_closed", "shadow_window_done"]
ModelAlias = Literal["champion", "challenger", "previous_champion"]


class ObjectAlreadyExists(Exception):
    """A write-once key already exists in the object store."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Object already exists: {key}")
        self.key = key


class ObjectNotFound(Exception):
    """The requested key does not exist in the object store."""

    def __init__(self, key: str) -> None:
        super().__init__(f"Object not found: {key}")
        self.key = key


class ObjectStore(Protocol):
    """Key/value blob storage over a single bucket (S3 API: SeaweedFS locally, GCS later)."""

    def put_bytes(self, key: str, data: bytes, *, if_absent: bool = False) -> None:
        """Store `data` at `key`. With `if_absent`, raise `ObjectAlreadyExists` if it exists."""
        ...

    def get_bytes(self, key: str) -> bytes:
        """Return the object at `key`, or raise `ObjectNotFound`."""
        ...

    def exists(self, key: str) -> bool:
        """Return True if an object is stored at `key`."""
        ...

    def list_keys(self, prefix: str) -> Iterator[str]:
        """Yield every key that starts with `prefix`."""
        ...


class Clock(Protocol):
    """Simulated event time. Business logic asks this, never `datetime.now()`."""

    def now(self) -> datetime:
        """Return the current simulated time."""
        ...


class PipelineTrigger(Protocol):
    """Hands an event to the orchestrator (LocalTrigger outbox in Part A, Airflow in Part B)."""

    def emit(self, kind: TriggerKind, payload: Mapping[str, Any]) -> None:
        """Request a pipeline run. Must be idempotent per (kind, sim_date)."""
        ...


@dataclass(frozen=True, slots=True)
class ModelVersion:
    """One registered model version, independent of MLflow's own types."""

    name: str
    version: str
    source_uri: str
    run_id: str | None = None


class ModelRegistry(Protocol):
    """Registered models and their aliases (MLflow). Aliases are the deployment pointer."""

    def register(self, model_uri: str, *, tags: Mapping[str, str] | None = None) -> ModelVersion:
        """Register a new version of the configured model from `model_uri`."""
        ...

    def get_by_alias(self, alias: ModelAlias) -> ModelVersion | None:
        """Resolve `alias` to the version it points to, or None if it is unset."""
        ...

    def set_alias(self, alias: ModelAlias, version: str) -> None:
        """Point `alias` at `version`. Only the promotion job moves `champion`."""
        ...
