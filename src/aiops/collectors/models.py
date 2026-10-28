from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Event(Model):
    event_id: str
    timestamp: AwareDatetime | None
    service: str
    source: str
    level: Literal[
        "UNKNOWN", "TRACE", "DEBUG", "INFO", "WARNING", "ERROR", "FATAL", "CRITICAL"
    ]
    message: str
    file: str
    metadata: dict = Field(default_factory=dict)


class Metric(Model):
    metric_id: str
    timestamp: AwareDatetime
    name: str
    value: float | str
    unit: str
    target: str


class Failure(Model):
    service: str
    reason: str
    file: str | None = None


class Coverage(Model):
    status: Literal["collected", "partial", "failed", "disabled"]
    files: int = 0
    bytes_read: int = 0
    events: int = 0
    warnings: list[str] = Field(default_factory=list)


class TimeRange(Model):
    start: AwareDatetime
    end: AwareDatetime


class Batch(Model):
    schema_version: Literal[1] = 1
    batch_id: str
    time_range: TimeRange
    collected_at: AwareDatetime
    masked: Literal[True] = True
    coverage: dict[str, Coverage]
    error_counts: dict[str, dict[str, int]]
    http_summary: dict[str, dict]
    error_groups: list[dict]
    events: list[Event]
    metrics: list[Metric]
    collection_failures: list[Failure]
    totals: dict[str, int]
    limitations: list[str]
