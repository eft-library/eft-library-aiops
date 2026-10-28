from pathlib import Path
from typing import Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


class Defaults(StrictModel):
    enabled: bool = True
    recursive: bool = False
    include_rotated: bool = False
    max_read_mb: float = Field(default=5, gt=0, le=100)
    timezone: str = "Asia/Seoul"


class Source(Defaults):
    type: Literal["file", "directory"]
    path: Path
    pattern: str = "*.log"

    @model_validator(mode="after")
    def valid(self):
        if (
            "/" in self.pattern
            or "\\" in self.pattern
            or self.pattern in {"", ".", ".."}
        ):
            raise ValueError("pattern must match a filename only")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError:
            raise ValueError("unknown source timezone") from None
        return self


class SystemConfig(StrictModel):
    enabled: bool = True
    disks: list[str] = Field(default_factory=lambda: ["/"], max_length=16)


class DockerConfig(StrictModel):
    enabled: bool = False
    containers: list[str] = Field(default_factory=list, max_length=50)
    timeout_seconds: float = Field(default=5, gt=0, le=30)

    @model_validator(mode="after")
    def explicit_targets(self):
        if self.enabled and not self.containers:
            raise ValueError(
                "enabled Docker collection requires explicit container names"
            )
        return self


class CollectionConfig(StrictModel):
    version: Literal[1] = 1
    defaults: Defaults = Field(default_factory=Defaults)
    sources: dict[str, Source] = Field(max_length=64)
    system: SystemConfig = Field(default_factory=SystemConfig)
    docker: DockerConfig = Field(default_factory=DockerConfig)
    max_total_read_mb: float = Field(default=20, gt=0, le=200)
    initial_tail_mb: float = Field(default=1, gt=0, le=20)
    timeout_seconds: float = Field(default=60, gt=0, le=3600)
    max_files: int = Field(default=2000, gt=0, le=20000)
    max_scan_entries: int = Field(default=10000, gt=0, le=100000)
    max_line_bytes: int = Field(default=65536, ge=256, le=1048576)
    max_records: int = Field(default=20000, gt=0, le=100000)
    representatives_per_service: int = Field(default=20, ge=1, le=100)
    max_snapshot_bytes: int = Field(default=90000, ge=10000, le=1000000)


def load_config(path: Path) -> CollectionConfig:
    # YAML is data only; bounded input and safe_load reject Python constructors.
    with path.open("rb") as stream:
        raw = stream.read(1_000_001)
    if len(raw) > 1_000_000:
        raise ValueError("configuration too large")
    try:
        document = yaml.safe_load(raw)
        if not isinstance(document, dict) or not isinstance(
            document.get("sources"), dict
        ):
            raise ValueError("sources mapping required")
        defaults = Defaults.model_validate(document.get("defaults", {})).model_dump()
        document["sources"] = {
            name: {**defaults, **value} for name, value in document["sources"].items()
        }
        config = CollectionConfig.model_validate(document)
    except (yaml.YAMLError, TypeError, RecursionError, AttributeError):
        raise ValueError("invalid collection configuration") from None
    import re

    for name, source in config.sources.items():
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", name) or name in {
            "system",
            "docker",
        }:
            raise ValueError("invalid or reserved service name")
        source.path = Path(source.path).expanduser()
        if not source.path.is_absolute():
            source.path = path.resolve().parent / source.path
        # Reject broad scans and known data stores even when running as root.
        target = source.path.resolve()
        forbidden = [
            Path(root).resolve()
            for root in ("/var/lib/docker", "/home/kafka/kraft-combined-logs")
        ]
        broad = {Path(root).resolve() for root in ("/", "/home", "/var", "/var/lib")}
        pg_data = Path("/var/lib/pgsql/17/data").resolve()
        pg_logs = pg_data / "log"
        if target in broad or any(
            target == root or root in target.parents for root in forbidden
        ):
            raise ValueError("unsafe collection root")
        if (target == pg_data or pg_data in target.parents) and not (
            target == pg_logs or pg_logs in target.parents
        ):
            raise ValueError("PostgreSQL data files are not collection sources")
    return config
