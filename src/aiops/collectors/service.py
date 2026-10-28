"""Shared collection service, callable from CLI and future API/worker layers."""

import re
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from aiops.collectors.config import CollectionConfig
from aiops.collectors.docker import DockerCollector
from aiops.collectors.files import DirectoryCollector, FileCollector, ScanError
from aiops.collectors.models import Batch, Coverage, Failure, TimeRange
from aiops.collectors.parse import normalize
from aiops.collectors.redact import mask
from aiops.collectors.store import BatchStore, collector_lock
from aiops.collectors.system import SystemCollector

IMPORTANT = {"WARNING", "ERROR", "FATAL", "CRITICAL"}


def since_duration(value: str) -> timedelta:
    match = re.fullmatch(r"([1-9]\d*)(m|h|d)", value)
    if not match:
        raise ValueError("since must be a positive duration such as 30m, 24h, 7d")
    return timedelta(seconds=int(match[1]) * {"m": 60, "h": 3600, "d": 86400}[match[2]])


def summarize(events, coverage, metrics, failures, config, batch_id, started, ended):
    errors, http, groups = defaultdict(Counter), {}, {}
    per_service = defaultdict(list)
    for event in events:
        per_service[event.service].append(event)
        if event.level in IMPORTANT:
            errors[event.service][event.level] += 1
            signature = re.sub(r"\b\d+\b", "#", event.message)
            key = (event.service, event.level, signature)
            if key not in groups:
                groups[key] = {
                    "service": event.service,
                    "level": event.level,
                    "count": 0,
                    "message": event.message[:1000],
                    "evidence_ids": [],
                }
            groups[key]["count"] += 1
            if len(groups[key]["evidence_ids"]) < 3:
                groups[key]["evidence_ids"].append(event.event_id)
    representatives = []
    for service, records in per_service.items():
        statuses = Counter(
            str(e.metadata["http_status"])
            for e in records
            if "http_status" in e.metadata
        )
        latencies = [
            e.metadata["response_time_ms"]
            for e in records
            if "response_time_ms" in e.metadata
        ]
        http[service] = {
            "status_counts": dict(statuses),
            "4xx": sum(n for status, n in statuses.items() if status.startswith("4")),
            "5xx": sum(n for status, n in statuses.items() if status.startswith("5")),
            "latency_samples": len(latencies),
            "mean_response_time_ms": sum(latencies) / len(latencies)
            if latencies
            else None,
            "max_response_time_ms": max(latencies) if latencies else None,
        }
        selected = set()
        # Sample distinct error groups before repeated occurrences, then neighboring context.
        unique = set()
        for index, event in enumerate(records):
            if event.level in IMPORTANT:
                key = (event.level, re.sub(r"\b\d+\b", "#", event.message))
                if (
                    key not in unique
                    and len(selected) < config.representatives_per_service
                ):
                    selected.add(index)
                    unique.add(key)
        anchors = sorted(selected)
        important_indexes = [
            i for i, event in enumerate(records) if event.level in IMPORTANT
        ]
        if important_indexes:
            normal_before = next(
                (
                    i
                    for i in range(important_indexes[0] - 1, -1, -1)
                    if records[i].level not in IMPORTANT
                ),
                None,
            )
            normal_after = next(
                (
                    i
                    for i in range(important_indexes[-1] + 1, len(records))
                    if records[i].level not in IMPORTANT
                ),
                None,
            )
            for neighbor in (normal_before, normal_after):
                if (
                    neighbor is not None
                    and len(selected) < config.representatives_per_service
                ):
                    selected.add(neighbor)
        for index in anchors:
            for neighbor in (index - 1, index + 1):
                if (
                    0 <= neighbor < len(records)
                    and len(selected) < config.representatives_per_service
                ):
                    selected.add(neighbor)
        if not selected:
            selected.update(
                range(min(len(records), config.representatives_per_service))
            )
        representatives.extend(records[index] for index in sorted(selected))
    timestamps = [event.timestamp for event in events if event.timestamp]
    timestamps.extend(metric.timestamp for metric in metrics)
    batch = Batch(
        batch_id=batch_id,
        time_range=TimeRange(
            start=min(timestamps, default=started), end=max(timestamps, default=ended)
        ),
        collected_at=ended,
        coverage=coverage,
        error_counts={
            service: dict(errors[service])
            for service in coverage
            if service not in {"system", "docker"}
        },
        http_summary=http,
        error_groups=sorted(groups.values(), key=lambda item: -item["count"])[:100],
        events=representatives,
        metrics=metrics,
        collection_failures=failures,
        totals={
            "events": len(events),
            "representative_events": len(representatives),
            "bytes_read": sum(item.bytes_read for item in coverage.values()),
            "parse_failed": sum(bool(e.metadata["parse_failed"]) for e in events),
        },
        limitations=[
            "Masking is rule-based and cannot guarantee removal of all sensitive data.",
            "Only newly read bounded log windows are summarized; multiline traces may cross batch boundaries.",
            "Naive timestamps use each source timezone; records without a timestamp are retained.",
            "since filters newly read records only; unknown timestamps are retained and checkpoints advance past filtered records.",
        ],
    )
    # Keep snapshots consumable by the Phase 1 default 100k input limit.
    trimmed = False
    while len(batch.model_dump_json().encode("utf-8")) > config.max_snapshot_bytes:
        trimmed = True
        if batch.events:
            # Drop normal context first, retaining errors as long as possible.
            index = next(
                (
                    i
                    for i, event in enumerate(batch.events)
                    if event.level not in IMPORTANT
                ),
                -1,
            )
            batch.events.pop(index)
        elif batch.error_groups:
            batch.error_groups.pop()
        elif batch.metrics:
            batch.metrics.pop()
        else:
            raise ValueError("snapshot metadata exceeds configured limit")
    if trimmed:
        batch.limitations.append(
            "Snapshot size limit omitted some representatives/groups/metrics; events.jsonl retains all collected events."
        )
        # Reserve space for the added limitation too.
        while len(batch.model_dump_json().encode("utf-8")) > config.max_snapshot_bytes:
            if batch.events:
                batch.events.pop()
            elif batch.error_groups:
                batch.error_groups.pop()
            elif batch.metrics:
                batch.metrics.pop()
            else:
                raise ValueError("snapshot metadata exceeds configured limit")
    batch.totals["representative_events"] = len(batch.events)
    return batch


class CollectionService:
    def __init__(
        self, config: CollectionConfig, data_dir: Path, *, system=None, docker=None
    ):
        self.config, self.data_dir = config, data_dir.resolve() / "collection"
        self.system = system or SystemCollector()
        self.docker = docker or DockerCollector()

    def collect(self, services=None, *, since=None, backfill=False):
        selected = list(services) if services else list(self.config.sources)
        if set(selected) - self.config.sources.keys():
            raise ValueError("unknown service")
        for source in self.config.sources.values():
            if source.type == "directory" and (
                source.path.resolve() == self.data_dir
                or (source.recursive and source.path.resolve() in self.data_dir.parents)
            ):
                raise ValueError(
                    "collector data directory cannot be inside a scanned source"
                )
        cutoff = datetime.now(timezone.utc) - since_duration(since) if since else None
        with collector_lock(self.data_dir):
            store = BatchStore(self.data_dir)
            try:
                store.recover()
                return self._collect(store, selected, cutoff, backfill)
            finally:
                store.close()

    def _collect(self, store, selected, cutoff, backfill):
        config = self.config
        started = datetime.now(timezone.utc)
        deadline = time.monotonic() + config.timeout_seconds
        remaining = int(config.max_total_read_mb * 1024 * 1024)
        batch_id = "batch-" + uuid4().hex
        events, checkpoints, failures, coverage = [], [], [], {}
        physical_lines, files = 0, 0
        enabled = [name for name in selected if config.sources[name].enabled]
        for name in selected:
            source = config.sources[name]
            if not source.enabled:
                coverage[name] = Coverage(status="disabled")
                continue
            info = Coverage(status="collected")
            coverage[name] = info
            known = store.checkpoints(name)
            collector = (
                FileCollector if source.type == "file" else DirectoryCollector
            )(source, config)
            share = min(
                int(source.max_read_mb * 1024 * 1024), remaining // max(1, len(enabled))
            )
            enabled.remove(name)
            try:
                candidates = collector.candidates(known, deadline)
            except (OSError, ScanError) as exc:
                reason = self.reason(exc)
                failures.append(
                    Failure(service=name, file=mask(str(source.path)), reason=reason)
                )
                info.status = "failed"
                continue
            warnings = list(collector.warnings)
            seen = set()
            for path, expected in candidates:
                key = expected.st_dev, expected.st_ino
                if key in seen:
                    continue
                seen.add(key)
                if (
                    share <= 0
                    or remaining <= 0
                    or files >= config.max_files
                    or physical_lines >= config.max_records
                ):
                    warnings.append("run_limit")
                    break
                if time.monotonic() >= deadline:
                    warnings.append("timeout")
                    break
                allowance = min(share, remaining)
                try:
                    result = collector.read(
                        path,
                        expected,
                        known.get(key),
                        allowance,
                        deadline,
                        backfill=backfill,
                        max_lines=config.max_records - physical_lines,
                    )
                    remaining -= result.bytes_read
                    share -= result.bytes_read
                    info.bytes_read += result.bytes_read
                    physical_lines += len(result.lines)
                    files += 1
                    info.files += 1
                    warnings.extend(result.warnings)
                    result.checkpoint["last_success"] = datetime.now(
                        timezone.utc
                    ).isoformat()
                    checkpoints.append((name, result.checkpoint))
                    for event in normalize(
                        result.lines,
                        service=name,
                        path=str(path),
                        timezone=source.timezone,
                        source_type=source.type,
                    ):
                        if cutoff and event.timestamp and event.timestamp < cutoff:
                            continue
                        if cutoff and event.timestamp is None:
                            warnings.append("since_unknown_timestamp_retained")
                        event.event_id = f"{batch_id}:evt-{len(events) + 1}"
                        events.append(event)
                        info.events += 1
                except (OSError, ScanError) as exc:
                    # A failure after a partial read must not allow exceeding the global budget.
                    remaining -= allowance
                    share -= allowance
                    files += 1
                    failures.append(
                        Failure(
                            service=name, file=mask(str(path)), reason=self.reason(exc)
                        )
                    )
                    info.status = "partial"
            info.warnings = sorted(set(warnings))
            actionable = set(info.warnings) - {
                "renamed_file_followed",
                "file_rewritten_or_truncated",
                "initial_history_skipped",
            }
            if actionable:
                info.status = "partial"
            if not candidates:
                info.status = "failed"
            if info.status in {"partial", "failed"}:
                for warning in sorted(actionable):
                    failures.append(Failure(service=name, reason=warning))
        metrics = []
        for name, enabled_flag, collect in (
            (
                "system",
                config.system.enabled,
                lambda: self.system.collect(config.system.disks),
            ),
            (
                "docker",
                config.docker.enabled,
                lambda: self.docker.collect(
                    config.docker,
                    max(
                        0.01,
                        min(config.docker.timeout_seconds, deadline - time.monotonic()),
                    ),
                ),
            ),
        ):
            if not enabled_flag:
                coverage[name] = Coverage(status="disabled")
                continue
            if time.monotonic() >= deadline:
                coverage[name] = Coverage(status="failed", warnings=["timeout"])
                failures.append(Failure(service=name, reason="timeout"))
                continue
            try:
                values, errors = collect()
                metrics.extend(values)
                failures.extend(Failure.model_validate(error) for error in errors)
                coverage[name] = Coverage(status="partial" if errors else "collected")
            except Exception:
                coverage[name] = Coverage(status="failed")
                failures.append(Failure(service=name, reason="metrics_unavailable"))
        for index, metric in enumerate(metrics, 1):
            metric.metric_id = f"{batch_id}:metric-{index}"
        batch = summarize(
            events,
            coverage,
            metrics,
            failures,
            config,
            batch_id,
            started,
            datetime.now(timezone.utc),
        )
        snapshot = store.commit(batch, events, checkpoints)
        return {
            "batch_id": batch_id,
            "snapshot": str(snapshot),
            "events_file": str(snapshot.parent / "events.jsonl"),
            "coverage": {
                name: value.model_dump() for name, value in batch.coverage.items()
            },
            "totals": batch.totals,
            "status": "partial"
            if any(item.status in {"failed", "partial"} for item in coverage.values())
            else "completed",
        }

    @staticmethod
    def reason(exc):
        if isinstance(exc, ScanError):
            return exc.reason
        if isinstance(exc, PermissionError):
            return "permission_denied"
        if isinstance(exc, FileNotFoundError):
            return "path_missing"
        return "file_io_error"
