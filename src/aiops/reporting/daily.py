import math
import re
from collections import Counter, defaultdict
from datetime import datetime

from aiops.collectors.models import Batch
from aiops.reporting.periods import day_for
from aiops.reporting.selection import IMPORTANCE, balanced_groups

IMPORTANT = {"WARNING", "ERROR", "FATAL", "CRITICAL"}


def daily_input(collection, period, expected_services, interval):
    day = period.label
    references = collection.day_batches(day)
    counts, http, groups, representatives = (
        defaultdict(Counter),
        {},
        {},
        defaultdict(list),
    )
    metric_stats, unknown = {}, 0
    failures, warnings = Counter(), Counter()
    successful_slots = defaultdict(set)
    observed_slots = set()
    batch_ids, legacy_metrics = [], []
    total_events, omitted_groups = 0, 0
    unclassified, decoding_errors = 0, 0
    excluded_by_time = 0
    for batch_id, _, _, unknown_count, observed, metrics_complete in references:
        batch_ids.append(batch_id)
        batch = Batch.model_validate_json(
            (collection.batches / batch_id / "snapshot.json").read_text()
        )
        excluded_by_time += batch.selection.excluded_by_day.get(day, 0)
        if not metrics_complete:
            legacy_metrics.append(batch_id)
        if observed:
            slot = int(
                (batch.collected_at - period.start).total_seconds() // (interval * 60)
            )
            observed_slots.add(slot)
            unknown += unknown_count
            for service, state in batch.coverage.items():
                if (
                    state.status == "collected"
                    and "initial_history_skipped" not in state.warnings
                ):
                    successful_slots[service].add(slot)
                for warning in state.warnings:
                    warnings[f"{service}:{warning}"] += 1
            for failure in batch.collection_failures:
                failures[f"{failure.service}:{failure.reason}"] += 1
        for event in collection.read_events(batch_id):
            if event.timestamp is None:
                if observed:
                    decoding_errors += int(bool(event.metadata.get("encoding_error")))
                continue
            if day_for(event.timestamp) != day:
                continue
            unclassified += int(event.level == "UNKNOWN")
            decoding_errors += int(bool(event.metadata.get("encoding_error")))
            total_events += 1
            counts[event.service][event.level] += 1
            sample = representatives[event.service]
            if len(sample) < 10:
                sample.append(event.model_dump(mode="json"))
            else:
                incoming_latency = event.metadata.get("response_time_ms") is not None
                latency_count = sum(value.get("metadata", {}).get("response_time_ms") is not None for value in sample)
                existing_latency = latency_count > 0
                candidates = [
                    i for i, value in enumerate(sample)
                    if value.get("metadata", {}).get("response_time_ms") is None
                    or latency_count > 1
                ]
                if candidates:
                    index = min(candidates, key=lambda i: IMPORTANCE.get(sample[i]["level"], 0))
                    if (incoming_latency and not existing_latency) or IMPORTANCE.get(event.level, 0) > IMPORTANCE.get(sample[index]["level"], 0):
                        sample[index] = event.model_dump(mode="json")
            if event.level in IMPORTANT:
                signature = re.sub(r"\b\d+\b", "#", event.message)
                key = (event.service, event.level, signature)
                if key in groups:
                    groups[key]["count"] += 1
                elif len(groups) < 500:
                    groups[key] = {
                        "service": event.service,
                        "level": event.level,
                        "count": 1,
                        "message": event.message[:700],
                        "evidence_ids": [event.event_id],
                    }
                else:
                    omitted_groups += 1
            stats = http.setdefault(
                event.service,
                {
                    "status_counts": Counter(),
                    "4xx": 0,
                    "5xx": 0,
                    "latency_samples": 0,
                    "latency_sum_ms": 0.0,
                    "max_response_time_ms": None,
                },
            )
            status = event.metadata.get("http_status")
            if isinstance(status, int):
                stats["status_counts"][str(status)] += 1
                stats["4xx"] += int(400 <= status < 500)
                stats["5xx"] += int(500 <= status < 600)
            latency = event.metadata.get("response_time_ms")
            if isinstance(latency, (int, float)) and math.isfinite(latency):
                stats["latency_samples"] += 1
                stats["latency_sum_ms"] += latency
                stats["max_response_time_ms"] = max(
                    stats["max_response_time_ms"] or 0, latency
                )
        for metric in collection.read_metrics(batch_id, batch):
            if day_for(metric.timestamp) != day:
                continue
            key = (metric.name, metric.unit, metric.target)
            if key not in metric_stats and len(metric_stats) >= 500:
                continue
            stats = metric_stats.setdefault(
                key,
                {
                    "name": metric.name,
                    "unit": metric.unit,
                    "target": metric.target,
                    "samples": 0,
                    "min": None,
                    "max": None,
                    "sum": 0.0,
                    "last": None,
                    "last_at": "",
                    "evidence_ids": [],
                },
            )
            stats["samples"] += 1
            if len(stats["evidence_ids"]) < 3:
                stats["evidence_ids"].append(metric.metric_id)
            if isinstance(metric.value, (int, float)):
                stats["min"] = min(
                    stats["min"] if stats["min"] is not None else metric.value,
                    metric.value,
                )
                stats["max"] = max(
                    stats["max"] if stats["max"] is not None else metric.value,
                    metric.value,
                )
                stats["sum"] += metric.value
            if not stats["last_at"] or metric.timestamp >= datetime.fromisoformat(
                stats["last_at"]
            ):
                stats["last"], stats["last_at"] = (
                    metric.value,
                    metric.timestamp.isoformat(),
                )
    for stats in metric_stats.values():
        total = stats.pop("sum")
        stats["mean"] = total / stats["samples"] if stats["min"] is not None else None
    for stats in http.values():
        stats["status_counts"] = dict(stats["status_counts"])
        total = stats.pop("latency_sum_ms")
        stats["mean_response_time_ms"] = (
            total / stats["latency_samples"] if stats["latency_samples"] else None
        )
    expected_slots = math.ceil(1440 / interval)
    missing = {
        service: expected_slots - len(successful_slots[service])
        for service in sorted(expected_services)
    }
    complete = (
        bool(expected_services)
        and all(value == 0 for value in missing.values())
        and not unknown
        and not failures
        and not legacy_metrics
        and not unclassified
        and not decoding_errors
        and not excluded_by_time
    )
    data_quality = {
        "complete": complete,
        "expected_collection_interval_minutes": interval,
        "expected_slots": expected_slots,
        "observed_slots": len(observed_slots),
        "missing_or_unsuccessful_slots_by_service": missing,
        "unassigned_timestamp_events": unknown,
        "unclassified_level_events": unclassified,
        "encoding_error_events": decoding_errors,
        "excluded_by_collection_time_filter": excluded_by_time,
        "legacy_metrics_batches": len(legacy_metrics),
        "collection_failures": dict(failures),
        "collection_warnings": dict(warnings),
    }
    manifest = {
        "schema_version": 1,
        "period": period.as_dict(),
        "batch_ids": batch_ids,
        "day_versions": {day: collection.day_version(day)},
        "policy": {
            "expected_services": sorted(expected_services),
            "interval_minutes": interval,
        },
        "legacy_metrics_batch_ids": legacy_metrics,
    }
    snapshot = {
        "schema_version": 1,
        "analysis_type": "daily",
        "analysis_sections": ["incident_analysis", "performance_analysis"],
        "period": period.as_dict(),
        "source_batch_ids": batch_ids[:64],
        "source_batch_count": len(batch_ids),
        "data_quality": data_quality,
        "totals": {"events": total_events},
        "level_counts": {key: dict(value) for key, value in counts.items()},
        "http_summary": http,
        "error_groups": balanced_groups(groups.values()),
        "events": [
            value
            for service in sorted(representatives)
            for value in representatives[service]
        ],
        "metrics": list(metric_stats.values()),
        "limitations": [
            "Calendar day uses Asia/Seoul with an exclusive end.",
            "Unknown event timestamps are excluded from daily event counts, not assigned an invented date.",
            "Metrics summarize observed samples only; they do not represent unobserved hours.",
            "Daily polling coverage is evidence of collection, not proof of zero log loss.",
            "Representative events/groups/metric series are bounded; complete lineage is in manifest.json.",
            "Rule-based masking cannot guarantee removal of every secret.",
            f"Additional untracked error occurrences after the group cap: {omitted_groups}.",
        ],
    }
    return snapshot, manifest
