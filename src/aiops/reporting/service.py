import hashlib
import json
from pathlib import Path

from aiops.collectors.config import load_config
from aiops.collectors.store import BatchStore, collector_lock
from aiops.data_permissions import data_directory
from aiops.providers.base import AnalysisRequest, ErrorKind, ProviderError
from aiops.reporting.daily import daily_input
from aiops.reporting.periods import for_period
from aiops.reporting.store import ReportStore, encoded


def fit_input(snapshot, limit):
    """Keep every primary daily report ID while trimming optional detail."""
    if len(encoded(snapshot).encode()) <= limit:
        return snapshot
    snapshot["limitations"].append(
        "Input size cap omitted representative detail; manifest.json retains complete batch/report lineage."
    )
    while len(encoded(snapshot).encode()) > limit:
        if snapshot.get("events"):
            snapshot["events"].pop()
        elif snapshot.get("error_groups"):
            snapshot["error_groups"].pop()
        elif snapshot.get("metrics"):
            snapshot["metrics"].pop()
        elif snapshot.get("weekly_context"):
            snapshot["weekly_context"].pop()
        else:
            children = snapshot.get("daily_reports", [])
            detailed = [child for child in children if child["findings"]]
            if detailed:
                max(detailed, key=lambda item: len(encoded(item)))["findings"].pop()
            else:
                long = [child for child in children if len(child["summary"]) > 160]
                if not long:
                    raise ValueError("report metadata exceeds configured input limit")
                child = max(long, key=lambda item: len(item["summary"]))
                child["summary"] = child["summary"][
                    : max(160, len(child["summary"]) // 2)
                ]
    return snapshot


def validate_references(result, snapshot):
    allowed = set()

    def visit(value):
        if isinstance(value, dict):
            for key, item in value.items():
                if key in {"event_id", "metric_id", "report_id"} and isinstance(
                    item, str
                ):
                    allowed.add(item)
                elif key == "evidence_ids" and isinstance(item, list):
                    allowed.update(value for value in item if isinstance(value, str))
                else:
                    visit(item)
        elif isinstance(value, list):
            for item in value:
                visit(item)

    visit(snapshot)
    if any(
        evidence not in allowed
        for finding in result.report.findings
        for evidence in finding.evidence_ids
    ):
        raise ProviderError(ErrorKind.OUTPUT)


def brief(row):
    result = json.loads(row["result_json"])["report"]
    source = json.loads(row["input_json"])
    if row["kind"] == "daily":
        levels = source["level_counts"].values()
        observed = {
            "events": source["totals"]["events"],
            "warning_events": sum(value.get("WARNING", 0) for value in levels),
            "error_events": sum(
                sum(value.get(level, 0) for level in ("ERROR", "FATAL", "CRITICAL"))
                for value in source["level_counts"].values()
            ),
            "http_4xx": sum(value["4xx"] for value in source["http_summary"].values()),
            "http_5xx": sum(value["5xx"] for value in source["http_summary"].values()),
        }
    else:
        observed = source.get("observed_totals", {})
    findings = []
    for finding in result["findings"][:3]:
        findings.append(
            {"issue": finding["issue"][:300], "hypothesis": finding["hypothesis"][:300]}
        )
    return {
        "report_id": row["report_id"],
        "observed_totals": observed,
        "date": row["period_start"],
        "revision": row["revision"],
        "coverage_complete": bool(row["coverage_complete"]),
        "summary": result["summary"][:800],
        "severity": result["severity"],
        "findings": findings,
        "limitations": [value[:200] for value in result["limitations"][:3]],
    }


class ReportingService:
    def __init__(
        self,
        settings,
        *,
        provider=None,
        sources_config=Path("config/log_sources.fixture.yaml"),
        interval_minutes=5,
        now=None,
    ):
        if not 1 <= interval_minutes <= 1440:
            raise ValueError("interval_minutes must be between 1 and 1440")
        self.settings, self.provider = settings, provider
        self.sources_config, self.interval = sources_config, interval_minutes
        self.now = now
        self.root = settings.data_dir.resolve()
        data_directory(self.root)
        self.reports = None

    def execute(self, kind, date=None, *, analyze=False):
        period = for_period(kind, date, now=self.now)
        with collector_lock(self.root / "reports"):
            self.reports = ReportStore(self.root / "reports")
            try:
                with collector_lock(self.root / "collection"):
                    collection = BatchStore(self.root / "collection")
                    try:
                        collection.recover()
                        collection.index_existing()
                        snapshot, manifest = self._input(period, collection)
                    finally:
                        collection.close()
                from aiops.providers.instructions import INSTRUCTIONS

                manifest["analysis_policy"] = {
                    "provider": "codex-cli",
                    "model": self.settings.model,
                    "instructions_sha256": hashlib.sha256(
                        INSTRUCTIONS.encode()
                    ).hexdigest(),
                    "input_contract": 2,
                }
                manifest["max_input_bytes"] = min(90000, self.settings.max_input_bytes)
                fingerprint = hashlib.sha256(encoded(manifest).encode()).hexdigest()
                snapshot["input_fingerprint"] = fingerprint
                snapshot = fit_input(snapshot, manifest["max_input_bytes"])
                row, reused = self.reports.save_prepared(
                    period,
                    fingerprint,
                    snapshot,
                    manifest,
                    snapshot["data_quality"]["complete"],
                )
                analysis_reused = bool(analyze and row["status"] == "COMPLETED")
                if analyze and row["status"] != "COMPLETED":
                    if not snapshot.get("source_batch_count", 0) and not snapshot.get(
                        "daily_reports"
                    ):
                        raise ValueError(
                            "no collected data or valid daily reports for analysis"
                        )
                    if self.provider is None:
                        raise ValueError("analysis provider required")
                    self.reports.status(row["report_id"], "RUNNING")
                    try:
                        result = self.provider.analyze(
                            AnalysisRequest(row["input_json"])
                        )
                        validate_references(result, json.loads(row["input_json"]))
                        row = self.reports.status(
                            row["report_id"], "COMPLETED", result=result.model_dump()
                        )
                    except ProviderError as exc:
                        row = self.reports.status(
                            row["report_id"], "FAILED", error=exc.kind.value
                        )
                        self.reports.export(row)
                        raise
                    except BaseException:
                        # Persist interruption without copying exception text or log contents.
                        row = self.reports.status(
                            row["report_id"], "FAILED", error="interrupted"
                        )
                        self.reports.export(row)
                        raise
                directory = self.reports.export(row)
                return {
                    "report_id": row["report_id"],
                    "type": period.kind,
                    "period": period.as_dict(),
                    "revision": row["revision"],
                    "status": row["status"],
                    "reused": reused,
                    "analysis_reused": analysis_reused,
                    "coverage_complete": bool(row["coverage_complete"]),
                    "input": str(directory / "input.json"),
                    "manifest": str(directory / "manifest.json"),
                    "result": str(directory / "result.json")
                    if row["status"] == "COMPLETED"
                    else None,
                    "data_quality": json.loads(row["input_json"])["data_quality"],
                }
            finally:
                self.reports.close()

    def list(self, kind=None):
        with collector_lock(self.root / "reports"):
            self.reports = ReportStore(self.root / "reports")
            try:
                with collector_lock(self.root / "collection"):
                    collection = BatchStore(self.root / "collection")
                    try:
                        collection.recover()
                        collection.index_existing()
                        rows = self.reports.list(kind)
                        for row in rows:
                            full = self.reports.row(row["report_id"])
                            row["coverage_complete"] = bool(row["coverage_complete"])
                            row["stale"] = not self._current(full, collection)
                            self.reports.export(full)
                        return rows
                    finally:
                        collection.close()
            finally:
                self.reports.close()

    def _daily_sources(self, period, collection):
        available, missing, outdated = [], [], []
        for day in period.days:
            row = self.reports.latest_completed("daily", day)
            if row is None:
                missing.append(day)
            elif not self._current(row, collection):
                missing.append(day)
                outdated.append(row["report_id"])
            else:
                available.append(row)
        return available, missing, outdated

    def _weekly_context(self, period, collection):
        rows = self.reports.db.execute(
            "SELECT * FROM reports WHERE kind='weekly' AND status='COMPLETED' AND period_start<? AND period_end>? ORDER BY period_start,revision DESC",
            (period.end.date().isoformat(), period.label),
        ).fetchall()
        selected = {}
        for value in rows:
            row = dict(value)
            if row["period_start"] not in selected and self._current(row, collection):
                selected[row["period_start"]] = row
        return list(selected.values())

    def _current(self, row, collection):
        manifest = json.loads(row["manifest_json"])
        if row["kind"] == "daily":
            return all(
                collection.day_version(day) == generation
                for day, generation in manifest["day_versions"].items()
            )
        period = for_period(row["kind"], row["period_start"], now=self.now)
        available, missing, outdated = self._daily_sources(period, collection)
        if manifest["daily_report_ids"] != {
            value["period_start"]: value["report_id"] for value in available
        }:
            return False
        if (
            manifest["missing_dates"] != missing
            or manifest["outdated_report_ids"] != outdated
        ):
            return False
        if row["kind"] == "monthly":
            context = self._weekly_context(period, collection)
            return manifest["weekly_context_ids"] == [
                value["report_id"] for value in context
            ]
        return True

    def _input(self, period, collection):
        if period.kind == "daily":
            config = load_config(self.sources_config)
            expected = [
                name for name, source in config.sources.items() if source.enabled
            ]
            expected += ["system"] if config.system.enabled else []
            expected += ["docker"] if config.docker.enabled else []
            return daily_input(collection, period, expected, self.interval)
        available, missing, outdated = self._daily_sources(period, collection)
        context = (
            self._weekly_context(period, collection) if period.kind == "monthly" else []
        )
        incomplete = [
            row["report_id"] for row in available if not row["coverage_complete"]
        ]
        manifest = {
            "schema_version": 1,
            "period": period.as_dict(),
            "daily_report_ids": {
                row["period_start"]: row["report_id"] for row in available
            },
            "missing_dates": missing,
            "outdated_report_ids": outdated,
            "weekly_context_ids": [row["report_id"] for row in context],
        }
        daily_briefs = [brief(row) for row in available]
        snapshot = {
            "schema_version": 1,
            "analysis_type": period.kind,
            "period": period.as_dict(),
            "data_quality": {
                "complete": not missing and not incomplete,
                "expected_daily_reports": len(period.days),
                "available_daily_reports": len(available),
                "missing_dates": missing,
                "outdated_report_ids": outdated,
                "incomplete_daily_reports": incomplete,
            },
            "daily_reports": daily_briefs,
            "observed_totals": {
                key: sum(value["observed_totals"].get(key, 0) for value in daily_briefs)
                for key in (
                    "events",
                    "warning_events",
                    "error_events",
                    "http_4xx",
                    "http_5xx",
                )
            },
            "weekly_context": [brief(row) for row in context],
            "limitations": [
                "Daily reports are the primary evidence. Weekly context is supplemental; never add its counts again.",
                "Do not infer healthy days from missing or outdated reports.",
                "Only supplied report IDs are valid evidence references for this summary.",
                "Each source report is summarized; full results and lineage remain in the report catalogue.",
            ],
        }
        return snapshot, manifest
