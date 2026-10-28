import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from aiops.cli import main
from aiops.collectors.models import Batch, Coverage, Event, Metric, TimeRange
from aiops.collectors.store import BatchStore
from aiops.config import Settings
from aiops.providers.base import AnalysisResult, ErrorKind, ProviderError
from aiops.reporting.periods import SEOUL, day_for, for_period
from aiops.reporting.service import ReportingService, fit_input
from aiops.reporting.store import encoded

NOW = datetime(2026, 11, 2, 3, tzinfo=SEOUL)


class FakeProvider:
    def __init__(self):
        self.calls = []
        self.fail = False

    def analyze(self, request):
        self.calls.append(json.loads(request.snapshot))
        if self.fail:
            raise ProviderError(ErrorKind.NETWORK)
        return AnalysisResult(
            job_id="fake-job",
            provider="fake",
            model=None,
            thread_id="fake-thread",
            report={
                "summary": "확인된 데이터의 기간별 요약",
                "severity": "WARNING",
                "findings": [],
                "limitations": ["테스트 결과"],
                "automatic_changes_performed": False,
                "incident_analysis": {
                    "status": "INSUFFICIENT_DATA", "summary": "관측 범위 제한",
                    "findings": [], "limitations": ["일부 데이터만 관측됨"],
                },
                "performance_analysis": {
                    "status": "INSUFFICIENT_DATA", "summary": "성능 판단 근거 부족",
                    "findings": [], "limitations": ["충분한 성능 표본 없음"],
                },
            },
        )


@pytest.fixture
def reporting(tmp_path):
    sources = tmp_path / "sources.yaml"
    sources.write_text(
        yaml.safe_dump(
            {
                "sources": {"fastapi": {"type": "file", "path": "app.log"}},
                "system": {"enabled": False},
            }
        )
    )
    provider = FakeProvider()
    service = ReportingService(
        Settings(data_dir=tmp_path / "data"),
        provider=provider,
        sources_config=sources,
        interval_minutes=1440,
        now=NOW,
    )
    return service, provider


def add_batch(
    service,
    batch_id,
    events,
    *,
    collected_at=None,
    metrics=None,
    status="collected",
    legacy=False,
    selection=None,
    level="ERROR",
):
    collected_at = collected_at or datetime(2026, 10, 27, 12, tzinfo=SEOUL)
    values = [
        Event(
            event_id=f"{batch_id}:evt-{i}",
            timestamp=stamp,
            service="fastapi",
            source="file",
            level=level,
            message=f"masked error {i}",
            file="app.log",
            metadata={"parse_failed": stamp is None},
        )
        for i, stamp in enumerate(events)
    ]
    metrics = metrics or []
    batch = Batch(
        batch_id=batch_id,
        time_range=TimeRange(start=collected_at, end=collected_at),
        collected_at=collected_at,
        selection=selection or {},
        coverage={"fastapi": Coverage(status=status)},
        error_counts={},
        http_summary={},
        error_groups=[],
        events=values[:1],
        metrics=metrics[:1],
        collection_failures=[],
        totals={"events": len(values)},
        limitations=[],
    )
    store = BatchStore(service.root / "collection")
    try:
        store.commit(batch, values, [], metrics=metrics)
        if legacy:
            # Simulate on-disk Phase 2 data from before the day index/metrics artifact existed.
            (store.batches / batch_id / "metrics.jsonl").unlink()
            with store.db:
                store.db.execute(
                    "DELETE FROM indexed_batches WHERE batch_id=?", (batch_id,)
                )
                store.db.execute("DELETE FROM batch_days WHERE batch_id=?", (batch_id,))
    finally:
        store.close()


def content(result, field="input"):
    return json.loads(Path(result[field]).read_text())


def test_period_defaults_and_week_boundary():
    assert for_period("daily", now=NOW).days == ["2026-11-01"]
    assert for_period("weekly", now=NOW).days == [
        f"2026-10-{day}" for day in range(26, 32)
    ] + ["2026-11-01"]
    assert for_period("monthly", now=NOW).label == "2026-10-01"
    assert len(for_period("monthly", "2024-02-15", now=NOW).days) == 29
    december = for_period("monthly", "2025-12-25", now=NOW)
    assert december.end.isoformat().startswith("2026-01-01T00:00:00+09:00")
    assert for_period("weekly", "2026-10-28", now=NOW).label == "2026-10-26"


def test_period_rejects_open_period_and_naive_clock():
    with pytest.raises(ValueError):
        for_period("daily", "2026-11-02", now=NOW)
    with pytest.raises(ValueError):
        for_period("monthly", "2026-11-01", now=NOW)
    with pytest.raises(ValueError):
        for_period("daily", now=datetime(2026, 11, 2))


def test_midnight_utc_conversion():
    assert (
        day_for(datetime(2026, 10, 27, 14, 59, 59, tzinfo=timezone.utc)) == "2026-10-27"
    )
    assert day_for(datetime(2026, 10, 27, 15, tzinfo=timezone.utc)) == "2026-10-28"


def test_daily_uses_all_events_not_representative_snapshot(reporting):
    service, provider = reporting
    stamps = [datetime(2026, 10, 27, 10 + hour, tzinfo=SEOUL) for hour in range(3)]
    stamps.append(datetime(2026, 10, 28, tzinfo=SEOUL))
    add_batch(service, "batch-one", stamps)
    result = service.execute("daily", "2026-10-27")
    assert provider.calls == []
    assert result["status"] == "PREPARED"
    data = content(result)
    assert data["totals"]["events"] == 3
    assert data["period"]["end"] == "2026-10-28T00:00:00+09:00"
    assert data["data_quality"]["complete"] is True
    assert content(result, "manifest")["batch_ids"] == ["batch-one"]


def test_unknown_timestamp_not_assigned_a_date(reporting):
    service, _ = reporting
    add_batch(service, "batch-unknown", [None])
    result = service.execute("daily", "2026-10-27")
    data = content(result)
    assert data["totals"]["events"] == 0
    assert data["data_quality"]["unassigned_timestamp_events"] == 1
    assert result["coverage_complete"] is False


def test_metrics_are_grouped_by_measurement_time_and_preserved_fully(reporting):
    service, _ = reporting
    metrics = [
        Metric(
            metric_id=f"metric-{i}",
            timestamp=datetime(2026, 10, 27, 9 + i, tzinfo=SEOUL),
            name="cpu_percent",
            value=value,
            unit="percent",
            target="host",
        )
        for i, value in enumerate((10, 20, 30))
    ]
    add_batch(service, "batch-metrics", [], metrics=metrics)
    result = service.execute("daily", "2026-10-27")
    stats = content(result)["metrics"][0]
    assert stats["samples"] == 3 and stats["mean"] == 20
    assert stats["min"] == 10 and stats["max"] == 30


def test_same_input_reuses_version_and_ai_result(reporting):
    service, provider = reporting
    add_batch(service, "batch-one", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)])
    first = service.execute("daily", "2026-10-27", analyze=True)
    second = service.execute("daily", "2026-10-27", analyze=True)
    assert first["report_id"] == second["report_id"]
    assert second["reused"] is True
    assert len(provider.calls) == 1
    assert Path(first["result"]).is_file()


def test_late_log_creates_new_revision_and_stale_ancestors(reporting):
    service, provider = reporting
    stamp = datetime(2026, 10, 27, 10, tzinfo=SEOUL)
    add_batch(service, "batch-one", [stamp])
    daily = service.execute("daily", "2026-10-27", analyze=True)
    weekly = service.execute("weekly", "2026-10-26", analyze=True)
    monthly = service.execute("monthly", "2026-10-01", analyze=True)
    add_batch(
        service,
        "batch-late",
        [stamp + timedelta(hours=1)],
        collected_at=datetime(2026, 10, 29, 12, tzinfo=SEOUL),
    )
    stale = {row["report_id"]: row["stale"] for row in service.list()}
    assert (
        stale[daily["report_id"]]
        and stale[weekly["report_id"]]
        and stale[monthly["report_id"]]
    )
    revised = service.execute("daily", "2026-10-27", analyze=True)
    assert revised["revision"] == 2
    assert content(revised)["totals"]["events"] == 2
    assert content(daily)["totals"]["events"] == 1
    revised_week = service.execute("weekly", "2026-10-26", analyze=True)
    assert revised_week["revision"] == 2
    assert (
        content(revised_week, "manifest")["daily_report_ids"]["2026-10-27"]
        == revised["report_id"]
    )


def test_weekly_marks_missing_and_stale_days(reporting):
    service, _ = reporting
    stamp = datetime(2026, 10, 27, 10, tzinfo=SEOUL)
    add_batch(service, "batch-one", [stamp])
    daily = service.execute("daily", "2026-10-27", analyze=True)
    week = service.execute("weekly", "2026-10-26")
    data = content(week)
    assert len(data["daily_reports"]) == 1
    assert data["data_quality"]["expected_daily_reports"] == 7
    assert len(data["data_quality"]["missing_dates"]) == 6
    add_batch(service, "batch-late", [stamp])
    rebuilt = service.execute("weekly", "2026-10-26")
    data = content(rebuilt)
    assert data["daily_reports"] == []
    assert daily["report_id"] in data["data_quality"]["outdated_report_ids"]
    assert len(data["data_quality"]["missing_dates"]) == 7


def test_month_uses_daily_reports_and_weekly_only_as_context(reporting):
    service, _ = reporting
    for day in (27, 31):
        add_batch(
            service,
            f"batch-{day}",
            [datetime(2026, 10, day, 12, tzinfo=SEOUL)],
            collected_at=datetime(2026, 10, day, 13, tzinfo=SEOUL),
        )
        service.execute("daily", f"2026-10-{day}", analyze=True)
    # A completed week can span October/November; context does not change primary month inputs.
    service.execute("weekly", "2026-10-26", analyze=True)
    result = service.execute("monthly", "2026-10-01")
    data = content(result)
    assert len(data["daily_reports"]) == 2
    assert len(data["weekly_context"]) == 1
    assert data["data_quality"]["expected_daily_reports"] == 31
    assert data["period"]["end"] == "2026-11-01T00:00:00+09:00"


def test_failed_analysis_can_retry_without_recollecting(reporting):
    service, provider = reporting
    add_batch(service, "batch-one", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)])
    prepared = service.execute("daily", "2026-10-27")
    provider.fail = True
    with pytest.raises(ProviderError):
        service.execute("daily", "2026-10-27", analyze=True)
    assert service.list()[0]["status"] == "FAILED"
    provider.fail = False
    result = service.execute("daily", "2026-10-27", analyze=True)
    assert result["report_id"] == prepared["report_id"]
    assert result["status"] == "COMPLETED"


def test_old_batches_are_indexed_once_and_metrics_limit_reported(reporting):
    service, _ = reporting
    add_batch(
        service, "batch-old", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)], legacy=True
    )
    first = service.execute("daily", "2026-10-27")
    second = service.execute("daily", "2026-10-27")
    assert first["report_id"] == second["report_id"]
    assert content(first)["data_quality"]["legacy_metrics_batches"] == 1
    assert content(first)["totals"]["events"] == 1


def test_json_views_recover_from_database(reporting):
    service, _ = reporting
    add_batch(service, "batch-one", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)])
    result = service.execute("daily", "2026-10-27", analyze=True)
    Path(result["result"]).unlink()
    service.list()
    assert Path(result["result"]).is_file()


def test_model_change_creates_new_revision(reporting):
    service, _ = reporting
    add_batch(service, "batch-one", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)])
    first = service.execute("daily", "2026-10-27")
    from dataclasses import replace

    service.settings = replace(service.settings, model="different-test-model")
    second = service.execute("daily", "2026-10-27")
    assert second["revision"] == first["revision"] + 1


def test_monthly_size_limit_keeps_all_primary_report_ids():
    children = [
        {
            "report_id": f"daily-day-{i}",
            "summary": "요약" * 1000,
            "findings": [{"issue": "오류" * 500}] * 3,
        }
        for i in range(31)
    ]
    data = {
        "daily_reports": children,
        "weekly_context": [],
        "limitations": ["Monthly input"],
    }
    result = fit_input(data, 20000)
    assert len(encoded(result).encode()) <= 20000
    assert len(result["daily_reports"]) == 31


def test_report_prepare_cli_never_calls_codex(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("AIOPS_DATA_DIR", str(tmp_path))
    provider = FakeProvider()
    # Historical empty period is valid to prepare but explicitly incomplete.
    code = main(
        ["report", "prepare", "--type", "daily", "--date", "2020-01-01"],
        provider=provider,
    )
    assert code == 10
    data = json.loads(capsys.readouterr().out)
    assert data["status"] == "PREPARED" and data["coverage_complete"] is False
    assert provider.calls == []


def test_full_week_and_month_require_every_daily_report(reporting):
    service, _ = reporting
    for day in range(1, 32):
        stamp = datetime(2026, 10, day, 12, tzinfo=SEOUL)
        add_batch(service, f"batch-{day}", [stamp], collected_at=stamp)
        daily = service.execute("daily", f"2026-10-{day:02d}", analyze=True)
        assert daily["coverage_complete"] is True
    # Oct 19-25 is fully inside the month.
    weekly = service.execute("weekly", "2026-10-19", analyze=True)
    assert weekly["coverage_complete"] is True
    assert len(content(weekly)["daily_reports"]) == 7
    monthly = service.execute("monthly", "2026-10-01", analyze=True)
    assert monthly["coverage_complete"] is True
    assert len(content(monthly)["daily_reports"]) == 31
    assert not content(monthly)["data_quality"]["missing_dates"]
    assert content(weekly)["observed_totals"]["events"] == 7
    assert content(monthly)["observed_totals"]["events"] == 31


def test_invalid_evidence_is_not_persisted_as_success(reporting):
    service, _ = reporting
    add_batch(service, "batch-one", [datetime(2026, 10, 27, 10, tzinfo=SEOUL)])

    class InvalidProvider(FakeProvider):
        def analyze(self, request):
            result = super().analyze(request)
            from aiops.providers.base import Finding

            result.report.findings = [
                Finding(
                    issue="test",
                    evidence_ids=["fabricated-reference"],
                    hypothesis="test",
                    recommended_checks=[],
                )
            ]
            return result

    service.provider = InvalidProvider()
    with pytest.raises(ProviderError) as error:
        service.execute("daily", "2026-10-27", analyze=True)
    assert error.value.kind == ErrorKind.OUTPUT
    assert service.list()[0]["status"] == "FAILED"


def test_snapshot_without_metrics_file_is_only_legacy_fallback(reporting):
    service, _ = reporting
    stamp = datetime(2026, 10, 27, 12, tzinfo=SEOUL)
    metric = Metric(
        metric_id="metric-a",
        timestamp=stamp,
        name="memory_percent",
        value=50,
        unit="percent",
        target="host",
    )
    add_batch(service, "batch-old", [stamp], metrics=[metric], legacy=True)
    result = service.execute("daily", "2026-10-27")
    assert content(result)["metrics"][0]["mean"] == 50
    assert result["coverage_complete"] is False


def test_partial_collection_cannot_be_called_complete(reporting):
    service, _ = reporting
    add_batch(
        service,
        "batch-partial",
        [datetime(2026, 10, 27, 12, tzinfo=SEOUL)],
        status="partial",
    )
    result = service.execute("daily", "2026-10-27")
    assert result["coverage_complete"] is False
    assert (
        result["data_quality"]["missing_or_unsuccessful_slots_by_service"]["fastapi"]
        == 1
    )


def test_excluded_logs_still_mark_original_day_incomplete(reporting):
    service, _ = reporting
    add_batch(
        service,
        "batch-filtered",
        [],
        collected_at=datetime(2026, 10, 29, 12, tzinfo=SEOUL),
        selection={
            "since_cutoff": datetime(2026, 10, 29, tzinfo=SEOUL),
            "excluded_by_day": {"2026-10-27": 3},
        },
    )
    result = service.execute("daily", "2026-10-27")
    assert result["coverage_complete"] is False
    assert content(result)["data_quality"]["excluded_by_collection_time_filter"] == 3
    assert content(result, "manifest")["batch_ids"] == ["batch-filtered"]


def test_unclassified_levels_are_not_reported_as_healthy(reporting):
    service, _ = reporting
    add_batch(
        service,
        "batch-unknown-level",
        [datetime(2026, 10, 27, 12, tzinfo=SEOUL)],
        level="UNKNOWN",
    )
    result = service.execute("daily", "2026-10-27")
    assert result["coverage_complete"] is False
    assert content(result)["data_quality"]["unclassified_level_events"] == 1
