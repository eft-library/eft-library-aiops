import copy
import json
from datetime import datetime
from pathlib import Path

import pytest

from aiops.providers.base import AnalysisResult, ProviderError, Report
from aiops.reporting.periods import SEOUL
from aiops.reporting.selection import balanced_groups
from aiops.reporting.service import brief, fit_input, validate_references
from aiops.reporting.store import encoded

LEGACY = {
    "summary": "기존 보고서", "severity": "WARNING", "findings": [],
    "limitations": [], "automatic_changes_performed": False,
}


def section(status="INSUFFICIENT_DATA"):
    return {"status": status, "summary": "판단 근거 부족", "findings": [], "limitations": ["일부 시간대만 관측됨"]}


def result():
    return AnalysisResult(job_id="fake", provider="fake", model=None, thread_id=None, report={
        **LEGACY, "incident_analysis": section(), "performance_analysis": section(),
    })


def snapshot():
    return {"analysis_type": "daily", "analysis_sections": ["incident_analysis", "performance_analysis"],
            "data_quality": {"complete": False}, "events": [{"event_id": "evt-1"}], "metrics": []}


def test_legacy_reports_stay_readable_and_new_daily_requires_both_sections():
    assert Report.model_validate(LEGACY).incident_analysis is None
    output = AnalysisResult(job_id="old", provider="fake", model=None, thread_id=None, report=LEGACY)
    validate_references(output, {"events": []})
    with pytest.raises(ProviderError):
        validate_references(output, snapshot())
    validate_references(result(), snapshot())


@pytest.mark.parametrize("key", ["incident_analysis", "performance_analysis"])
def test_specialist_findings_require_present_evidence(key):
    output = result()
    data = output.model_dump()
    data["report"][key] = {**section("ISSUES_OBSERVED"), "findings": [{
        "issue": "관측한 오류", "hypothesis": "원인은 미확정", "recommended_checks": [], "evidence_ids": ["invented"],
    }]}
    with pytest.raises(ProviderError):
        validate_references(AnalysisResult.model_validate(data), snapshot())
    data["report"][key]["findings"][0]["evidence_ids"] = ["evt-1"]
    validate_references(AnalysisResult.model_validate(data), snapshot())
    data["report"][key]["findings"][0]["evidence_ids"] = []
    with pytest.raises(ProviderError):
        validate_references(AnalysisResult.model_validate(data), snapshot())


def test_missing_performance_samples_and_partial_coverage_cannot_be_healthy():
    output = result()
    output.report.performance_analysis.status = "NO_ISSUES_OBSERVED"
    source = snapshot()
    with pytest.raises(ProviderError):
        validate_references(output, source)
    source["data_quality"]["complete"] = True
    with pytest.raises(ProviderError):
        validate_references(output, source)
    source["metrics"] = [{"evidence_ids": ["metric-1"], "samples": 2, "min": 65, "max": 65}]
    validate_references(output, source)
    output.report.incident_analysis.status = "ISSUES_OBSERVED"
    with pytest.raises(ProviderError):
        validate_references(output, source)


def test_input_cap_keeps_severe_service_and_metrics_amid_many_proxy_warnings():
    groups = [{"service": "npm", "level": "WARNING", "count": 200-i, "message": "warning"*150, "evidence_ids": [f"npm-{i}"]} for i in range(100)]
    groups.append({"service": "rustfs", "level": "ERROR", "count": 1, "message": "error"*250, "evidence_ids": ["rustfs-1"]})
    selected = balanced_groups(groups)
    assert any(item["service"] == "rustfs" for item in selected)
    source = {
        "analysis_type": "daily", "events": [
            {"event_id": f"{service}-{i}", "service": service, "level": "ERROR" if service=="rustfs" else "WARNING", "message": "x"*2500}
            for service in ("kafka", "npm", "rustfs") for i in range(10)
        ],
        "error_groups": selected, "metrics": [{"name": "memory_percent", "target": "host", "evidence_ids": ["metric-1"], "samples": 5}],
        "limitations": [], "totals": {"events": 2000},
    }
    fitted = fit_input(source, 6500)
    assert len(encoded(fitted).encode()) <= 6500
    assert {item["service"] for item in fitted["events"]} == {"kafka", "npm", "rustfs"}
    assert fitted["metrics"][0]["evidence_ids"] == ["metric-1"]
    assert fitted["totals"]["events"] == 2000


def test_specialist_summaries_reach_weekly_input_and_fit_monthly_budget():
    report = result().model_dump()["report"]
    source = {"level_counts": {}, "totals": {"events": 0}, "http_summary": {}}
    row = {"report_id": "daily-id", "kind": "daily", "result_json": json.dumps({"report": report}),
           "input_json": json.dumps(source), "period_start": "2026-10-27", "revision": 1, "coverage_complete": False}
    child = brief(row)
    assert child["specialist_analysis"]["performance_analysis"]["status"] == "INSUFFICIENT_DATA"
    children = []
    for i in range(31):
        item = copy.deepcopy(child)
        item["report_id"] = f"daily-{i}"
        item["summary"] = "요약"*400
        for value in item["specialist_analysis"].values():
            value["summary"] = "상세 분석"*100
        children.append(item)
    fitted = fit_input({"daily_reports": children, "weekly_context": [], "limitations": []}, 60000)
    assert len(fitted["daily_reports"]) == 31
    assert len(encoded(fitted).encode()) <= 60000
    assert all(item["specialist_analysis"]["performance_analysis"]["status"] == "INSUFFICIENT_DATA" for item in children)


@pytest.fixture
def source_reporting(tmp_path):
    from test_reporting import reporting

    return reporting.__wrapped__(tmp_path)


def test_severe_and_latency_samples_replace_repeated_warnings(source_reporting, monkeypatch):
    from test_reporting import add_batch

    from aiops.collectors.models import Metric
    from aiops.collectors.store import BatchStore

    read_events = BatchStore.read_events

    def with_latency(store, batch_id):
        for event in read_events(store, batch_id):
            if batch_id == "batch-latency":
                event.metadata["response_time_ms"] = 1200
            yield event

    monkeypatch.setattr(BatchStore, "read_events", with_latency)

    service, _ = source_reporting
    stamp = datetime(2026, 10, 27, 12, tzinfo=SEOUL)
    add_batch(service, "batch-warnings", [stamp]*10, level="WARNING")
    add_batch(service, "batch-latency", [stamp], level="INFO", metrics=[Metric(
        metric_id="metric-one", timestamp=stamp, name="memory_percent", value=65, unit="percent", target="host",
    )])
    add_batch(service, "batch-error", [stamp], level="ERROR")
    prepared = service.execute("daily", "2026-10-27")
    source = json.loads(Path(prepared["input"]).read_text())
    assert "performance_analysis" in source["analysis_sections"]
    assert any(event["level"] == "ERROR" for event in source["events"])
    assert any(event["metadata"].get("response_time_ms") == 1200 for event in source["events"])
    assert source["metrics"][0]["evidence_ids"] == ["metric-one"]
