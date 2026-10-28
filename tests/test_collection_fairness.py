import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from aiops.collectors.config import CollectionConfig, Source
from aiops.collectors.parse import normalize
from aiops.collectors.service import CollectionService

LINE = "2026-10-28 09:00:00 INFO request complete\n"


def get_events(result):
    return [
        json.loads(line)
        for line in Path(result["events_file"]).read_text().splitlines()
    ]


def test_no_service_can_consume_entire_record_budget(tmp_path):
    sources = {}
    for name in (
        "frontend",
        "backend",
        "postgresql",
        "redis",
        "kafka",
        "airflow",
        "npm",
        "rustfs",
    ):
        path = tmp_path / (name + ".log")
        path.write_text(LINE * 100)
        sources[name] = Source(type="file", path=path)
    config = CollectionConfig(
        sources=sources, max_records=24, system={"enabled": False}
    )
    result = CollectionService(config, tmp_path / "data").collect()
    assert sum(value["events"] for value in result["coverage"].values()) == 24
    assert all(result["coverage"][name]["events"] == 3 for name in sources)
    assert all(result["coverage"][name]["files"] == 1 for name in sources)


def test_order_rotates_when_budget_cannot_fit_every_service(tmp_path):
    sources = {}
    for name in ("first", "second", "third"):
        path = tmp_path / (name + ".log")
        path.write_text(LINE * 10)
        sources[name] = Source(type="file", path=path)
    config = CollectionConfig(sources=sources, max_records=1, system={"enabled": False})
    service = CollectionService(config, tmp_path / "data")
    visited = {get_events(service.collect())[0]["service"] for _ in range(3)}
    assert visited == set(sources)


def test_file_limit_also_shared_between_sources(tmp_path):
    sources = {}
    for name in ("first", "second"):
        root = tmp_path / name
        root.mkdir()
        for index in range(4):
            (root / f"{index}.log").write_text(LINE)
        sources[name] = Source(type="directory", path=root)
    config = CollectionConfig(sources=sources, max_files=2, system={"enabled": False})
    result = CollectionService(config, tmp_path / "data").collect()
    assert result["coverage"]["first"]["files"] == 1
    assert result["coverage"]["second"]["files"] == 1


def test_discovery_cursor_eventually_visits_all_files(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(9):
        (root / f"{index}.log").write_text(LINE)
    config = CollectionConfig(
        sources={"airflow": Source(type="directory", path=root)},
        max_scan_entries=2,
        system={"enabled": False},
    )
    found = set()
    warnings = set()
    for _ in range(20):
        # A new service instance simulates an independent scheduled CLI execution.
        result = CollectionService(config, tmp_path / "data").collect()
        found.update(event["file"] for event in get_events(result))
        warnings.update(result["coverage"]["airflow"]["warnings"])
    assert len(found) == 9
    assert "scan_entry_limit" in warnings
    assert "scan_limit" not in warnings


def test_recursive_cursor_drains_a_small_bounded_directory_queue(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(8):
        directory = root / f"dag-{index}" / "run-1"
        directory.mkdir(parents=True)
        (directory / "task.log").write_text(LINE)
    config = CollectionConfig(
        sources={"airflow": Source(type="directory", path=root, recursive=True)},
        max_scan_entries=2,
        system={"enabled": False},
    )
    found = set()
    for _ in range(80):
        result = CollectionService(config, tmp_path / "data").collect()
        found.update(event["file"] for event in get_events(result))
    assert len(found) == 8


def test_exact_active_file_is_not_hidden_by_directory_scan_limit(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(10):
        (root / f"archived-{index}.txt").write_text("not a configured log")
    active = root / "active.log"
    active.write_text(LINE)
    config = CollectionConfig(
        sources={"fastapi": Source(type="file", path=active)},
        max_scan_entries=1,
        system={"enabled": False},
    )
    assert len(get_events(CollectionService(config, tmp_path / "data").collect())) == 1


def test_changing_selected_services_does_not_re_read_the_same_source_twice(tmp_path):
    path = tmp_path / "app.log"
    path.write_text(LINE)
    config = CollectionConfig(
        sources={"fastapi": Source(type="file", path=path)}, system={"enabled": False}
    )
    result = CollectionService(config, tmp_path / "data").collect(
        ["fastapi", "fastapi"]
    )
    assert len(get_events(result)) == 1


def parse(text, service="fastapi", timezone_name="Asia/Seoul"):
    return list(
        normalize(
            [(line, False) for line in text.splitlines()],
            service=service,
            path="sample.log",
            timezone=timezone_name,
            source_type="file",
        )
    )


def test_fastapi_trailing_latency_is_measured_in_correct_units():
    events = parse(
        '2026-10-28 08:07:07 - INFO - [REDACTED] - "GET /health HTTP/1.1" 200 - 0.011s'
    )
    assert len(events) == 1
    assert events[0].metadata["response_time_ms"] == 11
    assert events[0].timestamp.isoformat() == "2026-10-28T08:07:07+09:00"


def test_untimestamped_uvicorn_messages_stay_unassigned():
    events = parse(
        "INFO:     ('[REDACTED]', 12345) - \"WebSocket /updates\" [accepted]\nINFO:     connection open"
    )
    assert len(events) == 2
    assert all(event.timestamp is None for event in events)
    assert all(event.metadata["timestamp_status"] == "absent" for event in events)
    assert all(event.level == "INFO" for event in events)


def test_nextjs_error_object_tail_belongs_to_the_stack_trace():
    text = "⨯ TypeError: request failed\n    at handler (.next/server/test.js:1:20) {\n  digest: 'fake-digest'\n}\n✓ Ready in 100ms"
    events = parse(text, service="nextjs")
    assert len(events) == 2
    assert events[0].level == "ERROR"
    assert events[0].message.endswith("}")
    assert events[1].level == "INFO"
    assert all(event.timestamp is None for event in events)


def test_timestamped_indented_record_not_swallowed_into_trace():
    events = parse(
        '2026-10-28 09:00:00 ERROR failed\n  File "test.py"\n  [2026-10-28 09:00:01] INFO recovered'
    )
    assert len(events) == 2
    assert events[1].level == "INFO"


def test_explicit_postgresql_utc_is_not_interpreted_as_korea_time():
    events = parse(
        "2026-10-27 23:00:00.123 UTC [1] ERROR example", service="postgresql"
    )
    assert events[0].timestamp.utcoffset() == timedelta(0)
    assert events[0].timestamp.astimezone(timezone(timedelta(hours=9))).day == 28


def test_redis_native_date_and_severity():
    events = parse(
        "1234:M 28 Oct 2026 09:10:11.123 * Ready to accept connections\n1234:M 28 Oct 2026 09:10:12.123 # Memory warning",
        service="redis",
    )
    assert [event.level for event in events] == ["INFO", "WARNING"]
    assert all(event.metadata["parse_failed"] is False for event in events)


def test_nginx_error_slash_date_and_ansi_prefix():
    events = parse(
        "\x1b[31m2026/10/28 09:00:00 [error] upstream failed\x1b[0m", service="npm"
    )
    assert events[0].timestamp == datetime(
        2026, 10, 28, 9, tzinfo=timezone(timedelta(hours=9))
    )
    assert events[0].level == "ERROR"
    assert "\x1b" not in events[0].message


def test_datetime_in_a_request_path_is_not_an_event_timestamp():
    event = parse("INFO request failed at /archive/2026-10-28T09:00:00/file")[0]
    assert event.timestamp is None
    assert event.metadata["timestamp_status"] == "absent"


def test_record_budget_is_shared_within_one_directory_source(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(3):
        (root / f"proxy-{index}.log").write_text(LINE * 100)
    config = CollectionConfig(
        sources={"npm": Source(type="directory", path=root)},
        max_records=9,
        system={"enabled": False},
    )
    result = CollectionService(config, tmp_path / "data").collect()
    events = get_events(result)
    assert len(events) == 9
    assert len({event["file"] for event in events}) == 3


def test_scan_state_rolls_back_with_failed_batch_commit(tmp_path, monkeypatch):
    from aiops.collectors.store import BatchStore

    root = tmp_path / "logs"
    root.mkdir()
    for index in range(5):
        (root / f"{index}.log").write_text(LINE)
    config = CollectionConfig(
        sources={"airflow": Source(type="directory", path=root)},
        max_scan_entries=1,
        system={"enabled": False},
    )
    service = CollectionService(config, tmp_path / "data")
    original = BatchStore.commit

    def fail(self, batch, events, checkpoints, **kwargs):
        def crash():
            raise OSError("simulated storage failure")

        return original(self, batch, events, checkpoints, before_commit=crash, **kwargs)

    monkeypatch.setattr(BatchStore, "commit", fail)
    import pytest

    with pytest.raises(OSError):
        service.collect()
    store = BatchStore(service.data_dir)
    try:
        assert store.state("scan:airflow") is None
        assert store.checkpoints("airflow") == {}
    finally:
        store.close()
    monkeypatch.setattr(BatchStore, "commit", original)
    assert len(get_events(service.collect())) == 1


def test_nextjs_timestamp_wrapper_preserves_startup_levels_and_explicit_zone():
    events = parse(
        "[2026-10-28 10:45:06 +0900] $ next start\n"
        "[2026-10-28 10:45:06 +0900] ▲ Next.js example\n"
        "[2026-10-28 10:45:06 +0900] - Local: http://localhost:4002\n"
        "[2026-10-28 10:45:06 +0900] ✓ Ready in 111ms",
        service="nextjs",
        timezone_name="UTC",
    )
    assert len(events) == 4
    assert all(
        event.timestamp.isoformat() == "2026-10-28T10:45:06+09:00" for event in events
    )
    assert all(
        event.level == "INFO" and not event.metadata["parse_failed"] for event in events
    )


def test_timestamp_on_every_nextjs_trace_line_still_forms_one_error():
    events = parse(
        "[2026-10-28 10:45:06 +0900] ⨯ TypeError: example failure\n"
        "[2026-10-28 10:45:06 +0900]     at handler (example.js:1:2) {\n"
        "[2026-10-28 10:45:07 +0900]   digest: 'synthetic-value'\n"
        "[2026-10-28 10:45:07 +0900] }\n"
        "[2026-10-28 10:45:08 +0900] INFO recovered",
        service="nextjs",
    )
    assert len(events) == 2
    assert events[0].level == "ERROR" and "digest" in events[0].message
    assert events[0].timestamp.second == 6
    assert events[1].level == "INFO" and events[1].timestamp.second == 8


def test_timestamped_websocket_query_token_is_masked():
    secret = "synthetic-test-token-" * 20
    events = parse(
        f"2026-10-28 11:00:47 INFO:     ('192.0.2.4', 12345) - \"WebSocket /ws?token={secret}\" [accepted]\n"
        "2026-10-28 11:00:47 INFO:     connection open"
    )
    assert len(events) == 2
    assert all(
        event.timestamp.isoformat() == "2026-10-28T11:00:47+09:00" for event in events
    )
    assert all(not event.metadata["parse_failed"] for event in events)
    assert secret not in events[0].model_dump_json()
    assert "192.0.2.4" not in events[0].message
    assert "token=[REDACTED]" in events[0].message
