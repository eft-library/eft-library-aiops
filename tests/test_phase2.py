import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from aiops.cli import main
from aiops.collectors.config import CollectionConfig, Source, load_config
from aiops.collectors.docker import collect_with_client
from aiops.collectors.models import Batch
from aiops.collectors.redact import mask
from aiops.collectors.service import CollectionService
from aiops.collectors.store import BatchStore, collector_lock
from aiops.collectors.system import SystemCollector
from aiops.providers.base import ErrorKind, ProviderError

LINE = "2026-10-28T08:31:00+09:00 ERROR database timeout\n"


def setup(tmp_path, content=LINE, **options):
    path = tmp_path / "application.log"
    path.write_text(content)
    config = CollectionConfig(
        sources={"fastapi": Source(type="file", path=path)},
        system={"enabled": False},
        **options,
    )
    service = CollectionService(config, tmp_path / "runtime")
    return path, config, service


def records(result):
    return [
        json.loads(line)
        for line in Path(result["events_file"]).read_text().splitlines()
    ]


def snapshot(result):
    return Batch.model_validate_json(Path(result["snapshot"]).read_text())


def test_incremental_restart_and_original_unchanged(tmp_path):
    path, config, service = setup(tmp_path)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    first = service.collect()
    assert len(records(first)) == 1
    assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    assert records(CollectionService(config, tmp_path / "runtime").collect()) == []
    with path.open("a") as stream:
        stream.write(LINE.replace("timeout", "new failure"))
    third = service.collect()
    assert len(records(third)) == 1 and "new failure" in records(third)[0]["message"]
    assert records(first)[0]["event_id"] != records(third)[0]["event_id"]
    assert snapshot(first).error_counts == {"fastapi": {"ERROR": 1}}


def test_rotation_drains_tracked_renamed_file(tmp_path):
    path, _, service = setup(tmp_path)
    service.collect()
    with path.open("a") as stream:
        stream.write(LINE.replace("timeout", "before rotation"))
    path.rename(tmp_path / "application.log.1")
    path.write_text(LINE.replace("timeout", "after rotation"))
    result = service.collect()
    assert len(records(result)) == 2
    assert any("before rotation" in event["message"] for event in records(result))
    assert any("after rotation" in event["message"] for event in records(result))
    assert records(service.collect()) == []


def test_truncate(tmp_path):
    path, _, service = setup(tmp_path, LINE * 20)
    service.collect()
    path.write_text(LINE.replace("timeout", "short"))
    result = service.collect()
    assert len(records(result)) == 1
    assert "file_rewritten_or_truncated" in result["coverage"]["fastapi"]["warnings"]


def test_weekday_filename_same_inode_reuse(tmp_path):
    path, config, _ = setup(tmp_path, LINE)
    weekday = tmp_path / "postgresql-Wed.log"
    path.rename(weekday)
    config.sources = {
        "postgresql": Source(
            type="directory", path=tmp_path, pattern="postgresql-*.log"
        )
    }
    service = CollectionService(config, tmp_path / "runtime")
    inode = weekday.stat().st_ino
    service.collect()
    weekday.write_text(LINE.replace("timeout", "another weekday error longer"))
    assert weekday.stat().st_ino == inode
    result = service.collect()
    assert (
        len(records(result)) == 1 and "another weekday" in records(result)[0]["message"]
    )


def test_equal_size_middle_rewrite_uses_mtime(tmp_path):
    path, _, service = setup(tmp_path, LINE * 30)
    service.collect()
    original = path.read_text()
    path.write_text(
        original[:700]
        + original[700:800].replace("timeout", "failure")
        + original[800:]
    )
    stat = path.stat()
    os.utime(path, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1000000))
    result = service.collect()
    assert len(records(result)) == 30
    assert any("failure" in event["message"] for event in records(result))


def test_gzip_bak_and_untracked_rotation_excluded(tmp_path):
    path, config, _ = setup(tmp_path)
    for suffix in (".1", ".bak", ".gz"):
        path.with_name(path.name + suffix).write_text(LINE)
    config.sources["fastapi"] = Source(
        type="directory", path=tmp_path, pattern="application.log*"
    )
    result = CollectionService(config, tmp_path / "runtime").collect()
    assert len(records(result)) == 1


def test_opt_in_uncompressed_rotated_files(tmp_path):
    path, config, _ = setup(tmp_path)
    path.with_name(path.name + ".1").write_text(LINE)
    path.with_name(path.name + ".gz").write_text(LINE)
    config.sources["fastapi"].include_rotated = True
    assert len(records(CollectionService(config, tmp_path / "runtime").collect())) == 2


def test_recursive_and_symlink_excluded(tmp_path):
    root = tmp_path / "logs"
    leaf = root / "dag_id=test" / "run_id=one" / "task_id=job"
    leaf.mkdir(parents=True)
    (leaf / "attempt=1.log").write_text(LINE)
    outside = tmp_path / "secret.log"
    outside.write_text(LINE.replace("timeout", "must not collect"))
    (root / "external.log").symlink_to(outside)
    (root / "external-directory").symlink_to(tmp_path)
    config = CollectionConfig(
        sources={"airflow": Source(type="directory", path=root, recursive=True)},
        system={"enabled": False},
    )
    result = CollectionService(config, tmp_path / "runtime").collect()
    assert len(records(result)) == 1


def test_missing_source_isolated(tmp_path):
    _, config, service = setup(tmp_path)
    config.sources["redis"] = Source(type="file", path=tmp_path / "missing.log")
    result = service.collect()
    assert result["status"] == "partial"
    assert result["coverage"]["redis"]["status"] == "failed"
    assert len(records(result)) == 1
    assert snapshot(result).collection_failures


def test_permission_error_isolated(tmp_path, monkeypatch):
    from aiops.collectors.files import FileCollector

    _, config, service = setup(tmp_path)
    config.sources["redis"] = Source(type="file", path=tmp_path / "redis.log")
    config.sources["redis"].path.write_text(LINE)
    original = FileCollector.read

    def read(self, path, *args, **kwargs):
        if path.name == "redis.log":
            raise PermissionError("private server detail")
        return original(self, path, *args, **kwargs)

    monkeypatch.setattr(FileCollector, "read", read)
    result = service.collect()
    text = Path(result["snapshot"]).read_text()
    assert "private server detail" not in text and "permission_denied" in text
    assert len(records(result)) == 1


def test_encoding_failure_flag(tmp_path):
    path, _, service = setup(tmp_path)
    path.write_bytes(LINE.rstrip().encode() + b"\xff\n")
    result = service.collect()
    assert records(result)[0]["metadata"]["encoding_error"] is True
    assert result["status"] == "partial"


def test_incomplete_line_is_retried_when_completed(tmp_path):
    path, _, service = setup(tmp_path, LINE.rstrip())
    assert not records(service.collect())
    with path.open("a") as stream:
        stream.write("\n")
    assert len(records(service.collect())) == 1
    assert not records(service.collect())


def test_streaming_budget_and_eventual_progress(tmp_path):
    _, _, service = setup(tmp_path, LINE * 100, max_total_read_mb=0.001)
    total = 0
    for _ in range(10):
        result = service.collect()
        assert result["totals"]["bytes_read"] <= int(0.001 * 1024 * 1024)
        total += len(records(result))
        if total == 100:
            break
    assert total == 100


def test_record_limit_does_not_lose_events(tmp_path):
    _, _, service = setup(tmp_path, LINE * 5, max_records=2)
    sizes = [len(records(service.collect())) for _ in range(4)]
    assert sizes == [2, 2, 1, 0]


def test_oversize_line_skips_boundedly_and_resumes(tmp_path):
    _, _, service = setup(
        tmp_path, "x" * 4000 + "\n" + LINE, max_line_bytes=256, max_total_read_mb=0.001
    )
    events = []
    for _ in range(6):
        events.extend(records(service.collect()))
    assert len(events) == 1 and "database timeout" in events[0]["message"]


def test_first_read_tail_and_explicit_backfill(tmp_path):
    path, config, service = setup(tmp_path, LINE * 100, initial_tail_mb=0.001)
    result = service.collect()
    assert 0 < len(records(result)) < 100
    assert "initial_history_skipped" in result["coverage"]["fastapi"]["warnings"]
    replay = CollectionService(config, tmp_path / "backfill").collect(backfill=True)
    assert len(records(replay)) == 100
    assert not records(
        CollectionService(config, tmp_path / "backfill").collect(backfill=True)
    )


def test_since_filters_only_new_records_and_keeps_unknown_timestamps(tmp_path):
    now = datetime.now(timezone.utc)
    old = (now - timedelta(hours=48)).isoformat() + " ERROR old failure\n"
    _, _, service = setup(
        tmp_path, old + now.isoformat() + " INFO current\nunknown record\n"
    )
    result = service.collect(since="24h")
    assert len(records(result)) == 2
    assert any(event["timestamp"] is None for event in records(result))
    assert not records(service.collect())


@pytest.mark.parametrize(
    "text,secret",
    [
        ("password=supersecret", "supersecret"),
        ('{"api_key":"private-value"}', "private-value"),
        ("{'token': 'abcxyz'}", "abcxyz"),
        ("Authorization: Bearer abc.def.ghi", "abc.def.ghi"),
        ("Cookie: sessionid=secret; other=secret2", "secret2"),
        ("postgresql://demo:password123@db.example/database", "password123"),
        ("GET /?token=sensitive&user_id=12345", "sensitive"),
        ("trace /home/alice/private.py email=alice@example.test", "alice"),
        ("client 192.0.2.9", "192.0.2.9"),
        ("client 2001:db8::1", "2001:db8::1"),
        ("api sk-proj-abcdefghijklmnop", "abcdefghijklmnop"),
        ("jwt eyJabc.eyJdef.signature", "eyJabc"),
        ("phone=010-1234-5678", "1234"),
    ],
)
def test_mask_secrets(text, secret):
    assert secret not in mask(text)
    assert "[REDACTED]" in mask(text)


def test_mask_does_not_remove_time():
    assert mask("2026-10-28T08:30:00+09:00 INFO") == "2026-10-28T08:30:00+09:00 INFO"


def test_multiline_and_grouped_errors(tmp_path):
    _, _, service = setup(
        tmp_path,
        LINE
        + 'Traceback (most recent call last):\n  File "/home/alice/a.py"\nConnectionError: failed\n'
        + LINE,
    )
    result = service.collect()
    assert len(records(result)) == 2
    assert "Traceback" in records(result)[0]["message"]
    assert "alice" not in Path(result["events_file"]).read_text()


def test_npm_http_codes_and_latency(tmp_path):
    source = Path("tests/fixtures/logs/npm/proxy-host-2_access.txt").read_text()
    _, _, service = setup(tmp_path, source)
    result = service.collect()
    http = snapshot(result).http_summary["fastapi"]
    assert http["4xx"] == 1 and http["5xx"] == 1
    assert http["mean_response_time_ms"] == 250
    assert all(not event["metadata"]["parse_failed"] for event in records(result))
    assert "demo-token" not in Path(result["snapshot"]).read_text()


def test_error_group_counts_and_normal_context(tmp_path):
    normal = LINE.replace("ERROR", "INFO").replace("timeout", "normal")
    _, _, service = setup(tmp_path, normal + LINE * 3 + normal)
    batch = snapshot(service.collect())
    assert batch.error_groups[0]["count"] == 3
    assert any(event.level == "INFO" for event in batch.events)
    assert batch.error_counts["fastapi"]["ERROR"] == 3


def test_snapshot_size_cap_and_full_events(tmp_path):
    content = "".join(
        LINE.rstrip() + f" unique-{chr(65 + i)} " + "x" * 4000 + "\n" for i in range(20)
    )
    _, _, service = setup(tmp_path, content, max_snapshot_bytes=10000)
    result = service.collect()
    assert Path(result["snapshot"]).stat().st_size <= 10000
    assert len(records(result)) == 20
    batch = snapshot(result)
    assert batch.totals["representative_events"] == len(batch.events)
    assert any("Snapshot size" in item for item in batch.limitations)


def test_crash_before_checkpoint_commit_recovers_without_duplicate_batch(
    tmp_path, monkeypatch
):
    _, _, service = setup(tmp_path)
    original = BatchStore.commit

    def crash(self, batch, events, checkpoints):
        def fail():
            raise RuntimeError("simulated crash")

        return original(self, batch, events, checkpoints, before_commit=fail)

    monkeypatch.setattr(BatchStore, "commit", crash)
    with pytest.raises(RuntimeError):
        service.collect()
    assert not list((service.data_dir / "batches").iterdir())
    monkeypatch.setattr(BatchStore, "commit", original)
    result = service.collect()
    assert len(records(result)) == 1
    assert len(list((service.data_dir / "batches").iterdir())) == 1
    assert not list((service.data_dir / ".staging").iterdir())


def test_crash_after_checkpoint_commit_recovers_pending_publish(tmp_path, monkeypatch):
    _, _, service = setup(tmp_path)
    original = BatchStore.publish
    monkeypatch.setattr(
        BatchStore, "publish", lambda *args: (_ for _ in ()).throw(OSError("crash"))
    )
    with pytest.raises(OSError):
        service.collect()
    assert len(list((service.data_dir / ".staging").iterdir())) == 1
    monkeypatch.setattr(BatchStore, "publish", original)
    result = service.collect()
    assert records(result) == []
    assert len(list((service.data_dir / "batches").iterdir())) == 2
    assert (
        sum(
            len(path.read_text().splitlines())
            for path in (service.data_dir / "batches").glob("*/events.jsonl")
        )
        == 1
    )


def test_collector_lock_is_exclusive(tmp_path):
    with collector_lock(tmp_path):
        with pytest.raises(ProviderError) as error:
            with collector_lock(tmp_path):
                pass
        assert error.value.kind == ErrorKind.BUSY


def test_timeout_reports_coverage(tmp_path):
    _, _, service = setup(tmp_path, timeout_seconds=0.00000001)
    result = service.collect()
    assert result["status"] == "partial"
    assert len(records(result)) == 0


@pytest.mark.parametrize(
    "patch",
    [
        {"unexpected": True},
        {"sources": {"fastapi": {"type": "file", "path": "/"}}},
        {
            "sources": {
                "fastapi": {"type": "directory", "path": "/var/lib/pgsql/17/data"}
            }
        },
        {
            "sources": {
                "fastapi": {"type": "directory", "path": "/var/lib/docker/containers"}
            }
        },
        {
            "sources": {
                "fastapi": {
                    "type": "directory",
                    "path": "/home/kafka/kraft-combined-logs",
                }
            }
        },
        {
            "sources": {
                "fastapi": {"type": "directory", "path": "/tmp/logs", "pattern": "../*"}
            }
        },
        {"docker": {"enabled": True, "containers": []}},
        {"sources": {"system": {"type": "file", "path": "/tmp/log"}}},
    ],
)
def test_configuration_rejects_unsafe_values(tmp_path, patch):
    path = tmp_path / "sources.yaml"
    path.write_text(yaml.safe_dump({"version": 1, "sources": {}, **patch}))
    with pytest.raises(ValueError):
        load_config(path)


def test_yaml_safe_load_and_relative_paths(tmp_path):
    path = tmp_path / "sources.yaml"
    path.write_text('!!python/object/apply:os.system ["touch should-not-exist"]')
    with pytest.raises(ValueError):
        load_config(path)
    assert not (tmp_path / "should-not-exist").exists()
    path.write_text(
        yaml.safe_dump(
            {"sources": {"fastapi": {"type": "file", "path": "application.log"}}}
        )
    )
    assert load_config(path).sources["fastapi"].path == tmp_path / "application.log"


def test_fixture_cli_does_not_call_codex(tmp_path, monkeypatch, capsys):
    from aiops.providers.codex import CodexProvider

    monkeypatch.setenv("AIOPS_DATA_DIR", str(tmp_path))
    monkeypatch.setattr(
        CodexProvider, "analyze", lambda *_: pytest.fail("Codex must not be called")
    )
    assert main(["collect", "--all"]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["totals"]["events"] == 11
    assert snapshot(result).masked is True
    assert main(["collect", "--all"]) == 0
    assert json.loads(capsys.readouterr().out)["totals"]["events"] == 0


def test_system_metrics_with_fake_psutil():
    api = SimpleNamespace(
        cpu_percent=lambda interval: 12,
        virtual_memory=lambda: SimpleNamespace(percent=65, available=35, total=100),
        swap_memory=lambda: SimpleNamespace(percent=10, used=1, total=10),
        disk_usage=lambda path: SimpleNamespace(percent=20, free=80, total=100),
    )
    metrics, failures = SystemCollector(api).collect(["/"])
    assert not failures
    assert next(item.value for item in metrics if item.name == "memory_percent") == 65
    assert next(item.unit for item in metrics if item.name == "disk_free") == "bytes"


def test_docker_metrics_with_fake_client():
    class Container:
        status = "running"

        def stats(self, **kwargs):
            assert kwargs == {"stream": False, "one_shot": True}
            return {
                "memory_stats": {"usage": 100, "limit": 1000},
                "cpu_stats": {
                    "cpu_usage": {"total_usage": 40},
                    "system_cpu_usage": 200,
                    "online_cpus": 2,
                },
                "precpu_stats": {
                    "cpu_usage": {"total_usage": 20},
                    "system_cpu_usage": 100,
                },
            }

    client = SimpleNamespace(containers=SimpleNamespace(get=lambda name: Container()))
    result = collect_with_client(client, ["airflow"])
    assert not result["failures"]
    assert (
        next(
            item["value"]
            for item in result["metrics"]
            if item["name"] == "container_cpu_percent"
        )
        == 40
    )


def test_docker_disabled_without_daemon_and_metrics_failure_isolated(tmp_path):
    _, config, _ = setup(tmp_path)
    config.system.enabled = True

    class Unavailable:
        def collect(self, *args):
            raise OSError("private detail")

    result = CollectionService(
        config, tmp_path / "runtime", system=Unavailable()
    ).collect()
    assert len(records(result)) == 1
    assert result["coverage"]["system"]["status"] == "failed"
    assert result["coverage"]["docker"]["status"] == "disabled"


def test_directory_rotated_opt_in_does_not_read_unrelated_files(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    (root / "server.log").write_text(LINE)
    (root / "server.log.1").write_text(LINE)
    (root / "credentials.txt").write_text("must never collect\n")
    config = CollectionConfig(
        sources={"kafka": Source(type="directory", path=root, include_rotated=True)},
        system={"enabled": False},
    )
    result = CollectionService(config, tmp_path / "runtime").collect()
    assert len(records(result)) == 2
    assert "must never collect" not in Path(result["events_file"]).read_text()


def test_cli_partial_returns_10_and_unknown_service_returns_2(
    tmp_path, monkeypatch, capsys
):
    config_file = tmp_path / "sources.yaml"
    config_file.write_text(
        yaml.safe_dump(
            {
                "sources": {"missing": {"type": "file", "path": "missing.log"}},
                "system": {"enabled": False},
            }
        )
    )
    monkeypatch.setenv("AIOPS_DATA_DIR", str(tmp_path / "runtime"))
    assert main(["collect", "--all", "--config", str(config_file)]) == 10
    assert json.loads(capsys.readouterr().out)["status"] == "partial"
    assert main(["collect", "--service", "unknown", "--config", str(config_file)]) == 2
    assert json.loads(capsys.readouterr().err)["error"] == "configuration"


def test_file_replaced_between_scan_and_open_is_reported(tmp_path, monkeypatch):
    from aiops.collectors.files import FileCollector

    path, _, service = setup(tmp_path)
    original = FileCollector.read

    def replace(self, candidate, *args, **kwargs):
        path.rename(tmp_path / "old.log")
        path.write_text(LINE)
        return original(self, candidate, *args, **kwargs)

    monkeypatch.setattr(FileCollector, "read", replace)
    result = service.collect()
    assert not records(result)
    assert any(
        f.reason == "file_replaced_during_open"
        for f in snapshot(result).collection_failures
    )


def test_mid_transaction_failure_rolls_back_catalogue_and_checkpoints(
    tmp_path, monkeypatch
):
    import sqlite3

    _, _, service = setup(tmp_path)
    original = BatchStore.commit

    def failing(self, *args, **kwargs):
        self.db.execute(
            "CREATE TRIGGER fail_checkpoint BEFORE INSERT ON checkpoints BEGIN SELECT RAISE(ABORT, 'test crash'); END"
        )
        self.db.commit()
        return original(self, *args, **kwargs)

    monkeypatch.setattr(BatchStore, "commit", failing)
    with pytest.raises(sqlite3.IntegrityError):
        service.collect()
    with sqlite3.connect(service.data_dir / "checkpoints.sqlite3") as db:
        assert db.execute("SELECT count(*) FROM batches").fetchone()[0] == 0
        assert db.execute("SELECT count(*) FROM checkpoints").fetchone()[0] == 0
        db.execute("DROP TRIGGER fail_checkpoint")
    monkeypatch.setattr(BatchStore, "commit", original)
    assert len(records(service.collect())) == 1


def test_configuration_profiles_are_loadable():
    assert len(load_config(Path("config/log_sources.yaml")).sources) == 11
    assert len(load_config(Path("config/log_sources.fixture.yaml")).sources) == 4


def test_file_budget_prioritizes_new_files_over_unchanged_files(tmp_path):
    root = tmp_path / "logs"
    root.mkdir()
    for index in range(3):
        (root / f"{index}.log").write_text(LINE)
    config = CollectionConfig(
        sources={"kafka": Source(type="directory", path=root)},
        system={"enabled": False},
        max_files=1,
    )
    service = CollectionService(config, tmp_path / "runtime")
    batches = [service.collect() for _ in range(3)]
    assert sum(len(records(batch)) for batch in batches) == 3
    assert len({records(batch)[0]["file"] for batch in batches}) == 3
