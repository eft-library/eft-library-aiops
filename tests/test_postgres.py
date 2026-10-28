"""Real PostgreSQL tests; opt in to an ephemeral Unix-socket cluster."""

import json
import os
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
import yaml
from test_reporting import FakeProvider, add_batch

from aiops.cli import main
from aiops.config import Settings
from aiops.providers.base import AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider
from aiops.reporting.periods import SEOUL
from aiops.reporting.service import ReportingService
from aiops.storage.import_sqlite import import_sqlite
from aiops.storage.jobs import JobQueue, submit
from aiops.storage.postgres import CODEX_LOCK, advisory_lock, doctor, initialize
from aiops.storage.reports import PostgresReportStore
from aiops.worker import Worker

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        os.getenv("AIOPS_RUN_POSTGRES_TESTS") != "1",
        reason="explicit PostgreSQL opt-in required",
    ),
]


@pytest.fixture(scope="session")
def postgres_url():
    binary = Path(
        os.getenv("AIOPS_POSTGRES_BIN", "/opt/homebrew/opt/postgresql@17/bin")
    )
    if not (binary / "initdb").is_file():
        pytest.fail("Set AIOPS_POSTGRES_BIN to a PostgreSQL 17 bin directory")
    with tempfile.TemporaryDirectory(prefix="aiops-pg-", dir="/tmp") as temp:
        root = Path(temp)
        data, sockets = root / "db", root / "sockets"
        sockets.mkdir()
        subprocess.run(
            [
                str(binary / "initdb"),
                "-D",
                str(data),
                "-A",
                "trust",
                "-U",
                "postgres",
                "--no-locale",
                "-E",
                "UTF8",
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [
                str(binary / "pg_ctl"),
                "-D",
                str(data),
                "-l",
                str(root / "server.log"),
                "-w",
                "start",
                "-o",
                f"-h '' -k {sockets} -p 5432",
            ],
            check=True,
            capture_output=True,
        )
        try:
            base = f"postgresql://postgres@/postgres?host={sockets}&port=5432"
            with psycopg.connect(base, autocommit=True) as conn:
                conn.execute("CREATE DATABASE aiops_test")
            yield f"postgresql://postgres@/aiops_test?host={sockets}&port=5432"
        finally:
            subprocess.run(
                [
                    str(binary / "pg_ctl"),
                    "-D",
                    str(data),
                    "-m",
                    "immediate",
                    "-w",
                    "stop",
                ],
                check=True,
                capture_output=True,
            )


@pytest.fixture
def pg_setup(postgres_url, tmp_path):
    # This is exclusively the fixture-created database, never a production DSN.
    with psycopg.connect(postgres_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA IF EXISTS aiops CASCADE")
    settings = Settings(data_dir=tmp_path / "data", database_url=postgres_url)
    initialize(settings)
    config = tmp_path / "sources.yaml"
    config.write_text(
        yaml.safe_dump(
            {
                "sources": {"fastapi": {"type": "file", "path": "app.log"}},
                "system": {"enabled": False},
            }
        )
    )
    service = ReportingService(settings, sources_config=config, interval_minutes=1440)
    return settings, service, config


def seed(service, day="2020-01-01", suffix="one"):
    stamp = datetime.fromisoformat(day + "T12:00:00").replace(tzinfo=SEOUL)
    add_batch(service, "batch-" + suffix, [stamp], collected_at=stamp)
    return service.execute("daily", day)


def test_migration_is_explicit_idempotent_and_scoped(pg_setup):
    settings, _, _ = pg_setup
    initialize(settings)
    assert doctor(settings)["migration"] == 1
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='aiops'"
        ).fetchall()
        assert {row[0] for row in rows} == {
            "schema_migrations",
            "batches",
            "reports",
            "report_versions",
            "report_dependencies",
            "analysis_jobs",
            "job_attempts",
        }


def test_prepare_persists_batch_reference_and_version(pg_setup):
    settings, service, _ = pg_setup
    first = seed(service)
    second = service.execute("daily", "2020-01-01")
    assert first["report_id"] == second["report_id"] and second["reused"]
    store = PostgresReportStore(settings.data_dir / "reports", settings)
    try:
        assert store.row(first["report_id"])["status"] == "PREPARED"
        assert (
            store.db.execute("SELECT count(*) AS n FROM aiops.batches").fetchone()["n"]
            == 1
        )
        assert (
            store.db.execute(
                "SELECT count(*) AS n FROM aiops.report_dependencies"
            ).fetchone()["n"]
            == 1
        )
    finally:
        store.close()


def test_queue_is_idempotent_and_worker_saves_result(pg_setup):
    settings, service, config = pg_setup
    prepared = seed(service)
    first = submit(
        settings, "daily", "2020-01-01", config=config, interval_minutes=1440
    )
    second = submit(
        settings, "daily", "2020-01-01", config=config, interval_minutes=1440
    )
    assert first["job_id"] == second["job_id"]
    provider = FakeProvider()
    with Worker(settings, provider=provider) as worker:
        result = worker.once()
        assert result["status"] == "COMPLETED"
        assert worker.once()["status"] == "idle"
    assert len(provider.calls) == 1
    assert service.list()[0]["status"] == "COMPLETED"
    assert Path(prepared["input"]).with_name("result.json").is_file()
    assert (
        submit(settings, "daily", "2020-01-01", config=config, interval_minutes=1440)[
            "status"
        ]
        == "COMPLETED"
    )


def test_priority_then_completed_lower_report_dependency(pg_setup):
    settings, service, _ = pg_setup
    normal = seed(service)
    critical = seed(service, "2020-01-02", "two")
    queue = JobQueue(settings)
    try:
        queue.enqueue(normal["report_id"])
        queue.enqueue(critical["report_id"], priority="critical")
        with Worker(settings, provider=FakeProvider()) as worker:
            assert worker.once()["report_id"] == critical["report_id"]
            assert worker.once()["report_id"] == normal["report_id"]
    finally:
        queue.close()
    week = service.execute("weekly", "2019-12-30")
    assert (
        json.loads(Path(week["manifest"]).read_text())["daily_report_ids"]["2020-01-01"]
        == normal["report_id"]
    )


def test_second_worker_is_rejected(pg_setup):
    settings, _, _ = pg_setup
    with Worker(settings, provider=FakeProvider()):
        with pytest.raises(ProviderError) as error:
            with Worker(settings, provider=FakeProvider()):
                pass
        assert error.value.kind == ErrorKind.BUSY


def test_direct_codex_and_worker_share_database_execution_gate(pg_setup):
    settings, _, _ = pg_setup
    with advisory_lock(settings, CODEX_LOCK):
        with pytest.raises(ProviderError) as error:
            CodexProvider(settings).analyze(AnalysisRequest("{}"))
        assert error.value.kind == ErrorKind.BUSY


def test_network_retry_has_backoff_and_attempt_history(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    provider = FakeProvider()
    provider.fail = True
    try:
        job = queue.enqueue(report["report_id"], max_attempts=2)
        with Worker(settings, provider=provider) as worker:
            failed = worker.once()
            assert failed["status"] == "PENDING" and failed["error"] == "network"
            assert worker.once()["status"] == "idle"
            queue.db.execute(
                "UPDATE aiops.analysis_jobs SET available_at=now() WHERE job_id=%s",
                (job["job_id"],),
            )
            provider.fail = False
            assert worker.once()["status"] == "COMPLETED"
        attempts = queue.db.execute(
            "SELECT status FROM aiops.job_attempts ORDER BY attempt"
        ).fetchall()
        assert [row["status"] for row in attempts] == ["FAILED", "COMPLETED"]
        assert queue.list()[0]["attempt_count"] == 2
    finally:
        queue.close()


@pytest.mark.parametrize("kind", [ErrorKind.AUTH, ErrorKind.RATE_LIMIT])
def test_auth_and_rate_limits_do_not_loop(pg_setup, kind):
    settings, service, _ = pg_setup
    report = seed(service)

    class Failing(FakeProvider):
        def analyze(self, request):
            raise ProviderError(kind)

    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        with Worker(settings, provider=Failing()) as worker:
            result = worker.once()
            assert result["status"] == "FAILED" and result["attempt_count"] == 1
            assert worker.once()["status"] == "idle"
    finally:
        queue.close()


def test_retry_budget_terminal_timeout_and_manual_retry(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)

    class Timeout(FakeProvider):
        def analyze(self, request):
            raise ProviderError(ErrorKind.TIMEOUT)

    queue = JobQueue(settings)
    try:
        job = queue.enqueue(report["report_id"], max_attempts=1)
        with Worker(settings, provider=Timeout()) as worker:
            result = worker.once()
            assert result["status"] == "TIMEOUT"
        queue.retry(job["job_id"])
        with Worker(settings, provider=FakeProvider()) as worker:
            assert worker.once()["status"] == "COMPLETED"
    finally:
        queue.close()


def test_new_worker_recovers_interruption_and_fences_old_owner(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        old_owner = uuid4()
        claimed = queue.claim(old_owner)
        with Worker(settings, provider=FakeProvider()) as worker:
            assert worker.recovered == 1
            assert queue.list()[0]["status"] == "PENDING"
            assert queue.finish(claimed, old_owner, error="execution") is None
    finally:
        queue.close()


def test_stale_job_cancelled_without_codex(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    provider = FakeProvider()
    try:
        queue.enqueue(report["report_id"])
        seed(service, suffix="late")
        with Worker(settings, provider=provider) as worker:
            result = worker.once()
            assert result["status"] == "CANCELLED" and result["error"] == "stale_input"
        assert not provider.calls
    finally:
        queue.close()


def test_pending_cancellation(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    try:
        job = queue.enqueue(report["report_id"])
        assert queue.cancel(job["job_id"])["status"] == "CANCELLED"
        with Worker(settings, provider=FakeProvider()) as worker:
            assert worker.once()["status"] == "idle"
    finally:
        queue.close()


def test_sqlite_import_preserves_id_result_and_source(pg_setup):
    from dataclasses import replace

    settings, service, config = pg_setup
    local = ReportingService(
        replace(settings, database_url=None),
        provider=FakeProvider(),
        sources_config=config,
        interval_minutes=1440,
    )
    seed(local)
    old = local.execute("daily", "2020-01-01", analyze=True)
    sqlite_path = settings.data_dir / "reports" / "reports.sqlite3"
    before = sqlite_path.read_bytes()
    assert import_sqlite(settings)["imported"] == 1
    assert import_sqlite(settings)["imported"] == 0
    assert sqlite_path.read_bytes() == before
    assert service.list()[0]["report_id"] == old["report_id"]
    assert service.list()[0]["status"] == "COMPLETED"


def test_report_run_cli_enqueues_instead_of_calling_codex(
    pg_setup, monkeypatch, capsys
):
    settings, service, config = pg_setup
    seed(service)
    monkeypatch.setenv("AIOPS_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("AIOPS_DATABASE_URL", settings.database_url)
    provider = FakeProvider()
    assert (
        main(
            [
                "report",
                "run",
                "--type",
                "daily",
                "--date",
                "2020-01-01",
                "--config",
                str(config),
                "--interval-minutes",
                "1440",
            ],
            provider=provider,
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "PENDING"
    assert not provider.calls


def test_migration_rollback_only_removes_aiops_objects(pg_setup):
    from importlib.resources import files

    settings, _, _ = pg_setup
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.must_survive(id integer)")
        conn.execute(
            files("aiops.storage").joinpath("sql/001_rollback.sql").read_text(),
            prepare=False,
        )
        assert (
            conn.execute("SELECT to_regclass('public.must_survive')").fetchone()[0]
            == "must_survive"
        )
        conn.execute("DROP TABLE public.must_survive")


def test_sync_registers_unreferenced_batches_without_raw_logs(pg_setup):
    from aiops.storage.import_sqlite import sync_batches

    settings, service, _ = pg_setup
    add_batch(
        service,
        "batch-not-analyzed",
        [datetime(2020, 1, 1, 12, tzinfo=SEOUL)],
        collected_at=datetime(2020, 1, 1, 12, tzinfo=SEOUL),
    )
    assert sync_batches(settings)["registered"] == 1
    assert sync_batches(settings)["registered"] == 0
    store = PostgresReportStore(settings.data_dir / "reports", settings)
    try:
        row = store.db.execute("SELECT * FROM aiops.batches").fetchone()
        assert row["relative_path"] == "collection/batches/batch-not-analyzed"
        assert "message" not in row
    finally:
        store.close()


def test_sqlite_weekly_and_monthly_import_preserves_deferred_dependencies(pg_setup):
    from dataclasses import replace

    settings, service, config = pg_setup
    local = ReportingService(
        replace(settings, database_url=None),
        provider=FakeProvider(),
        sources_config=config,
        interval_minutes=1440,
    )
    seed(local)
    local.execute("daily", "2020-01-01", analyze=True)
    local.execute("weekly", "2019-12-30", analyze=True)
    local.execute("monthly", "2020-01-01", analyze=True)
    assert import_sqlite(settings)["imported"] == 3
    assert all(row["status"] == "COMPLETED" for row in service.list())
    assert len(service.list()) == 3


def test_completion_is_atomic_if_report_update_fails(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        owner = uuid4()
        job = queue.claim(owner)
        queue.db.execute("""CREATE FUNCTION aiops.reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.status='COMPLETED' THEN RAISE EXCEPTION 'test failure'; END IF; RETURN NEW; END $$""")
        queue.db.execute(
            "CREATE TRIGGER reject_completion BEFORE UPDATE ON aiops.report_versions FOR EACH ROW EXECUTE FUNCTION aiops.reject_completion()"
        )
        result = FakeProvider().analyze(AnalysisRequest("{}")).model_dump()
        with pytest.raises(psycopg.Error):
            queue.finish(job, owner, result=result)
        assert queue.list()[0]["status"] == "RUNNING"
        assert (
            queue.db.execute("SELECT status FROM aiops.job_attempts").fetchone()[
                "status"
            ]
            == "RUNNING"
        )
        assert (
            queue.db.execute("SELECT status FROM aiops.report_versions").fetchone()[
                "status"
            ]
            == "RUNNING"
        )
        queue.db.execute("DROP TRIGGER reject_completion ON aiops.report_versions")
        assert queue.finish(job, owner, result=result)["status"] == "COMPLETED"
    finally:
        queue.close()


def test_report_list_never_resets_a_running_worker_job(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        queue.claim(uuid4())
        assert service.list()[0]["status"] == "RUNNING"
        assert queue.list()[0]["status"] == "RUNNING"
    finally:
        queue.close()


def test_actual_provider_path_uses_global_gate_inside_worker(pg_setup, monkeypatch):
    import sys
    from dataclasses import replace

    from test_phase1 import events

    from aiops.providers import codex

    settings, service, _ = pg_setup
    settings = replace(settings, codex_bin=sys.executable)
    report = seed(service)
    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        calls = []

        def fake_run(*args, **kwargs):
            calls.append(1)
            with pytest.raises(ProviderError) as error:
                CodexProvider(settings).analyze(AnalysisRequest("{}"))
            assert error.value.kind == ErrorKind.BUSY
            assert "PGPASSWORD" not in kwargs["env"]
            assert "AIOPS_DATABASE_URL" not in kwargs["env"]
            return 0, events(), ""

        monkeypatch.setattr(codex, "run", fake_run)
        with Worker(settings) as worker:
            assert worker.once()["status"] == "COMPLETED"
        assert len(calls) == 1
    finally:
        queue.close()


def test_null_dependency_is_rejected(pg_setup):
    settings, service, _ = pg_setup
    report = seed(service)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            conn.execute(
                "INSERT INTO aiops.report_dependencies(report_id,dependency_type,dependency_id) VALUES (%s,'batch','missing')",
                (report["report_id"],),
            )


def test_worker_pins_requested_model_from_submission(pg_setup, monkeypatch):
    from dataclasses import replace

    from aiops import worker as worker_module

    settings, service, _ = pg_setup
    service.settings = replace(settings, model="submitted-model")
    report = seed(service)
    queue = JobQueue(settings)
    selected = []

    class Capturing(FakeProvider):
        def __init__(self, settings):
            super().__init__()
            selected.append(settings.model)

    monkeypatch.setattr(worker_module, "CodexProvider", Capturing)
    try:
        queue.enqueue(report["report_id"])
        with Worker(replace(settings, model="changed-worker-default")) as worker:
            assert worker.once()["status"] == "COMPLETED"
        assert selected == ["submitted-model"]
    finally:
        queue.close()


def test_invalid_evidence_fails_without_storing_success(pg_setup):
    from aiops.providers.base import Finding

    settings, service, _ = pg_setup
    report = seed(service)

    class Invalid(FakeProvider):
        def analyze(self, request):
            result = super().analyze(request)
            result.report.findings = [
                Finding(
                    issue="test",
                    hypothesis="test",
                    evidence_ids=["fabricated"],
                    recommended_checks=[],
                )
            ]
            return result

    queue = JobQueue(settings)
    try:
        queue.enqueue(report["report_id"])
        with Worker(settings, provider=Invalid()) as worker:
            result = worker.once()
            assert result["status"] == "FAILED" and result["error"] == "invalid_output"
        assert service.list()[0]["status"] == "FAILED"
    finally:
        queue.close()


def test_migration_runs_as_non_superuser_database_owner(pg_setup):
    from dataclasses import replace

    from psycopg.conninfo import make_conninfo

    settings, _, _ = pg_setup
    with psycopg.connect(settings.database_url, autocommit=True) as admin:
        admin.execute(
            "CREATE ROLE aiops_test_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE"
        )
        admin.execute("CREATE DATABASE aiops_test_role OWNER aiops_test_app")
        try:
            role_settings = replace(
                settings,
                database_url=make_conninfo(
                    settings.database_url,
                    user="aiops_test_app",
                    dbname="aiops_test_role",
                ),
            )
            initialize(role_settings)
            assert doctor(role_settings)["status"] == "ready"
            queue = JobQueue(role_settings)
            try:
                assert queue.list() == []
                assert (
                    queue.db.execute(
                        "SELECT rolsuper FROM pg_roles WHERE rolname=current_user"
                    ).fetchone()["rolsuper"]
                    is False
                )
            finally:
                queue.close()
        finally:
            admin.execute("DROP DATABASE aiops_test_role")
            admin.execute("DROP ROLE aiops_test_app")
