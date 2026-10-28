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
        conn.execute("DROP SCHEMA public CASCADE")
        conn.execute("CREATE SCHEMA public")
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
    assert doctor(settings)["migration"] == 2
    with psycopg.connect(settings.database_url) as conn:
        rows = conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'aiops_%'"
        ).fetchall()
        assert {row[0] for row in rows} == {
            "aiops_schema_migrations",
            "aiops_batches",
            "aiops_reports",
            "aiops_report_versions",
            "aiops_report_dependencies",
            "aiops_analysis_jobs",
            "aiops_job_attempts",
        }


def test_prepare_persists_batch_reference_and_version(pg_setup):
    settings, service, _ = pg_setup
    first = seed(service)
    second = service.execute("daily", "2020-01-01")
    assert first["report_id"] == second["report_id"] and second["reused"]
    store = PostgresReportStore(settings.data_dir / "aiops_reports", settings)
    try:
        assert store.row(first["report_id"])["status"] == "PREPARED"
        assert (
            store.db.execute(
                "SELECT count(*) AS n FROM public.aiops_batches"
            ).fetchone()["n"]
            == 1
        )
        assert (
            store.db.execute(
                "SELECT count(*) AS n FROM public.aiops_report_dependencies"
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
                "UPDATE public.aiops_analysis_jobs SET available_at=now() WHERE job_id=%s",
                (job["job_id"],),
            )
            provider.fail = False
            assert worker.once()["status"] == "COMPLETED"
        attempts = queue.db.execute(
            "SELECT status FROM public.aiops_job_attempts ORDER BY attempt"
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
    store = PostgresReportStore(settings.data_dir / "aiops_reports", settings)
    try:
        row = store.db.execute("SELECT * FROM public.aiops_batches").fetchone()
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
        queue.db.execute("""CREATE FUNCTION public.reject_completion() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN IF NEW.status='COMPLETED' THEN RAISE EXCEPTION 'test failure'; END IF; RETURN NEW; END $$""")
        queue.db.execute(
            "CREATE TRIGGER reject_completion BEFORE UPDATE ON public.aiops_report_versions FOR EACH ROW EXECUTE FUNCTION public.reject_completion()"
        )
        result = FakeProvider().analyze(AnalysisRequest("{}")).model_dump()
        with pytest.raises(psycopg.Error):
            queue.finish(job, owner, result=result)
        assert queue.list()[0]["status"] == "RUNNING"
        assert (
            queue.db.execute("SELECT status FROM public.aiops_job_attempts").fetchone()[
                "status"
            ]
            == "RUNNING"
        )
        assert (
            queue.db.execute(
                "SELECT status FROM public.aiops_report_versions"
            ).fetchone()["status"]
            == "RUNNING"
        )
        queue.db.execute(
            "DROP TRIGGER reject_completion ON public.aiops_report_versions"
        )
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
                "INSERT INTO public.aiops_report_dependencies(report_id,dependency_type,dependency_id) VALUES (%s,'batch','missing')",
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


def make_legacy_schema(settings, *, keep_unrelated=False):
    from importlib.resources import files


    names = (
        "schema_migrations",
        "batches",
        "reports",
        "report_versions",
        "report_dependencies",
        "analysis_jobs",
        "job_attempts",
    )
    legacy = files("aiops.storage").joinpath("sql/001_initial.sql").read_text()
    for name in names:
        legacy = legacy.replace("public.aiops_" + name, "aiops." + name)
    legacy = legacy.replace("aiops_analysis_jobs_pending", "analysis_jobs_pending")
    legacy = legacy.replace("VALUES (1),(2)", "VALUES (1)")
    legacy = legacy.replace("BEGIN;", "BEGIN; CREATE SCHEMA aiops;", 1)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public", prepare=False)
        conn.execute(legacy, prepare=False)
        # Intentionally collide with generic old index/sequence names in public.
        conn.execute(
            "CREATE TABLE public.reports(period_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY)"
        )
        conn.execute("INSERT INTO public.reports DEFAULT VALUES")
        period_id = conn.execute(
            "INSERT INTO aiops.reports(kind,period_start,period_end) VALUES ('daily','2020-01-01','2020-01-02') RETURNING period_id"
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO aiops.report_versions(report_id,period_id,revision,fingerprint,status,input_json,manifest_json,coverage_complete) VALUES ('preserved-version',%s,1,%s,'PREPARED','{}','{}',false)",
            (period_id, "a" * 64),
        )
        conn.execute(
            "INSERT INTO aiops.analysis_jobs(job_id,report_id,status) VALUES (%s,'preserved-version','PENDING')",
            (uuid4(),),
        )
        if keep_unrelated:
            conn.execute("CREATE TABLE aiops.unrelated_table(id integer)")
    return period_id


@pytest.mark.parametrize("keep_unrelated", [False, True])
def test_legacy_upgrade_preserves_data_foreign_keys_and_identity(
    pg_setup, keep_unrelated
):
    settings, _, _ = pg_setup
    previous_id = make_legacy_schema(settings, keep_unrelated=keep_unrelated)
    assert initialize(settings)["schema"] == "public"
    assert doctor(settings)["migration"] == 2
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        assert (
            conn.execute("SELECT report_id FROM public.aiops_analysis_jobs").fetchone()[
                0
            ]
            == "preserved-version"
        )
        assert conn.execute("SELECT count(*) FROM public.reports").fetchone()[0] == 1
        new_id = conn.execute(
            "INSERT INTO public.aiops_reports(kind,period_start,period_end) VALUES ('daily','2020-01-02','2020-01-03') RETURNING period_id"
        ).fetchone()[0]
        assert new_id > previous_id
        assert (
            conn.execute("SELECT to_regnamespace('aiops') IS NOT NULL").fetchone()[0]
            == keep_unrelated
        )
    initialize(settings)
    store = PostgresReportStore(settings.data_dir / "reports", settings)
    try:
        assert store.row("preserved-version")["fingerprint"] == "a" * 64
    finally:
        store.close()


def test_legacy_upgrade_collision_rolls_back_without_overwriting(pg_setup):
    settings, _, _ = pg_setup
    make_legacy_schema(settings)
    with psycopg.connect(settings.database_url, autocommit=True) as conn:
        conn.execute("CREATE TABLE public.aiops_reports(original_value text)")
        conn.execute("INSERT INTO public.aiops_reports VALUES ('keep-existing-data')")
    with pytest.raises(psycopg.Error):
        initialize(settings)
    with psycopg.connect(settings.database_url) as conn:
        assert (
            conn.execute("SELECT original_value FROM public.aiops_reports").fetchone()[
                0
            ]
            == "keep-existing-data"
        )
        assert (
            conn.execute("SELECT count(*) FROM aiops.report_versions").fetchone()[0]
            == 1
        )
        assert (
            conn.execute(
                "SELECT to_regclass('public.aiops_schema_migrations')"
            ).fetchone()[0]
            is None
        )


def test_simple_flow_setup_collect_status_and_registration_recovery(
    pg_setup, monkeypatch, capsys
):
    from aiops.storage import import_sqlite as imports

    settings, _, config = pg_setup
    monkeypatch.setenv("AIOPS_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("AIOPS_DATABASE_URL", settings.database_url)
    monkeypatch.setenv("AIOPS_SOURCES_CONFIG", str(config))
    # Repeated setup is safe and needs no separate import or sync commands.
    assert main(["setup"]) == 0
    assert main(["setup"]) == 0
    capsys.readouterr()
    real_sync = imports.sync_batches

    def unavailable(settings):
        raise psycopg.OperationalError("private server details")

    monkeypatch.setattr(imports, "sync_batches", unavailable)
    assert main(["collect", "--all"]) == 11
    failed = capsys.readouterr().out
    assert "private server details" not in failed
    first = json.loads(failed)
    assert first["database_registration"]["status"] == "pending"
    assert Path(first["snapshot"]).is_file()
    monkeypatch.setattr(imports, "sync_batches", real_sync)
    # A later collection catches up both the old and the new durable batches.
    assert main(["collect", "--all"]) in (0, 10)
    collected = json.loads(capsys.readouterr().out)
    assert collected["database_registration"]["registered"] == 2
    assert main(["status"]) == 0
    overview = json.loads(capsys.readouterr().out)
    assert overview["collection"]["batches"] == 2
    assert overview["registered_batches"] == 2


def test_missing_report_input_has_actionable_safe_error(pg_setup, monkeypatch, capsys):
    settings, _, config = pg_setup
    monkeypatch.setenv("AIOPS_DATA_DIR", str(settings.data_dir))
    monkeypatch.setenv("AIOPS_DATABASE_URL", settings.database_url)
    monkeypatch.setenv("AIOPS_SOURCES_CONFIG", str(config))
    assert main(["report", "run", "--type", "daily", "--date", "2020-01-01"]) == 2
    error = json.loads(capsys.readouterr().err)
    assert error["error"] == "no_report_input"
    assert "전날" in error["message"]
    assert settings.database_url not in error["message"]


def test_http_api_enqueues_executes_and_exposes_result_without_airflow_files(pg_setup):
    from fastapi.testclient import TestClient

    from aiops.api import create_app

    settings, service, config = pg_setup
    seed(service)
    provider = FakeProvider()
    with TestClient(create_app(
        settings, token="test-only-key", sources_config=config, provider=provider,
    )) as client:
        assert client.post("/v1/worker/run-once").status_code == 401
        client.headers["X-AIOps-Key"] = "wrong"
        assert client.get("/v1/status").status_code == 401
        client.headers["X-AIOps-Key"] = "test-only-key"
        assert client.post("/v1/reports", json={
            "type": "daily", "date": "2020-01-01", "config": "/etc/passwd",
        }).status_code == 422
        response = client.post("/v1/reports", json={"type": "daily", "date": "2020-01-01"})
        assert response.status_code == 202
        job = response.json()
        assert job["status"] == "PENDING"
        assert not provider.calls
        repeated = client.post("/v1/reports", json={"type": "daily", "date": "2020-01-01"})
        assert repeated.json()["job_id"] == job["job_id"]
        outcome = client.post("/v1/worker/run-once")
        assert outcome.status_code == 200
        assert outcome.json()["status"] == "COMPLETED"
        assert client.get("/v1/jobs/" + job["job_id"]).json()["status"] == "COMPLETED"
        report = client.get("/v1/reports/" + job["report_id"]).json()
        assert report["status"] == "COMPLETED"
        assert report["result"]["report"]["summary"]
        assert client.post("/v1/worker/run-once").json()["status"] == "idle"
        assert client.get("/v1/jobs/" + str(uuid4())).status_code == 404
        assert client.post("/v1/reports", json={"type": "daily", "date": "1999-01-01"}).json()["error"] == "no_report_input"
        with Worker(settings, provider=provider):
            assert client.post("/v1/worker/run-once").status_code == 409
        collected = client.post("/v1/collect", json={})
        assert collected.status_code == 200
        assert collected.json()["database_registration"]["status"] == "synced"


def test_readonly_website_queries_filter_page_and_preserve_versions(pg_setup):
    from fastapi.testclient import TestClient

    from aiops.api import create_app
    from aiops.storage import views

    settings, service, config = pg_setup
    seed(service, day="2020-01-01")
    first = submit(settings, "daily", "2020-01-01", config=config, interval_minutes=1440)
    with Worker(settings, provider=FakeProvider()) as worker:
        worker.once()
    seed(service, day="2020-01-02", suffix="two")
    seed(service, day="2020-01-01", suffix="revision-two")
    with TestClient(create_app(settings, token="key", sources_config=config)) as client:
        client.headers["X-AIOps-Key"] = "key"
        latest = client.get("/v1/reports").json()
        assert latest["total"] == 2
        assert latest["items"][0]["period"]["start"] == "2020-01-02"
        assert latest["items"][1]["revision"] == 2
        history = client.get("/v1/reports?history=true&page_size=1&page=2").json()
        assert history["total"] == 3 and len(history["items"]) == 1
        filtered = client.get("/v1/reports?history=true&severity=WARNING&status=COMPLETED").json()
        assert filtered["total"] == 1
        assert client.get("/v1/reports?start=2020-01-02&end=2020-01-02").json()["total"] == 1
        assert client.get("/v1/reports?page=100").json()["total"] == 2
        assert client.get("/v1/reports?start=2020-01-03&end=2020-01-01").status_code == 422
        assert client.get("/v1/reports?page_size=101").status_code == 422
        assert client.get("/v1/reports?type=invalid").status_code == 422
        detail = client.get("/v1/reports/" + first["report_id"]).json()
        assert detail["result"]["report"]["summary"]
        assert detail["evidence"]
        assert "input_json" not in detail
        assert client.get("/v1/reports/not-existing").status_code == 404
        assert client.get("/v1/jobs?status=COMPLETED").json()["total"] == 1
        assert client.get("/v1/collections").json()["total"] == 3
    # Queries use a read-only session: an accidental write cannot succeed.
    with views.connection(settings) as db:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            db.execute("DELETE FROM public.aiops_analysis_jobs")
    assert views.reports(settings, history=True)["total"] == 3
