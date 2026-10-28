import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

from aiops.config import Settings
from aiops.operations import NoReportInput
from aiops.providers.base import AIProvider, AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider, doctor

EXIT_CODES = {
    ErrorKind.DATABASE: 11,
    ErrorKind.CONFIG: 2,
    ErrorKind.AUTH: 3,
    ErrorKind.NETWORK: 4,
    ErrorKind.RATE_LIMIT: 5,
    ErrorKind.TIMEOUT: 6,
    ErrorKind.BUSY: 7,
    ErrorKind.OUTPUT: 8,
    ErrorKind.EXECUTION: 9,
}


def main(argv=None, *, provider: AIProvider | None = None) -> int:
    parser = argparse.ArgumentParser(description="Read-only EFT Library log analysis")
    parser.add_argument(
        "--env-file", type=Path, help="Explicit dotenv file (existing environment wins)"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    serve = commands.add_parser("serve", help="Run the Airflow HTTP API on port 9023")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=9023)
    commands.add_parser("setup", help="Initialize DB and register existing local data")
    commands.add_parser("status", help="Show collection, database and recent jobs")
    commands.add_parser("doctor", help="Check local Python, Codex and login status")
    analyze = commands.add_parser(
        "analyze", help="Analyze a sanitized sample (real Codex call)"
    )
    analyze.add_argument("--input", type=Path, required=True)
    collect = commands.add_parser(
        "collect", help="Collect logs and metrics without Codex"
    )
    selection = collect.add_mutually_exclusive_group(required=True)
    selection.add_argument(
        "--all", action="store_true", help="All configured log sources"
    )
    selection.add_argument(
        "--service", action="append", help="Source name; repeat for multiple sources"
    )
    collect.add_argument(
        "--config",
        type=Path,
        default=None,
        help="YAML profile; otherwise AIOPS_SOURCES_CONFIG or local fixtures",
    )
    collect.add_argument(
        "--since", help="Filter newly read timestamps, e.g. 24h (not a historical seek)"
    )
    collect.add_argument(
        "--backfill",
        action="store_true",
        help="Start unseen files at byte zero (still bounded)",
    )
    report = commands.add_parser(
        "report", help="Prepare/run versioned daily, weekly, monthly reports"
    )
    actions = report.add_subparsers(dest="report_action", required=True)
    for action in ("prepare", "run"):
        target = actions.add_parser(
            action,
            help="No AI call"
            if action == "prepare"
            else "Queue analysis for the Worker (local mode runs Codex directly)",
        )
        target.add_argument(
            "--type", choices=["daily", "weekly", "monthly"], required=True
        )
        target.add_argument(
            "--date",
            help="Target calendar date; default is last closed period in Asia/Seoul",
        )
        target.add_argument(
            "--config",
            type=Path,
            default=None,
            help="Expected collection sources for daily coverage",
        )
        target.add_argument(
            "--interval-minutes",
            type=int,
            default=5,
            help="Expected collection cadence, for coverage only",
        )
    listing = actions.add_parser(
        "list", help="List report revisions and data staleness"
    )
    listing.add_argument("--type", choices=["daily", "weekly", "monthly"])
    db = commands.add_parser("db", help="Explicit PostgreSQL setup and migration tools")
    db.add_argument(
        "db_action",
        choices=["print-sql", "init", "doctor", "import-sqlite", "sync-batches"],
    )
    jobs = commands.add_parser("jobs", help="Persistent PostgreSQL analysis queue")
    job_actions = jobs.add_subparsers(dest="jobs_action", required=True)
    job_submit = job_actions.add_parser("submit")
    job_submit.add_argument(
        "--type", choices=["daily", "weekly", "monthly"], required=True
    )
    job_submit.add_argument("--date")
    job_submit.add_argument(
        "--config", type=Path, default=None
    )
    job_submit.add_argument("--interval-minutes", type=int, default=5)
    job_submit.add_argument(
        "--priority", choices=["critical", "high", "normal", "low"], default="normal"
    )
    job_submit.add_argument("--max-attempts", type=int, default=3)
    job_actions.add_parser("list")
    for action in ("retry", "cancel"):
        command = job_actions.add_parser(action)
        command.add_argument("job_id")
    worker = commands.add_parser(
        "worker", help="Run a single PostgreSQL analysis Worker"
    )
    worker.add_argument("--once", action="store_true")
    worker.add_argument("--poll-seconds", type=float, default=5)
    args = parser.parse_args(argv)
    try:
        if args.env_file:
            if not args.env_file.is_file():
                raise ProviderError(ErrorKind.CONFIG)
            load_dotenv(args.env_file, override=False)
        settings = Settings.from_env()
        if hasattr(args, "config") and args.config is None:
            args.config = Path(os.getenv(
                "AIOPS_SOURCES_CONFIG", "config/log_sources.fixture.yaml"
            ))
        if args.command == "serve":
            import uvicorn

            from aiops.api import create_app

            app = create_app(
                settings, token=os.getenv("AIOPS_API_TOKEN", ""),
                sources_config=Path(os.getenv(
                    "AIOPS_SOURCES_CONFIG", "config/log_sources.yaml"
                )),
            )
            uvicorn.run(app, host=args.host, port=args.port, workers=1)
            return 0
        elif args.command == "setup":
            from aiops.operations import setup

            output = setup(settings)
        elif args.command == "status":
            from aiops.operations import status

            output = status(settings)
        elif args.command == "db":
            from aiops.storage import postgres

            if args.db_action == "print-sql":
                print(postgres.migration_sql())
                return 0
            if args.db_action == "init":
                output = postgres.initialize(settings)
            elif args.db_action == "doctor":
                output = postgres.doctor(settings)
            elif args.db_action == "sync-batches":
                from aiops.storage.import_sqlite import sync_batches

                output = sync_batches(settings)
            else:
                from aiops.storage.import_sqlite import import_sqlite

                output = import_sqlite(settings)
        elif args.command == "jobs":
            from aiops.storage.jobs import JobQueue, submit

            if args.jobs_action == "submit":
                output = submit(
                    settings,
                    args.type,
                    args.date,
                    config=args.config,
                    interval_minutes=args.interval_minutes,
                    priority=args.priority,
                    max_attempts=args.max_attempts,
                )
            else:
                queue = JobQueue(settings)
                try:
                    output = (
                        queue.list()
                        if args.jobs_action == "list"
                        else getattr(queue, args.jobs_action)(args.job_id)
                    )
                finally:
                    queue.close()
        elif args.command == "worker":
            from aiops.worker import run

            return run(
                settings,
                once=args.once,
                poll_seconds=args.poll_seconds,
                provider=provider,
            )
        elif args.command == "doctor":
            output = doctor(settings)
        elif args.command == "collect":
            from aiops.collectors.config import load_config
            from aiops.collectors.service import CollectionService

            result = CollectionService(
                load_config(args.config), settings.data_dir
            ).collect(args.service, since=args.since, backfill=args.backfill)
            registration_failed = False
            if settings.database_url:
                from aiops.storage.import_sqlite import sync_batches

                try:
                    result["database_registration"] = sync_batches(settings)
                except (psycopg.Error, OSError, ValueError, sqlite3.Error, ProviderError):
                    registration_failed = True
                    result["database_registration"] = {
                        "status": "pending",
                        "message": "수집 파일은 저장되었습니다. DB 등록은 다음 수집 시 자동 재시도됩니다. DB 연결과 aiops setup 실행 여부를 확인하세요.",
                    }
            print(json.dumps(result, ensure_ascii=False, indent=2))
            if registration_failed:
                return 11
            return 10 if result["status"] == "partial" else 0
        elif args.command == "report":
            from aiops.reporting.service import ReportingService

            if args.report_action == "list":
                output = ReportingService(settings).list(args.type)
            elif args.report_action == "run" and settings.database_url:
                from aiops.storage.jobs import submit

                output = submit(
                    settings,
                    args.type,
                    args.date,
                    config=args.config,
                    interval_minutes=args.interval_minutes,
                )
                print(json.dumps(output, ensure_ascii=False, indent=2))
                return 0
            else:
                output = ReportingService(
                    settings,
                    provider=provider or CodexProvider(settings),
                    sources_config=args.config,
                    interval_minutes=args.interval_minutes,
                ).execute(args.type, args.date, analyze=args.report_action == "run")
            print(json.dumps(output, ensure_ascii=False, indent=2))
            return (
                10
                if isinstance(output, dict) and not output["coverage_complete"]
                else 0
            )
        else:
            # Bound the read even for unexpectedly large files; never expose paths/content in errors.
            with args.input.open("rb") as stream:
                raw = stream.read(settings.max_input_bytes + 1)
            if not raw or len(raw) > settings.max_input_bytes:
                raise ProviderError(ErrorKind.CONFIG)
            snapshot = raw.decode("utf-8")
            json.loads(snapshot)  # Phase 1 accepts sanitized JSON snapshots only.
            output = (
                (provider or CodexProvider(settings))
                .analyze(AnalysisRequest(snapshot))
                .model_dump()
            )
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    except NoReportInput:
        print(json.dumps({
            "error": "no_report_input",
            "message": "대상 기간에 수집 데이터 또는 완료된 일일 보고서가 없습니다. aiops status로 데이터 경로와 수집 상태를 확인하세요. 일일 분석 기본 대상은 한국 시간 기준 전날입니다.",
        }, ensure_ascii=False), file=sys.stderr)
        return 2
    except ProviderError as exc:
        print(json.dumps({"error": exc.kind.value}), file=sys.stderr)
        return EXIT_CODES[exc.kind]
    except psycopg.Error:
        print(json.dumps({"error": "database"}), file=sys.stderr)
        return 11
    except (OSError, ValueError, sqlite3.Error):
        print(json.dumps({"error": ErrorKind.CONFIG.value}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
