"""Single synchronous PostgreSQL Worker. No in-memory background job queue."""

import hashlib
import json
import signal
import threading
from dataclasses import replace
from uuid import uuid4

from aiops.providers.base import AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider
from aiops.reporting.service import ReportingService, validate_references
from aiops.storage.jobs import JobQueue
from aiops.storage.postgres import WORKER_LOCK, advisory_lock
from aiops.storage.reports import PostgresReportStore


class Worker:
    def __init__(self, settings, *, provider=None):
        self.settings = settings
        self.provider = provider
        self.worker_id = uuid4()

    def __enter__(self):
        self.guard_context = advisory_lock(self.settings, WORKER_LOCK)
        self.guard = self.guard_context.__enter__()
        self.queue = None
        try:
            self.queue = JobQueue(self.settings)
            self.recovered = self.queue.recover()
        except BaseException:
            if self.queue is not None:
                self.queue.close()
            self.guard_context.__exit__(None, None, None)
            raise
        return self

    def __exit__(self, *args):
        self.queue.close()
        self.guard_context.__exit__(*args)

    def once(self):
        self.guard.execute("SELECT 1")
        job = self.queue.claim(self.worker_id)
        if job is None:
            return {"status": "idle", "recovered_jobs": self.recovered}
        reports = PostgresReportStore(
            self.settings.data_dir / "reports", self.settings, lock=False
        )
        interrupted = False
        try:
            row = reports.row(job["report_id"])
            try:
                from aiops.providers.instructions import INSTRUCTIONS

                policy = json.loads(row["manifest_json"]).get("analysis_policy", {})
                policy_current = (
                    policy.get("instructions_sha256")
                    == hashlib.sha256(INSTRUCTIONS.encode()).hexdigest()
                )
                if not policy_current or not ReportingService(self.settings).is_current(
                    job["report_id"]
                ):
                    outcome = self.queue.finish(
                        job, self.worker_id, error="stale_input"
                    )
                else:
                    provider = self.provider or CodexProvider(
                        replace(self.settings, model=policy["model"])
                    )
                    result = provider.analyze(AnalysisRequest(row["input_json"]))
                    validate_references(result, json.loads(row["input_json"]))
                    self.guard.execute("SELECT 1")
                    outcome = self.queue.finish(
                        job, self.worker_id, result=result.model_dump()
                    )
            except ProviderError as exc:
                if exc.kind == ErrorKind.DATABASE:
                    raise
                self.guard.execute("SELECT 1")
                outcome = self.queue.finish(job, self.worker_id, error=exc.kind.value)
            except (OSError, ValueError):
                outcome = self.queue.finish(job, self.worker_id, error="configuration")
            except KeyboardInterrupt:
                outcome = self.queue.finish(job, self.worker_id, error="interrupted")
                interrupted = True
            if outcome is not None:
                # DB commit precedes file publication. A report list/run can regenerate views.
                reports.export(reports.row(job["report_id"]))
            if interrupted:
                raise KeyboardInterrupt
            return outcome or {"job_id": str(job["job_id"]), "status": "lease_lost"}
        finally:
            reports.close()


def run(settings, *, once=False, poll_seconds=5, provider=None):
    if not 0.1 <= poll_seconds <= 60:
        raise ValueError("invalid poll interval")
    stop = threading.Event()

    def terminate(signum, frame):
        stop.set()
        raise KeyboardInterrupt

    previous = signal.signal(signal.SIGTERM, terminate)
    try:
        with Worker(settings, provider=provider) as worker:
            while not stop.is_set():
                result = worker.once()
                if once or result["status"] != "idle":
                    print(json.dumps(result, ensure_ascii=False))
                if once:
                    if result["status"] in {"FAILED", "TIMEOUT"}:
                        return {
                            "authentication": 3,
                            "network": 4,
                            "rate_limit": 5,
                            "timeout": 6,
                            "invalid_output": 8,
                            "configuration": 2,
                        }.get(result.get("error"), 9)
                    return 0
                if result.get("error") in {"authentication", "rate_limit"}:
                    # Stop without an automatic supervisor restart storm. Operator fixes auth/quota first.
                    return 0
                stop.wait(poll_seconds)
    except KeyboardInterrupt:
        return 130
    finally:
        signal.signal(signal.SIGTERM, previous)
