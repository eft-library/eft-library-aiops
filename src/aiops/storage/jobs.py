import json
from datetime import date, datetime
from pathlib import Path
from uuid import UUID, uuid4

from psycopg.types.json import Jsonb

from aiops.reporting.service import ReportingService
from aiops.storage.postgres import connect, require_schema

PRIORITIES = {"critical": 0, "high": 1, "normal": 2, "low": 3}
RETRYABLE = {"network", "timeout", "execution", "interrupted"}


def public_job(row):
    result = dict(row)
    for key, value in result.items():
        if isinstance(value, (datetime, date)):
            result[key] = value.isoformat()
        elif isinstance(value, UUID):
            result[key] = str(value)
    return result


class JobQueue:
    def __init__(self, settings):
        self.db = connect(settings)
        try:
            require_schema(self.db)
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    def enqueue(self, report_id, *, priority="normal", max_attempts=3):
        if priority not in PRIORITIES or not 1 <= max_attempts <= 10:
            raise ValueError("invalid job policy")
        with self.db.transaction():
            version = self.db.execute(
                "SELECT status FROM public.aiops_report_versions WHERE report_id=%s FOR UPDATE",
                (report_id,),
            ).fetchone()
            if not version:
                raise ValueError("unknown report")
            status = "COMPLETED" if version["status"] == "COMPLETED" else "PENDING"
            self.db.execute(
                """INSERT INTO public.aiops_analysis_jobs
                (job_id,report_id,priority,status,max_attempts,finished_at)
                VALUES (%s,%s,%s,%s,%s,CASE WHEN %s='COMPLETED' THEN now() END)
                ON CONFLICT(report_id) DO UPDATE SET priority=CASE WHEN aiops_analysis_jobs.status='PENDING'
                    THEN LEAST(aiops_analysis_jobs.priority,EXCLUDED.priority) ELSE aiops_analysis_jobs.priority END""",
                (
                    uuid4(),
                    report_id,
                    PRIORITIES[priority],
                    status,
                    max_attempts,
                    status,
                ),
            )
            row = self.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs WHERE report_id=%s",
                (report_id,),
            ).fetchone()
        return public_job(row)

    def list(self, limit=100):
        return [
            public_job(row)
            for row in self.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs ORDER BY requested_at DESC LIMIT %s",
                (limit,),
            )
        ]

    def claim(self, worker_id):
        with self.db.transaction():
            row = self.db.execute("""SELECT * FROM public.aiops_analysis_jobs WHERE status='PENDING' AND available_at<=now()
                ORDER BY priority,requested_at,job_id FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if not row:
                return None
            row = self.db.execute(
                """UPDATE public.aiops_analysis_jobs SET status='RUNNING',attempt_count=attempt_count+1,
                worker_id=%s,started_at=now(),finished_at=NULL WHERE job_id=%s RETURNING *""",
                (worker_id, row["job_id"]),
            ).fetchone()
            self.db.execute(
                "UPDATE public.aiops_report_versions SET status='RUNNING',error=NULL WHERE report_id=%s",
                (row["report_id"],),
            )
            self.db.execute(
                "INSERT INTO public.aiops_job_attempts(job_id,attempt,worker_id,status) VALUES (%s,%s,%s,'RUNNING')",
                (row["job_id"], row["attempt_count"], worker_id),
            )
        return row

    def finish(self, job, worker_id, *, result=None, error=None):
        with self.db.transaction():
            current = self.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs WHERE job_id=%s FOR UPDATE",
                (job["job_id"],),
            ).fetchone()
            if current["status"] != "RUNNING" or current["worker_id"] != worker_id:
                return None  # Another owner recovered it: do not overwrite the result.
            if result is not None:
                status, version_status, delay = "COMPLETED", "COMPLETED", 0
            elif error == "stale_input":
                status, version_status, delay = "CANCELLED", "CANCELLED", 0
            elif error == "busy" or (
                error in RETRYABLE
                and current["attempt_count"] < current["max_attempts"]
            ):
                status, version_status = "PENDING", "PREPARED"
                delay = (
                    5
                    if error == "busy"
                    else min(3600, 30 * 2 ** (current["attempt_count"] - 1))
                )
            else:
                status = "TIMEOUT" if error == "timeout" else "FAILED"
                version_status, delay = "FAILED", 0
            self.db.execute(
                """UPDATE public.aiops_analysis_jobs SET status=%s,error=%s,
                available_at=now()+(%s * interval '1 second'), finished_at=CASE WHEN %s='PENDING' THEN NULL ELSE now() END,
                worker_id=NULL WHERE job_id=%s""",
                (status, error, delay, status, job["job_id"]),
            )
            attempt_status = (
                "COMPLETED"
                if result is not None
                else (
                    "CANCELLED"
                    if error == "stale_input"
                    else ("TIMEOUT" if error == "timeout" else "FAILED")
                )
            )
            self.db.execute(
                """UPDATE public.aiops_job_attempts SET status=%s,error=%s,finished_at=now(),provider_job_id=%s,thread_id=%s
                WHERE job_id=%s AND attempt=%s""",
                (
                    attempt_status,
                    error,
                    result.get("job_id") if result else None,
                    result.get("thread_id") if result else None,
                    job["job_id"],
                    current["attempt_count"],
                ),
            )
            # Busy isn't a Codex attempt. Remove the placeholder attempt before reusing its number.
            if error == "busy":
                self.db.execute(
                    "DELETE FROM public.aiops_job_attempts WHERE job_id=%s AND attempt=%s",
                    (job["job_id"], current["attempt_count"]),
                )
                self.db.execute(
                    "UPDATE public.aiops_analysis_jobs SET attempt_count=attempt_count-1 WHERE job_id=%s",
                    (job["job_id"],),
                )
            self.db.execute(
                """UPDATE public.aiops_report_versions SET status=%s,error=%s,result_json=%s,
                completed_at=CASE WHEN %s='COMPLETED' THEN now() ELSE NULL END WHERE report_id=%s""",
                (
                    version_status,
                    error,
                    Jsonb(result) if result is not None else None,
                    version_status,
                    job["report_id"],
                ),
            )
            row = self.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs WHERE job_id=%s",
                (job["job_id"],),
            ).fetchone()
        return public_job(row)

    def recover(self):
        jobs = self.db.execute(
            "SELECT * FROM public.aiops_analysis_jobs WHERE status='RUNNING'"
        ).fetchall()
        for job in jobs:
            self.finish(job, job["worker_id"], error="interrupted")
        return len(jobs)

    def retry(self, job_id):
        job_id = UUID(job_id)
        with self.db.transaction():
            job = self.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs WHERE job_id=%s FOR UPDATE",
                (job_id,),
            ).fetchone()
            if (
                not job
                or job["status"] not in {"FAILED", "TIMEOUT", "CANCELLED"}
                or job["attempt_count"] >= 100
            ):
                raise ValueError("only terminal failed jobs can be retried")
            self.db.execute(
                """UPDATE public.aiops_analysis_jobs SET status='PENDING',error=NULL,available_at=now(),
                finished_at=NULL,worker_id=NULL,max_attempts=%s WHERE job_id=%s""",
                (min(100, job["attempt_count"] + 3), job_id),
            )
            self.db.execute(
                "UPDATE public.aiops_report_versions SET status='PREPARED',error=NULL WHERE report_id=%s",
                (job["report_id"],),
            )
            return public_job(
                self.db.execute(
                    "SELECT * FROM public.aiops_analysis_jobs WHERE job_id=%s",
                    (job_id,),
                ).fetchone()
            )

    def cancel(self, job_id):
        job_id = UUID(job_id)
        with self.db.transaction():
            row = self.db.execute(
                "UPDATE public.aiops_analysis_jobs SET status='CANCELLED',finished_at=now(),error='cancelled' WHERE job_id=%s AND status='PENDING' RETURNING *",
                (job_id,),
            ).fetchone()
            if not row:
                raise ValueError("only pending jobs can be cancelled")
            self.db.execute(
                "UPDATE public.aiops_report_versions SET status='CANCELLED',error='cancelled' WHERE report_id=%s",
                (row["report_id"],),
            )
            return public_job(row)


def submit(
    settings,
    kind,
    date=None,
    *,
    config,
    interval_minutes=5,
    priority="normal",
    max_attempts=3,
):
    if not settings.database_url:
        raise ValueError("PostgreSQL is required for the job queue")
    prepared = ReportingService(
        settings, sources_config=config, interval_minutes=interval_minutes
    ).execute(kind, date)
    snapshot = json.loads(Path(prepared["input"]).read_text())
    if not snapshot.get("source_batch_count", 0) and not snapshot.get("daily_reports"):
        from aiops.operations import NoReportInput

        raise NoReportInput(
            "no collected input or completed daily reports for this period"
        )
    queue = JobQueue(settings)
    try:
        return queue.enqueue(
            prepared["report_id"], priority=priority, max_attempts=max_attempts
        )
    finally:
        queue.close()
