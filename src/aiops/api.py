"""HTTP entry points for Airflow; files, credentials and execution stay on this host."""

import json
import secrets
import sqlite3
from datetime import date as CalendarDate
from pathlib import Path
from typing import Literal
from uuid import UUID

import psycopg
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field

from aiops.collectors.config import load_config
from aiops.collectors.service import CollectionService
from aiops.operations import NoReportInput, status
from aiops.providers.base import ErrorKind, ProviderError
from aiops.storage.import_sqlite import sync_batches
from aiops.storage.jobs import JobQueue, public_job, submit
from aiops.storage.reports import PostgresReportStore
from aiops.worker import Worker


class CollectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    services: list[str] | None = Field(default=None, min_length=1, max_length=64)


class ReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["daily", "weekly", "monthly"]
    date: CalendarDate | None = None
    interval_minutes: int = Field(default=5, ge=1, le=1440)


def create_app(settings, *, token: str, sources_config: Path, provider=None):
    if not token or not settings.database_url:
        raise ValueError("API requires AIOPS_API_TOKEN and AIOPS_DATABASE_URL")
    header = APIKeyHeader(name="X-AIOps-Key", auto_error=False)

    def authenticate(key: str | None = Depends(header)):
        if key is None or not secrets.compare_digest(key.encode(), token.encode()):
            raise HTTPException(status_code=401, detail="unauthorized")

    app = FastAPI(
        title="EFT Library AIOps", version="1",
        dependencies=[Depends(authenticate)],
        docs_url=None, redoc_url=None, openapi_url=None,
    )

    @app.exception_handler(NoReportInput)
    def no_input(request: Request, exc: NoReportInput):
        return JSONResponse(status_code=422, content={
            "error": "no_report_input",
            "message": "대상 기간의 수집 데이터 또는 완료된 일일 보고서가 없습니다. 일일 기본 대상은 한국 시간 기준 전날입니다.",
        })

    @app.exception_handler(psycopg.Error)
    def database_error(request: Request, exc: psycopg.Error):
        return JSONResponse(status_code=503, content={"error": "database"})

    @app.exception_handler(ProviderError)
    def provider_error(request: Request, exc: ProviderError):
        code = 409 if exc.kind == ErrorKind.BUSY else 503
        return JSONResponse(status_code=code, content={"error": exc.kind.value})

    def configuration_error(request, exc):
        return JSONResponse(status_code=500, content={"error": "configuration"})

    for kind in (OSError, ValueError, sqlite3.Error):
        app.add_exception_handler(kind, configuration_error)

    @app.get("/v1/status")
    def overview():
        return status(settings)

    @app.post("/v1/collect")
    def collect(body: CollectRequest):
        result = CollectionService(load_config(sources_config), settings.data_dir).collect(
            body.services
        )
        try:
            result["database_registration"] = sync_batches(settings)
        except (psycopg.Error, OSError, ValueError, sqlite3.Error, ProviderError):
            result["database_registration"] = {"status": "pending"}
            return JSONResponse(status_code=503, content={
                "error": "batch_registration_pending",
                "message": "수집 파일은 저장되었습니다. 다음 수집 시 DB 등록을 재시도합니다.",
                "collection": result,
            })
        return result

    @app.post("/v1/reports", status_code=202)
    def report(body: ReportRequest):
        return submit(
            settings, body.type, body.date.isoformat() if body.date else None,
            config=sources_config, interval_minutes=body.interval_minutes,
        )

    @app.post("/v1/worker/run-once")
    def worker_once():
        # A synchronous HTTP request, no memory-only background task queue.
        # PostgreSQL's Worker lock also fences CLI and overlapping HTTP calls.
        with Worker(settings, provider=provider) as worker:
            return worker.once()

    @app.get("/v1/jobs/{job_id}")
    def job(job_id: UUID):
        queue = JobQueue(settings)
        try:
            row = queue.db.execute(
                "SELECT * FROM public.aiops_analysis_jobs WHERE job_id=%s", (job_id,)
            ).fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="unknown_job")
            return public_job(row)
        finally:
            queue.close()

    @app.get("/v1/reports/{report_id}")
    def report_result(report_id: str):
        store = PostgresReportStore(settings.data_dir / "reports", settings)
        try:
            row = store.row(report_id)
            if row is None:
                raise HTTPException(status_code=404, detail="unknown_report")
            return {
                "report_id": row["report_id"], "status": row["status"],
                "coverage_complete": bool(row["coverage_complete"]),
                "error": row["error"],
                "result": json.loads(row["result_json"]) if row["result_json"] else None,
            }
        finally:
            store.close()

    return app
