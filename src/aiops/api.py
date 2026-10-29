"""HTTP entry points for Airflow; files, credentials and execution stay on this host."""

import secrets
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from datetime import date as CalendarDate
from pathlib import Path
from typing import Literal
from uuid import UUID

import anyio
import psycopg
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.security import APIKeyHeader
from pydantic import BaseModel, ConfigDict, Field

from aiops.collectors.config import load_config
from aiops.collectors.service import CollectionService
from aiops.operations import NoReportInput, status
from aiops.providers.base import ErrorKind, ProviderError
from aiops.storage.import_sqlite import sync_batches
from aiops.storage.jobs import JobQueue, public_job, submit
from aiops.worker import Worker


class CollectRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    services: list[str] | None = Field(default=None, min_length=1, max_length=64)


class ReportRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: Literal["daily", "weekly", "monthly"]
    date: CalendarDate | None = None
    interval_minutes: int = Field(default=5, ge=1, le=1440)


class InvestigationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    report_id: str = Field(min_length=1, max_length=128, pattern=r"^daily-[0-9-]+-v[0-9]+$")
    request_key: str = Field(default="default", min_length=1, max_length=128, pattern=r"^[A-Za-z0-9_.:-]+$")


class InvestigationRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    investigation_id: UUID | None = None


def create_app(settings, *, token: str, sources_config: Path, provider=None, investigation_provider=None):
    if not token or not settings.database_url:
        raise ValueError("API requires AIOPS_API_TOKEN and AIOPS_DATABASE_URL")
    header = APIKeyHeader(name="X-AIOps-Key", auto_error=False)

    def authenticate(key: str | None = Depends(header)):
        if key is None or not secrets.compare_digest(key.encode(), token.encode()):
            raise HTTPException(status_code=401, detail="unauthorized")

    streams = set()
    stream_lock = threading.Lock()

    @asynccontextmanager
    async def lifespan(app):
        try:
            yield
        finally:
            with stream_lock:
                active = list(streams)
            for stream in active:
                stream.cancel.set()
            for stream in active:
                await anyio.to_thread.run_sync(lambda stream=stream: stream.thread.join(timeout=15))

    app = FastAPI(
        title="EFT Library AIOps", version="1",
        dependencies=[Depends(authenticate)], lifespan=lifespan,
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

    @app.post("/v1/investigations", status_code=202)
    def investigation_submit(body: InvestigationRequest):
        from aiops.investigation.store import InvalidParent, InvestigationStore

        store = InvestigationStore(settings)
        try:
            return store.enqueue(body.report_id, body.request_key, settings.model)
        except InvalidParent:
            raise HTTPException(status_code=422, detail="completed_daily_report_required") from None
        finally:
            store.close()

    @app.get("/v1/investigations/{investigation_id}")
    def investigation_get(investigation_id: UUID):
        from aiops.investigation.store import InvestigationStore

        store = InvestigationStore(settings)
        try:
            result = store.get(investigation_id)
            if result is None:
                raise HTTPException(status_code=404, detail="unknown_investigation")
            return result
        finally:
            store.close()

    @app.post("/v1/investigations/worker/run-once")
    def investigation_run(body: InvestigationRunRequest):
        from aiops.investigation.service import run_once

        return run_once(settings, sources_config, investigation_id=body.investigation_id,
                        provider=investigation_provider)

    @app.post("/v1/worker/run-once")
    def worker_once():
        # A synchronous HTTP request, no memory-only background task queue.
        # PostgreSQL's Worker lock also fences CLI and overlapping HTTP calls.
        with Worker(settings, provider=provider) as worker:
            return worker.once()

    @app.post("/v1/worker/run-once/stream")
    async def worker_stream(request: Request):
        from aiops.streaming import HEARTBEAT_SECONDS, WorkerStream, sse

        def finished(stream):
            with stream_lock:
                streams.discard(stream)

        with stream_lock:
            if streams:
                raise HTTPException(status_code=409, detail="busy")
            stream = WorkerStream(settings, provider=provider, on_done=finished, secrets=(token,))
            streams.add(stream)
        try:
            await anyio.to_thread.run_sync(stream.start)
        except BaseException:
            stream.cancel.set()
            raise

        if await request.is_disconnected():
            stream.cancel.set()

        async def events():
            last_sent = time.monotonic()
            try:
                while True:
                    if await request.is_disconnected():
                        break
                    event = await anyio.to_thread.run_sync(stream.next_event)
                    if event is not None:
                        yield sse(event)
                        last_sent = time.monotonic()
                    elif stream.done.is_set():
                        break
                    elif time.monotonic() - last_sent >= HEARTBEAT_SECONDS:
                        yield sse({"event": "heartbeat", "job_id": stream.job_id,
                                   "elapsed_seconds": round(time.monotonic() - stream.started, 1)})
                        last_sent = time.monotonic()
            finally:
                if not stream.done.is_set():
                    stream.cancel.set()

        return StreamingResponse(events(), media_type="text/event-stream", headers={
            "Cache-Control": "no-cache, no-transform", "X-Accel-Buffering": "no",
        })

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

    @app.get("/v1/reports")
    def report_list(
        type: Literal["daily", "weekly", "monthly"] | None = None,
        status: Literal["PREPARED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED"] | None = None,
        severity: Literal["OK", "INFO", "WARNING", "CRITICAL"] | None = None,
        start: CalendarDate | None = None, end: CalendarDate | None = None,
        page: int = Query(default=1, ge=1), page_size: int = Query(default=12, ge=1, le=100),
        history: bool = False,
    ):
        from aiops.storage import views

        if start and end and start > end:
            raise HTTPException(status_code=422, detail="invalid_date_range")
        return views.reports(settings, kind=type, status=status, severity=severity,
            start=start, end=end, page=page, page_size=page_size, history=history)

    @app.get("/v1/collections")
    def collection_list(page: int = Query(default=1, ge=1), page_size: int = Query(default=12, ge=1, le=100)):
        from aiops.storage import views

        return views.collections(settings, page=page, page_size=page_size)

    @app.get("/v1/jobs")
    def job_list(
        status: Literal["PENDING", "RUNNING", "COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"] | None = None,
        page: int = Query(default=1, ge=1), page_size: int = Query(default=12, ge=1, le=100),
    ):
        from aiops.storage import views

        return views.jobs(settings, status=status, page=page, page_size=page_size)

    @app.get("/v1/reports/{report_id}")
    def report_result(report_id: str):
        from aiops.storage import views

        result = views.report_detail(settings, report_id)
        if result is None:
            raise HTTPException(status_code=404, detail="unknown_report")
        return result

    return app
