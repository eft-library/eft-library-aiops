"""User-facing setup and operational overview."""

import sqlite3

from aiops.storage import postgres
from aiops.storage.import_sqlite import import_sqlite, sync_batches
from aiops.storage.jobs import JobQueue


class NoReportInput(ValueError):
    """A closed reporting period has no usable collected input."""


def setup(settings):
    if not settings.database_url:
        raise ValueError("PostgreSQL configuration is required")
    return {
        "database": postgres.initialize(settings),
        "reports": import_sqlite(settings),
        "batches": sync_batches(settings),
    }


def status(settings):
    root = settings.data_dir.resolve()
    path = root / "collection" / "checkpoints.sqlite3"
    collection = {"batches": 0, "latest_collected_at": None}
    if path.is_file():
        db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        try:
            count, latest = db.execute(
                "SELECT count(*), max(created_at) FROM batches"
            ).fetchone()
            collection = {"batches": count, "latest_collected_at": latest}
        finally:
            db.close()
    output = {"data_dir": str(root), "collection": collection}
    if settings.database_url:
        output["database"] = postgres.doctor(settings)
        queue = JobQueue(settings)
        try:
            output["jobs"] = queue.list(limit=20)
            output["registered_batches"] = queue.db.execute(
                "SELECT count(*) AS count FROM public.aiops_batches"
            ).fetchone()["count"]
        finally:
            queue.close()
    else:
        output["database"] = {"status": "not_configured", "mode": "local"}
    return output
