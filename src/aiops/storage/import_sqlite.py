"""Explicit, non-destructive import. The source SQLite file is opened read-only."""

import sqlite3

from aiops.collectors.store import collector_lock
from aiops.storage.reports import PostgresReportStore


def import_sqlite(settings):
    root = settings.data_dir.resolve() / "reports"
    path = root / "reports.sqlite3"
    if not path.is_file():
        return {"status": "no_local_reports", "imported": 0}
    with collector_lock(root):
        source = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
        source.row_factory = sqlite3.Row
        target = None
        try:
            rows = [
                dict(row)
                for row in source.execute(
                    "SELECT * FROM reports ORDER BY kind,period_start,revision"
                )
            ]
            target = PostgresReportStore(root, settings)
            count = target.import_rows(rows)
            return {
                "status": "imported",
                "imported": count,
                "local_sqlite_preserved": True,
            }
        finally:
            source.close()
            if target:
                target.close()


def sync_batches(settings):
    from aiops.collectors.store import BatchStore

    root = settings.data_dir.resolve()
    with collector_lock(root / "reports"):
        target = PostgresReportStore(root / "reports", settings)
        try:
            with collector_lock(root / "collection"):
                source = BatchStore(root / "collection")
                try:
                    source.recover()
                    known = {
                        row["batch_id"]
                        for row in target.db.execute(
                            "SELECT batch_id FROM aiops.batches"
                        )
                    }
                    ids = [
                        row[0]
                        for row in source.db.execute("SELECT batch_id FROM batches")
                    ]
                    count = 0
                    with target.db.transaction():
                        for batch_id in ids:
                            if batch_id not in known:
                                count += int(target.register_batch(batch_id))
                    return {
                        "status": "synced",
                        "registered": count,
                        "total_local_batches": len(ids),
                    }
                finally:
                    source.close()
        finally:
            target.close()
