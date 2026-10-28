import json
from datetime import date, datetime

from psycopg.types.json import Jsonb

from aiops.collectors.models import Batch
from aiops.data_permissions import data_directory
from aiops.providers.base import ErrorKind, ProviderError
from aiops.reporting.store import ReportStore, encoded
from aiops.storage.postgres import CATALOGUE_LOCK, connect, require_schema

REPORT_SELECT = """SELECT v.*, p.kind, p.period_start, p.period_end
                  FROM aiops.report_versions v JOIN aiops.reports p USING(period_id)"""


def compatible(row):
    if row is None:
        return None
    row = dict(row)
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            row[key] = value.isoformat()
        elif (
            key in {"input_json", "manifest_json", "result_json"} and value is not None
        ):
            row[key] = encoded(value)
    return row


class PostgresReportStore(ReportStore):
    """Same report contract, PostgreSQL authority, existing JSON export layout."""

    def __init__(self, root, settings, *, lock=True):
        self.root, self.settings = root, settings
        data_directory(root)
        self.db = connect(settings)
        try:
            require_schema(self.db)
            if (
                lock
                and not self.db.execute(
                    "SELECT pg_try_advisory_lock(%s) AS acquired", (CATALOGUE_LOCK,)
                ).fetchone()["acquired"]
            ):
                raise ProviderError(ErrorKind.BUSY)
        except BaseException:
            self.db.close()
            raise

    def row(self, report_id):
        return compatible(
            self.db.execute(
                REPORT_SELECT + " WHERE report_id=%s", (report_id,)
            ).fetchone()
        )

    def latest_completed(self, kind, start):
        return compatible(
            self.db.execute(
                REPORT_SELECT
                + " WHERE kind=%s AND period_start=%s AND status='COMPLETED' ORDER BY revision DESC LIMIT 1",
                (kind, start),
            ).fetchone()
        )

    def completed_overlapping(self, kind, start, end):
        return [
            compatible(row)
            for row in self.db.execute(
                REPORT_SELECT
                + " WHERE kind=%s AND status='COMPLETED' AND period_start<%s AND period_end>%s ORDER BY period_start,revision DESC",
                (kind, end, start),
            )
        ]

    def _period(self, kind, start, end):
        self.db.execute(
            """INSERT INTO aiops.reports(kind,period_start,period_end)
                           VALUES (%s,%s,%s) ON CONFLICT(kind,period_start) DO NOTHING""",
            (kind, start, end),
        )
        return self.db.execute(
            "SELECT period_id FROM aiops.reports WHERE kind=%s AND period_start=%s FOR UPDATE",
            (kind, start),
        ).fetchone()["period_id"]

    def register_batch(self, batch_id):
        if "/" in batch_id or "\\" in batch_id or batch_id in {".", ".."}:
            raise ValueError("invalid batch identifier")
        if self.db.execute(
            "SELECT 1 FROM aiops.batches WHERE batch_id=%s", (batch_id,)
        ).fetchone():
            return False
        collection = self.root.parent / "collection" / "batches"
        batch = Batch.model_validate_json(
            (collection / batch_id / "snapshot.json").read_text()
        )
        self.db.execute(
            """INSERT INTO aiops.batches
            (batch_id,collected_at,period_start,period_end,relative_path,coverage,totals,masked)
            VALUES (%s,%s,%s,%s,%s,%s,%s,true) ON CONFLICT(batch_id) DO NOTHING""",
            (
                batch_id,
                batch.collected_at,
                batch.time_range.start,
                batch.time_range.end,
                f"collection/batches/{batch_id}",
                Jsonb(
                    {key: value.model_dump() for key, value in batch.coverage.items()}
                ),
                Jsonb(batch.totals),
            ),
        )
        return True

    def _dependencies(self, report_id, manifest):
        for batch_id in manifest.get("batch_ids", []):
            self.register_batch(batch_id)
            self.db.execute(
                """INSERT INTO aiops.report_dependencies
                (report_id,dependency_type,dependency_id,batch_id) VALUES (%s,'batch',%s,%s) ON CONFLICT DO NOTHING""",
                (report_id, batch_id, batch_id),
            )
        for kind, ids in (
            ("daily", manifest.get("daily_report_ids", {}).values()),
            ("weekly_context", manifest.get("weekly_context_ids", [])),
        ):
            for child in ids:
                self.db.execute(
                    """INSERT INTO aiops.report_dependencies
                    (report_id,dependency_type,dependency_id,child_report_id) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                    (report_id, kind, child, child),
                )

    def save_prepared(self, period, fingerprint, snapshot, manifest, complete):
        with self.db.transaction():
            period_id = self._period(period.kind, period.label, period.end.date())
            row = self.db.execute(
                "SELECT report_id FROM aiops.report_versions WHERE period_id=%s AND fingerprint=%s",
                (period_id, fingerprint),
            ).fetchone()
            if row:
                return self.row(row["report_id"]), True
            revision = self.db.execute(
                "SELECT COALESCE(MAX(revision),0)+1 AS revision FROM aiops.report_versions WHERE period_id=%s",
                (period_id,),
            ).fetchone()["revision"]
            report_id = f"{period.kind}-{period.label}-v{revision:04d}"
            self.db.execute(
                """INSERT INTO aiops.report_versions
                (report_id,period_id,revision,fingerprint,status,input_json,manifest_json,coverage_complete)
                VALUES (%s,%s,%s,%s,'PREPARED',%s,%s,%s)""",
                (
                    report_id,
                    period_id,
                    revision,
                    fingerprint,
                    Jsonb(snapshot),
                    Jsonb(manifest),
                    bool(complete),
                ),
            )
            self._dependencies(report_id, manifest)
        return self.row(report_id), False

    def status(self, report_id, status, *, result=None, error=None):
        self.db.execute(
            """UPDATE aiops.report_versions SET status=%s,result_json=%s,error=%s,
            completed_at=CASE WHEN %s='COMPLETED' THEN now() ELSE NULL END WHERE report_id=%s""",
            (
                status,
                Jsonb(result) if result is not None else None,
                error,
                status,
                report_id,
            ),
        )
        return self.row(report_id)

    def list(self, kind=None):
        return [
            compatible(row)
            for row in self.db.execute(
                """SELECT v.report_id,p.kind,p.period_start,p.period_end,
            v.revision,v.status,v.coverage_complete,v.error FROM aiops.report_versions v
            JOIN aiops.reports p USING(period_id) WHERE (%s::text IS NULL OR kind=%s)
            ORDER BY period_start DESC,kind,revision DESC""",
                (kind, kind),
            )
        ]

    def import_rows(self, rows):
        count = 0
        with self.db.transaction():
            for row in rows:
                existing = self.row(row["report_id"])
                if existing:
                    if existing["fingerprint"] != row["fingerprint"]:
                        raise ValueError(
                            "report ID collision; use a fresh aiops database or reconcile versions"
                        )
                    continue
                period_id = self._period(
                    row["kind"], row["period_start"], row["period_end"]
                )
                status = "FAILED" if row["status"] == "RUNNING" else row["status"]
                self.db.execute(
                    """INSERT INTO aiops.report_versions
                    (report_id,period_id,revision,fingerprint,status,created_at,completed_at,input_json,
                     manifest_json,coverage_complete,result_json,error)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (
                        row["report_id"],
                        period_id,
                        row["revision"],
                        row["fingerprint"],
                        status,
                        row["created_at"],
                        row["completed_at"],
                        Jsonb(json.loads(row["input_json"])),
                        Jsonb(json.loads(row["manifest_json"])),
                        bool(row["coverage_complete"]),
                        Jsonb(json.loads(row["result_json"]))
                        if row["result_json"]
                        else None,
                        "interrupted_import"
                        if row["status"] == "RUNNING"
                        else row["error"],
                    ),
                )
                self._dependencies(row["report_id"], json.loads(row["manifest_json"]))
                count += 1
        return count
