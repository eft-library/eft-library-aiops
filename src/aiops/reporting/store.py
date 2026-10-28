"""Local report catalogue; SQLite is authoritative, JSON files are recoverable views."""

import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from aiops.collectors.store import fsync_dir


def encoded(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    )


class ReportStore:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        path = root / "reports.sqlite3"
        fd = os.open(path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(fd)
        self.db = sqlite3.connect(path, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS reports (
                report_id TEXT PRIMARY KEY, kind TEXT NOT NULL, period_start TEXT NOT NULL,
                period_end TEXT NOT NULL, revision INTEGER NOT NULL, fingerprint TEXT NOT NULL,
                status TEXT NOT NULL, created_at TEXT NOT NULL, completed_at TEXT,
                input_json TEXT NOT NULL, manifest_json TEXT NOT NULL,
                coverage_complete INTEGER NOT NULL, result_json TEXT, error TEXT,
                UNIQUE(kind, period_start, fingerprint), UNIQUE(kind, period_start, revision)
            );
        """)
        # Caller holds the reporting lock: no other live report owner exists.
        with self.db:
            self.db.execute(
                "UPDATE reports SET status='FAILED', error='interrupted' WHERE status='RUNNING'"
            )

    def close(self):
        self.db.close()

    def row(self, report_id):
        row = self.db.execute(
            "SELECT * FROM reports WHERE report_id=?", (report_id,)
        ).fetchone()
        return dict(row) if row else None

    def latest_completed(self, kind, start):
        row = self.db.execute(
            "SELECT * FROM reports WHERE kind=? AND period_start=? AND status='COMPLETED' ORDER BY revision DESC LIMIT 1",
            (kind, start),
        ).fetchone()
        return dict(row) if row else None

    def save_prepared(self, period, fingerprint, snapshot, manifest, complete):
        row = self.db.execute(
            "SELECT report_id FROM reports WHERE kind=? AND period_start=? AND fingerprint=?",
            (period.kind, period.label, fingerprint),
        ).fetchone()
        if row:
            return self.row(row[0]), True
        revision = self.db.execute(
            "SELECT COALESCE(MAX(revision), 0)+1 FROM reports WHERE kind=? AND period_start=?",
            (period.kind, period.label),
        ).fetchone()[0]
        report_id = f"{period.kind}-{period.label}-v{revision:04d}"
        with self.db:
            self.db.execute(
                """INSERT INTO reports
                (report_id,kind,period_start,period_end,revision,fingerprint,status,created_at,
                 input_json,manifest_json,coverage_complete) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    report_id,
                    period.kind,
                    period.label,
                    period.end.date().isoformat(),
                    revision,
                    fingerprint,
                    "PREPARED",
                    datetime.now(timezone.utc).isoformat(),
                    encoded(snapshot),
                    encoded(manifest),
                    int(complete),
                ),
            )
        return self.row(report_id), False

    def status(self, report_id, status, *, result=None, error=None):
        with self.db:
            self.db.execute(
                "UPDATE reports SET status=?, result_json=?, error=?, completed_at=? WHERE report_id=?",
                (
                    status,
                    encoded(result) if result is not None else None,
                    error,
                    datetime.now(timezone.utc).isoformat()
                    if status == "COMPLETED"
                    else None,
                    report_id,
                ),
            )
        return self.row(report_id)

    def export(self, row):
        directory = (
            self.root / row["kind"] / row["period_start"] / f"v{row['revision']:04d}"
        )
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        objects = {
            "input.json": row["input_json"],
            "manifest.json": row["manifest_json"],
            "metadata.json": encoded(
                {
                    key: row[key]
                    for key in (
                        "report_id",
                        "kind",
                        "period_start",
                        "period_end",
                        "revision",
                        "status",
                        "created_at",
                        "completed_at",
                        "coverage_complete",
                        "error",
                    )
                }
            ),
        }
        if row["result_json"] is not None:
            objects["result.json"] = row["result_json"]
        for name, content in objects.items():
            fd, temporary = tempfile.mkstemp(
                dir=directory, prefix="write-", suffix=".tmp"
            )
            try:
                with os.fdopen(fd, "w") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, directory / name)
            finally:
                Path(temporary).unlink(missing_ok=True)
        fsync_dir(directory)
        return directory

    def list(self, kind=None):
        return [
            dict(row)
            for row in self.db.execute(
                "SELECT report_id,kind,period_start,period_end,revision,status,coverage_complete,error "
                "FROM reports WHERE (? IS NULL OR kind=?) ORDER BY period_start DESC,kind,revision DESC",
                (kind, kind),
            )
        ]
