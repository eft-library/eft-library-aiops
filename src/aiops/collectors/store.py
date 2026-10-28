"""SQLite checkpoints + durable directory staging, serialized by a collector lock."""

import fcntl
import json
import os
import shutil
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from aiops.collectors.models import Batch, Event, Metric
from aiops.data_permissions import data_directory, data_file
from aiops.providers.base import ErrorKind, ProviderError
from aiops.reporting.periods import day_for


def fsync_dir(path: Path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def collector_lock(root: Path):
    data_directory(root)
    with os.fdopen(
        data_file(root / "collect.lock", os.O_WRONLY | os.O_CREAT | os.O_APPEND), "a"
    ) as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ProviderError(ErrorKind.BUSY) from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


class BatchStore:
    def __init__(self, root: Path):
        self.root = root.resolve()
        self.staging = self.root / ".staging"
        self.batches = self.root / "batches"
        for path in (self.root, self.staging, self.batches):
            data_directory(path)
        self.db_path = self.root / "checkpoints.sqlite3"
        descriptor = data_file(self.db_path, os.O_WRONLY | os.O_CREAT)
        os.close(descriptor)
        self.db = sqlite3.connect(self.db_path, timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS collection_state (
                name TEXT PRIMARY KEY, value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS checkpoints (
                source TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
                state TEXT NOT NULL, PRIMARY KEY(source, device, inode)
            );
            CREATE TABLE IF NOT EXISTS day_versions (
                day TEXT PRIMARY KEY, generation INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batch_days (
                day TEXT NOT NULL, batch_id TEXT NOT NULL,
                events INTEGER NOT NULL, metrics INTEGER NOT NULL,
                unknown_events INTEGER NOT NULL, observed INTEGER NOT NULL,
                PRIMARY KEY(day, batch_id)
            );
            CREATE TABLE IF NOT EXISTS indexed_batches (
                batch_id TEXT PRIMARY KEY, metrics_complete INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY, created_at TEXT NOT NULL
            );
        """)

    def close(self):
        self.db.close()

    def recover(self):
        registered = {row[0] for row in self.db.execute("SELECT batch_id FROM batches")}
        for batch_id in registered:
            final, staging = self.batches / batch_id, self.staging / batch_id
            if not final.is_dir():
                if not staging.is_dir():
                    raise ValueError(
                        "registered batch data is missing; restore the data directory backup"
                    )
                self.publish(batch_id)
        # A crash before the DB commit leaves no advanced checkpoints.
        for path in self.staging.iterdir():
            if (
                path.is_dir()
                and path.name.startswith("batch-")
                and path.name not in registered
            ):
                shutil.rmtree(path)
        fsync_dir(self.staging)

    def checkpoints(self, source: str) -> dict:
        return {
            (device, inode): json.loads(state)
            for device, inode, state in self.db.execute(
                "SELECT device, inode, state FROM checkpoints WHERE source=?", (source,)
            )
        }

    def commit(
        self,
        batch,
        events,
        checkpoints,
        *,
        before_commit=None,
        metrics=None,
        states=None,
    ):
        """Persist all masked data before atomically committing catalogue + checkpoints."""
        metrics = batch.metrics if metrics is None else metrics
        stage = self.staging / batch.batch_id
        data_directory(stage)
        for name, data in (
            ("snapshot.json", batch.model_dump_json()),
            (
                "metrics.jsonl",
                "".join(metric.model_dump_json() + "\n" for metric in metrics),
            ),
            (
                "events.jsonl",
                "".join(event.model_dump_json() + "\n" for event in events),
            ),
        ):
            fd = data_file(stage / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            with os.fdopen(fd, "w", encoding="utf-8") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        fsync_dir(stage)
        fsync_dir(self.staging)
        if before_commit:
            before_commit()
        with self.db:
            self.db.execute(
                "INSERT INTO batches VALUES (?, ?)",
                (batch.batch_id, batch.collected_at.isoformat()),
            )
            self.index_batch(batch, events, metrics, metrics_complete=True)
            for source, state in checkpoints:
                self.db.execute(
                    "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?, ?)",
                    (source, state["device"], state["inode"], json.dumps(state)),
                )
            for name, value in states or []:
                self.db.execute(
                    "INSERT OR REPLACE INTO collection_state VALUES (?, ?)",
                    (name, json.dumps(value)),
                )
        self.publish(batch.batch_id)
        return self.batches / batch.batch_id / "snapshot.json"

    def publish(self, batch_id):
        os.replace(self.staging / batch_id, self.batches / batch_id)
        fsync_dir(self.batches)
        fsync_dir(self.staging)

    def index_batch(self, batch, events, metrics, *, metrics_complete):
        from collections import defaultdict

        days = defaultdict(
            lambda: {"events": 0, "metrics": 0, "unknown_events": 0, "observed": 0}
        )
        observed = day_for(batch.collected_at)
        days[observed]["observed"] = 1
        for event in events:
            if event.timestamp is None:
                days[observed]["unknown_events"] += 1
            else:
                days[day_for(event.timestamp)]["events"] += 1
        for metric in metrics:
            days[day_for(metric.timestamp)]["metrics"] += 1
        for day in batch.selection.excluded_by_day:
            days[day]  # Index affected days even if a time filter excluded every event.
        for day, counts in days.items():
            self.db.execute(
                "INSERT INTO batch_days VALUES (?, ?, ?, ?, ?, ?)",
                (
                    day,
                    batch.batch_id,
                    counts["events"],
                    counts["metrics"],
                    counts["unknown_events"],
                    counts["observed"],
                ),
            )
            self.db.execute(
                "INSERT INTO day_versions VALUES (?, 1) ON CONFLICT(day) DO UPDATE SET generation=generation+1",
                (day,),
            )
        self.db.execute(
            "INSERT INTO indexed_batches VALUES (?, ?)",
            (batch.batch_id, int(metrics_complete)),
        )

    def index_existing(self):
        """Upgrade old batches without resetting checkpoints or inventing timestamps."""
        rows = self.db.execute(
            "SELECT batch_id FROM batches WHERE batch_id NOT IN (SELECT batch_id FROM indexed_batches)"
        ).fetchall()
        for (batch_id,) in rows:
            folder = self.batches / batch_id
            batch = Batch.model_validate_json((folder / "snapshot.json").read_text())
            complete = (folder / "metrics.jsonl").is_file()
            with self.db:
                self.index_batch(
                    batch,
                    self.read_events(batch_id),
                    self.read_metrics(batch_id, batch),
                    metrics_complete=complete,
                )

    def read_events(self, batch_id):
        with (self.batches / batch_id / "events.jsonl").open() as stream:
            for line in stream:
                yield Event.model_validate_json(line)

    def read_metrics(self, batch_id, batch):
        path = self.batches / batch_id / "metrics.jsonl"
        if path.is_file():
            with path.open() as stream:
                for line in stream:
                    yield Metric.model_validate_json(line)
        else:
            yield from batch.metrics

    def day_version(self, day):
        row = self.db.execute(
            "SELECT generation FROM day_versions WHERE day=?", (day,)
        ).fetchone()
        return row[0] if row else 0

    def day_batches(self, day):
        return self.db.execute(
            """SELECT d.batch_id, d.events, d.metrics, d.unknown_events,
            d.observed, i.metrics_complete FROM batch_days d JOIN indexed_batches i
            ON d.batch_id=i.batch_id WHERE d.day=? ORDER BY d.batch_id""",
            (day,),
        ).fetchall()

    def state(self, name, default=None):
        row = self.db.execute(
            "SELECT value FROM collection_state WHERE name=?", (name,)
        ).fetchone()
        return json.loads(row[0]) if row else default
