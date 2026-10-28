"""SQLite checkpoints + durable directory staging, serialized by a collector lock."""

import fcntl
import json
import os
import shutil
import sqlite3
from contextlib import contextmanager
from pathlib import Path

from aiops.providers.base import ErrorKind, ProviderError


def fsync_dir(path: Path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


@contextmanager
def collector_lock(root: Path):
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (root / "collect.lock").open("a") as lock:
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
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db_path = self.root / "checkpoints.sqlite3"
        descriptor = os.open(self.db_path, os.O_WRONLY | os.O_CREAT, 0o600)
        os.close(descriptor)
        self.db = sqlite3.connect(self.db_path, timeout=5)
        self.db.execute("PRAGMA journal_mode=DELETE")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS checkpoints (
                source TEXT NOT NULL, device INTEGER NOT NULL, inode INTEGER NOT NULL,
                state TEXT NOT NULL, PRIMARY KEY(source, device, inode)
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

    def commit(self, batch, events, checkpoints, *, before_commit=None):
        """Persist all masked data before atomically committing catalogue + checkpoints."""
        stage = self.staging / batch.batch_id
        stage.mkdir(mode=0o700)
        for name, data in (
            ("snapshot.json", batch.model_dump_json()),
            (
                "events.jsonl",
                "".join(event.model_dump_json() + "\n" for event in events),
            ),
        ):
            fd = os.open(stage / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
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
            for source, state in checkpoints:
                self.db.execute(
                    "INSERT OR REPLACE INTO checkpoints VALUES (?, ?, ?, ?)",
                    (source, state["device"], state["inode"], json.dumps(state)),
                )
        self.publish(batch.batch_id)
        return self.batches / batch.batch_id / "snapshot.json"

    def publish(self, batch_id):
        os.replace(self.staging / batch_id, self.batches / batch_id)
        fsync_dir(self.batches)
        fsync_dir(self.staging)
