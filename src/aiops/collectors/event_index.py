"""Disposable event-time/byte-offset indexes; source JSONL remains authoritative."""
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def build_event_index(folder):
    from aiops.investigation.probes import open_regular

    folder = Path(folder)
    fd, temporary = tempfile.mkstemp(prefix='.events-index-', dir=folder)
    os.close(fd)
    try:
        with os.fdopen(open_regular(folder / 'events.jsonl'), 'rb') as source:
            before = os.fstat(source.fileno())
            db = sqlite3.connect(temporary)
            try:
                db.executescript('''
                    CREATE TABLE metadata (version INTEGER, size INTEGER, mtime_ns INTEGER, invalid INTEGER);
                    CREATE TABLE entries (service TEXT, stamp TEXT, offset INTEGER, length INTEGER);
                ''')
                invalid = 0
                while True:
                    offset = source.tell()
                    line = source.readline()
                    if not line:
                        break
                    try:
                        event = json.loads(line)
                        stamp = datetime.fromisoformat(event['timestamp'])
                        if stamp.tzinfo is None or not line.endswith(b'\n'):
                            raise ValueError('invalid event')
                        db.execute('INSERT INTO entries VALUES (?, ?, ?, ?)', (
                            event['service'], stamp.astimezone(timezone.utc).isoformat(timespec='microseconds'),
                            offset, len(line)))
                    except (ValueError, KeyError, TypeError):
                        invalid += 1
                after = os.fstat(source.fileno())
                if (before.st_size, before.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                    raise ValueError('events changed while indexing')
                db.execute('INSERT INTO metadata VALUES (1, ?, ?, ?)',
                           (after.st_size, after.st_mtime_ns, invalid))
                db.execute('CREATE INDEX service_time ON entries(service, stamp, offset)')
                db.commit()
            finally:
                db.close()
        os.chmod(temporary, 0o644)
        with open(temporary, 'rb') as stream:
            os.fsync(stream.fileno())
        os.replace(temporary, folder / 'events.index.sqlite3')
        from aiops.collectors.store import fsync_dir

        fsync_dir(folder)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def event_offsets(folder, target, start, end, deadline):
    """Return at most 41 matching locations; refuse stale or incomplete indexes."""
    import time
    from aiops.investigation.probes import open_regular

    descriptor = open_regular(folder / 'events.index.sqlite3')
    try:
        db = sqlite3.connect(f'file:/dev/fd/{descriptor}?mode=ro&immutable=1', uri=True)
        try:
            db.set_progress_handler(lambda: int(time.monotonic() >= deadline), 100)
            version, size, modified, invalid = db.execute('SELECT * FROM metadata').fetchone()
            event_fd = open_regular(folder / 'events.jsonl')
            try:
                info = os.fstat(event_fd)
            finally:
                os.close(event_fd)
            if version != 1 or (size, modified) != (info.st_size, info.st_mtime_ns):
                raise ValueError('stale event index')
            bounds = [value.astimezone(timezone.utc).isoformat(timespec='microseconds') for value in (start, end)]
            rows = db.execute('''SELECT offset, length FROM entries
                WHERE service=? AND stamp>=? AND stamp<? ORDER BY stamp, offset LIMIT 41''',
                (target, *bounds)).fetchall()
            return rows, invalid
        finally:
            db.close()
    finally:
        os.close(descriptor)
