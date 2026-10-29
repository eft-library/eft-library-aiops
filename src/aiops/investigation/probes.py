"""Fixed read-only probes. No shell, model-generated query, or checkpoint update."""

import fnmatch
import json
import os
import stat
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg

from aiops.collectors.config import load_config
from aiops.collectors.docker import DockerCollector
from aiops.collectors.system import SystemCollector
from aiops.providers.base import ProviderError
from aiops.providers.progress import PublicProgress
from aiops.storage.postgres import connect


def now():
    return datetime.now(timezone.utc).isoformat()


def open_regular(path):
    """Open each component relative to an FD, without following any symlink."""
    path = Path(os.path.abspath(path))
    descriptor = os.open(path.anchor, os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=descriptor)
            os.close(descriptor)
            descriptor = child
        result = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=descriptor)
    finally:
        os.close(descriptor)
    if not stat.S_ISREG(os.fstat(result).st_mode):
        os.close(result)
        raise ValueError("not a regular file")
    return result


def log_excerpt(source):
    # Scan only a bounded portion. This is explicitly NOT historical recovery.
    candidates, scanned = [], 0
    deadline = time.monotonic() + 1
    if source.type == "file":
        candidates = [source.path]
    else:
        pending = [source.path]
        while pending and scanned < 256 and time.monotonic() < deadline:
            directory = pending.pop()
            if directory.is_symlink() or any(p.is_symlink() for p in directory.parents):
                continue
            with os.scandir(directory) as entries:
                for entry in entries:
                    scanned += 1
                    if scanned > 256 or time.monotonic() >= deadline:
                        break
                    if entry.is_symlink():
                        continue
                    if entry.is_dir(follow_symlinks=False) and source.recursive:
                        pending.append(Path(entry.path))
                    elif entry.is_file(follow_symlinks=False) and fnmatch.fnmatch(entry.name, source.pattern) and not entry.name.endswith('.gz'):
                        candidates.append(Path(entry.path))
        candidates.sort(key=lambda path: path.stat(follow_symlinks=False).st_mtime, reverse=True)
    if not candidates:
        raise OSError("no log in bounded scan")
    descriptor = open_regular(candidates[0])
    try:
        size = os.fstat(descriptor).st_size
        offset = max(0, size - 8192)
        os.lseek(descriptor, offset, os.SEEK_SET)
        raw = os.read(descriptor, 8192)
        if offset:
            raw = raw.partition(b'\n')[2]
        # Do not present a half-written final record as a complete observation.
        raw = raw.rpartition(b'\n')[0]
        return {"text": raw.decode('utf-8', errors='replace'), "file_size_bytes": size,
                "scope": "One recent file tail, up to 8192 bytes, from at most 256 scanned entries. Not complete history."}
    finally:
        os.close(descriptor)


def postgres_statistics(settings):
    with connect(settings) as db:
        with db.transaction():
            db.execute("SET TRANSACTION READ ONLY")
            db.execute("SET LOCAL statement_timeout = '2000ms'")
            db.execute("SET LOCAL lock_timeout = '500ms'")
            database = db.execute("""SELECT numbackends,xact_commit,xact_rollback,deadlocks,
                temp_bytes,blks_read,blks_hit,stats_reset FROM pg_stat_database
                WHERE datname=current_database()""").fetchone()
            activity = db.execute("""SELECT state,wait_event_type,count(*) AS sessions,
                max(EXTRACT(epoch FROM (now()-xact_start))) AS oldest_transaction_seconds
                FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid()
                GROUP BY state,wait_event_type LIMIT 20""").fetchall()
            locks = db.execute("""SELECT count(*) AS waiting_locks FROM pg_locks
                WHERE NOT granted AND database=(SELECT oid FROM pg_database WHERE datname=current_database())""").fetchone()
            return {"scope": "Configured AIOps database only; activity visibility depends on role privileges. Counters are cumulative since stats_reset, not historical period rates.",
                    "database": database, "activity": activity, "locks": locks}


def collect_observations(settings, sources_config):
    config = load_config(sources_config)
    cleaner = PublicProgress(settings, lambda event: None)
    observations = []

    def add(service, probe, function):
        started = now()
        try:
            value = function()
            # Clean recursively, before persistence and model input. Truncation is explicit.
            def clean(item):
                if isinstance(item, str):
                    return cleaner.clean(item)
                if isinstance(item, dict):
                    return {str(key): clean(val) for key, val in item.items()}
                if isinstance(item, (list, tuple)):
                    return [clean(val) for val in item]
                if isinstance(item, (int, float, bool)) or item is None:
                    return item
                return cleaner.clean(str(item))
            value = clean(value)
            status = "partial" if isinstance(value, dict) and value.get("failures") else "observed"
        except (OSError, ValueError, ProviderError, psycopg.Error):
            value, status = {"reason": "probe_unavailable"}, "unavailable"
        observations.append({"observation_id": f"obs-{len(observations)+1}", "service": service,
                             "probe": probe, "started_at": started, "finished_at": now(),
                             "status": status, "data": value})

    if config.system.enabled:
        def system():
            metrics, failures = SystemCollector().collect(config.system.disks)
            return {"metrics": [m.model_dump(mode='json') for m in metrics], "failures": failures,
                    "scope": "Current point samples; not the historical reporting period."}
        add('system', 'system_metrics', system)
    for service, source in config.sources.items():
        if source.enabled:
            add(service, 'recent_log_tail', lambda source=source: log_excerpt(source))
    if config.docker.enabled:
        def docker():
            metrics, failures = DockerCollector().collect(config.docker, min(10, config.docker.timeout_seconds))
            return {"metrics": [m.model_dump(mode='json') for m in metrics], "failures": failures,
                    "scope": "Configured containers only; no stdout, environment, inspect or command execution."}
        add('docker', 'container_metrics', docker)
    add('postgresql', 'aiops_database_statistics', lambda: postgres_statistics(settings))
    # The structured object, not any unmasked raw probe output, is persisted.
    json.dumps(observations, ensure_ascii=False)
    return observations
