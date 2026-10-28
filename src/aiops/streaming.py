"""Bounded SSE transport; PostgreSQL remains the only analysis job queue."""

import json
import threading
import time
from collections import deque
from datetime import datetime, timezone

import psycopg

from aiops.providers.base import ErrorKind, ProviderError
from aiops.providers.progress import PublicProgress
from aiops.worker import Worker

HEARTBEAT_SECONDS = 10


class WorkerStream:
    def __init__(self, settings, *, provider=None, on_done=None, secrets=()):
        self.settings = settings
        self.sanitizer = PublicProgress(settings, lambda event: None, extra_secrets=secrets)
        self.provider = provider
        self.on_done = on_done
        self.cancel = threading.Event()
        self.ready = threading.Event()
        self.done = threading.Event()
        self.condition = threading.Condition()
        self.events = deque()
        self.sequence = 0
        self.dropped = 0
        self.started = time.monotonic()
        self.startup_error = None
        self.job_id = None
        self.thread = threading.Thread(target=self._run, name="aiops-worker-stream", daemon=True)

    def emit(self, event):
        event = dict(event)
        for field in ("message", "summary"):
            if isinstance(event.get(field), str):
                event[field] = self.sanitizer.clean(event[field])
        with self.condition:
            if event.get("event") == "worker.claimed":
                self.job_id = event["job_id"]
            self.sequence += 1
            event = {**event, "sequence": self.sequence,
                     "timestamp": datetime.now(timezone.utc).isoformat()}
            if self.job_id:
                event.setdefault("job_id", self.job_id)
            if len(self.events) == 128:
                self.events.popleft()
                self.dropped += 1
            if event.get("event") == "worker.finished":
                event["dropped_events"] = self.dropped
            self.events.append(event)
            self.condition.notify_all()

    def next_event(self):
        with self.condition:
            if not self.events and not self.done.is_set():
                self.condition.wait(timeout=1)
            return self.events.popleft() if self.events else None

    def start(self):
        try:
            self.thread.start()
        except RuntimeError:
            self.done.set()
            if self.on_done:
                self.on_done(self)
            raise ProviderError(ErrorKind.EXECUTION) from None
        if not self.ready.wait(timeout=35):
            self.cancel.set()
            raise ProviderError(ErrorKind.TIMEOUT)
        if self.startup_error is not None:
            raise self.startup_error

    def _run(self):
        worker = Worker(self.settings, provider=self.provider)
        entered = False
        try:
            # Resolve DB/Worker lock failures before committing HTTP 200 headers.
            try:
                worker.__enter__()
                entered = True
            except (ProviderError, psycopg.Error, OSError, ValueError) as exc:
                self.startup_error = exc
                return
            self.ready.set()
            self.emit({"event": "worker.started"})
            try:
                result = worker.once(on_event=self.emit, cancel=self.cancel)
            finally:
                worker.__exit__(None, None, None)
                entered = False
            self.emit({"event": "worker.finished", "job": result,
                       "dropped_events": self.dropped})
        except ProviderError as exc:
            self.emit({"event": "error", "error": exc.kind.value})
        except psycopg.Error:
            self.emit({"event": "error", "error": "database"})
        except Exception:
            self.emit({"event": "error", "error": "execution"})
        finally:
            if entered:
                try:
                    worker.__exit__(None, None, None)
                except Exception:
                    pass
            self.ready.set()
            self.done.set()
            with self.condition:
                self.condition.notify_all()
            if self.on_done:
                self.on_done(self)


def sse(event):
    name = event["event"]
    return f"event: {name}\ndata: {json.dumps(event, ensure_ascii=False)}\n\n"

