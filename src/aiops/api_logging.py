"""API stdout/stderr grouped by calendar date in Asia/Seoul."""

import io
import os
import threading
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from aiops.data_permissions import data_directory, data_file

SEOUL = ZoneInfo("Asia/Seoul")


class DailyLogStream(io.TextIOBase):
    def __init__(self, directory: Path, *, clock=None):
        self.directory = directory
        self.clock = clock or (lambda: datetime.now(SEOUL))
        self.lock = threading.Lock()
        data_directory(directory)

    @property
    def encoding(self):
        return "utf-8"

    def writable(self):
        return True

    def write(self, text):
        if not text:
            return 0
        with self.lock:
            day = self.clock().astimezone(SEOUL).date().isoformat()
            path = self.directory / f"api-{day}.log"
            descriptor = data_file(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND)
            with os.fdopen(descriptor, "w", encoding="utf-8", errors="replace") as stream:
                stream.write(text)
        return len(text)

    def flush(self):
        # Each write closes its file, so there is no pending buffer at midnight.
        pass


@contextmanager
def api_log_output(directory):
    if directory is None:
        yield
    else:
        stream = DailyLogStream(directory)
        with redirect_stdout(stream), redirect_stderr(stream):
            yield
