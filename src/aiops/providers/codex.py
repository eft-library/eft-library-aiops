import fcntl
import json
import os
import shutil
import sys
import tempfile
import time
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from aiops.config import Settings
from aiops.data_permissions import data_directory, data_file
from aiops.process import run
from aiops.providers.base import (
    AnalysisRequest,
    AnalysisResult,
    ErrorKind,
    ProviderError,
    Report,
)
from aiops.providers.instructions import INSTRUCTIONS, classify


@contextmanager
def execution_lock(directory):
    data_directory(directory)
    with os.fdopen(
        data_file(directory / "codex.lock", os.O_WRONLY | os.O_CREAT | os.O_APPEND), "a"
    ) as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ProviderError(ErrorKind.BUSY) from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def executable(settings: Settings) -> str:
    path = shutil.which(settings.codex_bin)
    if path is None:
        raise ProviderError(ErrorKind.CONFIG)
    return str(os.path.realpath(path))


def doctor(settings: Settings) -> dict:
    binary = executable(settings)
    code, version, _ = run([binary, "--version"], timeout=15)
    if code:
        raise ProviderError(ErrorKind.CONFIG)
    code, _, _ = run([binary, "login", "status"], timeout=15)
    if code:
        raise ProviderError(ErrorKind.AUTH)
    return {
        "python": sys.version.split()[0],
        "codex_version": version.strip(),
        "authentication": "configured",
        "provider": "codex-cli",
        "note": "Local login status only; remote access and quota are not checked.",
    }


class CodexProvider:
    def __init__(self, settings: Settings):
        self.settings = settings
        data_directory(settings.data_dir)

    def analyze(self, request: AnalysisRequest) -> AnalysisResult:
        if len(request.snapshot.encode("utf-8")) > self.settings.max_input_bytes:
            raise ProviderError(ErrorKind.CONFIG)
        job_id = str(uuid4())
        started = time.monotonic()
        if self.settings.database_url:
            from aiops.storage.postgres import CODEX_LOCK, advisory_lock

            gate = advisory_lock(self.settings, CODEX_LOCK)
        else:
            gate = nullcontext()
        with gate, execution_lock(self.settings.data_dir):
            status = "FAILED"
            error = None
            thread_id = None
            try:
                binary = executable(self.settings)
                # Invoke without a shell or user MCP configuration.
                # Keep auth in the user's normal Codex credential store.
                env = {
                    key: value
                    for key, value in os.environ.items()
                    if key
                    in {
                        "PATH",
                        "HOME",
                        "USER",
                        "TMPDIR",
                        "LANG",
                        "LC_ALL",
                        "CODEX_HOME",
                        "SSL_CERT_FILE",
                        "SSL_CERT_DIR",
                        "CODEX_CA_CERTIFICATE",
                        "HTTPS_PROXY",
                        "HTTP_PROXY",
                        "NO_PROXY",
                    }
                }
                with tempfile.TemporaryDirectory(
                    prefix="snapshot-", dir=self.settings.data_dir.resolve()
                ) as workspace:
                    data_directory(Path(workspace))
                    schema = os.path.join(workspace, "report.schema.json")
                    with os.fdopen(
                        data_file(Path(schema), os.O_WRONLY | os.O_CREAT | os.O_TRUNC),
                        "w",
                    ) as stream:
                        json.dump(Report.model_json_schema(), stream)
                    command = [
                        binary,
                        "--ask-for-approval",
                        "never",
                        "exec",
                        "--ignore-user-config",
                        "--ignore-rules",
                        "--ephemeral",
                        "--sandbox",
                        "read-only",
                        "--skip-git-repo-check",
                        "--json",
                        "--config",
                        'web_search="disabled"',
                        "--config",
                        "project_doc_max_bytes=0",
                        "--output-schema",
                        schema,
                        "-",
                    ]
                    if self.settings.model:
                        command[
                            command.index("exec") + 1 : command.index("exec") + 1
                        ] = ["--model", self.settings.model]
                    code, stdout, stderr = run(
                        command,
                        timeout=self.settings.timeout,
                        cwd=workspace,
                        env=env,
                        stdin=INSTRUCTIONS
                        + "\n<untrusted_snapshot>\n"
                        + request.snapshot
                        + "\n</untrusted_snapshot>",
                    )
                try:
                    events = [
                        json.loads(line) for line in stdout.splitlines() if line.strip()
                    ]
                    errors = [
                        event
                        for event in events
                        if event.get("type") in ("error", "turn.failed")
                    ]
                    if code or errors:
                        raise ProviderError(classify(json.dumps(errors) + stderr))
                    if not any(
                        event.get("type") == "turn.completed" for event in events
                    ):
                        raise ProviderError(ErrorKind.OUTPUT)
                    messages = [
                        event["item"]["text"]
                        for event in events
                        if event.get("type") == "item.completed"
                        and event.get("item", {}).get("type") == "agent_message"
                    ]
                    thread_id = next(
                        (
                            event["thread_id"]
                            for event in events
                            if event.get("type") == "thread.started"
                        ),
                        None,
                    )
                    if not messages:
                        raise ProviderError(ErrorKind.OUTPUT)
                    result = AnalysisResult(
                        job_id=job_id,
                        provider="codex-cli",
                        model=self.settings.model,
                        thread_id=thread_id,
                        report=Report.model_validate_json(messages[-1]),
                    )
                except (ValueError, TypeError, KeyError, AttributeError):
                    raise ProviderError(ErrorKind.OUTPUT) from None
                thread_id = result.thread_id
                status = "COMPLETED"
                return result
            except OSError:
                error = ErrorKind.CONFIG.value
                raise ProviderError(ErrorKind.CONFIG) from None
            except ProviderError as exc:
                error = exc.kind.value
                status = "TIMEOUT" if exc.kind == ErrorKind.TIMEOUT else "FAILED"
                raise
            finally:
                metadata = {
                    "job_id": job_id,
                    "status": status,
                    "error": error,
                    "thread_id": thread_id,
                    "provider": "codex-cli",
                    "model": self.settings.model,
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                    "duration_seconds": round(time.monotonic() - started, 3),
                }
                path = self.settings.data_dir / f"{job_id}.json"
                descriptor = data_file(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
                with os.fdopen(descriptor, "w") as stream:
                    json.dump(metadata, stream)
