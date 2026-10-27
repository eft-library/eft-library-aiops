"""POSIX process-group lifecycle; used on macOS and Linux."""
import os
import signal
import subprocess

from aiops.providers.base import ErrorKind, ProviderError


def stop_group(process: subprocess.Popen) -> None:
    # Also kill descendants if the group leader has already exited.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run(command: list[str], *, timeout: float, cwd=None, env=None, stdin=None):
    try:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, start_new_session=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding='utf-8', errors='replace',
        )
    except OSError:
        raise ProviderError(ErrorKind.CONFIG) from None
    try:
        stdout, stderr = process.communicate(stdin, timeout=timeout)
        return process.returncode, stdout, stderr
    except subprocess.TimeoutExpired:
        raise ProviderError(ErrorKind.TIMEOUT) from None
    finally:
        stop_group(process)
