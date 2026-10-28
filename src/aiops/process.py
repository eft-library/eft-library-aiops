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


def run_streamed(command: list[str], *, timeout: float, cwd=None, env=None,
                 stdin=None, on_stdout_line=None, cancel=None):
    """Read complete stdout JSONL frames while draining stdin/stderr concurrently."""
    import selectors
    import time

    if cancel is not None and cancel.is_set():
        raise ProviderError(ErrorKind.INTERRUPTED)
    try:
        process = subprocess.Popen(
            command, cwd=cwd, env=env, start_new_session=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
    except OSError:
        raise ProviderError(ErrorKind.CONFIG) from None
    stdout, stderr, pending = bytearray(), bytearray(), bytearray()
    payload = (stdin or '').encode('utf-8')
    written = 0
    deadline = time.monotonic() + timeout
    try:
        with selectors.DefaultSelector() as selector:
            for stream in (process.stdin, process.stdout, process.stderr):
                os.set_blocking(stream.fileno(), False)
            if payload:
                selector.register(process.stdin, selectors.EVENT_WRITE, 'stdin')
            else:
                process.stdin.close()
            selector.register(process.stdout, selectors.EVENT_READ, 'stdout')
            selector.register(process.stderr, selectors.EVENT_READ, 'stderr')
            while selector.get_map() or process.poll() is None:
                if cancel is not None and cancel.is_set():
                    raise ProviderError(ErrorKind.INTERRUPTED)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProviderError(ErrorKind.TIMEOUT)
                for key, _ in selector.select(min(0.1, remaining)):
                    stream = key.fileobj
                    if key.data == 'stdin':
                        try:
                            written += os.write(stream.fileno(), payload[written:written + 65536])
                        except BrokenPipeError:
                            written = len(payload)
                        except BlockingIOError:
                            continue
                        if written == len(payload):
                            selector.unregister(stream)
                            stream.close()
                        continue
                    try:
                        chunk = os.read(stream.fileno(), 65536)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(stream)
                        stream.close()
                        continue
                    target = stdout if key.data == 'stdout' else stderr
                    target.extend(chunk)
                    if len(stdout) + len(stderr) > 16 * 1024 * 1024:
                        raise ProviderError(ErrorKind.OUTPUT)
                    if key.data == 'stdout':
                        pending.extend(chunk)
                        while b'\n' in pending:
                            line, _, tail = pending.partition(b'\n')
                            pending[:] = tail
                            if len(line) > 1024 * 1024:
                                raise ProviderError(ErrorKind.OUTPUT)
                            if on_stdout_line is not None and line.strip():
                                on_stdout_line(line.decode('utf-8', errors='replace'))
                        if len(pending) > 1024 * 1024:
                            raise ProviderError(ErrorKind.OUTPUT)
            if pending.strip() and on_stdout_line is not None:
                on_stdout_line(pending.decode('utf-8', errors='replace'))
        return process.wait(), stdout.decode('utf-8', errors='replace'), stderr.decode('utf-8', errors='replace')
    finally:
        stop_group(process)
        for stream in (process.stdin, process.stdout, process.stderr):
            if not stream.closed:
                stream.close()
