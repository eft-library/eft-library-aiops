import json
import sys
import threading
import time

import pytest

from aiops.config import Settings
from aiops.process import run_streamed
from aiops.providers.base import AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.progress import PublicProgress
from aiops.streaming import WorkerStream, sse


def test_process_delivers_lines_before_exit_and_drains_large_stdin_stderr(tmp_path):
    marker = tmp_path / "received"
    code = '''import json,sys,time
from pathlib import Path
sys.stderr.write('e'*200000);sys.stderr.flush()
payload=sys.stdin.read()
print(json.dumps({'message':'공개 메시지','bytes':len(payload)}),flush=True)
limit=time.monotonic()+3
while not Path(sys.argv[1]).exists() and time.monotonic()<limit:time.sleep(.01)
assert Path(sys.argv[1]).exists(),'callback did not run before process exited'
print(json.dumps({'done':True}),flush=True)
'''
    lines = []

    def receive(line):
        lines.append(json.loads(line))
        marker.touch()

    result, stdout, stderr = run_streamed([sys.executable, "-c", code, str(marker)],
        timeout=5, stdin="p"*200000, on_stdout_line=receive)
    assert result == 0
    assert lines == [{"message": "공개 메시지", "bytes": 200000}, {"done": True}]
    assert len(stderr) == 200000
    assert len(stdout.splitlines()) == 2


@pytest.mark.parametrize("cancelled", [False, True])
def test_stream_timeout_or_cancel_kills_process_group(tmp_path, cancelled):
    marker = tmp_path / "should-not-exist"
    child_code = f"import time;from pathlib import Path;time.sleep(1);Path({str(marker)!r}).touch();time.sleep(30)"
    code = f"import subprocess,sys,time;subprocess.Popen([sys.executable,'-c',{child_code!r}]);print('{{}}',flush=True);time.sleep(30)"
    cancel = threading.Event()
    with pytest.raises(ProviderError) as error:
        run_streamed([sys.executable, "-c", code], timeout=0.3 if not cancelled else 5,
            on_stdout_line=lambda line: cancel.set() if cancelled else None, cancel=cancel)
    assert error.value.kind == (ErrorKind.INTERRUPTED if cancelled else ErrorKind.TIMEOUT)
    time.sleep(1.05)
    assert not marker.exists()


def test_stream_limits_oversized_output():
    with pytest.raises(ProviderError) as error:
        run_streamed([sys.executable,"-c","print('x'*1100000,flush=True)"], timeout=3)
    assert error.value.kind == ErrorKind.OUTPUT


def test_public_progress_never_exposes_reasoning_tools_or_secrets(monkeypatch):
    monkeypatch.setenv("AIOPS_API_TOKEN", "private-api-key")
    emitted = []
    progress = PublicProgress(Settings(database_url="host=localhost dbname=test password=private-db-pass"), emitted.append)
    for event in [
        {"type":"item.completed", "item":{"type":"reasoning", "text":"private reasoning text"}},
        {"type":"item.updated", "item":{"type":"agent_message", "text":"partial secret"}},
        {"type":"item.completed", "item":{"type":"command_execution", "command":"private command", "aggregated_output":"private output"}},
        {"type":"item.completed", "item":{"type":"agent_message", "id":"one", "text":"분석 중 private-api-key private-db-pass password=otherpass 10.20.30.40"}},
        {"type":"turn.completed", "usage":{"input_tokens":100,"output_tokens":30,"secret":"private usage"}},
        {"type":"turn.failed", "error":{"message":"401 private error text"}},
    ]:
        progress.line(json.dumps(event))
    text = json.dumps(emitted,ensure_ascii=False)
    for secret in ("private reasoning", "partial secret", "private command", "private output", "private-api-key", "private-db-pass", "otherpass", "10.20.30.40", "private usage", "private error"):
        assert secret not in text
    assert any(event["event"] == "codex.message" for event in emitted)
    assert emitted[-2]["usage"] == {"input_tokens":100,"output_tokens":30}
    assert emitted[-1]["error"] == "authentication"
    assert "private-api-key" not in progress.clean("private-\x1b[31mapi-key")


def test_structured_final_is_only_a_candidate_until_worker_validation():
    emitted = []
    progress = PublicProgress(Settings(),emitted.append)
    progress.line(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":json.dumps({"summary":"not validated", "findings":[]})}}))
    assert emitted[0]["event"] == "codex.report_candidate"
    assert "not validated" not in json.dumps(emitted)


def test_stream_buffer_is_bounded_and_terminal_event_is_retained():
    stream = WorkerStream(Settings(), secrets=("private-param-key",))
    for i in range(500):
        stream.emit({"event":"codex.message", "message":f"public {i} private-param-key"})
    stream.emit({"event":"worker.finished","job":{"status":"COMPLETED"}})
    assert len(stream.events) == 128
    assert stream.dropped == 373
    events = list(stream.events)
    assert events[-1]["event"] == "worker.finished"
    assert events[-1]["dropped_events"] == stream.dropped
    assert "private-param-key" not in json.dumps(events)
    encoded = sse(events[0])
    assert encoded.startswith("event: codex.message\ndata: ") and encoded.endswith("\n\n")


def test_codex_streaming_preserves_final_report_validation(tmp_path, monkeypatch):
    from test_phase1 import REPORT

    from aiops.providers import codex

    output_events = [
        {"type":"thread.started","thread_id":"test-thread"},
        {"type":"turn.started"},
        {"type":"item.completed","item":{"type":"reasoning","text":"do not forward"}},
        {"type":"item.completed","item":{"id":"message","type":"agent_message","text":"공개 진행 메시지"}},
        {"type":"item.completed","item":{"id":"report","type":"agent_message","text":json.dumps(REPORT)}},
        {"type":"turn.completed","usage":{"input_tokens":10,"output_tokens":5}},
    ]

    def runner(*args, **kwargs):
        for event in output_events:
            kwargs["on_stdout_line"](json.dumps(event))
        return 0, "\n".join(json.dumps(event) for event in output_events), ""

    monkeypatch.setattr(codex,"run_streamed",runner)
    emitted=[]
    result=codex.CodexProvider(Settings(codex_bin=sys.executable,data_dir=tmp_path)).analyze_stream(AnalysisRequest("{}"),emitted.append)
    assert result.thread_id == "test-thread" and result.report.summary == REPORT["summary"]
    assert "do not forward" not in json.dumps(emitted)
    assert any(e["event"]=="codex.message" for e in emitted)
