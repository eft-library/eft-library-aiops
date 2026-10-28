from datetime import datetime

from aiops.api_logging import DailyLogStream, api_log_output


def test_daily_logs_switch_at_korean_midnight_and_append_after_restart(tmp_path):
    clock = iter([
        datetime.fromisoformat("2026-10-28T14:59:59+00:00"),
        datetime.fromisoformat("2026-10-28T15:00:00+00:00"),
    ])
    directory = tmp_path / "log_history"
    stream = DailyLogStream(directory, clock=lambda: next(clock))
    stream.write("before midnight\n")
    stream.write("after midnight\n")
    restarted = DailyLogStream(directory, clock=lambda: datetime.fromisoformat("2026-10-29T01:00:00+09:00"))
    restarted.write("restarted\n")
    assert (directory / "api-2026-10-28.log").read_text() == "before midnight\n"
    today = directory / "api-2026-10-29.log"
    assert today.read_text() == "after midnight\nrestarted\n"
    assert today.stat().st_mode & 0o777 == 0o644
    assert directory.stat().st_mode & 0o777 == 0o755


def test_api_log_output_captures_stdout_and_stderr(tmp_path):
    import sys

    with api_log_output(tmp_path):
        print("access log")
        print("error log", file=sys.stderr)
    content = next(tmp_path.glob("api-*.log")).read_text()
    assert content == "access log\nerror log\n"
