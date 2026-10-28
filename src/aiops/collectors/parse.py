import json
import math
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from aiops.collectors.models import Event
from aiops.collectors.redact import mask

STAMP = re.compile(
    r"\d{4}-\d\d-\d\d[ T]\d\d:\d\d:\d\d(?:[.,]\d+)?(?:Z|[+-]\d\d:?\d\d)?"
)
ACCESS_TIME = re.compile(r"\[(\d\d/[A-Za-z]{3}/\d{4}:\d\d:\d\d:\d\d [+-]\d{4})\]")
LEVEL = re.compile(
    r"\b(TRACE|DEBUG|INFO|WARNING|WARN|ERROR|FATAL|CRITICAL|PANIC)\b", re.I
)
HTTP = re.compile(
    r'"(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\s+[^"\r\n]+"\s+(\d{3})\b', re.I
)
NPM_HTTP = re.compile(
    r"(?:\[(\d{3})\]\s+\[(\d{3})\]|\]\s+-\s+(\d{3})\s+(\d{3})\s+-)\s+(?:GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b"
)
LATENCY = re.compile(
    r"\b(?:request_time|response_time|duration)\s*[=:]\s*(\d+(?:\.\d+)?)\s*(ms|s)?\b",
    re.I,
)
CONTINUATION = re.compile(
    r"^(?:\s+|Traceback\b|Caused by:|During handling|[\w.]+(?:Error|Exception):)"
)


def timestamp(text: str, timezone: str) -> datetime | None:
    match = STAMP.search(text)
    try:
        if match:
            value = datetime.fromisoformat(
                match.group().replace(",", ".").replace("Z", "+00:00")
            )
            return value if value.tzinfo else value.replace(tzinfo=ZoneInfo(timezone))
        match = ACCESS_TIME.search(text)
        if match:
            return datetime.strptime(match[1], "%d/%b/%Y:%H:%M:%S %z")
    except ValueError:
        pass
    return None


def normalize(lines, *, service: str, path: str, timezone: str, source_type: str):
    """Group stack trace continuations within a read window; never execute log text."""
    groups = []
    for line, decoding_error in lines:
        if groups and CONTINUATION.match(line) and not STAMP.match(line.lstrip()):
            previous, bad, truncated = groups[-1]
            combined = previous + "\n" + line
            groups[-1] = (
                combined[:16384],
                bad or decoding_error,
                truncated or len(combined) > 16384,
            )
        else:
            groups.append((line[:16384], decoding_error, len(line) > 16384))
    for text, decoding_error, truncated in groups:
        meta = {"parse_failed": False, "encoding_error": decoding_error}
        if service == "npm":
            domain = {
                "2": "back.eftlibrary.com",
                "3": "image.eftlibrary.com",
                "4": "eftlibrary.com",
            }
            host = re.match(r"proxy-host-(\d+)_", Path(path).name)
            if host and host[1] in domain:
                meta["domain"] = domain[host[1]]
        stamp = timestamp(text, timezone)
        level_match = LEVEL.search(text)
        level = level_match[1].upper() if level_match else "UNKNOWN"
        if level == "WARN":
            level = "WARNING"
        if level == "PANIC":
            level = "FATAL"
        try:
            structured = json.loads(text)
            if isinstance(structured, dict):
                value = str(
                    structured.get("level", structured.get("levelname", ""))
                ).upper()
                if value in {
                    "TRACE",
                    "DEBUG",
                    "INFO",
                    "WARNING",
                    "ERROR",
                    "FATAL",
                    "CRITICAL",
                }:
                    level = value
        except (ValueError, RecursionError):
            pass
        status = HTTP.search(text)
        npm = NPM_HTTP.search(text)
        if status or npm:
            code = int(status[1] if status else (npm[2] or npm[4]))
            meta["http_status"] = code
            if code >= 500:
                level = "ERROR"
            elif code >= 400:
                level = "WARNING"
            elif level == "UNKNOWN":
                level = "INFO"
        latency = LATENCY.search(text)
        if latency:
            meta["response_time_ms"] = float(latency[1]) * (
                1000 if latency[2] == "s" else 1
            )
            if latency[2] is None:
                # Nginx request_time is seconds; ambiguous duration has no invented unit.
                if "request_time" in latency[0]:
                    meta["response_time_ms"] *= 1000
                else:
                    del meta["response_time_ms"]
        if not math.isfinite(meta.get("response_time_ms", 0)):
            meta.pop("response_time_ms", None)
        meta["parse_failed"] = stamp is None or level == "UNKNOWN"
        if truncated:
            meta["message_truncated"] = True
        yield Event(
            event_id="",
            timestamp=stamp,
            service=service,
            source=source_type,
            level=level,
            message=mask(text),
            file=mask(path),
            metadata=meta,
        )
