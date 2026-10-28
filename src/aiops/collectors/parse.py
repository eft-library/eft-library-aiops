import json
import math
import re
from datetime import datetime
from datetime import timezone as datetime_timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from aiops.collectors.models import Event
from aiops.collectors.redact import mask

STAMP = re.compile(
    r"\d{4}[-/]\d\d[-/]\d\d[ T]\d\d:\d\d:\d\d(?:[.,]\d+)?(?:Z|[+-]\d\d(?::?\d\d)?)?"
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


ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
REDIS_TIME = re.compile(
    r"^(?:\d+:[A-Z]\s+)?(\d{1,2} [A-Za-z]{3} \d{4} \d\d:\d\d:\d\d(?:\.\d+)?)\s+([.*#-])\s"
)
ERROR_CLASS = re.compile(r"\b(?:[A-Za-z_][\w.]*)(?:Error|Exception):")
FRAME_ERROR = re.compile(r"^\s*[⨯×]")
STACK_TAIL = re.compile(
    r"^\s*(?:[}\]](?:[,;])?\s*$|(?:digest|code|cause|errno|syscall|address|port):)"
)
FASTAPI_LATENCY = re.compile(r"\s-\s(\d+(?:\.\d+)?)(ms|s)\s*$")
WRAPPED = re.compile(
    r"^\[(?P<stamp>\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2}|\s+[+-]\d{4})?)\] ?(?P<body>.*)$"
)


def log_body(line):
    wrapper = WRAPPED.match(line)
    return wrapper["body"] if wrapper else line


def timestamp(text: str, timezone: str) -> datetime | None:
    match = STAMP.search(text)
    if match:
        prefix = text[: match.start()]
        # A date inside a URL/error payload is not necessarily the log's event time.
        if not re.fullmatch(r"\s*\[?", prefix) and not re.search(
            r'(?i)(?:timestamp|asctime|time|date)["\x27]?\s*[=:]\s*["\x27]?$', prefix
        ):
            match = None
    try:
        if match:
            value = datetime.fromisoformat(
                match.group().replace("/", "-").replace(",", ".").replace("Z", "+00:00")
            )
            if value.tzinfo:
                return value
            suffix = text[match.end() :]
            offset = re.match(r"\s+([+-]\d{2}:?\d{2})(?=\]|\s|$)", suffix)
            if offset:
                return datetime.fromisoformat(value.isoformat() + offset[1])
            named = re.match(r"\s+(UTC|GMT|KST)\b", suffix)
            zone = (
                datetime_timezone.utc
                if named and named[1] in {"UTC", "GMT"}
                else ZoneInfo(timezone)
            )
            if named and named[1] == "KST":
                zone = ZoneInfo("Asia/Seoul")
            return value.replace(tzinfo=zone)
        match = ACCESS_TIME.search(text)
        if match:
            return datetime.strptime(match[1], "%d/%b/%Y:%H:%M:%S %z")
        match = REDIS_TIME.search(text)
        if match:
            fmt = "%d %b %Y %H:%M:%S.%f" if "." in match[1] else "%d %b %Y %H:%M:%S"
            return datetime.strptime(match[1], fmt).replace(tzinfo=ZoneInfo(timezone))
    except ValueError:
        pass
    return None


def starts_record(line):
    stripped = line.lstrip()
    return bool(
        STAMP.match(stripped.lstrip("["))
        or ACCESS_TIME.match(stripped)
        or REDIS_TIME.match(stripped)
        or (stripped.startswith("{") and STAMP.search(stripped))
    )


def error_context(line):
    line = log_body(line.splitlines()[0])
    return bool(
        FRAME_ERROR.match(line)
        or ERROR_CLASS.search(line)
        or re.search(r"\b(?:ERROR|FATAL|CRITICAL|PANIC)\b", line, re.I)
    )


def normalize(lines, *, service: str, path: str, timezone: str, source_type: str):
    """Group stack trace continuations within a read window; never execute log text."""
    groups = []
    for line, decoding_error in lines:
        line = ANSI.sub("", line)
        if not line.strip():
            continue
        wrapper = WRAPPED.match(line)
        body = log_body(line)
        continuation = CONTINUATION.match(body) or (
            groups and STACK_TAIL.match(body) and error_context(groups[-1][0])
        )
        if wrapper:
            continues_previous = bool(
                groups
                and continuation
                and error_context(groups[-1][0])
                and not LEVEL.match(body.lstrip())
            )
        else:
            continues_previous = bool(
                groups and continuation and not starts_record(line)
            )
        if continues_previous:
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
        first_line = text.splitlines()[0]
        stamp = timestamp(first_line, timezone)
        body = log_body(first_line)
        level_match = LEVEL.search(body)
        level = level_match[1].upper() if level_match else "UNKNOWN"
        if FRAME_ERROR.match(body) or ERROR_CLASS.search(body):
            level = "ERROR"
        if service == "nextjs" and level == "UNKNOWN":
            if body.lstrip().startswith("⚠"):
                level = "WARNING"
            elif body.lstrip().startswith(("✓", "▲", "$ ")) or re.match(
                r"^\s*-\s+(?:Local|Network):", body
            ):
                level = "INFO"
        redis = REDIS_TIME.match(first_line)
        if redis:
            level = {".": "DEBUG", "-": "DEBUG", "*": "INFO", "#": "WARNING"}[redis[2]]
        if level == "WARN":
            level = "WARNING"
        if level == "PANIC":
            level = "FATAL"
        try:
            structured = json.loads(body if WRAPPED.match(first_line) else text)
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
        latency = LATENCY.search(first_line)
        trailing_latency = (
            FASTAPI_LATENCY.search(first_line)
            if service == "fastapi" and status
            else None
        )
        if trailing_latency:
            meta["response_time_ms"] = float(trailing_latency[1]) * (
                1000 if trailing_latency[2] == "s" else 1
            )
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
        meta["timestamp_status"] = (
            "parsed"
            if stamp
            else (
                "unrecognized"
                if re.match(
                    r"^\s*\[?(?:\d{4}[-/]\d{1,2}[-/]\d{1,2}[ T]|(?:\d+:[A-Z]\s+)?\d{1,2} [A-Za-z]{3} \d{4}\b|\d\d:\d\d:\d\d)",
                    first_line,
                )
                else "absent"
            )
        )
        meta["level_unrecognized"] = level == "UNKNOWN"
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
