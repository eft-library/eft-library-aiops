"""Best-effort masking. This is not a guarantee that arbitrary logs are secret-free."""

import ipaddress
import re

REDACTED = "[REDACTED]"
KEYS = r"(?:password|passwd|pwd|secret|api[_-]?key|access[_-]?token|refresh[_-]?token|token|session[_-]?id|user[_-]?id|username|email|phone|authorization|cookie|set-cookie)"
RULES = [
    (
        re.compile(
            r"-----BEGIN [^-]*PRIVATE KEY-----.*?-----END [^-]*PRIVATE KEY-----", re.S
        ),
        REDACTED,
    ),
    (
        re.compile(r"(?im)\b(Authorization|Cookie|Set-Cookie)\s*:\s*[^\r\n]+"),
        r"\1: " + REDACTED,
    ),
    (
        re.compile(r'(?i)(["\']' + KEYS + r'["\']\s*:\s*)"(?:\\.|[^"\\])*"'),
        r'\1"' + REDACTED + '"',
    ),
    (
        re.compile(r"(?i)([\"']" + KEYS + r"[\"']\s*:\s*)'(?:\\.|[^'\\])*'"),
        r"\1'" + REDACTED + "'",
    ),
    (
        re.compile(
            r"(?i)(\b"
            + KEYS
            + r'\b\s*[=:]\s*)(?:"[^"\r\n]*"|\x27[^\x27\r\n]*\x27|[^\s&,;"\x27}\]]+)'
        ),
        r"\1" + REDACTED,
    ),
    (re.compile(r"(?i)\b(?:Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+"), REDACTED),
    (
        re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@"),
        r"\1" + REDACTED + "@",
    ),
    (re.compile(r"\b(?:sk-[A-Za-z0-9_-]{10,}|AKIA[A-Z0-9]{16})\b"), REDACTED),
    (re.compile(r"(?m)^[A-Za-z0-9+/=]{40,}$"), REDACTED),
    (re.compile(r"\beyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\b"), REDACTED),
    (re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"), REDACTED),
    (re.compile(r"(?<![\w.])(?:\d{1,3}\.){3}\d{1,3}(?![\w.])"), REDACTED),
    (re.compile(r'(?i)(/(?:users|user|home)/)[^/\s"?]+'), r"\1" + REDACTED),
    (re.compile(r"\b01[016789][- ]?\d{3,4}[- ]?\d{4}\b"), REDACTED),
]


def mask(text: str) -> str:
    for pattern, replacement in RULES:
        text = pattern.sub(replacement, text)

    # Validate IPv6 candidates so times such as 08:30:00 are not redacted.
    def ipv6(match):
        try:
            ipaddress.IPv6Address(match.group())
            return REDACTED
        except ValueError:
            return match.group()

    return re.sub(r"(?<!\w)(?:[0-9a-fA-F]{0,4}:){2,}[0-9a-fA-F:.%]+", ipv6, text)
