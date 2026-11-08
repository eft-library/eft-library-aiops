"""Fixed read-only incident probes. Model text is only a literal search value."""
import fnmatch
import hashlib
import heapq
import os
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from aiops.collectors.parse import HTTP, LEVEL, timestamp
from aiops.investigation.probes import open_regular
from aiops.providers.progress import PublicProgress


def incident_logs(settings, source, target, anchor, terms, period_start, period_end):
    """Scan one explicitly configured daily file; never accept a path from a plan."""
    if source.type != 'directory' or not source.daily_filename or anchor is None:
        raise ValueError('daily log source and anchor required')
    day = anchor.astimezone(ZoneInfo(source.timezone)).date()
    label = day.strftime(source.daily_filename)
    if not fnmatch.fnmatch(label, source.pattern):
        raise ValueError('daily filename outside configured pattern')
    start = max(period_start, anchor - timedelta(minutes=15))
    end = min(period_end, anchor + timedelta(minutes=15))
    terms = terms or []
    counts = Counter()
    candidates = []
    scanned = matched = unknown = read_bytes = 0
    complete = False
    last_event = None
    reason = None
    deadline = time.monotonic() + 4
    cleaner = PublicProgress(settings, lambda event: None)
    descriptor = open_regular(source.path / label)
    with os.fdopen(descriptor, 'rb') as stream:
        info = os.fstat(stream.fileno())
        while time.monotonic() < deadline and read_bytes < 64 * 1024 * 1024:
            raw = stream.readline(min(65537, 64 * 1024 * 1024 - read_bytes))
            if not raw:
                complete = True
                break
            read_bytes += len(raw)
            scanned += 1
            if not raw.endswith(b'\n'):
                reason = 'oversized_or_incomplete_record'
                break
            line = raw.decode('utf-8', errors='replace').rstrip('\n')
            for term in terms:
                if term in line:
                    counts[term] += 1
            stamp = timestamp(line, source.timezone)
            if stamp is None:
                unknown += 1
                if last_event is not None:
                    combined = last_event['message'] + '\n' + cleaner.mask_text(line)
                    last_event['message'] = combined[:1500]
                    last_event['message_truncated'] |= len(combined) > 1500
                continue
            last_event = None
            if not start <= stamp < end:
                continue
            level_match = LEVEL.search(line)
            level = level_match[1].upper() if level_match else 'UNKNOWN'
            status_match = HTTP.search(line)
            status = int(status_match[1]) if status_match else None
            error = level in {'ERROR', 'FATAL', 'CRITICAL', 'WARNING', 'WARN'} or (status is not None and status >= 500)
            term_match = bool(terms) and all(term in line for term in terms)
            # Include neighbouring errors even when they do not share the request term.
            if terms and not term_match and not error:
                continue
            priority = 0 if term_match and error else 1 if error else 2
            score = (priority, abs((stamp - anchor).total_seconds()), scanned)
            matched += 1
            # Selection uses raw values, but only masked evidence leaves this probe.
            message = cleaner.mask_text(line)
            event = {'timestamp': stamp.isoformat(), 'service': target, 'level': level,
                     'http_status': status, 'source_line': scanned, 'file_label': label,
                     'message': message[:1500], 'message_truncated': len(message) > 1500}
            last_event = event
            heapq.heappush(candidates, ((-score[0], -score[1], -score[2]), scanned, event))
            if len(candidates) > 32:
                heapq.heappop(candidates)
        after = os.fstat(stream.fileno())
    changed = (info.st_size, info.st_mtime_ns) != (after.st_size, after.st_mtime_ns)
    rows = [item[2] for item in sorted(candidates, key=lambda item: tuple(-v for v in item[0]))]
    reasons = []
    if not complete: reasons.append(reason or 'scan_budget')
    if changed: reasons.append('source_changed')
    if matched > len(rows): reasons.append('event_limit')
    if any(row['message_truncated'] for row in rows): reasons.append('message_limit')
    return {'events': rows, 'file_label': label, 'file_size_bytes': info.st_size,
            'file_modified_at': datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat(),
            'window_start': start.isoformat(), 'window_end': end.isoformat(),
            'scanned_records': scanned, 'bytes_read': read_bytes,
            'timestamp_unassigned_records': unknown, 'matched_records': matched,
            'literal_term_counts': {term: counts[term] for term in terms}, 'full_file_scan_completed': complete and not changed,
            'truncated': bool(reasons), 'truncation_reasons': reasons,
            'omitted_records': max(0, matched - len(rows)),
            'stop_reason': reason or ('source_changed' if changed else 'end_of_file' if complete else 'scan_budget'),
            'scope': 'One configured daily raw log, literal term counts over the scanned portion, nearest incident window and surrounding errors. Different requests/users are not correlated without a shared identifier. Missing messages do not identify an exception branch. Full file scan is not full service coverage.'}


def source_code(settings, source, terms):
    """Read explicit source files without importing/executing code or invoking git."""
    cleaner = PublicProgress(settings, lambda event: None)
    files, chunks = [], []
    failures = []
    clipped = False
    for label in source.files:
        try:
            with os.fdopen(open_regular(source.root / label), 'rb') as stream:
                info = os.fstat(stream.fileno())
                raw = stream.read(131073)
                after = os.fstat(stream.fileno())
            if (info.st_size, info.st_mtime_ns) != (after.st_size, after.st_mtime_ns):
                failures.append({'file': label, 'reason': 'source_changed'})
                continue
            truncated = len(raw) > 131072
            raw = raw[:131072]
            lines = raw.decode('utf-8', errors='replace').splitlines()
            hits = [i for i, line in enumerate(lines) if any(term in line for term in terms)]
            selected = set()
            for hit in hits[:4]:
                selected.update(range(max(0, hit - 8), min(len(lines), hit + 14)))
            rendered = []
            budget = 6000 // len(source.files)
            for index in sorted(selected):
                line = cleaner.mask_text(f'{index+1}: {lines[index]}')
                if len(('\n'.join(rendered + [line])).encode()) > budget:
                    truncated = True
                    break
                rendered.append(line)
            clipped = clipped or truncated or len(hits) > 4
            files.append({'file': label, 'sha256': hashlib.sha256(raw).hexdigest() if info.st_size <= 131072 else None,
                          'size_bytes': info.st_size, 'matched_lines': len(hits), 'truncated': truncated or len(hits) > 4,
                          'modified_at': datetime.fromtimestamp(info.st_mtime, timezone.utc).isoformat()})
            if rendered:
                chunks.append(label + '\n' + '\n'.join(rendered))
        except (OSError, ValueError):
            failures.append({'file': label, 'reason': 'unreadable_source'})
    return {'text': '\n\n'.join(chunks), 'files': files, 'failures': failures,
            'truncated': clipped, 'deployment_match': 'unverified',
            'scope': 'Literal excerpts from explicitly configured current source files, with line numbers and full-file hashes when available. Never executed. Incident-time deployment revision is NOT verified; code paths suggest hypotheses, not root-cause proof.'}
