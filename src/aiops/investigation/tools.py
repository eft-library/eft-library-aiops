"""Typed probe dispatch. Model text is never executable code or a file/SQL target."""

import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import psutil

from aiops.collectors.docker import DockerCollector
from aiops.collectors.system import SystemCollector
from aiops.investigation.models import PlanningResult
from aiops.investigation.probes import log_excerpt, now, open_regular, postgres_statistics
from aiops.providers.base import ErrorKind, ProviderError


SEOUL = ZoneInfo('Asia/Seoul')


def period_bounds(parent):
    def date_bound(value):
        return datetime.combine(datetime.fromisoformat(str(value)[:10]).date(), datetime.min.time(), SEOUL)
    return date_bound(parent['period_start']), date_bound(parent['period_end'])


def catalogue(config):
    targets = [name for name, source in config.sources.items() if source.enabled]
    tools = [
        {'tool': 'historical_logs', 'targets': targets, 'scope': 'Stored normalized daily logs, not live state. ±15 minutes around anchor or bounded daily sample; 2MiB/100 files/3 seconds/40 matching events max.'},
        {'tool': 'recent_logs', 'targets': targets, 'scope': 'One configured file tail <=8KiB; not full history. Acquisition time is not event time.'},
        {'tool': 'database_statistics', 'targets': ['aiops_database'], 'scope': 'Configured AIOps DB statistics only; fixed read-only SQL, no application DB access or query text.'},
        {'tool': 'process_summary', 'targets': ['host'], 'scope': 'Counts/RSS for known program names; <=512 processes/2 seconds. No arguments, environment, executable paths or proof of service health.'},
    ]
    if config.system.enabled:
        tools.append({'tool': 'system_metrics', 'targets': ['system'], 'scope': 'Configured current system and disk samples, not historical metrics.'})
    if config.docker.enabled:
        tools.append({'tool': 'docker_metrics', 'targets': ['docker'], 'scope': 'Configured containers only; status/resource metrics, not logs/exec/inspect.'})
    return tools


def validate_plan(result, catalog, parent):
    try:
        plan = PlanningResult.model_validate(result.model_dump())
        if len(plan.model_dump_json().encode()) > 16000:
            raise ValueError('plan too large')
        allowed = {entry['tool']: set(entry['targets']) for entry in catalog}
        start, end = period_bounds(parent)
        seen = set()
        for request in plan.report.requests:
            if request.target not in allowed.get(request.tool, set()):
                raise ValueError('unlisted tool or target')
            if request.anchor is not None:
                if request.tool != 'historical_logs' or request.anchor.tzinfo is None or not start <= request.anchor < end:
                    raise ValueError('invalid historical anchor')
            identity = (request.tool, request.target, request.anchor.isoformat() if request.anchor else None)
            if identity in seen:
                raise ValueError('duplicate probe request')
            seen.add(identity)
        return plan
    except (ValueError, AttributeError, TypeError):
        raise ProviderError(ErrorKind.OUTPUT) from None


def historical_logs(settings, parent, target, anchor):
    manifest = parent['manifest_json']
    if isinstance(manifest, str):
        manifest = json.loads(manifest)
    start, end = period_bounds(parent)
    if anchor is not None:
        start, end = max(start, anchor - timedelta(minutes=15)), min(end, anchor + timedelta(minutes=15))
    deadline, budget = time.monotonic() + 3, 2 * 1024 * 1024
    events, scanned, files, omitted, unavailable = [], 0, 0, 0, 0
    batch_ids = manifest.get('batch_ids', [])
    exhausted = False
    for batch_id in batch_ids:
        if files >= 100 or budget <= 0 or time.monotonic() >= deadline or len(events) >= 40:
            exhausted = True
            break
        if not isinstance(batch_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,128}', batch_id):
            raise ValueError('invalid stored batch id')
        path = settings.data_dir / 'collection' / 'batches' / batch_id / 'events.jsonl'
        files += 1
        try:
            descriptor = open_regular(path)
            with os.fdopen(descriptor, 'rb') as stream:
                while budget > 0 and time.monotonic() < deadline and len(events) < 40:
                    line = stream.readline(min(65537, budget))
                    budget -= len(line)
                    if not line:
                        break
                    scanned += 1
                    if not line.endswith(b'\n'):
                        omitted += 1
                        # Stop this file rather than interpreting the rest of an oversized record.
                        break
                    try:
                        event = json.loads(line)
                        stamp = datetime.fromisoformat(event['timestamp'])
                        if stamp.tzinfo is None:
                            raise ValueError('unassigned timezone')
                        if event.get('service') == target and start <= stamp < end:
                            events.append({'event_id': str(event.get('event_id', ''))[:128],
                                           'timestamp': stamp.isoformat(), 'service': target,
                                           'level': str(event.get('level', 'UNKNOWN'))[:32],
                                           'message': str(event.get('message', ''))[:2000]})
                    except (ValueError, TypeError, KeyError, AttributeError):
                        omitted += 1
                else:
                    exhausted = True
        except OSError:
            unavailable += 1
    return {'events': events, 'scanned_records': scanned, 'files_checked': files,
            'unavailable_files': unavailable, 'omitted_records': omitted,
            'truncated': exhausted or len(batch_ids) > files,
            'window_start': start.isoformat(), 'window_end': end.isoformat(),
            'scope': 'Bounded sample of stored historical logs. Empty results do not prove no errors; older files may be omitted by limits. Window end exclusive.'}


def process_summary():
    names = {'python', 'python3', 'uvicorn', 'node', 'java', 'redis-server', 'postgres', 'codex'}
    counts, rss = Counter(), Counter()
    scanned, inaccessible, truncated = 0, 0, False
    deadline = time.monotonic() + 2
    for process in psutil.process_iter():
        if scanned >= 512 or time.monotonic() >= deadline:
            truncated = True
            break
        scanned += 1
        try:
            name = process.name().lower()
            # Normalize versioned Python names without exposing command lines.
            if re.fullmatch(r'python(?:\d+(?:\.\d+)*)?', name):
                name = 'python'
            if name in names:
                counts[name] += 1
                rss[name] += process.memory_info().rss
        except (psutil.Error, OSError):
            inaccessible += 1
    return {'program_counts': dict(counts), 'rss_bytes_sum': dict(rss),
            'scanned_processes': scanned, 'inaccessible_processes': inaccessible, 'truncated': truncated,
            'scope': 'Known program names only, no arguments or environment. Counts cannot identify a particular service or establish health. RSS sums may double-count shared memory.'}


def dispatch(request, settings, config, parent):
    if request.tool == 'historical_logs':
        return historical_logs(settings, parent, request.target, request.anchor)
    if request.tool == 'recent_logs':
        return log_excerpt(config.sources[request.target])
    if request.tool == 'database_statistics':
        return postgres_statistics(settings)
    if request.tool == 'process_summary':
        return process_summary()
    if request.tool == 'system_metrics':
        metrics, failures = SystemCollector().collect(config.system.disks)
    elif request.tool == 'docker_metrics':
        metrics, failures = DockerCollector().collect(config.docker, min(10, config.docker.timeout_seconds))
    else:
        raise ValueError('unknown tool')
    return {'metrics': [m.model_dump(mode='json') for m in metrics], 'failures': failures,
            'scope': 'Current point samples for configured targets only; not the historical period.'}


def execute_plan(plan, settings, config, parent, cleaner, *, budget_seconds=20):
    # plan MUST be validated in full before this function is reached.
    import psycopg

    observations = []
    deadline = time.monotonic() + budget_seconds
    for index, request in enumerate(plan.report.requests):
        started = now()
        if time.monotonic() >= deadline:
            data, status = {'reason': 'additional_probe_budget_exhausted'}, 'unavailable'
        else:
            try:
                data = cleaner(dispatch(request, settings, config, parent))
                if len(json.dumps(data, ensure_ascii=False, default=str).encode()) > 12000:
                    data['truncated'] = True
                    for field in ('events', 'metrics'):
                        while data.get(field) and len(json.dumps(data, ensure_ascii=False, default=str).encode()) > 12000:
                            data[field].pop()
                    if len(json.dumps(data, ensure_ascii=False, default=str).encode()) > 12000:
                        data = {'truncated': True, 'reason': 'probe_output_cap', 'scope': 'Output exceeded 12KB; no health conclusion may be drawn.'}
                partial = any(data.get(key) for key in ('failures', 'truncated', 'omitted_records', 'unavailable_files', 'inaccessible_processes'))
                status = 'partial' if partial else 'observed'
            except (OSError, ValueError, ProviderError, psycopg.Error, psutil.Error):
                data, status = {'reason': 'probe_unavailable'}, 'unavailable'
        observations.append({'observation_id': f'extra-{index+1}', 'service': request.target,
                             'probe': request.tool, 'started_at': started, 'finished_at': now(),
                             'status': status, 'data': data, 'requested_reason': cleaner(request.reason),
                             'acquired_by': 'allowlisted_probe'})
    return observations
