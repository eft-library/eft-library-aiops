"""Explicit second stage: bounded probes, typed model plan, additional probes, then operator handoff."""

import json
import time
from dataclasses import replace
from uuid import uuid4

from aiops.investigation.models import (
    INSTRUCTIONS, PLANNING_INSTRUCTIONS, InvestigationReport, InvestigationResult,
    PlanningReport, PlanningResult,
)
from aiops.collectors.config import load_config
from aiops.investigation.tools import catalogue, execute_plan, validate_plan
from aiops.investigation.probes import collect_observations
from aiops.investigation.store import POLICY_HASH, InvestigationStore
from aiops.providers.base import AnalysisRequest, ErrorKind, ProviderError
from aiops.providers.codex import CodexProvider
from aiops.providers.progress import PublicProgress
from aiops.storage.postgres import WORKER_LOCK, advisory_lock


def encoded(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), default=str)


def prepare_input(parent, observations, limit):
    result = parent['result_json']
    source = parent['input_json']
    if isinstance(result, str):
        result = json.loads(result)
    if isinstance(source, str):
        source = json.loads(source)
    snapshot = {
        'analysis_type': 'daily_investigation', 'report_id': parent['report_id'],
        'period': source.get('period'), 'historical_report': result['report'],
        'historical_data_quality': source.get('data_quality', {}),
        'observations': observations,
        'infrastructure_context': {
            'scope': 'EFT Library architecture reference, not runtime-verified health or connectivity. Use only configured observed service aliases; verify deployment identifiers before proposing commands.',
            'roles': {
                'nextjs': 'Frontend pages and static JSON',
                'fastapi': 'Python REST/WebSocket API; application database and Redis clients; Kafka producer',
                'postgresql': 'Service data; AIOps reports and job queue use the configured AIOps database',
                'redis': 'Cache, presence and Pub/Sub; not permanent application storage',
                'kafka': 'Event broker for access logs and notifications',
                'kafka_consumer': 'Python consumers; database persistence and Redis notification publishing',
                'airflow': 'Scheduled ETL, static JSON and AIOps API requests',
                'npm': 'Nginx Proxy Manager reverse proxy',
                'rustfs': 'S3-compatible image and upload object storage',
            },
            'deployment': 'One host, mixed host processes and Docker containers. Do not infer unit/container names or mount paths from aliases.',
        },
        'limitations': ['Current observations cannot reconstruct the historical reporting period.',
                        'Log tails and source scans are bounded; text may be masked/truncated.',
                        'No Kafka lag, Redis runtime statistics or application DB queries are performed without configured access.'],
    }
    cap = min(limit, 90000)
    if len(encoded(snapshot).encode()) > cap:
        snapshot['limitations'].append('Input limit omitted observation details; unavailable or omitted is not healthy.')
        # Retain the historical report; discard detail fairly across log sources first.
        for observation in observations:
            if observation['probe'] == 'recent_log_tail':
                observation['data']['text'] = observation['data'].get('text', '')[:800]
        while len(encoded(snapshot).encode()) > cap and snapshot['observations']:
            snapshot['observations'].pop()
        if len(encoded(snapshot).encode()) > cap:
            raise ProviderError(ErrorKind.CONFIG)
    return snapshot


def validate_result(result, snapshot):
    result = InvestigationResult.model_validate(result.model_dump())
    if len(encoded(result.model_dump()).encode()) > 150000:
        raise ProviderError(ErrorKind.OUTPUT)
    if result.report.operator_handoff is None:
        raise ProviderError(ErrorKind.OUTPUT)
    if any(response.expected_effect is None or response.side_effects is None or response.pre_checks is None
           for issue in result.report.findings for response in issue.responses):
        raise ProviderError(ErrorKind.OUTPUT)
    allowed = {snapshot['report_id'], *(o['observation_id'] for o in snapshot['observations'])}
    if result.report.operator_handoff.urgency == 'ACT_NOW':
        current = {item['observation_id'] for item in snapshot['observations']
                   if item.get('status') == 'observed' and item.get('probe') not in {'historical_logs'}}
        if not any(ref in current for action in result.report.operator_handoff.first_actions for ref in action.evidence_ids):
            raise ProviderError(ErrorKind.OUTPUT)
    references = [ref for issue in result.report.findings for ref in issue.evidence_ids]
    references += [ref for action in result.report.operator_handoff.first_actions for ref in action.evidence_ids]
    if any(ref not in allowed for ref in references):
        raise ProviderError(ErrorKind.OUTPUT)
    return result


def bound_snapshot(snapshot, limit):
    cap = min(limit, 90000)
    if len(encoded(snapshot).encode()) > cap:
        snapshot['limitations'].append('Input cap omitted historical examples or observation detail; omitted data is unknown, not healthy.')
        while len(encoded(snapshot).encode()) > cap and snapshot.get('historical_examples'):
            snapshot['historical_examples'].pop()
        while len(encoded(snapshot).encode()) > cap and snapshot['observations']:
            snapshot['observations'].pop()
    if len(encoded(snapshot).encode()) > cap:
        raise ProviderError(ErrorKind.CONFIG)
    return snapshot


def run_once(settings, sources_config, *, investigation_id=None, provider=None, planner=None, probes=None):
    worker_id = uuid4()
    with advisory_lock(settings, WORKER_LOCK) as guard:
        store = InvestigationStore(settings)
        try:
            store.recover()
            job = store.claim(worker_id, investigation_id)
            if job is None:
                return {'status': 'idle'}
            try:
                if job['policy_hash'] != POLICY_HASH:
                    raise ProviderError(ErrorKind.CONFIG)
                parent = store.parent(job['report_id'])
                if not parent or parent['kind'] != 'daily' or parent['status'] != 'COMPLETED':
                    raise ProviderError(ErrorKind.CONFIG)
                deadline = time.monotonic() + settings.timeout
                def remaining():
                    value = deadline - time.monotonic()
                    if value <= 0:
                        raise ProviderError(ErrorKind.TIMEOUT)
                    return value

                observations = (probes or collect_observations)(settings, sources_config)
                config = load_config(sources_config)
                catalog = catalogue(config)
                snapshot = prepare_input(parent, observations, settings.max_input_bytes)
                # Also mask legacy report text before either model call or DB persistence.
                sanitizer = PublicProgress(settings, lambda event: None)
                def clean(item):
                    if isinstance(item, str):
                        return sanitizer.clean(item)
                    if isinstance(item, list):
                        return [clean(value) for value in item]
                    if isinstance(item, dict):
                        return {key: clean(value) for key, value in item.items()}
                    return item
                source = parent['input_json']
                if isinstance(source, str):
                    source = json.loads(source)
                snapshot['historical_examples'] = [
                    {key: event.get(key) for key in ('event_id', 'timestamp', 'service', 'level', 'message')}
                    for event in source.get('events', [])[:20]
                ]
                snapshot['tool_catalog'] = catalog
                snapshot['stage'] = 'planning'
                snapshot = bound_snapshot(clean(snapshot), settings.max_input_bytes)
                store.save_input(job, worker_id, snapshot)
                # Test providers must explicitly supply a planner; never fall through to real AI.
                plan_call = planner or (getattr(provider, 'plan', None) if provider is not None else None)
                if provider is not None and plan_call is None:
                    raise ProviderError(ErrorKind.CONFIG)
                if plan_call is None:
                    plan_call = CodexProvider(replace(settings, model=job['model'], timeout=min(120, remaining()/2)),
                        instructions=PLANNING_INSTRUCTIONS, report_type=PlanningReport, result_type=PlanningResult).analyze
                planned = validate_plan(plan_call(AnalysisRequest(encoded(snapshot))), catalog, parent)
                # The entire request list is validated BEFORE any selected probe runs.
                snapshot['investigation_plan'] = clean(planned.report.model_dump(mode='json'))
                snapshot['planning_run'] = {'job_id': planned.job_id, 'provider': planned.provider,
                                            'model': planned.model, 'thread_id': planned.thread_id}
                snapshot['stage'] = 'additional_probes'
                snapshot = bound_snapshot(clean(snapshot), settings.max_input_bytes)
                store.save_input(job, worker_id, snapshot)
                extra = execute_plan(planned, settings, config, parent, clean,
                                     budget_seconds=min(20, remaining()))
                snapshot['observations'] = extra + snapshot['observations']
                snapshot['probe_execution'] = [
                    {key: item[key] for key in ('observation_id', 'probe', 'service', 'status', 'started_at', 'finished_at')}
                    for item in extra
                ]
                snapshot['stage'] = 'operator_handoff'
                snapshot = bound_snapshot(snapshot, settings.max_input_bytes)
                store.save_input(job, worker_id, snapshot)
                model = provider or CodexProvider(replace(settings, model=job['model'], timeout=remaining()),
                    instructions=INSTRUCTIONS, report_type=InvestigationReport, result_type=InvestigationResult)
                result = validate_result(model.analyze(AnalysisRequest(encoded(snapshot))), snapshot)
                # Model output may repeat probe/legacy secrets: mask again before storage.
                result = InvestigationResult.model_validate(clean(result.model_dump()))
                guard.execute('SELECT 1')
                return store.finish(job, worker_id, result=result.model_dump()) or {'status': 'lease_lost'}
            except ProviderError as exc:
                if exc.kind == ErrorKind.DATABASE:
                    raise
                guard.execute('SELECT 1')
                return store.finish(job, worker_id, error=exc.kind.value) or {'status': 'lease_lost'}
            except (OSError, ValueError):
                guard.execute('SELECT 1')
                return store.finish(job, worker_id, error='configuration') or {'status': 'lease_lost'}
            except KeyboardInterrupt:
                guard.execute('SELECT 1')
                store.finish(job, worker_id, error='interrupted')
                raise
        finally:
            store.close()
