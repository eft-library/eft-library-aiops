"""Explicit second stage: bounded probes, then one isolated Codex invocation."""

import json
from dataclasses import replace
from uuid import uuid4

from aiops.investigation.models import INSTRUCTIONS, InvestigationReport, InvestigationResult
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
    allowed = {snapshot['report_id'], *(o['observation_id'] for o in snapshot['observations'])}
    if any(ref not in allowed for issue in result.report.findings for ref in issue.evidence_ids):
        raise ProviderError(ErrorKind.OUTPUT)
    return result


def run_once(settings, sources_config, *, investigation_id=None, provider=None, probes=None):
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
                observations = (probes or collect_observations)(settings, sources_config)
                snapshot = prepare_input(parent, observations, settings.max_input_bytes)
                # Also mask legacy report text before it enters this new stage.
                cleaner = PublicProgress(settings, lambda event: None)
                def clean(item):
                    if isinstance(item, str):
                        return cleaner.clean(item)
                    if isinstance(item, list):
                        return [clean(value) for value in item]
                    if isinstance(item, dict):
                        return {key: clean(value) for key, value in item.items()}
                    return item
                snapshot = clean(snapshot)
                store.save_input(job, worker_id, snapshot)
                model = provider or CodexProvider(replace(settings, model=job['model']), instructions=INSTRUCTIONS,
                    report_type=InvestigationReport, result_type=InvestigationResult)
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
