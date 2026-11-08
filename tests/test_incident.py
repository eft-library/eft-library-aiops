import hashlib
import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import yaml

from aiops.collectors.config import CodeSource, Source, load_config
from aiops.config import Settings
from aiops.investigation.incident import incident_logs, source_code
from aiops.investigation.models import ProbeRequest
from aiops.investigation.tools import catalogue, dispatch, execute_plan, validate_plan
from aiops.providers.base import ProviderError
from test_investigation import daily_parent, empty_plan, guide


def anchor():
    return datetime.fromisoformat('2020-01-01T11:15:13+09:00')


def test_daily_raw_probe_separates_times_and_retains_exception_context(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    raw = ('2020-01-01 11:15:13 - INFO - 192.0.2.10 - "GET /api/user/v3/user-info" 503 - 0.060s\n'
           '2020-01-01 11:15:13 - ERROR - token verification failed password=private-value\n'
           'Traceback: verification exception\n'
           '2020-01-01 11:15:14 - INFO - "GET /api/user/v3/user-info" 200 - 0.010s\n'
           '2020-01-01 22:40:26 - INFO - "POST /api/party/rooms/closed/join" 410 - 0.165s\n')
    path = root / '2020-01-01.out';path.write_text(raw)
    (root / 'log.out').write_text('legacy must not be read\n')
    monkeypatch.setenv('PRIVATE_PASSWORD', 'private-value')
    source = Source(type='directory', path=root, pattern='????-??-??.out', daily_filename='%Y-%m-%d.out')
    result = incident_logs(Settings(), source, 'fastapi', anchor(), ['user-info'],
                           anchor().replace(hour=0, minute=0, second=0), anchor().replace(hour=0, minute=0, second=0) + timedelta(days=1))
    assert result['full_file_scan_completed'] is True
    assert result['literal_term_counts']['user-info'] == 2
    assert result['matched_records'] == 3
    assert 'private-value' not in json.dumps(result)
    assert '192.0.2.10' not in json.dumps(result)
    assert all('closed/join' not in item['message'] for item in result['events'])
    assert any('Traceback: verification exception' in item['message'] for item in result['events'])
    assert {item['http_status'] for item in result['events']} == {503, 200, None}
    assert path.read_text() == raw


def test_daily_probe_marks_unfinished_or_oversized_scan_and_rejects_links(tmp_path):
    root = tmp_path.resolve()
    path = root / '2020-01-01.out'
    path.write_text('x' * 70000 + '\n')
    source = Source(type='directory', path=root, daily_filename='%Y-%m-%d.out', pattern='*.out')
    result = incident_logs(Settings(), source, 'fastapi', anchor(), [], anchor()-timedelta(hours=1), anchor()+timedelta(hours=1))
    assert result['truncated'] is True
    assert result['full_file_scan_completed'] is False
    assert result['stop_reason'] == 'oversized_or_incomplete_record'
    path.unlink();path.symlink_to(root / 'secret')
    with pytest.raises(OSError):
        incident_logs(Settings(), source, 'fastapi', anchor(), [], anchor()-timedelta(hours=1), anchor()+timedelta(hours=1))


def test_source_probe_reads_only_explicit_files_masks_and_records_hash(tmp_path, monkeypatch):
    root = tmp_path.resolve()
    text = 'raise RuntimeError("must not execute")\ndef verify_google_token():\n    password = "private-value"\n    return None\n'
    (root / 'service.py').write_text(text)
    (root / '.env').write_text('SECRET=do-not-read')
    monkeypatch.setenv('PRIVATE_PASSWORD', 'private-value')
    source = CodeSource(root=root, files=['service.py'], description='token verification')
    result = source_code(Settings(), source, ['verify_google_token'])
    assert result['deployment_match'] == 'unverified'
    assert result['files'][0]['sha256'] == hashlib.sha256(text.encode()).hexdigest()
    assert '2: def verify_google_token' in result['text']
    assert 'private-value' not in json.dumps(result)
    assert 'do-not-read' not in json.dumps(result)
    assert not result['failures']
    assert (root / 'service.py').read_text() == text
    (root / 'service.py').unlink();(root / 'service.py').symlink_to(root / '.env')
    assert source_code(Settings(), source, ['verify'])['failures']
    for value in ('../escape.py', '/absolute.py', '.env', '.git/config', 'service.py/../../escape.py'):
        with pytest.raises(ValueError):
            CodeSource(root=root, files=[value], description='invalid')


def test_new_probes_are_catalogued_and_plan_validated_before_execution(tmp_path):
    config = tmp_path / 'sources.yaml'
    config.write_text(yaml.safe_dump({'sources': {'fastapi': {
        'type':'directory', 'path': 'logs', 'pattern':'*.out', 'daily_filename':'%Y-%m-%d.out'}},
        'code_sources': {'user': {'root':'src', 'files':['user.py'], 'description':'user lookup'}}}))
    parsed = load_config(config)
    tools = catalogue(parsed)
    assert parsed.code_sources['user'].root == config.parent / 'src'
    assert {'incident_logs', 'source_code'} <= {tool['tool'] for tool in tools}
    request = {'tool':'incident_logs', 'target':'fastapi', 'anchor':anchor(), 'search_terms':['user-info'], 'reason':'failure'}
    validate_plan(empty_plan([request]), tools, daily_parent())
    for changes in ({'anchor':None}, {'target':'/etc/passwd'}, {'anchor':anchor()+timedelta(days=1)},
                    {'search_terms':['x\ncommand']}, {'search_terms':['x' * 161]}):
        with pytest.raises(ProviderError):
            validate_plan(empty_plan([{**request, **changes}]), tools, daily_parent())
    code = {'tool':'source_code', 'target':'user', 'anchor':None, 'search_terms':['verify_google_token'], 'reason':'branch'}
    validate_plan(empty_plan([code]), tools, daily_parent())
    for changes in ({'anchor':anchor()}, {'search_terms':None}, {'target':'unlisted'}):
        with pytest.raises(ProviderError):
            validate_plan(empty_plan([{**code, **changes}]), tools, daily_parent())


def test_automatic_review_is_required_for_new_results_and_legacy_still_parses():
    from aiops.investigation.models import InvestigationResult
    from aiops.investigation.service import validate_result
    result = guide()
    snapshot = {'report_id':'daily-2020-01-01-v0001','observations':[]}
    assert validate_result(result, snapshot).report.findings[0].automatic_review.requires_operator_review
    result.report.findings[0].automatic_review = None
    assert InvestigationResult.model_validate(result.model_dump()).report.findings[0].automatic_review is None
    with pytest.raises(ProviderError):
        validate_result(result, snapshot)


def test_planning_keeps_cited_error_after_unrelated_representative_records():
    from aiops.investigation.service import historical_examples
    events = [{'event_id': f'normal-{i}', 'message': 'routine', 'service': 'airflow'} for i in range(30)]
    events.append({'event_id': 'user-error', 'timestamp': '2020-01-01T11:15:13+09:00',
                   'service': 'fastapi', 'message': 'GET user-info 503'})
    report = {'findings': [], 'incident_analysis': {'findings': [{'evidence_ids': ['user-error']}]}}
    selected = historical_examples({'events': events}, report)
    assert selected[0]['event_id'] == 'user-error'
    assert selected[0]['timestamp'] == '2020-01-01T11:15:13+09:00'
    assert len(selected) == 20
