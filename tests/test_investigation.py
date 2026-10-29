import json
import os

import pytest
from fastapi.testclient import TestClient

from aiops.api import create_app
from aiops.collectors.config import Source
from aiops.config import Settings
from aiops.investigation.models import InvestigationResult
from aiops.investigation.probes import collect_observations, log_excerpt, open_regular
from aiops.investigation.service import prepare_input, validate_result
from aiops.providers.base import ProviderError


def guide(report_id='daily-2020-01-01-v0001'):
    return InvestigationResult.model_validate({
        'job_id': 'fake', 'provider': 'fake', 'model': None, 'thread_id': None,
        'report': {'summary': '과거 오류의 원인 확인과 대응 안내', 'automatic_changes_performed': False,
            'limitations': ['현재 관측으로 과거 원인을 확정할 수 없음'],
            'findings': [{'issue': '연결 시간 초과', 'historical_evidence': '일일 보고서의 오류',
                'current_observation': '현재 상태는 별도 확인 필요', 'assessment': '원인 미확정',
                'evidence_ids': [report_id],
                'checks': [{'purpose': '연결 상태 확인', 'command': 'SELECT count(*) FROM pg_stat_activity;',
                            'interpretation': '현재 관측, 과거 오류와 구분', 'next_step': '동시간대 로그 확인'}],
                'responses': [{'action': '연결 풀 조정 검토', 'condition': '풀 고갈이 확인된 경우',
                               'risk': '연결 수 증가', 'verification': '오류와 연결 수 관측', 'rollback': '기존 설정 복원'}],
                'capture_on_recurrence': ['발생 시각의 연결 수']}],
        }})


class GuideProvider:
    def __init__(self):
        self.requests = []

    def analyze(self, request):
        snapshot = json.loads(request.snapshot)
        self.requests.append(snapshot)
        return guide(snapshot['report_id'])


def test_rejects_symlink_components_and_special_files(tmp_path):
    folder = tmp_path/'real'
    folder.mkdir()
    (folder/'app.log').write_text('safe\n')
    (tmp_path/'link').symlink_to(folder)
    with pytest.raises(OSError):
        open_regular(tmp_path/'link'/'app.log')
    fifo = tmp_path/'fifo'
    os.mkfifo(fifo)
    with pytest.raises(ValueError):
        open_regular(fifo)


def test_log_probe_is_bounded_and_does_not_modify_source(tmp_path):
    path = tmp_path/'app.log'
    raw = ('old line\n'*2000 + 'last full line\npartial').encode()
    path.write_bytes(raw)
    result = log_excerpt(Source(type='file', path=path))
    assert 'last full line' in result['text']
    assert 'partial' not in result['text']
    assert len(result['text'].encode()) <= 8192
    assert path.read_bytes() == raw
    assert list(tmp_path.iterdir()) == [path]


def test_directory_probe_ignores_symlinks(tmp_path):
    secret = tmp_path/'secret'
    secret.write_text('secret\n')
    logs = tmp_path/'logs'
    logs.mkdir()
    (logs/'alias.log').symlink_to(secret)
    with pytest.raises(OSError):
        log_excerpt(Source(type='directory',path=logs))


def test_probes_mask_secrets_and_isolate_unavailable_sources(tmp_path, monkeypatch):
    path = tmp_path/'app.log'
    path.write_text('Authorization: Bearer private-value\n')
    config = tmp_path/'sources.yaml'
    config.write_text('sources:\n  fastapi:\n    type: file\n    path: app.log\n  redis:\n    type: file\n    path: missing.log\nsystem:\n  enabled: false\n')
    monkeypatch.setenv('PRIVATE_TOKEN', 'private-value')
    monkeypatch.setattr('aiops.investigation.probes.postgres_statistics', lambda settings: {'scope':'test'})
    obs = collect_observations(Settings(data_dir=tmp_path/'data'),config)
    assert 'private-value' not in json.dumps(obs)
    assert obs[1]['status'] == 'unavailable'
    assert obs[0]['started_at'] and obs[0]['finished_at']
    assert obs[2]['probe'] == 'aiops_database_statistics'


def test_unknown_evidence_and_automatic_changes_are_rejected():
    result = guide()
    with pytest.raises(ProviderError):
        validate_result(result, {'report_id':'other','observations':[]})
    data = result.model_dump()
    data['report']['automatic_changes_performed'] = True
    with pytest.raises(ValueError):
        InvestigationResult.model_validate(data)


def test_input_limit_is_hard_and_omissions_explicit():
    parent = {'report_id':'daily-2020-01-01-v0001','input_json':{},'result_json':{'report':{'summary':'history'}}}
    obs=[{'probe':'recent_log_tail','data':{'text':'x'*5000},'observation_id':f'obs-{i}'} for i in range(12)]
    snapshot = prepare_input(parent,obs,3000)
    assert len(json.dumps(snapshot,ensure_ascii=False,separators=(',',':')).encode()) <= 3000
    assert any('omitted' in value for value in snapshot['limitations'])
    assert snapshot['historical_report']['summary'] == 'history'


def test_new_api_requires_auth_and_accepts_no_arbitrary_commands(tmp_path):
    app = create_app(Settings(database_url='dummy'), token='secret', sources_config=tmp_path/'sources.yaml')
    client = TestClient(app)
    assert client.post('/v1/investigations',json={'report_id':'daily-2020-01-01-v0001'}).status_code == 401
    assert client.post('/v1/investigations',headers={'X-AIOps-Key':'secret'},json={'report_id':'weekly-2020-01-01-v0001'}).status_code == 422
    assert client.post('/v1/investigations/worker/run-once',headers={'X-AIOps-Key':'secret'},json={'command':'anything'}).status_code == 422


def test_followup_codex_uses_custom_schema_and_unchanged_execution_boundary(tmp_path, monkeypatch):
    import sys
    from aiops.providers import codex
    from aiops.providers.base import AnalysisRequest
    from aiops.investigation.models import INSTRUCTIONS, InvestigationReport

    def fake_run(command, **kwargs):
        schema=json.loads(open(command[command.index('--output-schema')+1]).read())
        assert 'severity' not in schema['properties']
        assert 'findings' in schema['properties']
        assert command[command.index('--sandbox')+1]=='read-only'
        assert '--ignore-user-config' in command and '--ignore-rules' in command
        assert kwargs['stdin'].startswith(INSTRUCTIONS)
        assert 'PGPASSWORD' not in kwargs['env']
        return 0,'\n'.join(json.dumps(e) for e in [
            {'type':'item.completed','item':{'type':'agent_message','text':guide().report.model_dump_json()}},
            {'type':'turn.completed'}]),''

    monkeypatch.setattr(codex,'run',fake_run)
    settings=Settings(codex_bin=sys.executable,data_dir=tmp_path/'data')
    provider=codex.CodexProvider(settings,instructions=INSTRUCTIONS,report_type=InvestigationReport,result_type=InvestigationResult)
    assert provider.analyze(AnalysisRequest('{}')).report.findings[0].checks
