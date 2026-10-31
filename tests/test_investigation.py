import json
import os

import pytest
from fastapi.testclient import TestClient

from aiops.api import create_app
from aiops.collectors.config import Source
from aiops.config import Settings
from aiops.investigation.models import InvestigationResult, PlanningResult
from aiops.investigation.probes import collect_observations, log_excerpt, open_regular
from aiops.investigation.service import prepare_input, validate_result
from aiops.providers.base import ProviderError


def guide(report_id='daily-2020-01-01-v0001'):
    return InvestigationResult.model_validate({
        'job_id': 'fake', 'provider': 'fake', 'model': None, 'thread_id': None,
        'report': {'summary': '과거 오류의 원인 확인과 대응 안내', 'automatic_changes_performed': False,
            'limitations': ['현재 관측으로 과거 원인을 확정할 수 없음'],
            'operator_handoff': {'urgency':'NEEDS_REVIEW','reason':'현재 영향 확인 필요',
                'first_actions':[{'action':'서비스 영향 확인','reason':'과거 오류만 확인됨',
                    'decision':'현재 영향이 있으면 대응 검토, 없으면 관찰', 'done_when':'현재 영향 여부 확인',
                    'evidence_ids':[report_id]}], 'unresolved':['실제 사용자 영향']},
            'findings': [{'issue': '연결 시간 초과', 'historical_evidence': '일일 보고서의 오류',
                'current_observation': '현재 상태는 별도 확인 필요', 'assessment': '원인 미확정',
                'evidence_ids': [report_id],
                'checks': [{'purpose': '연결 상태 확인', 'command': 'SELECT count(*) FROM pg_stat_activity;',
                            'interpretation': '현재 관측, 과거 오류와 구분', 'next_step': '동시간대 로그 확인'}],
                'responses': [{'action': '연결 풀 조정 검토', 'condition': '풀 고갈이 확인된 경우',
                               'risk': '연결 수 증가', 'expected_effect':'연결 대기 감소 가능', 'side_effects':['추정: DB 메모리 증가'], 'pre_checks':['최대 연결 수 확인'], 'verification': '오류와 연결 수 관측', 'rollback': '기존 설정 복원'}],
                'capture_on_recurrence': ['발생 시각의 연결 수']}],
        }})


def empty_plan(requests=None):
    return PlanningResult(job_id='plan-test', provider='fake', model=None, thread_id=None,
        report={'summary':'추가 조회 계획','requests':requests or [], 'limitations':[],
                'automatic_changes_performed':False})


class GuideProvider:
    def __init__(self):
        self.requests = []

    def plan(self, request):
        return empty_plan()

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


def daily_parent():
    return {'report_id':'daily-2020-01-01-v0001','period_start':'2020-01-01','period_end':'2020-01-02',
            'manifest_json':{'batch_ids':[]}}


def test_plan_rejects_arbitrary_tools_paths_sql_and_outside_period():
    from aiops.investigation.tools import validate_plan
    catalog=[{'tool':'historical_logs','targets':['fastapi']}, {'tool':'process_summary','targets':['host']}]
    cases=[
        {'tool':'historical_logs','target':'/etc/passwd','anchor':None,'reason':'test'},
        {'tool':'historical_logs','target':'fastapi','anchor':'2020-01-03T00:00:00+09:00','reason':'test'},
        {'tool':'historical_logs','target':'fastapi','anchor':'2020-01-01T00:00:00','reason':'test'},
        {'tool':'process_summary','target':'host','anchor':'2020-01-01T00:00:00+09:00','reason':'test'},
    ]
    for request in cases:
        with pytest.raises(ProviderError):validate_plan(empty_plan([request]),catalog,daily_parent())
    for extra in [{'command':'rm -rf /'}, {'sql':'SELECT * FROM secrets'}, {'path':'/etc/passwd'}]:
        with pytest.raises(ValueError):empty_plan([{'tool':'process_summary','target':'host','anchor':None,'reason':'test',**extra}])
    with pytest.raises(ValueError):empty_plan([{'tool':'shell','target':'host','anchor':None,'reason':'test'}])
    request={'tool':'process_summary','target':'host','anchor':None,'reason':'test'}
    with pytest.raises(ProviderError):validate_plan(empty_plan([request,request]),catalog,daily_parent())
    with pytest.raises(ValueError):empty_plan([request]*6)


def test_historical_probe_bounds_period_target_and_window_without_checkpoints(tmp_path):
    from aiops.investigation.tools import historical_logs
    from datetime import datetime
    parent=daily_parent()
    parent['manifest_json']={'batch_ids':['batch-test']}
    folder=tmp_path/'collection'/'batches'/'batch-test';folder.mkdir(parents=True)
    events=[{'event_id':str(i),'service':service,'timestamp':stamp,'level':'ERROR','message':message}
            for i,(service,stamp,message) in enumerate([
                ('fastapi','2020-01-01T10:00:00+09:00','wanted'),
                ('redis','2020-01-01T10:00:00+09:00','other service'),
                ('fastapi','2020-01-01T15:00:00+09:00','outside window'),
                ('fastapi','2019-12-31T23:59:00+09:00','outside day')])]
    path=folder/'events.jsonl';raw=''.join(json.dumps(item)+'\n' for item in events);path.write_text(raw)
    result=historical_logs(Settings(data_dir=tmp_path),parent,'fastapi',datetime.fromisoformat('2020-01-01T10:00:00+09:00'))
    assert [event['message'] for event in result['events']]==['wanted']
    assert path.read_text()==raw
    assert list(folder.iterdir())==[path]
    parent['manifest_json']['batch_ids']=['../../secret']
    with pytest.raises(ValueError):historical_logs(Settings(data_dir=tmp_path),parent,'fastapi',None)


def test_execute_plan_masks_bounds_and_records_unavailable_probes(tmp_path, monkeypatch):
    from aiops.investigation.tools import execute_plan
    from aiops.providers.progress import PublicProgress
    monkeypatch.setenv('PRIVATE_TOKEN','sensitive-value')
    sanitizer=PublicProgress(Settings(data_dir=tmp_path),lambda event:None)
    def clean(value):
        if isinstance(value,str):return sanitizer.clean(value)
        if isinstance(value,list):return [clean(v) for v in value]
        if isinstance(value,dict):return {k:clean(v) for k,v in value.items()}
        return value
    def fake_dispatch(request,*args):
        if request.tool=='process_summary':raise OSError('sensitive-value')
        return {'events':[{'message':'sensitive-value '+'x'*2000} for _ in range(40)]}
    monkeypatch.setattr('aiops.investigation.tools.dispatch',fake_dispatch)
    plan=empty_plan([{'tool':tool,'target':target,'anchor':None,'reason':'needed'} for tool,target in [('historical_logs','fastapi'),('process_summary','host')]])
    obs=execute_plan(plan,Settings(data_dir=tmp_path),None,daily_parent(),clean)
    assert len(json.dumps(obs[0]['data']).encode())<=12000
    assert obs[0]['status']=='partial' and obs[1]['status']=='unavailable'
    assert 'sensitive-value' not in json.dumps(obs)
    assert execute_plan(plan,Settings(data_dir=tmp_path),None,daily_parent(),clean,budget_seconds=0)[0]['status']=='unavailable'


def test_handoff_and_response_effects_required_for_new_outputs():
    from aiops.investigation.service import validate_result
    snapshot={'report_id':'daily-2020-01-01-v0001','observations':[]}
    result=guide()
    assert validate_result(result,snapshot).report.operator_handoff
    result.report.operator_handoff.first_actions[0].evidence_ids=['fake']
    with pytest.raises(ProviderError):validate_result(result,snapshot)
    result=guide();result.report.findings[0].responses[0].expected_effect=None
    with pytest.raises(ProviderError):validate_result(result,snapshot)
    result=guide();result.report.operator_handoff.urgency='ACT_NOW'
    with pytest.raises(ProviderError):validate_result(result,snapshot)
    # Stored legacy results still parse and can be displayed.
    legacy=guide().model_dump();legacy['report'].pop('operator_handoff')
    assert InvestigationResult.model_validate(legacy).report.operator_handoff is None
