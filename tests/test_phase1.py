import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from aiops.cli import main
from aiops.config import Settings
from aiops.process import run
from aiops.providers import codex
from aiops.providers.base import AnalysisRequest, AnalysisResult, ErrorKind, ProviderError

REPORT = {'summary': 'sample', 'severity': 'WARNING', 'findings': [],
          'limitations': ['sample only'], 'automatic_changes_performed': False}


class FakeProvider:
    def analyze(self, request):
        assert json.loads(request.snapshot)['batch_id'] == 'sample-001'
        return AnalysisResult(job_id='fake-job', provider='fake', model=None,
                              thread_id='fake-thread', report=REPORT)


def events(report=REPORT):
    return '\n'.join(json.dumps(e) for e in [
        {'type': 'thread.started', 'thread_id': 'thread-1'},
        {'type': 'item.completed', 'item': {'type': 'agent_message', 'text': json.dumps(report)}},
        {'type': 'turn.completed'},
    ])


@pytest.fixture
def settings(tmp_path):
    return Settings(codex_bin=sys.executable, data_dir=tmp_path / 'runtime')


def test_cli_fake_provider(capsys):
    assert main(['analyze', '--input', 'config/sample.json'], provider=FakeProvider()) == 0
    result = json.loads(capsys.readouterr().out)
    assert result['job_id'] == 'fake-job'
    assert result['report']['automatic_changes_performed'] is False


@pytest.mark.parametrize('kind,code', [(ErrorKind.AUTH, 3), (ErrorKind.NETWORK, 4),
    (ErrorKind.RATE_LIMIT, 5), (ErrorKind.TIMEOUT, 6), (ErrorKind.BUSY, 7),
    (ErrorKind.OUTPUT, 8), (ErrorKind.EXECUTION, 9)])
def test_cli_error_codes(kind, code, capsys):
    class FailingProvider:
        def analyze(self, request):
            raise ProviderError(kind)
    assert main(['analyze', '--input', 'config/sample.json'], provider=FailingProvider()) == code
    assert json.loads(capsys.readouterr().err) == {'error': kind.value}


@pytest.mark.parametrize('data', [b'', b'not JSON', b'\xff'])
def test_bad_input(tmp_path, data, capsys):
    sample = tmp_path / 'sample.json'
    sample.write_bytes(data)
    assert main(['analyze', '--input', str(sample)], provider=FakeProvider()) == 2
    assert capsys.readouterr().out == ''


def test_input_read_limit(tmp_path, monkeypatch):
    monkeypatch.setenv('AIOPS_MAX_INPUT_BYTES', '10')
    sample = tmp_path / 'large.json'
    sample.write_bytes(b'x' * 11)
    assert main(['analyze', '--input', str(sample)], provider=FakeProvider()) == 2


@pytest.mark.parametrize('timeout', ['0', '-1', 'nan', 'inf', 'invalid'])
def test_invalid_config(monkeypatch, timeout):
    monkeypatch.setenv('AIOPS_TIMEOUT_SECONDS', timeout)
    with pytest.raises(ValueError):
        Settings.from_env()


def test_provider_result_and_metadata(settings, monkeypatch):
    monkeypatch.setenv('TEST_SECRET', 'must-not-pass')
    def fake_run(command, **kwargs):
        assert command[0] == str(Path(sys.executable).resolve())
        assert command[command.index('--sandbox') + 1] == 'read-only'
        assert command[command.index('--ask-for-approval') + 1] == 'never'
        assert '--ignore-user-config' in command and '--ignore-rules' in command
        assert 'TEST_SECRET' not in kwargs['env']
        assert 'OPENAI_API_KEY' not in kwargs['env']
        assert Path(kwargs['cwd']).is_dir()
        schema = json.loads(Path(command[command.index('--output-schema') + 1]).read_text())
        assert set(schema['required']) == set(schema['properties'])
        assert 'incident_analysis' in schema['properties']
        assert 'performance_analysis' in schema['properties']
        return 0, events(), ''
    monkeypatch.setattr(codex, 'run', fake_run)
    result = codex.CodexProvider(settings).analyze(AnalysisRequest('{"secret": "input-only"}'))
    assert result.thread_id == 'thread-1'
    metadata = list(settings.data_dir.glob('*.json'))
    assert len(metadata) == 1
    text = metadata[0].read_text()
    assert 'input-only' not in text and 'sample only' not in text
    assert json.loads(text)['status'] == 'COMPLETED'
    assert metadata[0].stat().st_mode & 0o777 == 0o644
    assert not list(settings.data_dir.glob('snapshot-*'))


@pytest.mark.parametrize('message,kind', [
    ('401 Unauthorized sensitive-data', ErrorKind.AUTH),
    ('DNS connection failed sensitive-data', ErrorKind.NETWORK),
    ('429 usage limit sensitive-data', ErrorKind.RATE_LIMIT),
    ('request timed out sensitive-data', ErrorKind.TIMEOUT),
    ('unexpected argument sensitive-data', ErrorKind.CONFIG),
    ('unknown failure sensitive-data', ErrorKind.EXECUTION),
])
def test_provider_failure_classification(settings, monkeypatch, message, kind):
    monkeypatch.setattr(codex, 'run', lambda *a, **k: (1, '', message))
    with pytest.raises(ProviderError) as error:
        codex.CodexProvider(settings).analyze(AnalysisRequest('{}'))
    assert error.value.kind == kind
    assert 'sensitive-data' not in str(error.value)
    metadata = next(settings.data_dir.glob('*.json')).read_text()
    assert 'sensitive-data' not in metadata
    assert json.loads(metadata)['error'] == kind.value


@pytest.mark.parametrize('output', ['not-json', '[]', '{}', json.dumps({'type': 'turn.completed'}),
    events({**REPORT, 'automatic_changes_performed': True}),
    events({**REPORT, 'extra_field': 'not allowed'})])
def test_reject_invalid_output(settings, monkeypatch, output):
    monkeypatch.setattr(codex, 'run', lambda *a, **k: (0, output, ''))
    with pytest.raises(ProviderError) as error:
        codex.CodexProvider(settings).analyze(AnalysisRequest('{}'))
    assert error.value.kind == ErrorKind.OUTPUT


def test_global_lock_across_processes(tmp_path):
    with codex.execution_lock(tmp_path):
        child = subprocess.run([sys.executable, '-c',
            'from pathlib import Path; from aiops.providers.codex import execution_lock; '
            f'\nwith execution_lock(Path({str(tmp_path)!r})): pass'], capture_output=True, text=True)
        assert child.returncode != 0 and 'busy' in child.stderr
    with codex.execution_lock(tmp_path):
        pass


def test_timeout_kills_child_process(tmp_path):
    marker = tmp_path / 'child-survived'
    pid_file = tmp_path / 'child.pid'
    child_code = f'import time; from pathlib import Path; time.sleep(1); Path({str(marker)!r}).touch(); time.sleep(30)'
    parent_code = (
        'import subprocess,sys,time; from pathlib import Path; '
        f'p=subprocess.Popen([sys.executable,"-c",{child_code!r}]); '
        f'Path({str(pid_file)!r}).write_text(str(p.pid)); time.sleep(30)'
    )
    with pytest.raises(ProviderError) as error:
        run([sys.executable, '-c', parent_code], timeout=0.3)
    assert error.value.kind == ErrorKind.TIMEOUT
    assert pid_file.exists(), 'child was spawned before timeout'
    time.sleep(1.1)
    assert not marker.exists()


def test_provider_timeout_records_failure_and_releases_lock(settings, monkeypatch):
    def timeout(*args, **kwargs):
        raise ProviderError(ErrorKind.TIMEOUT)
    monkeypatch.setattr(codex, 'run', timeout)
    with pytest.raises(ProviderError):
        codex.CodexProvider(settings).analyze(AnalysisRequest('{}'))
    assert json.loads(next(settings.data_dir.glob('*.json')).read_text())['status'] == 'TIMEOUT'
    with codex.execution_lock(settings.data_dir):
        pass
    assert not list(settings.data_dir.glob('snapshot-*'))


@pytest.mark.parametrize('code,kind', [(1, ErrorKind.AUTH), (0, None)])
def test_doctor_auth(settings, monkeypatch, code, kind):
    monkeypatch.setattr(codex, 'run', lambda args, **kw:
                        (0, 'codex-cli test', '') if '--version' in args else (code, '', ''))
    if kind:
        with pytest.raises(ProviderError) as error:
            codex.doctor(settings)
        assert error.value.kind == kind
    else:
        assert codex.doctor(settings)['authentication'] == 'configured'


def test_missing_executable(tmp_path):
    with pytest.raises(ProviderError) as error:
        codex.doctor(Settings(codex_bin=str(tmp_path / 'missing')))
    assert error.value.kind == ErrorKind.CONFIG


@pytest.mark.integration
@pytest.mark.skipif(os.getenv('AIOPS_RUN_INTEGRATION') != '1', reason='explicit real-call opt-in required')
def test_real_codex(tmp_path):
    settings = Settings(data_dir=tmp_path, timeout=180)
    result = codex.CodexProvider(settings).analyze(AnalysisRequest(Path('config/sample.json').read_text()))
    assert result.thread_id
    assert result.report.automatic_changes_performed is False
