#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
action="${1:-restart}"
case "$action" in
  restart) uv sync --locked ;;
  stop|status) ;;
  *) echo '사용법: ./restart.sh [restart|stop|status]' >&2; exit 2 ;;
esac
if [[ ! -x .venv/bin/python ]]; then
  echo './restart.sh를 먼저 실행해 주세요.' >&2
  exit 2
fi
exec .venv/bin/python - "$action" <<'PY'
import fcntl
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from dotenv import load_dotenv

root = Path.cwd()
if not (root / '.env').is_file():
    sys.exit('.env 파일이 필요합니다.')
load_dotenv(root / '.env', override=False)
state = Path(os.getenv('AIOPS_DATA_DIR', 'aiops_data')).expanduser().resolve() / 'run'
state.mkdir(parents=True, exist_ok=True, mode=0o755)
pid_file = state / 'api.pid'
log_directory = root / 'log_history'
log_directory.mkdir(parents=True, exist_ok=True, mode=0o755)
log_directory.chmod(0o755)
log_file = log_directory / f"api-{datetime.now(ZoneInfo('Asia/Seoul')).date().isoformat()}.log"
host = os.getenv('AIOPS_API_HOST', '0.0.0.0')
try:
    port = int(os.getenv('AIOPS_API_PORT', '9023'))
    if not 1 <= port <= 65535:
        raise ValueError()
except ValueError:
    sys.exit('AIOPS_API_PORT는 1~65535여야 합니다.')
command = [sys.executable, '-m', 'aiops.cli', '--env-file', str(root / '.env'),
           'serve', '--host', host, '--port', str(port), '--log-dir', str(log_directory)]


def identity(pid):
    result = subprocess.run(['ps', '-p', str(pid), '-o', 'lstart=', '-o', 'command='],
                            capture_output=True, text=True)
    return result.stdout.strip() if result.returncode == 0 else ''


def managed():
    try:
        saved = json.loads(pid_file.read_text())
        pid = int(saved['pid'])
        expected = saved['identity']
        if pid <= 1 or not expected or identity(pid) != expected:
            return None
        # Only a process started by this project may be signalled.
        if ' '.join(command[:5]) not in expected or ' serve ' not in expected:
            sys.exit('PID 파일이 현재 프로젝트의 API를 가리키지 않습니다.')
        return pid
    except (FileNotFoundError, ValueError, KeyError, TypeError):
        return None


lock = (state / 'restart.lock').open('a')
try:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
except BlockingIOError:
    sys.exit('다른 시작/종료 작업이 진행 중입니다.')
pid = managed()
action = sys.argv[1]
if action == 'status':
    print(f'실행 중: PID {pid}, 포트 {port}, 로그 폴더 {log_directory}' if pid else '중지 상태 (이 스크립트가 관리하는 API 없음)')
    sys.exit(0 if pid else 1)
if action == 'restart':
    # Validate before stopping a working API; never source .env as shell code.
    if not os.getenv('AIOPS_API_TOKEN') or not os.getenv('AIOPS_DATABASE_URL'):
        sys.exit('.env에 AIOPS_API_TOKEN과 AIOPS_DATABASE_URL을 설정해 주세요.')
if pid:
    print(f'API 종료 대기: PID {pid}', flush=True)
    os.kill(pid, signal.SIGTERM)
    # In-flight analysis may take up to the configured Codex timeout.
    deadline = time.monotonic() + 30
    while managed() and time.monotonic() < deadline:
        time.sleep(0.2)
    if managed():
        sys.exit('아직 요청을 처리 중입니다. 강제 종료하지 않았습니다. 잠시 후 다시 실행해 주세요.')
pid_file.unlink(missing_ok=True)
if action == 'stop':
    print('API 중지 완료')
    sys.exit(0)

# The CLI does not manage manually started processes. Refuse an occupied port.
import socket
with socket.socket(socket.AF_INET6 if ':' in host else socket.AF_INET, socket.SOCK_STREAM) as probe:
    probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        probe.bind((host, port))
    except OSError:
        sys.exit(f'포트 {port}를 사용할 수 없습니다. 수동 실행 API가 있다면 먼저 종료해 주세요.')

with log_file.open('ab') as log:
    child = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                             start_new_session=True, env={**os.environ, 'PYTHONUNBUFFERED': '1'})
    stamp = identity(child.pid)
    if not stamp:
        sys.exit(f'API 시작 실패. 로그 확인: {log_file}')
    pid_file.write_text(json.dumps({'pid': child.pid, 'identity': stamp}))
    pid_file.chmod(0o644)

address = '127.0.0.1' if host == '0.0.0.0' else ('[::1]' if host == '::' else host)
url = f'http://{address}:{port}/v1/status'
request = Request(url, headers={'X-AIOps-Key': os.environ['AIOPS_API_TOKEN']})
deadline = time.monotonic() + 30
while time.monotonic() < deadline:
    if child.poll() is not None:
        pid_file.unlink(missing_ok=True)
        sys.exit(f'API 시작 실패. 로그 확인: {log_file}')
    try:
        with urlopen(request, timeout=2) as response:
            payload = json.load(response)
            if payload.get('database', {}).get('status') == 'ready':
                print(f'API 시작 완료: PID {child.pid}, 포트 {port}')
                print(f'로그: {log_file}')
                sys.exit(0)
    except (HTTPError, URLError, TimeoutError, ValueError):
        pass
    time.sleep(0.5)
sys.exit(f'API 프로세스는 실행 중이지만 상태 확인에 실패했습니다. 로그 확인: {log_file}')
PY
