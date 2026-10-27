# eft-library-aiops

EFT Library의 로그와 서버 상태를 수집하고 Codex로 장애 원인과 개선 방향을 분석하는 Python 도구입니다. 현재 구현 범위는 **Phase 1: 샘플 JSON의 읽기 전용 분석**입니다. 운영 로그 수집, DB 저장, Airflow 연동은 Phase 2~3에서 구현합니다.

## 준비

- Python 3.12 이상, `uv`, macOS 또는 Linux (POSIX 프로세스 그룹·파일 잠금 사용)
- Codex CLI: 로컬 검증 버전 `0.165.0`. 배포 시 검증한 버전을 고정하는 것을 권장합니다.
- CLI를 실행하는 동일 계정의 Codex 로그인

```sh
npm install -g @openai/codex@latest
codex login --device-auth
codex login status
uv sync --locked
```

기존 `.venv`를 사용합니다. `uv`가 필요한 의존성을 동기화합니다.

## 실행

모든 명령은 프로젝트 최상위 폴더(`eft-library-aiops/`)에서 실행합니다. `uv run`을 사용하므로 가상환경을 따로 활성화할 필요는 없습니다.

### 처음 실행하는 순서

1. 환경과 Codex 로그인 상태를 확인합니다.

   ```sh
   uv run aiops doctor
   ```

   오류 없이 `authentication`이 `configured`로 출력되면 다음 단계로 진행합니다. 오류가 발생하면 먼저 설치·로그인 상태를 확인합니다.

2. 샘플 JSON을 분석합니다.

   ```sh
   uv run aiops analyze --input config/sample.json
   ```

   실제 Codex를 호출합니다. 완료되면 작업 ID(`job_id`), Thread ID(`thread_id`), 분석 결과(`report`)가 출력됩니다. 기본 제한 시간은 10분입니다.

`doctor`는 최초 실행이나 환경·인증 문제를 확인할 때 사용하면 됩니다. 이후에는 분석 명령만 실행해도 됩니다.

```sh
uv run aiops analyze --input config/sample.json
```

`doctor`는 설치 버전과 로컬 로그인 상태만 점검합니다. 토큰 유효성·원격 연결·사용량 잔여분까지 보장하지 않습니다. `analyze`는 실제 Codex를 호출하고 작업 ID, Thread ID, 검증된 분석 JSON을 출력합니다. 입력은 UTF-8 JSON이고 기본 상한은 100,000바이트입니다. **Phase 1에서는 이미 마스킹된 샘플만 입력합니다. 자동 마스킹은 아직 없습니다.**

### .env를 사용하는 경우 (선택)

기본 설정으로 실행할 때는 `.env`가 없거나 비어 있어도 됩니다. ChatGPT 로그인은 기존 Codex 인증을 사용하므로 API 키나 토큰을 `.env`에 넣을 필요가 없습니다.

설정은 환경변수로 지정하며 `.env`는 자동으로 읽지 않습니다. 설정을 변경하려면 `.env.example`을 참고해 `.env`를 작성하고, 아래 명령을 순서대로 실행합니다. 기존 `.env`가 있다면 덮어쓰지 말고 필요한 값만 추가합니다.

```sh
uv run aiops --env-file .env doctor
uv run aiops --env-file .env analyze --input config/sample.json
```

기존 환경변수가 dotenv 파일보다 우선합니다.

| 변수 | 기본값 | 용도 |
|---|---|---|
| `AIOPS_CODEX_BIN` | `codex` | 기존 Codex 실행 파일 또는 절대 경로 |
| `AIOPS_DATA_DIR` | `.aiops` | 잠금과 실행 메타데이터 저장 경로 |
| `AIOPS_TIMEOUT_SECONDS` | `600` | Codex 실행 타임아웃(초) |
| `AIOPS_MAX_INPUT_BYTES` | `100000` | JSON 입력 최대 바이트 |
| `AIOPS_MODEL` | 미지정 | 계정에서 사용 가능한 모델; 미지정 시 CLI 기본값 |

Codex 사용자 설정과 프로젝트 규칙 파일은 불러오지 않습니다. 따라서 사용자 `config.toml`에 설정한 모델·MCP·사용자 정의 Provider는 사용하지 않습니다. 모델은 필요하면 위 변수로 지정합니다. 기존 ChatGPT 로그인은 재사용하며 API 키 환경변수는 자식 프로세스에 전달하지 않습니다. 기존 CLI가 API 키 방식으로 로그인되어 있다면 그 인증이 사용되므로 `codex login status`로 확인하세요.

ChatGPT 인증은 플랜의 사용량 제한을 따릅니다. API 키 로그인은 별도 API 과금 대상입니다. 계정별 한도는 고정하지 않습니다. 자세한 근거는 [Provider 결정 문서](docs/CODEX_PROVIDER.md)를 참고하세요.

## 결과와 오류

결과 본문은 stdout으로만 출력합니다. 보고서를 보관하려면 민감정보 제거를 확인한 뒤 Git 제외 데이터 경로에 저장하세요. `.aiops/<job_id>.json`에는 상태·오류 종류·시각·소요 시간·Provider·요청 모델·Thread ID만 기록합니다. 원문 로그·프롬프트·응답·인증 정보는 기록하지 않습니다. 모델을 미지정하면 메타데이터의 모델은 `null`이며 실제 모델을 추정하지 않습니다.

| 종료 코드 | 의미 |
|---|---|
| 0 | 성공 |
| 2 | 설정·입력·실행 파일 오류 |
| 3 | 인증 오류 |
| 4 | 네트워크 오류 |
| 5 | 사용량 제한 |
| 6 | 시간 초과 |
| 7 | 동일 데이터 경로에서 다른 분석 실행 중 |
| 8 | 잘못된 JSON 이벤트·결과 스키마 |
| 9 | 기타 Codex 실행 실패 |

오류는 stderr의 `{"error": "..."}`로 전달하며 Codex의 원문 진단은 노출하지 않습니다. CLI가 구조화된 원인을 제공하지 않는 일부 오류는 문구 기반으로 분류하므로 알 수 없는 오류는 `execution`으로 남깁니다. 자동 재시도는 하지 않습니다. 인증·네트워크·사용량 문제 해결 후 같은 입력으로 다시 실행하세요.

동시 실행은 동일한 `AIOPS_DATA_DIR`의 OS 파일 잠금으로 1개로 제한합니다. 프로세스가 종료되면 잠금은 해제됩니다. 남아 있는 `codex.lock` 파일은 잠금 상태를 의미하지 않으므로 삭제하지 마세요. 다른 데이터 경로나 직접 실행한 `codex`까지 막지는 않습니다. 운영에서는 모든 AIOps 실행이 동일 경로를 사용해야 합니다.

## 테스트

```sh
uv run pytest -q
# 실제 Codex 호출 포함 (명시적 opt-in)
AIOPS_RUN_INTEGRATION=1 uv run pytest -q -m integration
```

기본 테스트는 실제 Codex·네트워크 없이 실행합니다. 가짜 Provider, CLI 오류 코드, 입력 제한, 결과 검증, 비밀정보 미기록, 프로세스 간 잠금, 타임아웃 시 자식 프로세스 종료를 검증합니다.

[보안 및 배포 안내](docs/SECURITY.md), [입력 계약 초안](docs/BATCH_CONTRACT.md), [Phase 1](docs/PHASE1.md), [Phase 2](docs/PHASE2.md), [Phase 3](docs/PHASE3.md)를 참고하세요.
