# eft-library-aiops

EFT Library의 로그와 서버 상태를 수집하고 Codex로 장애 원인과 개선 방향을 분석하는 Python 도구입니다. 현재 **Phase 1의 Codex 분석 연결과 Phase 2의 로그·시스템 상태 수집**을 구현했습니다. 일일·주간·월간 입력 통합과 버전별 보고서 저장도 지원합니다. PostgreSQL 보고서 저장·작업 큐·단일 Worker도 지원합니다. 외부 Airflow 프로젝트에서 호출할 HTTP API도 제공합니다.

향후 AIOps 웹 서비스의 기본 포트는 **백엔드(FastAPI) 9023**, **프론트엔드(추가 시) 4003**으로 사용합니다. `aiops serve`는 백엔드 9023 포트를 사용하며 CLI 수집·분석은 포트를 열지 않습니다.

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

### 운영 실행 흐름

운영 기준은 **Docker Airflow → 호스트 AIOps API(9023)**입니다. Airflow에 프로젝트·로그·Codex 인증 볼륨을 공유하지 않습니다. Worker는 API 호출 시 한 건씩 실행합니다.

`.env`에 기존 DB 설정과 아래 값을 지정합니다.

```dotenv
AIOPS_SOURCES_CONFIG=config/log_sources.yaml
AIOPS_API_TOKEN=직접_생성한_비밀키
```

```sh
# 최초 설치만: 테이블 초기화와 기존 데이터 등록
uv run aiops --env-file .env setup

# API 시작. 기본 포트 9023. 이 프로세스는 계속 실행되어야 합니다.
uv run aiops --env-file .env serve --host 0.0.0.0
```

이후에는 Airflow가 수집·보고서 접수·Worker 실행을 HTTP로 호출합니다. **[API 호출 안내](docs/AIRFLOW_API.md)**를 참고하세요. 별도 상시 Worker 서비스는 필요하지 않습니다. Airflow DAG와 스케줄은 별도 프로젝트에서 관리합니다.

수동 진단은 `collect --all`, `report run --type daily`, `worker --once`, `status`로 할 수 있습니다. 수집은 DB 등록까지 자동 처리하고, report run은 입력 준비·큐 등록을 통합합니다. DB 없는 로컬 모드에서는 report run이 직접 Codex를 호출합니다. `db`, `jobs`, `report prepare`는 관리·복구용입니다.

### .env를 사용하는 경우 (선택)

기본 설정으로 실행할 때는 `.env`가 없거나 비어 있어도 됩니다. ChatGPT 로그인은 기존 Codex 인증을 사용하므로 API 키나 토큰을 `.env`에 넣을 필요가 없습니다.

설정은 환경변수로 지정하며 `.env`는 자동으로 읽지 않습니다. 설정을 변경하려면 `.env.example`을 참고해 `.env`를 작성하고, 아래 명령을 순서대로 실행합니다. 기존 `.env`가 있다면 덮어쓰지 말고 필요한 값만 추가합니다.

```sh
uv run aiops --env-file .env doctor
uv run aiops --env-file .env analyze --input config/sample.json
```

기존 환경변수가 dotenv 파일보다 우선합니다.

| 변수                    | 기본값   | 용도                                            |
| ----------------------- | -------- | ----------------------------------------------- |
| `AIOPS_CODEX_BIN`       | `codex`  | 기존 Codex 실행 파일 또는 절대 경로             |
| `AIOPS_DATA_DIR`        | `aiops_data` | 잠금과 실행 메타데이터 저장 경로                |
| `AIOPS_TIMEOUT_SECONDS` | `600`    | Codex 실행 타임아웃(초)                         |
| `AIOPS_MAX_INPUT_BYTES` | `100000` | JSON 입력 최대 바이트                           |
| `AIOPS_MODEL`           | 미지정   | 계정에서 사용 가능한 모델; 미지정 시 CLI 기본값 |

Codex 사용자 설정과 프로젝트 규칙 파일은 불러오지 않습니다. 따라서 사용자 `config.toml`에 설정한 모델·MCP·사용자 정의 Provider는 사용하지 않습니다. 모델은 필요하면 위 변수로 지정합니다. 기존 ChatGPT 로그인은 재사용하며 API 키 환경변수는 자식 프로세스에 전달하지 않습니다. 기존 CLI가 API 키 방식으로 로그인되어 있다면 그 인증이 사용되므로 `codex login status`로 확인하세요.

ChatGPT 인증은 플랜의 사용량 제한을 따릅니다. API 키 로그인은 별도 API 과금 대상입니다. 계정별 한도는 고정하지 않습니다. 자세한 근거는 [Provider 결정 문서](docs/CODEX_PROVIDER.md)를 참고하세요.

### 기존 숨김 데이터 폴더 이전

기본 데이터 폴더는 일반 `ls`에도 표시되는 `aiops_data/`입니다. 기존 서버에서 `.aiops/`를 사용했다면 AIOps 실행을 멈춘 상태에서, `aiops_data/`가 아직 없을 때 폴더 전체를 옮깁니다.

```sh
mv .aiops aiops_data
```

`.env`에 기존 경로를 설정했다면 `AIOPS_DATA_DIR=aiops_data`로 변경합니다. 폴더 전체를 옮겨야 SQLite 체크포인트와 기존 배치를 유지하며 이어 수집할 수 있습니다. 두 폴더가 이미 모두 있다면 덮어쓰거나 임의로 합치지 않습니다. 기존 `.aiops/`와 새 `aiops_data/`는 모두 Git 제외 대상입니다.

### 데이터 접근 권한

`aiops_data/`에 생성하는 폴더는 **755**, 일반 데이터 파일은 **644**로 설정합니다. 같은 서버의 다른 계정에서도 읽을 수 있으며 데이터 파일에는 실행 권한을 주지 않습니다. 기존 서버 파일은 AIOps 작업을 멈춘 상태에서 한 번 적용합니다.

```sh
find aiops_data -type d -exec chmod 755 {} +
find aiops_data -type f -exec chmod 644 {} +
```

Codex 인증 저장소나 `.env`의 권한은 변경하지 않습니다.

## 로그 수집 (Phase 2)

**`collect`는 Codex를 실행하지 않습니다.** 로그 읽기·정규화·마스킹·오류 집계·시스템 지표 수집 후 재사용 가능한 배치를 저장합니다. PostgreSQL 접속 설정도 필요하지 않습니다.

```sh
# 기본 설정: 로컬 테스트용 로그로 먼저 확인
uv run aiops collect --all

# 운영 서버: 실제 FastAPI 로그부터 확인
uv run aiops collect --config config/log_sources.yaml --service fastapi

# 운영 서버: 설정된 모든 서비스 수집
uv run aiops collect --config config/log_sources.yaml --all

# .env 설정을 사용할 경우
uv run aiops --env-file .env collect --config config/log_sources.yaml --all
```

운영 경로는 `config/log_sources.yaml`에서 확인·수정합니다. 현재 root 실행 환경을 지원하며, 원본 로그나 서비스 설정은 변경하지 않습니다. Docker 지표 수집은 기본 비활성입니다.

첫 실행은 파일별 최근 일부만 읽고, 다음 실행은 새로 추가된 부분만 읽습니다. 새 로그가 없으면 `events: 0`도 정상입니다. 출력의 `snapshot`은 요약 JSON, `events_file`은 마스킹한 정규화 이벤트 파일입니다. 기본 저장 경로는 `aiops_data/collection/`이고 체크포인트는 별도 SQLite에 저장합니다.

수집 한도는 서비스·파일 사이에 배분하며 시작 서비스와 디렉터리 탐색 위치를 저장해 다음 실행에 이어갑니다.

`status: partial`과 종료 코드 **10**은 일부 소스 실패·한도 도달·미완성 줄 보류가 있으나 배치는 저장했다는 의미입니다. `coverage`와 저장된 스냅샷의 `collection_failures`를 확인하세요. 새 경고 `scan_entry_limit`과 `scan_timeout`은 탐색 개수/시간을 구분합니다. `timestamp_absent`는 원문에 시각이 없는 로그이며, 이를 수집 시각으로 대체하지 않습니다.

수집만 할 때는 여기까지 실행합니다. 아래 `analyze`는 단일 배치 점검용입니다. 정기 보고서는 다음 절의 `report` 명령으로 날짜별로 묶어 분석합니다.

```sh
uv run aiops analyze --input <출력된_snapshot.json_경로>
# 모델 등 .env 설정이 있다면
uv run aiops --env-file .env analyze --input <출력된_snapshot.json_경로>
```

`--since 24h`는 이번 증분 읽기의 시간 필터이며 과거 24시간 전체를 재수집하지 않습니다. `--backfill`은 처음 보는 파일을 시작부터 읽되 기존 체크포인트는 유지합니다. 옵션·회전·중단 복구·백업·보관 정책과 Airflow 연결 설계는 [수집 안내](docs/COLLECTION.md)를 참고하세요. 마스킹은 규칙 기반이므로 처음 운영 수집한 배치의 내용을 직접 검토한 뒤 외부 분석에 사용하세요.

## 일일·주간·월간 보고서

아래 직접 실행 설명은 기본 로컬 SQLite 모드 기준입니다. PostgreSQL 설정 시 `report run`은 큐 등록으로 바뀌며 다음 절의 Worker가 처리합니다.

한국 시간 기준으로 **일일은 전날 데이터**, **주간은 지난 월~일의 일일 보고서 7개**, **월간은 지난달의 일일 보고서 전체**를 사용합니다. 월간의 주간 보고서는 보조 참고이며 중복 합산하지 않습니다. 수집 배치 하나를 하루로 간주하지 않습니다.

```sh
# AI 호출 없이 전날의 입력과 누락 상태부터 확인
uv run aiops --env-file .env report prepare --type daily --config config/log_sources.yaml

# 전날 일일 분석 (실제 Codex 호출, 결과 저장)
uv run aiops --env-file .env report run --type daily --config config/log_sources.yaml

# 일일 보고서가 완료된 뒤 실행: 지난주 / 지난달
uv run aiops --env-file .env report run --type weekly
uv run aiops --env-file .env report run --type monthly

# 특정 날짜 재분석 또는 입력 확인: --date 2026-10-27 추가
uv run aiops --env-file .env report list
```

`.env`를 쓰지 않으면 `--env-file .env`만 생략합니다. `--date`를 생략하면 마지막 종료된 기간을 자동 선택합니다. 현재 진행 중인 날짜·주·월은 받지 않습니다. 수집은 짧은 간격으로 별도 실행하며, 정기 수집에는 `--since`를 붙이지 않습니다.

보고서는 `aiops_data/reports/`에 저장합니다. 같은 입력의 완료된 결과는 재사용하고, 지연 로그나 하위 보고서가 바뀌면 새 버전으로 분석합니다. 누락·오래된 하위 보고서는 `coverage_complete: false`로 표시하고 종료 코드 10을 반환할 수 있습니다. `report list`의 `stale`로 갱신 필요 여부를 확인합니다.

커버리지의 기본 기대 수집 주기는 5분이며 실제 주기가 다르면 `--interval-minutes`로 맞춥니다. **Airflow DAG는 별도 프로젝트에서 관리합니다. API 호출 계약은 [API 안내](docs/AIRFLOW_API.md)를 따릅니다.** 기간·재실행·기존 데이터 호환·저장/복구의 상세 기준은 [정기 보고서 안내](docs/REPORTING.md)를 참고하세요.

## PostgreSQL와 단일 Worker (Phase 3)

PostgreSQL 테이블은 **public 스키마의 aiops_ 접두사**를 사용합니다. DB를 이미 만들었다면 .env의 dbname을 확인한 뒤 `setup`을 실행하면 됩니다. 기존 aiops 스키마 설치도 데이터를 유지하며 public으로 이전합니다. 정상 점검 결과는 `schema: public`, `migration: 2`입니다.

서버 최초 적용은 [PostgreSQL·Worker 안내](docs/POSTGRES_WORKER.md)의 **DB/역할 준비 → .env 설정 → setup → API 실행 → Airflow 연결** 순서를 따릅니다. 스키마는 [생성 SQL](src/aiops/storage/sql/001_initial.sql)로 준비했으며 운영 DB에 자동 적용하지 않습니다.

```dotenv
# .env에 추가. 실제 DB 비밀번호를 사용하고 이 파일은 Git에 넣지 않습니다.
AIOPS_DATABASE_URL="host=127.0.0.1 port=13245 dbname=aiops user=aiops_app"
PGPASSWORD="DB_비밀번호"
AIOPS_SOURCES_CONFIG=config/log_sources.yaml
```

```sh
uv sync --locked
uv run aiops --env-file .env setup
uv run aiops --env-file .env status
```

DB 모드의 `report run`은 즉시 AI 실행 대신 **큐에 등록**합니다. 실제 Codex 호출은 `worker --once` 또는 상시 `worker`에서 수행합니다. 같은 입력의 작업은 중복 등록하지 않으며 완료 결과를 재사용합니다. 요청 당시 모델을 유지하고, 인증·사용량 제한은 자동 반복하지 않습니다. 데이터가 오래되면 분석 대신 `CANCELLED/stale_input`으로 남기므로 해당 기간의 `report run`을 다시 실행합니다.

`collect`는 파일 저장을 마친 뒤 PostgreSQL이 설정되어 있으면 배치 정보를 자동 등록합니다. DB 등록이 실패해도 수집 파일은 보존됩니다. AIOPS_DATABASE_URL을 설정하지 않으면 기존 로컬 SQLite 보고서 모드가 유지됩니다. DB 오류 종료 코드는 **11**이며 연결값과 서버 오류 원문을 출력하지 않습니다. 기본 운영 경로는 [Airflow → API](docs/AIRFLOW_API.md)입니다. 기존 systemd Worker 예제는 선택적인 대체 방식입니다.

## 결과와 오류

아래는 `analyze` 명령의 결과와 오류입니다. `collect`의 저장 구조·종료 코드는 위 수집 안내를 참고하세요.

분석 결과 본문은 stdout으로만 출력합니다. 보고서를 보관하려면 민감정보 제거를 확인한 뒤 Git 제외 데이터 경로에 저장하세요. `aiops_data/<job_id>.json`에는 상태·오류 종류·시각·소요 시간·Provider·요청 모델·Thread ID만 기록합니다. 원문 로그·프롬프트·응답·인증 정보는 기록하지 않습니다. 모델을 미지정하면 메타데이터의 모델은 `null`이며 실제 모델을 추정하지 않습니다.

| 종료 코드 | 의미                                   |
| --------- | -------------------------------------- |
| 0         | 성공                                   |
| 2         | 설정·입력·실행 파일 오류               |
| 3         | 인증 오류                              |
| 4         | 네트워크 오류                          |
| 5         | 사용량 제한                            |
| 6         | 시간 초과                              |
| 7         | 동일 데이터 경로에서 다른 분석 실행 중 |
| 8         | 잘못된 JSON 이벤트·결과 스키마         |
| 9         | 기타 Codex 실행 실패                   |

오류는 stderr의 `{"error": "..."}`로 전달하며 Codex의 원문 진단은 노출하지 않습니다. CLI가 구조화된 원인을 제공하지 않는 일부 오류는 문구 기반으로 분류하므로 알 수 없는 오류는 `execution`으로 남깁니다. 자동 재시도는 하지 않습니다. 인증·네트워크·사용량 문제 해결 후 같은 입력으로 다시 실행하세요.

동시 실행은 동일한 `AIOPS_DATA_DIR`의 OS 파일 잠금으로 1개로 제한합니다. 프로세스가 종료되면 잠금은 해제됩니다. 남아 있는 `codex.lock` 파일은 잠금 상태를 의미하지 않으므로 삭제하지 마세요. 다른 데이터 경로나 직접 실행한 `codex`까지 막지는 않습니다. 운영에서는 모든 AIOps 실행이 동일 경로를 사용해야 합니다.

## 테스트

```sh
uv run pytest -q
# 실제 Codex 호출 포함 (명시적 opt-in)
AIOPS_RUN_INTEGRATION=1 uv run pytest -q -m integration
```

기본 테스트는 실제 Codex·PostgreSQL·네트워크·Docker daemon 없이 실행합니다. 실제 PostgreSQL 검증은 [별도 opt-in](docs/POSTGRES_WORKER.md)입니다. 정기 보고서는 기간 경계·윤년·지연 도착·버전 재사용·누락 전파·월간 입력 제한도 검증합니다. Phase 2는 증분·회전·덮어쓰기·미완성 줄·중단 복구·마스킹·HTTP 집계와 시스템/Docker 대역도 검증합니다. 가짜 Provider, CLI 오류 코드, 입력 제한, 결과 검증, 비밀정보 미기록, 프로세스 간 잠금, 타임아웃 시 자식 프로세스 종료를 검증합니다.

[보안 및 배포 안내](docs/SECURITY.md), [입력 계약 초안](docs/BATCH_CONTRACT.md), [Phase 1](docs/PHASE1.md), [Phase 2](docs/PHASE2.md), [Phase 3](docs/PHASE3.md)를 참고하세요.
