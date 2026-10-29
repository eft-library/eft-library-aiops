# PostgreSQL 저장소와 단일 Worker — 서버 적용 안내

문서의 `<...>` 값과 `/example/` 경로는 예시입니다. 복사해서 실행하기 전에 실제 배포 환경의 값으로 바꾸세요.

새 DB를 준비한 경우 아래 명령으로 최신 테이블을 초기화한 뒤 Worker를 검증한다. 현재 구현은 로컬의 실제 PostgreSQL 17과 가짜 AI Provider로 검증했다. 기존 서비스 테이블·설정·방화벽을 바꾸지 않는다.

## 운영 기준: Docker Airflow → API

운영은 [AIRFLOW_API.md](AIRFLOW_API.md)를 따른다. API는 호스트에서 상시 실행하고, Docker Airflow가 HTTP로 수집·보고서 접수·Worker 1회 실행을 호출한다. 별도 상시 Worker 서비스는 필요 없다. 아래 CLI·systemd 설명은 수동 진단 또는 대체 운영 방식이다.

## 수동 진단에서 사용할 명령

`.env`에 `AIOPS_SOURCES_CONFIG=config/log_sources.yaml`을 한 번 설정한다.

```sh
# 최초 한 번. 테이블 초기화·기존 이력 이전·배치 등록을 통합한다.
uv run aiops --env-file .env setup

# 정기 수집. DB 등록도 자동으로 처리한다.
uv run aiops --env-file .env collect --all

# 일일 분석 요청. 상시 실행 Worker가 처리한다.
uv run aiops --env-file .env report run --type daily

# 수집 상태·DB·최근 작업 확인
uv run aiops --env-file .env status
```

운영 Worker는 Airflow가 API로 한 번씩 실행한다. CLI 수동 검증에는 `worker --once`를 사용한다. 주간·월간은 `report run --type weekly`와 `report run --type monthly`를 사용한다. 하위 일일 분석이 완료된 후 요청한다. `report run`은 작업 접수이며 완료 상태는 `status`에서 확인한다. 기본 일일 대상은 한국 시간 기준 전날이다.

수집 후 DB 장애 시 파일은 유지하고 종료 코드 11을 반환하며, 다음 수집에서 미등록 배치를 자동 재등록한다. 아래의 세부 DB·큐 명령은 수동 진단과 복구 절차이며 일반 운영 시 전부 실행하는 순서가 아니다.

## 구성

```text
collect → 파일 배치 + SQLite 체크포인트
                 ↓ 배치 메타데이터 등록
report prepare → PostgreSQL 보고서 입력·버전·의존 관계
jobs submit    → PostgreSQL 작업 큐
                 ↓
단일 Worker → Codex → 결과 검증 → PostgreSQL 결과·시도 이력
```

`AIOPS_DATABASE_URL`이 없으면 기존 SQLite 보고서 모드가 유지된다. 설정하면 보고서 원본과 큐는 PostgreSQL을 사용한다. Collector는 계속 DB·Codex 없이 동작하며 체크포인트는 SQLite에 둔다. 같은 DB를 사용하는 모든 실행은 **같은 호스트·같은 AIOPS_DATA_DIR**를 사용한다. 여러 호스트의 로컬 파일을 공유하는 구조는 지원하지 않는다.

API와 조회 웹은 별도 프로세스다. Worker는 HTTP 포트를 열지 않으며 설정된 PostgreSQL에 접속한다. FastAPI API를 제공하며 Airflow DAG는 별도 프로젝트에서 관리한다.

## 이미 DB가 준비된 환경

기존 .env의 접속 대상·계정·비밀번호와 `AIOPS_SOURCES_CONFIG=config/log_sources.yaml`을 확인한다. DB를 이미 만들었으므로 create_database.sql은 다시 실행하지 않는다. 최신 코드를 반영한 뒤 `uv sync --locked`, `uv run aiops --env-file .env setup`을 실행하면 준비가 끝난다.

setup 결과 database 항목의 정상 기준은 `schema: public`, `migration: 3`다. 새 DB에는 public.aiops_* 9개 테이블이 생성된다. 실행 계정에 public의 CREATE 권한이 필요하다. DB 소유 계정이면 기본적으로 가능하며, 다른 계정을 사용한다면 관리자가 해당 계정에 public의 USAGE/CREATE 권한을 부여해야 한다.

이전 버전의 aiops 스키마가 남아 있다면 setup이 테이블·데이터·참조·identity sequence를 보존하며 public.aiops_*로 이동한다. 이름 충돌 또는 불완전한 구성이 있으면 트랜잭션을 중단한다. 비어 있는 이전 스키마만 제거하며, 다른 객체가 남아 있으면 유지한다. 기존 public의 다른 테이블은 변경하지 않는다.

아래는 계정 준비, 수동 진단·복구와 Worker 서비스 등록의 상세 안내다.

## 1. 서버에서 전용 DB와 접속 계정 준비

전용 DB와 접속 역할을 준비한다. 아래 SQL 스크립트의 DB·역할 이름은 예시이므로 배포 환경에 맞춰 검토한 뒤 PostgreSQL 관리자로 한 번 실행한다. 운영체제 계정과 PostgreSQL 로그인 역할은 별개다.

```sh
# PGPORT에 실제 DB 포트를 지정한다. psql 경로도 설치 환경에 맞게 조정한다.
runuser -u postgres -- /usr/pgsql-17/bin/psql -p "$PGPORT" -d postgres \
  < /example/aiops/deploy/create_database.sql
```

이 SQL은 `aiops_app` 역할을 만들고, `\password`로 비밀번호를 두 번 입력받은 뒤 전용 DB를 만든다. 비밀번호를 SQL 파일이나 채팅에 넣지 않는다. 역할·DB가 이미 있으면 스크립트를 반복 적용하지 말고 해당 생성 줄을 생략해 검토한다. 기존 운영 DB를 사용하기로 했다면 이후 연결값을 바꾸고 public의 aiops_ 접두사 테이블을 쓰되, 기존 public의 다른 테이블은 그대로 둔다.

## 2. .env 설정과 의존성 동기화

기존 모델·데이터 경로 값은 유지하고 다음을 추가한다.

```dotenv
AIOPS_DATABASE_URL="host=<DB_HOST> port=<DB_PORT> dbname=<DB_NAME> user=<DB_USER>"
PGPASSWORD="위에서_설정한_DB_비밀번호"
AIOPS_SOURCES_CONFIG=config/log_sources.yaml
```

비밀번호는 libpq의 PGPASSWORD로 읽는다. URL 안에 넣지 않아 URL 인코딩이 필요하지 않다. 비밀번호를 별도 파일에 두려면 libpq의 PGPASSFILE을 사용할 수 있으며 해당 파일은 600으로 제한하고 Git에서 제외한다. AIOPS_DATABASE_URL도 password 필드를 포함할 수 있으므로 프로그램은 값과 서버 오류 원문을 출력하지 않는다. Codex 자식 프로세스에는 DB URL·PGPASSWORD·PGPASSFILE을 전달하지 않는다.

```sh
cd /example/aiops
chmod 600 .env
uv sync --locked
```

aiops_data의 폴더 755·파일 644 정책과 인증 설정 파일의 권한은 별개다. Codex 로그인과 실행은 같은 계정을 사용한다.

## 3. 테이블 SQL 검토·적용·연결 확인

초기 테이블은 [001_initial.sql](../src/aiops/storage/sql/001_initial.sql), 기존 스키마 이전은 [002_move_to_public.sql](../src/aiops/storage/sql/002_move_to_public.sql)에 정의되어 있다. 설치된 패키지에서 하나의 트랜잭션으로 실행되는 전체 SQL을 출력할 수 있다.

```sh
# 출력만 한다. DB 연결·변경 없음
uv run aiops db print-sql

# 선택된 DB의 public에 aiops_ 테이블 적용 (이 명령에서만 명시적으로 DDL 수행)
uv run aiops --env-file .env db init

# 연결·서버 버전·마이그레이션 확인
uv run aiops --env-file .env db doctor
```

실행 시 자동 마이그레이션은 없다. init은 동일 버전 재적용이 가능하며 기존 애플리케이션 테이블을 변경하지 않는다. public.aiops_ 이름을 다른 용도로 이미 쓰고 있다면 적용 전에 확인한다. DB doctor 성공은 Codex 인증까지 검증한 것이 아니므로 기존 `aiops doctor`도 실행할 수 있다.

| 테이블 | 내용 |
|---|---|
| public.aiops_schema_migrations | 적용 버전 |
| public.aiops_batches | 배치 ID·관측 기간·상대 파일 경로·coverage·totals. 원문 로그 미저장 |
| public.aiops_reports | 종류·달력 기간·시간대의 정체성 |
| public.aiops_report_versions | revision·fingerprint·입력·manifest·분석 결과·데이터 완전성 |
| public.aiops_report_dependencies | 입력 배치와 하위 daily/weekly_context 연결 |
| public.aiops_analysis_jobs | 우선순위·대기/실행 상태·재시도 시각·소유 Worker |
| public.aiops_job_attempts | 시도별 시작/종료·오류 코드·Provider job/Thread ID |
| public.aiops_investigations | 일일 심화 조사 접수·상태·입력·결과 |
| public.aiops_investigation_attempts | 조사 시도별 입력·결과·오류 |

보고서와 작업 ID는 별개다. 보고서 ID는 기존 `daily-YYYY-MM-DD-v0001`을 유지하고 큐 작업은 UUID다. 새 배치 참조를 만들 때 관련 메타데이터도 등록한다.

## 4. 기존 SQLite 이력 이전과 배치 등록

보고서 작업·Worker가 없는 상태에서 실행한다. 기존 데이터 폴더와 체크포인트는 지우지 않는다.

```sh
uv run aiops --env-file .env db import-sqlite
uv run aiops --env-file .env db sync-batches
uv run aiops --env-file .env report list
```

import-sqlite는 reports.sqlite3를 읽기 전용으로 열어 보고서 ID·기간·버전·결과·참조를 같은 PostgreSQL 트랜잭션으로 옮긴다. 이미 같은 ID·fingerprint가 있으면 건너뛰며 충돌하면 롤백한다. 실행 중으로 남은 로컬 보고서는 FAILED/interrupted_import로 옮겨 명시적 재실행 대상으로 둔다. 원본 SQLite는 수정·삭제하지 않는다. 초기 사용자 구성처럼 PREPARED 보고서만 있어도 이전할 수 있다.

sync-batches는 아직 분석에 사용되지 않은 배치의 메타데이터도 등록한다. 원본 로그나 events.jsonl 전체를 DB에 올리지 않는다. Collector 성공과 DB 등록은 분리되어 있으므로 DB 장애가 수집을 되돌리지 않는다. 동일 명령은 이미 등록한 배치를 건너뛰며 새 배치가 생긴 뒤 다시 실행할 수 있다.

## 5. 분석 작업 등록·단일 실행 검증

실제 데이터가 있는 종료된 날짜를 명시한다. 예시 날짜를 그대로 쓰면 그 날짜 데이터가 없을 수 있다. `report prepare`로 source_batch_count·data_quality를 먼저 볼 수 있다.

```sh
# 전날 일일 입력만 준비: Codex 호출 없음
uv run aiops --env-file .env report prepare --type daily --config config/log_sources.yaml

# 대기열 등록: Codex 호출 없음
uv run aiops --env-file .env jobs submit --type daily --date YYYY-MM-DD \
  --config config/log_sources.yaml --priority normal

uv run aiops --env-file .env jobs list

# 준비된 작업 한 개 처리: 여기서 실제 Codex 호출·사용량 발생
uv run aiops --env-file .env worker --once

uv run aiops --env-file .env jobs list
uv run aiops --env-file .env report list
```

DB 모드에서는 기존 `report run`도 jobs submit의 별칭으로 **큐 등록까지만** 한다. 즉시 터미널에서 AI가 실행되는 로컬 SQLite 모드와 다르다. 실제 실행은 Worker가 담당한다. 데이터·유효한 하위 보고서가 전혀 없으면 submit을 거부하며, 일부 누락이 있는 데이터는 불완전성을 보존한 채 분석할 수 있다.

종류·기간·입력 fingerprint가 같으면 같은 report version과 job ID를 재사용한다. 이미 완료된 결과는 다시 Codex에 보내지 않는다. 요청 모델은 submit 당시 manifest에 고정하므로 Worker의 기본 모델이 나중에 달라져도 바뀌지 않는다. 분석 지침이 변경되었거나 입력이 오래됐으면 stale_input으로 취소하고 해당 기간을 다시 submit한다.

## 6. 선택 사항: 상시 Worker (현재 운영 기본 방식 아님)

수동 확인:

```sh
uv run aiops --env-file .env worker
```

운영 서비스 예시는 [eft-aiops-worker.service](../deploy/eft-aiops-worker.service)다. 프로젝트 경로와 `command -v node`, `command -v codex`의 실제 디렉터리를 확인해 unit의 PATH를 먼저 맞춘다. 필요하면 AIOPS_CODEX_BIN을 실제 절대 경로로 지정한다. 그 뒤 운영자가 등록한다.

```sh
cp deploy/eft-aiops-worker.service /etc/systemd/system/
systemctl daemon-reload
systemctl enable --now eft-aiops-worker
systemctl status eft-aiops-worker
journalctl -u eft-aiops-worker -n 50 --no-pager
```

서비스 실행 계정·작업 경로·PATH는 배포 환경에 맞게 지정한다. KillMode=control-group을 유지해 서비스 종료 시 Codex 자식도 정리한다. SIGTERM/CTRL-C는 현재 프로세스를 종료하고 작업 중단을 기록한다. DB 장애는 오류 종료하여 supervisor가 재시작할 수 있다. 인증/사용량 제한은 해당 작업을 FAILED로 남기고 Worker를 정상 종료해 자동 재시작으로 요청을 반복하지 않는다. 로그인/사용량 문제를 해결한 뒤 jobs retry와 service restart를 수행한다.

## 상태·재시도·취소

우선순위는 critical > high > normal > low이고, 실행 중 작업은 선점하지 않는다. 기본 최대 3회 시도다.

- network/execution/timeout/interrupted: 남은 시도가 있으면 30초, 60초… 지수 백오프(최대 1시간)로 PENDING. 최종 timeout은 TIMEOUT, 다른 실패는 FAILED.
- authentication/rate_limit/invalid_output/configuration: 자동 재시도하지 않음. 인증/한도 오류는 상시 Worker도 멈춤.
- busy: 실제 AI 시도로 계산하지 않고 5초 후 다시 대기.
- stale_input: CANCELLED. 원래 입력을 바꾸지 않고 새 기간 입력을 준비·submit해야 함.

```sh
uv run aiops --env-file .env jobs retry JOB_UUID
uv run aiops --env-file .env jobs cancel JOB_UUID
```

retry는 FAILED/TIMEOUT/CANCELLED 작업에 명시적으로 최대 3회 기회를 추가한다(총 시도 상한 100). 입력 버전은 동일하다. stale_input에는 retry 대신 해당 기간을 다시 submit한다. cancel은 PENDING만 취소하며 실행 중 작업을 임의로 끊지 않는다.

`worker --once`는 대기 작업이 없으면 idle/0, 완료하면 0, 최종 실패면 오류 코드로 종료한다. PENDING 재시도 예약과 CANCELLED는 상태를 출력하고 0으로 반환한다. DB 오류의 종료 코드는 **11**이다. prepare의 데이터 불완전 종료 코드 10은 기존과 같으며 큐 등록 성공이나 Worker의 작업 처리 성공과 구분한다.

## 잠금·복구 보장 범위

PostgreSQL 세션 advisory lock으로 Worker는 1개만 시작된다. 작업 선택은 FOR UPDATE SKIP LOCKED, 상태·시도·보고서 결과는 트랜잭션으로 함께 확정한다. Worker를 다시 시작하면 이전 RUNNING을 interrupted로 회수해 남은 시도 내에서 재처리한다. 소유 Worker ID가 바뀐 오래된 실행은 결과를 덮어쓸 수 없다.

실제 Codex 호출도 별도 DB advisory lock과 기존 파일 잠금을 함께 사용하므로 DB를 설정한 단발 analyze와 Worker가 직렬화된다. DB가 없던 로컬 모드까지 같은 호스트에서 혼합하면 공통 AIOPS_DATA_DIR의 파일 잠금이 필요하다. 지원 구성은 하나의 호스트·하나의 데이터 경로다. PostgreSQL에는 직접 연결하며 session advisory lock을 유지하지 못하는 transaction-pooling 연결은 사용하지 않는다. AIOps 밖에서 직접 실행한 codex 명령은 이 큐가 통제하지 않는다.

외부 AI 호출과 DB 커밋을 하나의 분산 트랜잭션으로 묶을 수는 없다. 호출 성공 직후 DB 저장 전 프로세스가 강제 종료되면 재시도에서 AI 호출이 다시 발생할 수 있다. 결과 행은 중복 방지하지만 사용량의 정확히 한 번 실행은 보장하지 않는다. 서비스는 control-group 종료를 사용하고, 수동 실행을 SIGKILL한 경우 남은 Codex 프로세스를 확인한 뒤 재시작한다. 원문 로그·서비스·운영 DB 설정에 대한 AI 자동 조치는 하지 않는다.

DB 결과가 원본이고 aiops_data/reports의 JSON은 출력 뷰다. DB 저장 후 파일 출력만 실패한 경우 report list로 재출력할 수 있다. SQLite로 자동 되돌아가지 않으며, DB를 끄려면 Worker/producer를 멈춘 뒤 데이터 이력을 점검하고 명시적으로 설정을 변경한다. 보고서 SQLite와 PostgreSQL의 양방향 동기화는 지원하지 않는다.

## 백업과 되돌리기

Worker와 작업 등록을 멈춘 뒤 PostgreSQL의 public.aiops_* 테이블과 aiops_data 전체를 함께 백업한다. 예: DB 계정 접속을 설정한 환경에서 `pg_dump -p "$PGPORT" -d "$PGDATABASE" -t 'public.aiops_*' -Fc -f aiops-backup.dump`. 비밀번호는 명령행 인자로 넣지 않는다. 체크포인트 SQLite만 삭제하거나 오래된 DB 백업과 새 파일을 임의로 섞지 않는다.

[001_rollback.sql](../src/aiops/storage/sql/001_rollback.sql)은 **해당 테이블의 데이터도 삭제하는 수동 복구 SQL**이다. Worker 중지·백업·검토 후에만 psql로 실행한다. 자동 rollback 명령은 제공하지 않는다. CASCADE를 쓰지 않으므로 외부 FK가 있으면 트랜잭션이 실패하며 기존 애플리케이션 테이블을 함께 삭제하지 않는다.

## 테스트와 공식 근거

기본 pytest는 PostgreSQL을 호출하지 않는다. 로컬 실제 PostgreSQL 17의 initdb/pg_ctl로 임시 Unix socket 전용 테스트 클러스터를 만들 때만 다음을 실행한다.

```sh
AIOPS_RUN_POSTGRES_TESTS=1 AIOPS_POSTGRES_BIN=/path/to/postgresql17/bin uv run pytest -q
```

테스트는 생성한 aiops_test DB만 사용하고 종료 후 클러스터를 중지·제거한다. root로 initdb를 실행하지 않는다. 가짜 AI Provider로 검증하며 Codex 호출은 하지 않는다.

잠금·트랜잭션 설계는 [PostgreSQL 17 잠금 문서](https://www.postgresql.org/docs/17/explicit-locking.html)와 [Psycopg 트랜잭션 문서](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)를 기준으로 했다. 장시간 AI 호출 중 SQL 트랜잭션을 열어두지 않으며 session lock만 유지한다. Airflow DAG·FastAPI·프론트의 실제 연결은 이 큐를 사용해 이후 구현한다.

## 일일 심화 조사

마이그레이션 3부터 별도 조사 큐를 지원한다. 기존 일일 보고서를 변경하지 않고 같은 단일 Worker 잠금 아래 현재 관측과 대응 가이드를 저장한다. 배포·Airflow 호출은 [INVESTIGATION.md](INVESTIGATION.md)를 따른다.
