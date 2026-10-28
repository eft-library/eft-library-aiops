# PostgreSQL 저장소와 단일 Worker — 서버 적용 안내

운영 서버에서는 아직 이 문서의 DB 생성·스키마 적용·Worker 시작을 수행하지 않았다. 현재 구현은 로컬의 실제 PostgreSQL 17과 가짜 AI Provider로 검증했다. 기존 서비스 테이블·설정·방화벽을 바꾸지 않는다.

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

백엔드 HTTP 9023·향후 프론트 4003은 별도다. 이번 Worker는 HTTP 포트를 열지 않으며 기존 PostgreSQL 13245에 접속한다. FastAPI·Airflow DAG·자동 스케줄은 아직 설치하지 않는다.

## 1. 서버에서 전용 DB와 접속 계정 준비

기본 제안은 기존 PostgreSQL 서버 안의 **DB eft_library_aiops / 역할 aiops_app**이다. PostgreSQL 관리자로 한 번 실행한다. 운영체제 root와 PostgreSQL 로그인 역할은 별개다.

```sh
# Rocky/PGDG 설치 경로 예시. 실제 psql 경로가 다르면 조정한다.
runuser -u postgres -- /usr/pgsql-17/bin/psql -p 13245 -d postgres \
  < /home/aiops/eft-library-aiops/deploy/create_database.sql
```

이 SQL은 `aiops_app` 역할을 만들고, `\password`로 비밀번호를 두 번 입력받은 뒤 전용 DB를 만든다. 비밀번호를 SQL 파일이나 채팅에 넣지 않는다. 역할·DB가 이미 있으면 스크립트를 반복 적용하지 말고 해당 생성 줄을 생략해 검토한다. 기존 운영 DB를 사용하기로 했다면 이후 연결값을 바꾸고 aiops 전용 스키마를 쓰되, 기존 public 테이블은 그대로 둔다.

## 2. .env 설정과 의존성 동기화

기존 모델·데이터 경로 값은 유지하고 다음을 추가한다.

```dotenv
AIOPS_DATABASE_URL="host=127.0.0.1 port=13245 dbname=eft_library_aiops user=aiops_app"
PGPASSWORD="위에서_설정한_DB_비밀번호"
```

비밀번호는 libpq의 PGPASSWORD로 읽는다. URL 안에 넣지 않아 URL 인코딩이 필요하지 않다. 비밀번호를 별도 파일에 두려면 libpq의 PGPASSFILE을 사용할 수 있으며 해당 파일은 600으로 제한하고 Git에서 제외한다. AIOPS_DATABASE_URL도 password 필드를 포함할 수 있으므로 프로그램은 값과 서버 오류 원문을 출력하지 않는다. Codex 자식 프로세스에는 DB URL·PGPASSWORD·PGPASSFILE을 전달하지 않는다.

```sh
cd /home/aiops/eft-library-aiops
chmod 600 .env
uv sync --locked
```

aiops_data의 폴더 755·파일 644 정책과 인증 설정 파일의 권한은 별개다. root는 기존과 동일하게 Codex 로그인한 계정을 사용한다.

## 3. 테이블 SQL 검토·적용·연결 확인

전체 SQL은 [001_initial.sql](../src/aiops/storage/sql/001_initial.sql)이다. 설치된 패키지에서도 출력할 수 있다.

```sh
# 출력만 한다. DB 연결·변경 없음
uv run aiops db print-sql

# 선택된 DB에 aiops 전용 스키마와 테이블 적용 (이 명령에서만 명시적으로 DDL 수행)
uv run aiops --env-file .env db init

# 연결·서버 버전·마이그레이션 확인
uv run aiops --env-file .env db doctor
```

실행 시 자동 마이그레이션은 없다. init은 동일 버전 재적용이 가능하며 기존 애플리케이션 테이블을 변경하지 않는다. aiops라는 이름을 다른 용도로 이미 쓰고 있다면 적용 전에 확인한다. DB doctor 성공은 Codex 인증까지 검증한 것이 아니므로 기존 `aiops doctor`도 실행할 수 있다.

| 테이블 | 내용 |
|---|---|
| aiops.schema_migrations | 적용 버전 |
| aiops.batches | 배치 ID·관측 기간·상대 파일 경로·coverage·totals. 원문 로그 미저장 |
| aiops.reports | 종류·달력 기간·시간대의 정체성 |
| aiops.report_versions | revision·fingerprint·입력·manifest·분석 결과·데이터 완전성 |
| aiops.report_dependencies | 입력 배치와 하위 daily/weekly_context 연결 |
| aiops.analysis_jobs | 우선순위·대기/실행 상태·재시도 시각·소유 Worker |
| aiops.job_attempts | 시도별 시작/종료·오류 코드·Provider job/Thread ID |

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

## 6. 상시 Worker

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

예시는 root·기존 경로를 기준으로 하며 다른 계정 전환을 요구하지 않는다. KillMode=control-group을 유지해 서비스 종료 시 Codex 자식도 정리한다. SIGTERM/CTRL-C는 현재 프로세스를 종료하고 작업 중단을 기록한다. DB 장애는 오류 종료하여 supervisor가 재시작할 수 있다. 인증/사용량 제한은 해당 작업을 FAILED로 남기고 Worker를 정상 종료해 자동 재시작으로 요청을 반복하지 않는다. 로그인/사용량 문제를 해결한 뒤 jobs retry와 service restart를 수행한다.

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

Worker와 작업 등록을 멈춘 뒤 PostgreSQL의 aiops 스키마와 aiops_data 전체를 함께 백업한다. 예: DB 계정 접속을 설정한 환경에서 `pg_dump -p 13245 -d eft_library_aiops -n aiops -Fc -f aiops-backup.dump`. 비밀번호는 명령행 인자로 넣지 않는다. 체크포인트 SQLite만 삭제하거나 오래된 DB 백업과 새 파일을 임의로 섞지 않는다.

[001_rollback.sql](../src/aiops/storage/sql/001_rollback.sql)은 **해당 테이블의 데이터도 삭제하는 수동 복구 SQL**이다. Worker 중지·백업·검토 후에만 psql로 실행한다. 자동 rollback 명령은 제공하지 않는다. CASCADE를 쓰지 않으므로 외부 FK가 있으면 트랜잭션이 실패하며 기존 애플리케이션 테이블을 함께 삭제하지 않는다.

## 테스트와 공식 근거

기본 pytest는 PostgreSQL을 호출하지 않는다. 로컬 실제 PostgreSQL 17의 initdb/pg_ctl로 임시 Unix socket 전용 테스트 클러스터를 만들 때만 다음을 실행한다.

```sh
AIOPS_RUN_POSTGRES_TESTS=1 AIOPS_POSTGRES_BIN=/path/to/postgresql17/bin uv run pytest -q
```

테스트는 생성한 aiops_test DB만 사용하고 종료 후 클러스터를 중지·제거한다. root로 initdb를 실행하지 않는다. 가짜 AI Provider로 검증하며 Codex 호출은 하지 않는다.

잠금·트랜잭션 설계는 [PostgreSQL 17 잠금 문서](https://www.postgresql.org/docs/17/explicit-locking.html)와 [Psycopg 트랜잭션 문서](https://www.psycopg.org/psycopg3/docs/basic/transactions.html)를 기준으로 했다. 장시간 AI 호출 중 SQL 트랜잭션을 열어두지 않으며 session lock만 유지한다. Airflow DAG·FastAPI·프론트의 실제 연결은 이 큐를 사용해 이후 구현한다.
