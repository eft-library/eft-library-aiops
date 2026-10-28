# Phase 3 PostgreSQL·단일 Worker 로컬 검증

검증일: 2026-10-28 (Asia/Seoul).

- 실제 PostgreSQL **17.11** 임시 클러스터를 사용했다. Unix socket만 열고 테스트 세션 종료 후 중지·제거했다. 상시 PostgreSQL 서비스를 시작하지 않았다.
- 로컬 검증용 PostgreSQL 실행 파일 설치 과정에서 Homebrew 기본 클러스터가 생성됐지만 테스트에는 별도 임시 클러스터를 사용했다. 운영 서버에는 접속하지 않았다.
- `AIOPS_RUN_POSTGRES_TESTS=1 uv run pytest -q`: **169 passed, 1 skipped**. PostgreSQL 검증 25개와 기존/추가 기본 테스트 144개 통과. 실제 Codex opt-in 테스트 1개는 제외했다.
- 모든 AI 처리는 가짜 Provider 또는 프로세스 실행 대역을 사용했다. 실제 ChatGPT/Codex 분석 호출은 하지 않았다.
- `uv sync --locked`, `uv build`, 수정 코드의 Ruff 기본 오류·import 검사, `git diff --check` 통과.
- wheel에 초기 마이그레이션과 수동 rollback SQL이 포함됨을 확인했다.

## 실제 DB에서 확인한 항목

- 전용 스키마·7개 테이블 생성, 같은 버전 재적용, 일반 DB 소유자(비슈퍼유저)로 마이그레이션.
- 입력 기간·version·batch 참조 저장과 같은 fingerprint 재사용.
- 같은 report의 중복 작업 방지, critical 우선 처리, 완료 결과 재사용.
- 두 번째 Worker 거부 및 단발 Provider/Worker 공통 Codex DB 잠금.
- 실제 Provider 경로를 실행 대역으로 통과시켜 DB gate와 JSON 결과 저장 확인. DB URL·PGPASSWORD가 Codex 환경에 전달되지 않음을 확인.
- 네트워크 지수 백오프, 시도 이력, 인증/사용량 제한의 자동 재시도 금지, 최종 TIMEOUT과 수동 재시도.
- 이전 RUNNING 복구, 오래된 소유자의 결과 덮어쓰기 거부, stale_input 취소 및 대기 작업 취소.
- 완료 저장 중 DB trigger로 실패를 주입해 작업·시도·보고서 모두 RUNNING으로 롤백됨을 확인한 뒤 정상 완료 확인.
- 잘못된 근거의 성공 저장 거부, 작업 등록 당시 모델 고정.
- 읽기 전용 SQLite 원본 보존, daily/weekly/monthly의 deferred FK 계보 이전, 반복 이전의 멱등성.
- 미분석 배치 메타데이터 등록·재등록 방지. 대량 원본 로그는 DB 미적재.
- DB 상태 조회가 실행 중 작업을 중단 상태로 바꾸지 않음을 확인.
- rollback이 public의 별도 테이블을 보존함을 확인. 누락된 dependency 참조도 DB 제약으로 거부.
- 연결 실패 시 DSN·서버 오류 원문을 출력하지 않고 DB 오류 코드 11 반환. print-sql은 접속하지 않음.

## 운영 검증과 남은 범위

전용 DB/계정·비밀번호, 기존 PostgreSQL 포트 13245 접속, 스키마 적용은 운영자가 [POSTGRES_WORKER.md](POSTGRES_WORKER.md)를 따라 수행해야 한다. Codex 로그인·모델 접근·systemd PATH·자원 사용량은 운영 환경에서 worker --once로 검증한다. 원본 서비스 설정·DB의 다른 테이블·방화벽은 변경하지 않았다.

상시 서비스 등록·Airflow DAG·수집/보고 스케줄·자동 연쇄 재분석·FastAPI 9023·프론트 4003은 아직 설치하지 않았다. 외부 AI 성공과 DB 커밋 사이 강제 종료 시 AI 요청이 재실행될 수 있어 사용량의 정확히 한 번 처리를 보장하지 않는다. DB 결과 중복과 오래된 소유자의 덮어쓰기는 차단한다.


## public 스키마 전환 검증

사용자 요청에 따라 기본 PostgreSQL 저장소를 public.aiops_* 테이블로 전환했다. 마이그레이션 버전은 2다. 새 DB 생성과 기존 aiops 스키마의 데이터 보존 이전을 모두 실제 임시 PostgreSQL에서 검증했다. 기존 public.reports 및 같은 일반 이름의 identity sequence/index가 있어도 보존됐고, public.aiops_* 대상이 이미 있으면 병합/덮어쓰기 없이 전체 이전을 중단했다. 별도 aiops 객체가 남아 있으면 해당 스키마를 삭제하지 않았다.

전체 테스트 172 passed, 1 skipped. SQL·코드 변경을 운영 DB에 직접 적용하지 않았다. 사용자가 다시 만든 운영 DB에는 최신 db init으로 public 테이블을 바로 생성한다.

## Docker Airflow → 호스트 API

- FastAPI의 인증 필수 HTTP API를 추가했다. 기본 포트는 9023이며 상시 Worker 대신 `/v1/worker/run-once`가 동기적으로 한 건을 처리한다. HTTP와 CLI는 같은 DB Worker 잠금을 공유한다.
- 실제 임시 PostgreSQL + 가짜 Provider로 HTTP 접수·중복 요청 재사용·한 건 실행·결과 조회·빈 큐·잠금 충돌·수집 후 DB 등록·데이터 없음 오류를 검증했다. API 시작 조건과 DB 오류의 접속 정보 미노출도 검증했다.
- API는 동기 호출이다. HTTP timeout이 작업 취소나 실패를 의미하지 않으므로 job_id로 완료를 확인한다. Airflow DAG와 스케줄은 별도 프로젝트에서 관리하며, 하위 보고서 누락은 기존 데이터 품질 계약으로 표시한다.
- 배포 설정과 HTTP 계약은 [AIRFLOW_API.md](AIRFLOW_API.md)를 따른다. 운영 Docker에서 호스트 9023까지의 실제 네트워크 연결·인증·Codex 호출은 별도 운영 검증 대상이다.

## 조회 전용 웹 검증

- 전체 opt-in PostgreSQL 테스트 포함 **183 passed, 1 skipped**. 실제 Codex 호출은 하지 않았다.
- 목록의 최신 버전 선택·이전 수정본·기간/상태/위험도 필터·페이지 범위·상세 근거·빈 결과를 임시 PostgreSQL에서 검증했다. 조회 연결은 DB read-only 모드로 강제했다.
- 웹 프록시의 GET 허용 목록, POST 거부, API 키의 클라이언트 미노출, API 인증 오류의 안전한 표시를 검증했다.
- 임시 PostgreSQL과 가짜 Provider를 연결한 로컬 서버에서 Chromium으로 데스크톱/모바일 화면, 보고서 필터·상세·근거 펼치기, 수집·작업 조회, 빈 결과·없는 보고서·연결 실패를 확인했다. 브라우저에서는 GET 요청만 발생했다. 검증용 데이터는 임시 경로에만 생성했고 제품에는 샘플 주입 기능이 없다.
- 실제 웹 프로세스의 시작·상태 확인·재시작·종료, API와 분리된 PID·날짜별 로그를 검증했다. wheel 빌드에 HTML/CSS/JS/SVG 네 파일이 포함됨을 확인했다.
- 운영 4003 접근·실제 운영 보고서 표시와 기존 네트워크 접근 정책은 운영 반영 후 확인한다. 웹 자체의 사용자 로그인 기능은 현재 범위에 포함하지 않는다.

## 일일 점검의 장애·성능 분석 확장

- 전체 opt-in PostgreSQL 테스트 **191 passed, 1 skipped**. 실제 Codex는 호출하지 않았다.
- 일일 입력 계약 3, 단일 분석 호출 안의 장애/성능 구역, 구역별 근거 검증, 지표·수집 근거 부족 시 정상 판정 거부, 이전 JSON 호환을 검증했다.
- 반복 프록시 경고가 많아도 다른 서비스의 심각한 오류와 성능 지표가 입력 제한에서 함께 보존되는지 검증했다. 높은 심각도 및 지연 표본의 대표 이벤트 선정과 월간 크기 제한에서 모든 일일 ID·구역 상태의 보존도 검증했다.
- 임시 PostgreSQL + 가짜 Provider의 Worker 실행 → JSON 저장 → HTTP 조회와 상위 보고서 요약 전달을 확인했다.
- 로컬 Chromium으로 장애·성능 구역, 판단 근거 부족 상태, 근거 펼치기, 이전 형식 보고서 안내와 모바일 너비를 확인했다. 패키지 빌드도 통과했다.

## Worker SSE 진행 메시지

- 전체 PostgreSQL opt-in 검증 포함 203 passed, 1 skipped. 실제 Codex 모델은 호출하지 않았으며 설치 CLI 0.165.0의 버전과 공식 JSONL 이벤트 계약을 확인했다.
- 실시간 subprocess stdout 수신, 큰 stdin/stderr의 동시 처리, 출력 상한, 타임아웃/취소 시 자식 프로세스 그룹 정리를 검증했다.
- 내부 reasoning·부분 delta·도구 인수와 출력 제외, API 키/DB 비밀번호/민감 패턴 마스킹, 완료 전 보고서 후보의 비노출을 검증했다.
- 임시 PostgreSQL + 가짜 Provider에서 SSE 인증, 기존 CLI/HTTP Worker와의 단일 실행 잠금, 최종 상태와 JSON 저장, 인증 실패·네트워크 재시도 상태를 확인했다.
- 실제 로컬 Uvicorn/HTTP 스트림과 가짜 Codex 실행 파일로 완료 전 공개 메시지 전달, heartbeat, 동시 호출 409, 연결 종료 시 프로세스 종료와 interrupted/PENDING 기록을 검증했다.
- 전송 버퍼 128개 상한과 최종 이벤트 보존·생략 개수를 검증했다. 수신 계약은 [WORKER_STREAM.md](WORKER_STREAM.md)를 따른다. 별도 Airflow 프로젝트의 실제 로그 연결은 운영 연동에서 확인한다.
