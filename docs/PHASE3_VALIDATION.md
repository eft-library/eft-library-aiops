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
