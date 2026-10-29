# Phase 1 로컬 검증 결과

검증일: 2026-10-28 (Asia/Seoul). 운영 서버에는 접속하거나 변경하지 않았다.

## 구현 범위

Python 패키지와 CLI, 환경변수·명시적 dotenv 설정, Codex CLI Provider, JSON Schema/Pydantic 결과 검증, 공통 파일 잠금, 프로세스 그룹 타임아웃, 민감정보 없는 실행 메타데이터를 구현했다. 운영 수집·DB·Airflow는 구현하지 않았다.

## 확인 결과

- Python 3.12.13, uv 0.11.14, Codex CLI 0.165.0, 기존 ChatGPT 로그인 사용.
- `uv sync`: 성공.
- `uv run pytest -q`: **36 passed, 1 skipped**. 실제 호출 테스트는 기본 제외.
- `AIOPS_RUN_INTEGRATION=1 uv run pytest -q -m integration`: **1 passed, 36 deselected**. 실제 샘플 분석과 결과 검증 성공.
- `uv run aiops doctor`: 설치·로컬 인증 점검 성공.
- `uv build`: sdist 및 wheel 빌드 성공.
- 타임아웃 테스트에서 자식 프로세스를 실제 생성하고, 종료 후 예약된 파일 쓰기가 발생하지 않음을 확인.
- 별도 Python 프로세스의 동일 잠금 획득을 차단하고, 해제 후 재획득 확인.

## 실제 샘플 분석 예

명령: `uv run aiops analyze --input config/sample.json`

- 작업 ID: 검증용 식별자 생략
- Thread ID: 검증용 식별자 생략
- 심각도: `WARNING`
- 요약: “관측 구간에서 FastAPI 데이터베이스 연결 시간 초과 오류가 2건 확인되었습니다. PostgreSQL 로그가 수집되지 않아 원인은 확인할 수 없습니다.”
- 근거: 샘플의 `evt-1`, `evt-2`.
- 제한: DB 로그, 요청 수·지연 시간·사용자 영향 및 지표 추이가 없어 원인과 영향 확정 불가.
- `automatic_changes_performed`: `false`.

분석 결과는 비결정적이므로 동일 입력에서도 표현이 달라질 수 있다. 샘플 오류는 실제 서비스 장애를 의미하지 않는다.

## 남은 검증

Rocky Linux의 파일 접근 권한·샌드박스 동작·자원 제한, 운영 CLI 버전 호환성은 배포 후 확인한다. 현재 디렉터리 제한은 읽기 접근 격리를 보장하지 않으며 OS 권한 격리가 필요하다. 자동 마스킹, 배치 계약 강제, 근거 ID 검증, 영속 작업 큐·복구는 Phase 2~3 범위다. 세부 내용은 SECURITY.md를 참고한다.
