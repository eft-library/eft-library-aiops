# Phase 2 로컬 검증 결과

검증일: 2026-10-28 (Asia/Seoul). 운영 서버 접속, 운영 로그 읽기, Codex 호출, PostgreSQL 접속·변경은 수행하지 않았다.

## 구현

- 설정 기반 FileCollector·DirectoryCollector·SystemCollector와 기본 비활성 DockerCollector.
- YAML fixture/운영 프로필, 서비스별 선택과 증분 시간 필터, 제한된 최초 tail·명시적 backfill.
- SQLite 체크포인트, rename/create·truncate·같은 inode 덮어쓰기 감지, 청크 읽기와 크기·줄·파일·탐색 한도.
- 다중행 정규화, 파싱/인코딩 실패 표시, 규칙 기반 마스킹, 오류 그룹·HTTP 4xx/5xx·명시적 응답 시간 집계.
- 배치 JSON Schema, 선별된 snapshot.json과 마스킹된 events.jsonl, 원자적 체크포인트·게시 및 중단 복구.
- 공유 CollectionService와 CLI. 호스트 Collector를 Airflow에서 호출하는 연결 경계·저장소·보관/백업 정책 문서화.

## 검증

- `uv sync --locked`: 성공.
- `uv run pytest -q`: **94 passed, 1 skipped**. Phase 1 기본 테스트 36개와 Phase 2 테스트 58개 통과. 실제 Codex opt-in 테스트는 실행하지 않음.
- `uv build`: sdist·wheel 빌드 성공.
- 새 코드의 Ruff 기본 오류·import 검사와 `git diff --check`: 통과.
- fixture 전량 첫 수집 **11개 이벤트**, 같은 체크포인트로 재실행 **0개 이벤트**.
- NPM fixture 집계: **4xx 1건, 5xx 1건**, 응답 시간 **250ms** 1개 샘플.
- fixture 파싱 실패 0건, 생성된 스냅샷 약 11KB로 기본 90KB 상한 이내.
- 원본 fixture 파일 SHA-256이 수집 전후 동일함을 확인.
- fixture 비밀번호·쿼리 토큰·IP가 영속 이벤트 파일에 남지 않음을 확인.
- Docker daemon 없이 SDK 대역으로 상태·메모리·CPU 계산을 검증.
- 배치 staging 후 트랜잭션 전 중단, 트랜잭션 내부 실패, 체크포인트 커밋 후 게시 전 중단을 각각 주입해 재수집/롤백/게시 복구 확인.
- 회전 파일의 미수집 끝부분과 새 active 파일, truncate, 주간 파일명 재사용, 같은 크기 중간 덮어쓰기, 미완성 줄·과대 줄·줄 수 한도·읽기량 한도 이후 진행을 검증.

## 운영 확인 대기

실제 서비스 로그 형식과 경로·회전 주기·루트 계정 접근, 스캔 규모와 처리 자원은 운영에서 확인해야 한다. 현재 운영 root 실행 조건을 지원하며 별도 계정 전환을 선행 조건으로 요구하지 않는다. 마스킹은 규칙 기반이고 압축 파일 수집·Docker stdout·DAG 배포·API/웹·분석 작업 큐·자동 보관 정리는 포함하지 않는다. 세부 한계와 명령은 [COLLECTION.md](COLLECTION.md)를 참고한다.
