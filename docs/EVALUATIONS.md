# 분석 결과 평가

보고서 상세의 **분석 평가**에서 발견 사항을 펼쳐 판정(맞음·일부 맞음·틀림·아직 미확인), 확인된 원인·결과, 확인 근거, 제안의 유용성을 저장합니다. 주요 발견 사항, 장애·성능 분석, 최근 완료된 심화 조사 10건의 발견 사항을 지원합니다.

평가는 보고서 버전에 귀속됩니다. 다른 보고서 버전으로 자동 복사하지 않습니다. 평가자는 웹 로그인 계정이며, 수정마다 새 revision과 당시 발견 사항의 사본을 저장합니다. 화면에는 항목별 최근 20개 이력을 표시하며 과거 이력은 DB에 계속 보존합니다. 동시 수정은 HTTP 409로 거절하므로 입력 내용을 보관한 뒤 새로고침합니다. 미확인은 오답으로 집계하지 않습니다. 이 입력은 AI 자동 재학습이나 원본 보고서 수정에 사용되지 않습니다.

## 운영 적용

새 환경 변수는 없습니다. 기존 AIOPS_DATABASE_URL과 웹 계정을 사용합니다. 기존 DB에 마이그레이션 1~3이 적용된 경우 다음 파일만 실행하면 됩니다.

`src/aiops/storage/sql/004_evaluations.sql`

기존 마이그레이션을 포함한 멱등 초기화 명령으로 적용할 수도 있습니다.

```sh
uv run aiops --env-file .env db init
```

마이그레이션 적용 후 API와 웹 프로세스를 재시작합니다. 기존 보고서를 재생성하거나 로그 인덱스를 다시 만들 필요는 없습니다. 테이블은 `public.aiops_finding_evaluations` 하나이며, 마이그레이션 버전은 4입니다. 운영 DB에는 개발 작업 중 자동 적용하지 않습니다.

## API

- `GET /v1/reports/{report_id}/evaluations`: 발견 사항과 최신 평가·최근 이력.
- `POST /v1/reports/{report_id}/evaluations`: `finding_key`, `expected_revision`(첫 저장 0), `verdict`, `actual_result`, `evidence`, `usefulness`, `reviewer`.
- 웹은 로그인 후 `/write/reports/{report_id}/evaluations`로 저장합니다. 로그인 계정으로 reviewer를 지정하고, 세션·동일 출처·전용 헤더를 검사합니다. API 키는 브라우저에 전달하지 않습니다.

완료된 보고서와 실제 존재하는 발견 사항만 평가할 수 있습니다. 판정은 `correct`, `partial`, `incorrect`, `unconfirmed`, 유용성은 `helpful`, `neutral`, `unhelpful` 또는 null입니다. 원인·결과와 확인 근거는 각각 최대 4,000자입니다. 자격증명이나 개인정보 원문을 평가에 입력하지 마세요.

이번 단계의 범위는 운영자 평가 기록입니다. API 오류율·지연 기반 이상 탐지 및 자동 분석 요청은 후속 단계이며 상관관계 분석은 제외합니다.
