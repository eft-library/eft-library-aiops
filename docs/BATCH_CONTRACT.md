# 분석 입력 계약 v1 — Phase 2

실제 모델은 `src/aiops/collectors/models.py`의 `Batch`, JSON Schema는 [config/batch.schema.json](../config/batch.schema.json)이다. Collector는 Pydantic 모델로 배치를 생성하고 영속화한다. 현재 Phase 1 `analyze` 입력 검사는 JSON 파싱·크기 제한이며 전체 배치 계약과 근거 참조 검증은 Phase 3에서 소비 시 강제한다. 기존 `config/sample.json`은 Phase 1의 간단한 예시이고 완전한 Phase 2 스키마 예시는 `collect`로 생성한다.

| 필드 | 의미 |
|---|---|
| `schema_version` | 현재 1 |
| `batch_id` | 배치 고유 ID `batch-<uuid>`; 저장된 배치를 재분석할 때 유지 |
| `time_range.start/end` | 포함된 이벤트·지표 시각의 최소/최대. 파싱된 시각이 전혀 없으면 수집 실행 시각 사용. 타임존 포함 |
| `collected_at` | 배치 수집 완료 시각(UTC) |
| `masked` | 마스킹 규칙을 적용했음을 의미하는 true; 비밀이 완전히 없음을 보증하지 않음 |
| `coverage` | 소스별 status(`collected/partial/failed/disabled`), 파일 수·payload 읽기 바이트·이벤트 수·경고 |
| `error_counts` | 서비스별 WARNING/ERROR/FATAL/CRITICAL 건수. 빈 집계는 coverage와 함께 해석 |
| `http_summary` | 서비스별 상태 코드 건수, 4xx/5xx, 명시적 단위가 있는 응답 시간의 평균·최댓값·샘플 수 |
| `error_groups` | 서비스·수준·숫자를 정규화한 메시지로 묶은 상위 반복 오류 그룹. count·대표 메시지·근거 ID 포함 |
| `events` | 서비스별 오류와 전후 문맥을 선별한 대표 이벤트. 크기 제한으로 일부 생략 가능 |
| `metrics` | 시스템·선택적 Docker 관측값. ID·측정 시각·지표명·값·단위·대상 포함 |
| `collection_failures` | 소스별 오류·누락·한도 도달·보류. 오류 원문 대신 안전한 reason 코드와 마스킹된 경로 |
| `totals` | 선별 전 이벤트 수·대표 이벤트 수·payload 읽기량·파싱 실패 수 |
| `limitations` | 마스킹·시간 필터·범위·스냅샷 생략에 대한 제한 |

이벤트는 `event_id`, 타임존을 포함한 `timestamp` 또는 null, `service`, `source`(`file/directory`), `level`, `message`, 마스킹된 `file`, `metadata`를 가진다. 시각 파싱 실패를 현재 시각으로 꾸미지 않는다. 타임존 없는 ISO 시각에는 소스 YAML의 `timezone`을 적용한다. 지원하지 않는 형식은 null과 `parse_failed`로 보존한다.

이벤트 metadata는 `parse_failed`, `encoding_error`, 선택적 `message_truncated`, `http_status`, `response_time_ms`를 포함한다. NPM의 알려진 proxy-host 2/3/4 파일에는 domain도 연결한다. 일반 duration에 단위가 없으면 응답 시간을 추정하지 않는다. Nginx의 `request_time`은 초로 해석한다.

ID 형식은 `<batch_id>:evt-<n>`, `<batch_id>:metric-<n>`이고 배치 내 유일하다. `error_groups.evidence_ids`는 같은 디렉터리 `events.jsonl`의 이벤트에 연결된다. 모든 이벤트가 대표 `events`에도 포함되지는 않는다. 분석자가 본 입력 범위와 전체 배치 근거를 구분하고, Phase 3에서 근거 참조가 실재하는지 검증해야 한다.

`events.jsonl`은 수집·정규화·마스킹 후 시간 필터를 통과한 전체 이벤트다. 스냅샷은 기본 90,000바이트 이하로 제한한다. 정상 로그만 있는 배치, 새 로그가 없는 배치, 로그 수집이 실패하고 시스템 지표만 남은 배치도 유효하다. '오류 없음'은 `coverage`가 실제 수집 성공일 때만 해석한다.

파일 체크포인트와 배치 저장은 [COLLECTION.md](COLLECTION.md)의 내구성 프로토콜을 따른다. 분석 결과·작업 상태는 이 계약과 별도로 Phase 3에서 저장하며, Codex 실패가 수집 체크포인트를 되돌리지 않는다.
