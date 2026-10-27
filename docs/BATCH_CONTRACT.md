# 분석 입력 계약 초안 v1

Phase 1은 입력을 UTF-8 JSON으로 파싱하고 크기만 검증한다. 아래 필드 전체의 스키마 강제와 정규화·마스킹은 Phase 2에서 구현한다. 예시는 `config/sample.json`이다.

| 필드 | 의미 |
|---|---|
| `schema_version` | 계약 버전; 초기 1 |
| `batch_id` | 영속화된 수집 배치 식별자; 재분석에도 동일 |
| `time_range.start/end` | 타임존을 포함한 ISO 8601 시간 범위 |
| `masked` | 민감정보 마스킹 수행 여부 |
| `coverage` | 서비스별 수집 상태; 정상과 미수집을 구분 |
| `error_counts` | 서비스·수준별 오류 집계 |
| `events` | 대표 로그; `event_id`, 서비스, 수준, 메시지 및 이후 timestamp/source 등 |
| `metrics` | `metric_id`, 지표명, 측정값; Phase 2에서 단위·측정 시각·대상 보강 |
| `collection_failures` | 서비스별 실패·누락 사유 |

Phase 2에서는 event/metric ID를 배치 내 유일하게 생성하고 재분석 가능한 JSON/JSONL을 영속화한다. Phase 3에서 근거 ID가 배치에 존재하는지 검증하고 작업·배치 식별자, 커버리지, 입력 크기 제한을 강제한다. Phase 1의 보고서 검증은 구조 검증이며 원인 정확성·근거 ID 실재 여부를 자동 보장하지 않는다.
