# Docker Airflow → AIOps HTTP API

운영 기준은 **Airflow 3.1.5(Docker) → 호스트의 AIOps API(9023)**다. Airflow 컨테이너에는 프로젝트·로그·Codex 인증을 마운트하지 않는다. AIOps는 기존 root 계정으로 호스트에서 실행하며 같은 aiops_data와 public.aiops_*를 사용한다. 스키마 변경은 없다.

```text
Docker Airflow ── HTTP + API key ──> 호스트 AIOps :9023
                                      ├─ 수집 → 파일 + DB 배치 정보
                                      ├─ 보고서 요청 → PostgreSQL 큐
                                      └─ Worker 1회 호출 → Codex → DB 결과
```

상시 실행하는 것은 API 서버다. Worker는 HTTP 요청이 올 때 한 건을 처리하고 끝난다. 이미 실행 중인 Worker가 있으면 DB 잠금으로 중복 실행을 막는다. systemd Worker를 별도로 등록할 필요는 없다.

## 호스트에서 최초 준비

최신 코드를 반영한 뒤 `uv sync --locked`를 실행한다. 기존 DB·모델·데이터 경로 설정은 유지하고 `.env`에 다음 값을 추가한다.

```dotenv
AIOPS_SOURCES_CONFIG=config/log_sources.yaml
AIOPS_API_TOKEN=직접_생성한_비밀키
```

비밀키는 `uv run python -c 'import secrets; print(secrets.token_urlsafe(32))'`로 생성할 수 있다. 채팅이나 Git에 넣지 않는다. DB를 이미 초기화했다면 다시 만들 필요 없다. 초기 준비는 `./scripts/init.sh`로 실행한다. 의존성 반영 → DB 테이블 초기화 → 기존 보고서 이전 → 배치 등록 → DB 연결 확인을 처리하며, 반복 실행할 수 있다. DB나 원본 데이터 파일을 삭제하지 않는다. API 실행과 수집·Codex 분석은 하지 않는다.

API 시작:

```sh
./restart.sh
```

기본 포트는 9023이다. `./restart.sh`는 의존성 반영 후 API와 조회 웹을 순서대로 재시작한다. API는 인증된 상태 조회에서 DB 연결까지 확인한다. API만 재시작하려면 `./scripts/restart-api.sh`를 사용한다. 로그는 한국 시간 기준 log_history/api-YYYY-MM-DD.log에 날짜별로 저장된다. 실행 중 자정이 지나면 다음 로그부터 새 날짜 파일에 기록하며 같은 날 재시작은 이어서 기록한다. log_history/는 Git에서 제외한다. `./restart.sh status`는 API·웹 전체 상태 확인, `./restart.sh stop`은 전체 종료다. PID와 잠금은 AIOPS_DATA_DIR/run에 저장한다. .env의 AIOPS_API_HOST/AIOPS_API_PORT로 주소와 포트를 변경할 수 있다(기본 0.0.0.0:9023).

수동 실행한 API나 다른 프로세스는 임의로 종료하지 않는다. 기존 요청이 30초 안에 끝나지 않으면 강제 종료 없이 재시작을 중단한다. 시스템 재부팅 후 자동 시작이나 장애 시 자동 재시작 기능은 포함하지 않는다.

## 외부 호출 프로젝트와의 경계

Airflow DAG·Connection·Docker 네트워크·스케줄은 별도 Airflow 프로젝트에서 관리한다. 이 저장소에는 AIOps API 서버와 호출 계약만 둔다. 외부 호출자는 호스트의 9023 포트에 접근 가능한 주소와 `X-AIOps-Key` 인증값을 설정하면 된다. 프로젝트·로그·Codex 인증 볼륨 공유는 필요 없다.

일일 기본 대상은 한국 시간 기준 전날, 주간은 지난주, 월간은 지난달이다. 재실행 시 같은 기간을 유지하려면 date를 명시한다. daily는 해당 날짜, weekly는 해당 날짜가 속한 주, monthly는 해당 날짜가 속한 달이다. 마감되지 않은 기간은 거부된다.

상위 보고서 요청 전 필요한 일일 보고서의 완료를 확인한다. 누락·미완료 하위 보고서가 있으면 보고서에 불완전성이 표시된다.

## 일일·주간·월간 분석 요청

세 종류 모두 같은 `POST /v1/reports`를 사용하며 body의 type으로 구분한다. 접수 후 Worker 한 건 실행과 job_id 완료 확인 절차도 동일하다.

| type | 기본 분석 기간 (한국 시간) | 분석 근거 | 외부 스케줄 예시 |
|---|---|---|---|
| daily | 전날 00:00부터 다음 날 00:00 직전 | 해당 날짜 로그·지표 | 매일 |
| weekly | 지난주 월요일 00:00부터 이번 주 월요일 00:00 직전 | 지난주 일일 보고서 7개 | 월요일, 필요한 일일 분석 완료 후 |
| monthly | 지난달 1일 00:00부터 이번 달 1일 00:00 직전 | 지난달 일일 보고서 전체; 주간 보고서는 보조 문맥 | 매월 1일, 필요한 일일 분석 완료 후 |

```json
{"type": "weekly"}
```

```json
{"type": "monthly"}
```

주간은 원본 로그를 다시 분석하지 않고 일일 결과를 통합한다. 월간도 일일 결과가 기본이며, 주간 결과의 건수를 다시 더하지 않는다. 완료된 유효 일일 보고서가 하나도 없으면 접수를 거부한다. 일부 날짜만 있으면 접수할 수 있지만 missing_dates/incomplete_daily_reports에 누락·불완전성을 표시한다. 현재 진행 중인 주·월은 분석 대상이 아니다.

재시도·지연 실행에서는 대상 기간에 속한 date를 고정한다. 예를 들어 `{"type":"weekly","date":"2026-10-26"}`은 10월 26일~11월 2일 직전, `{"type":"monthly","date":"2026-10-01"}`은 10월 전체이며 각각 해당 기간 종료 이후에만 접수할 수 있다. DAG 구현과 활성화는 외부 Airflow 프로젝트에서 수행한다.

## HTTP 계약

| 요청 | 의미 |
|---|---|
| GET /v1/reports | 보고서 목록. type/status/severity/start/end/page/page_size/history 필터. 기본 기간별 최신 수정본 |
| GET /v1/collections | 수집 배치 목록. page/page_size |
| GET /v1/jobs | 작업 목록. status/page/page_size |
| GET /v1/status | DB·수집·최근 작업 현황 |
| POST /v1/collect `{}` | 설정된 전체 소스 수집. `{"services":["fastapi"]}`로 선택 가능 |
| POST /v1/reports `{"type":"daily","date":"2026-10-27"}` | 입력 준비·큐 등록. date 생략 시 마지막 마감 기간. HTTP 202는 분석 접수 |
| POST /v1/worker/run-once | 동기적으로 작업 한 건 처리. 실제 Codex 호출 지점 |
| GET /v1/jobs/{job_id} | 접수한 특정 작업의 상태·오류 확인 |
| GET /v1/reports/{report_id} | 보고서 상태와 분석 결과. 결과 본문은 result.report |

Worker API는 분석이 끝날 때 응답한다. 호출자는 기본 Codex 제한 600초보다 충분히 긴 HTTP 제한 시간을 설정한다. AIOPS_TIMEOUT_SECONDS를 늘리면 호출자의 제한도 함께 조정한다. `idle`은 대기 작업 없음, `PENDING`은 내부 재시도 예약이다. HTTP 200이라도 본문의 작업 상태가 FAILED/TIMEOUT/CANCELLED이면 분석 성공이 아니다. 호출자는 특정 job_id를 조회해 COMPLETED인지 확인한다.

DB 잠금 충돌은 HTTP 409이며 다음 Worker 호출에서 다시 처리한다. 데이터 없음은 422/no_report_input, DB 장애는 503이다. 수집 파일 저장 후 DB 등록 실패도 503이며 파일은 유지되고 다음 수집에서 자동 등록한다. 부분 수집은 200/status=partial이며 누락은 coverage에 남는다.

분석의 재시도·백오프는 기존 PostgreSQL 큐가 관리한다. 호출자가 Worker API를 다시 호출하면 실행 가능한 작업 한 건을 처리한다. 인증·한도 오류는 자동 재시도하지 않는다. HTTP 연결이 끊겨도 호스트 작업이 이미 진행 중일 수 있으므로 특정 job_id를 확인한다. 새 요청이 이미 완료한 보고서를 다시 분석하지는 않지만 프로세스 강제 종료 구간의 외부 AI 호출은 정확히 한 번을 보장하지 않는다.

API 재시작 후에도 작업 큐·분석 결과는 DB에 남는다. 중단된 RUNNING 작업은 다음 Worker 호출 때 복구된다. API 내부에 별도의 메모리 작업 큐를 만들지 않는다.

공식 근거: [FastAPI API key 보안](https://fastapi.tiangolo.com/reference/security/).

## 조회 웹

조회 전용 웹은 별도 4003 포트에서 실행한다. `./scripts/restart-web.sh`로 시작하며 기존 API와 같은 DB를 API를 통해 조회한다. 브라우저에는 API 키를 전달하지 않고 웹 서버가 GET 허용 목록으로만 대리 조회한다. [WEB.md](WEB.md)를 참고한다.
