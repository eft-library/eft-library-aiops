# Worker 진행 메시지 스트리밍

기존 `POST /v1/worker/run-once`는 JSON 응답을 그대로 유지한다. 실시간 진행 로그가 필요한 호출자는 `POST /v1/worker/run-once/stream`을 사용한다. 인증은 기존 `X-AIOps-Key`이며 별도 환경변수·DB 마이그레이션은 없다. 운영 반영 후 `./restart.sh`로 적용한다.

이 저장소에서는 AIOps API만 구현한다. Airflow DAG와 `run_until_completed`의 HTTP 수신 코드는 별도 Airflow 프로젝트에서 변경한다. 기존 일일/주간/월간 보고서 접수 요청은 그대로다.

## 처리 흐름

```text
Airflow POST (stream=True)
  → AIOps가 DB Worker 잠금 확보
  → 대기 작업 한 건 선택
  → Codex JSONL을 줄 단위로 읽음
  → 허용된 공개 메시지·실행 상태를 SSE로 전달
  → 보고서 형식·근거 검증 및 DB 저장
  → worker.finished로 최종 작업 상태 전달
```

Codex의 `--json`이 실행 중 JSONL 이벤트를 출력하는 기능을 사용한다. [공식 OpenAI 문서](https://learn.chatgpt.com/docs/non-interactive-mode)에 이벤트 종류와 사용량 예시가 설명되어 있다. 설치된 로컬 CLI는 0.165.0이며 실제 AI 호출 없이 가짜 CLI로 스트리밍을 검증했다.

전달하는 것은 **완료된 공개 agent_message**와 허용된 실행 이벤트뿐이다. reasoning/analysis 본문, 원본 JSONL 전체, 입력 스냅샷, stderr, 도구 인수·명령·출력은 전달하지 않는다. 모델이 중간 공개 메시지를 만들지 않고 최종 JSON만 출력할 수도 있다. 이 경우 실행 상태와 heartbeat만 보이다가 최종 요약이 전달된다. 내부 사고 과정이나 글자 단위 응답을 제공하는 기능은 아니다.

기존 분석 한 번의 출력을 전달하므로 추가 Codex 호출은 없다. heartbeat도 애플리케이션이 생성하는 연결 유지 신호이며 AI 토큰을 사용하지 않는다. 메시지에 포함될 수 있는 알려진 인증값·DB 비밀번호와 일반적인 민감정보 패턴을 마스킹한다. 임의의 민감정보 전체를 제거한다는 보장은 아니므로 공개 메시지도 기존 운영 로그 접근 범위에서 취급한다.

## SSE 형식

Content-Type은 text/event-stream이다. 각 프레임의 data는 한 줄 JSON이며 줄바꿈은 JSON escape로 표현한다.

```text
event: codex.message
data: {"event":"codex.message","message":"공개 진행 메시지","job_id":"...","sequence":4,"timestamp":"2026-10-29T00:00:00+00:00"}

event: worker.finished
data: {"event":"worker.finished","job":{"job_id":"...","report_id":"...","status":"COMPLETED"},"dropped_events":0,"sequence":8,"timestamp":"..."}

```

예시의 공개 문장은 실제 Codex가 출력할 때만 존재한다. AIOps가 임의의 분석 단계 문장을 만들어 Codex 응답으로 내보내지 않는다.

| event | 의미 |
|---|---|
| worker.started | 단일 Worker 잠금 확보 |
| worker.claimed | 실제 선택한 job_id/report_id/attempt_count |
| codex.thread.started | Codex thread 시작 |
| codex.turn.started | Codex turn 시작 |
| codex.message | 완료된 공개 응답 메시지. 부분 delta는 전달하지 않음 |
| codex.activity | 허용된 도구/계획 항목의 종류와 상태만. 인수와 출력 없음 |
| codex.report_candidate | 구조화된 보고서 응답 수신. 검증·저장 전이므로 완료가 아님 |
| codex.turn.completed | 모델 실행 종료와 제공된 input_tokens/cached_input_tokens/output_tokens/reasoning_output_tokens 숫자. 작업 완료와 별개 |
| codex.error | CLI가 출력한 오류의 분류 코드. 이후 DB 저장 결과까지 확인 |
| worker.validating | 보고서와 근거 검증 단계 |
| worker.report | 검증·저장된 보고서의 마스킹된 summary/severity |
| heartbeat | 10초간 다른 전달 이벤트가 없을 때 연결 유지. elapsed_seconds/job_id 포함 |
| worker.finished | DB 결과 저장·Worker 잠금 해제 후의 최종 작업 정보. 대기 작업이 없으면 job.status=idle |
| error | 스트림 시작 후 발생한 API/DB/실행 오류의 안전한 분류 코드. 완료로 간주하지 않음 |

HTTP 200만으로 분석 성공을 판단하지 않는다. worker.finished.job.status가 COMPLETED여야 해당 작업의 분석 성공이다. PENDING이면 DB 재시도 시각을 기다려 다음 Worker 호출에서 처리한다. FAILED/TIMEOUT/CANCELLED는 해당 상태로 처리한다. 특정 보고서를 기다리는 Airflow 코드는 실제 처리된 job_id가 목표 작업과 다를 수 있으므로 **기존 GET /v1/jobs/{target_job_id} 확인도 유지**한다.

## Airflow 호출부에서 사용할 수신 함수 예시

다음은 DAG 파일이 아니라 외부 프로젝트에서 참고할 수신 함수다. `api_url`, `api_key`는 기존 Airflow Connection에서 가져오고 `log`는 태스크 logger를 전달한다.

```python
import json
import requests


def run_worker_stream(api_url, api_key, log):
    final_job = None
    with requests.post(
        api_url.rstrip("/") + "/v1/worker/run-once/stream",
        headers={"X-AIOps-Key": api_key},
        stream=True,
        timeout=(10, 45),  # connect/read; heartbeat가 무응답 시간을 끊어 줌
    ) as response:
        response.raise_for_status()
        # 작은 이벤트가 기본 버퍼에 묶이지 않도록 한 줄을 즉시 받는다.
        for line in response.iter_lines(chunk_size=1, decode_unicode=True):
            if not line or not line.startswith("data: "):
                continue
            event = json.loads(line[6:])
            name = event["event"]
            if name == "codex.message":
                log.info("Codex: %s", event["message"])
            elif name == "worker.report":
                log.info("분석 요약 [%s]: %s", event["severity"], event["summary"])
            elif name == "codex.turn.completed":
                log.info("Codex 제공 토큰 사용량: %s", event.get("usage", {}))
            elif name == "heartbeat":
                log.info("AIOps 연결 유지: job=%s, 경과=%ss",
                         event.get("job_id"), event["elapsed_seconds"])
            elif name == "worker.finished":
                final_job = event["job"]
                log.info("작업 결과: %s", final_job)
                if event.get("dropped_events"):
                    log.warning("느린 수신으로 진행 이벤트 %s개가 생략됨",
                                event["dropped_events"])
            elif name == "error":
                raise RuntimeError("AIOps stream error: " + event["error"])
            else:
                log.info("AIOps event: %s", event)
    if final_job is None:
        raise RuntimeError("최종 작업 상태를 받지 못했습니다. 목표 job_id를 조회하세요.")
    return final_job
```

Airflow 전체 태스크 제한은 기존 Codex 제한 600초와 DB 검증·저장 여유보다 길게 설정한다. 요청 라이브러리의 stream=True와 스트리밍 줄 수신이 모두 필요하다. 응답을 먼저 response.json()/response.text로 다 읽으면 실시간 로그가 아니다. 프록시를 거친다면 스트리밍 응답이 버퍼링되지 않아야 한다. API는 X-Accel-Buffering: no와 Cache-Control: no-cache, no-transform을 보낸다.

## 실패·연결 종료·복구

- 인증 실패는 스트림 시작 전 HTTP 401, 다른 Worker가 실행 중이면 409다. DB 준비 실패도 가능한 경우 응답 헤더를 보내기 전에 기존 오류 코드로 반환한다.
- 스트림 시작 후의 실패는 error 또는 worker.finished의 상태로 전달한다. codex.turn.completed나 보고서 후보 수신을 분석 성공으로 취급하지 않는다.
- 클라이언트 연결 종료 시 실행 중인 Codex 프로세스 그룹을 정리하고 interrupted를 DB에 기록한다. 재시도 여력이 있으면 PENDING으로 되돌린다. 이미 완료·저장된 작업은 COMPLETED일 수 있으므로 목표 job_id를 확인한다.
- 진행 메시지는 DB에 저장하지 않고 응답에서만 전달한다. Airflow가 log.info로 남긴 내용이 Airflow 로그가 된다. 기존 API access log에는 요청 자체만 남는다.
- 전송 버퍼는 최대 128개 이벤트다. 느린 클라이언트 때문에 넘치면 오래된 진행 이벤트를 생략하고 마지막 worker.finished에 dropped_events를 기록한다. 최종 상태는 DB를 기준으로 확인한다.
- 자동 재연결/Last-Event-ID 재생은 지원하지 않는다. 새 POST는 기존 실행에 접속하는 것이 아니라 대기 작업 한 건을 처리하는 요청이다. 끊김 후 즉시 같은 POST를 반복하기 전에 목표 job_id를 확인한다.
- 강제 프로세스 종료와 외부 AI 호출 사이의 정확히 한 번 실행은 기존과 마찬가지로 보장하지 않는다. 기존 단일 Worker·소유권 검증·시도 제한·재시도 백오프는 유지한다.
