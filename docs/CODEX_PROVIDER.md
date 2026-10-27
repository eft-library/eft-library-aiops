# Codex Provider 결정 — Phase 1

2026-10-28 기준 공식 문서와 로컬 설치 코드를 확인했다.

## 공식 SDK 검증

[공식 SDK 문서](https://learn.chatgpt.com/docs/codex-sdk)에 따르면 Python SDK는 안정 릴리스이며 설치명은 `openai-codex`, import는 `openai_codex`이다. Python 3.10 이상에서 로컬 app-server JSON-RPC를 사용한다. 배포 패키지에는 고정된 CLI 런타임이 포함되며 `CodexConfig(codex_bin=...)`으로 설치된 실행 파일을 지정할 수 있다.

로컬에서 `openai-codex==0.163.0`을 임시 설치하고 실제 시그니처를 확인했다.

- `Codex(CodexConfig(...))`
- `codex.thread_start(sandbox=Sandbox.read_only, approval_mode=ApprovalMode.deny_all, ...)`
- `thread.run(input, output_schema=...)`
- `thread.id`, `result.final_response`, `result.error`
- `ExternalMessage` 입력, 비동기 API와 스레드 재개 API 지원

SDK가 없거나 읽기 전용을 지원하지 않는다는 이유로 제외한 것은 아니다. 최종 프로젝트에서는 사용하지 않는 SDK 및 동봉 런타임 의존성을 제거했다.

## 이번 구현의 선택: CLI subprocess

사용자가 로컬·운영에 설치하고 ChatGPT 로그인한 CLI를 그대로 사용한다. [공식 비대화형 실행 문서](https://learn.chatgpt.com/docs/non-interactive-mode)의 `codex exec`는 기존 인증 재사용, JSONL 이벤트, JSON Schema 출력, 임시 세션과 명시적 샌드박스를 지원한다.

Phase 1에서는 `exec`의 `--ignore-user-config`와 `--ignore-rules`로 분석에 불필요한 개인 설정·MCP 및 규칙을 제외하고, POSIX 프로세스 그룹 전체의 수명을 직접 관리하는 단순한 어댑터를 선택했다. 로컬 CLI `0.165.0`의 `app-server`에는 해당 ignore 옵션이 없음을 확인했다. SDK로도 별도 설정 격리 설계가 가능하지만 현재 요구사항에는 CLI 방식이 더 작다.

호출은 shell 없이 인자 배열로 수행한다. `--sandbox read-only`, `--ask-for-approval never`, `--ephemeral`, `web_search="disabled"`, `project_doc_max_bytes=0`을 명시한다. 별도 임시 작업 디렉터리에서 실행하고 JSON 스냅샷을 stdin으로 전달한다. 원문은 프로세스 인자나 프로젝트 파일에 저장하지 않는다. JSONL의 Thread ID와 최종 agent message를 읽고 Pydantic으로 보고서를 검증한다. 진행 이벤트는 현재 내부 파싱에만 사용하며 외부 스트리밍 API는 제공하지 않는다.

Provider 인터페이스는 실행 방식과 분리되어 있으므로 향후 SDK 어댑터로 바꿀 수 있다. 임시 세션의 Thread ID는 식별용이며 세션 재개나 작업 복구를 보장하지 않는다.

## 인증·비용

[공식 인증 문서](https://learn.chatgpt.com/docs/auth)에 따르면 ChatGPT 로그인은 구독 접근을 사용하고 API 키 로그인은 OpenAI Platform의 API 요율로 청구된다. `codex exec`는 저장된 CLI 인증을 재사용한다. SDK도 로컬 Codex 런타임을 제어하지만 이번에는 SDK를 통한 실제 인증 재사용 호출까지 검증하지 않았으며, CLI 방식의 실제 샘플 분석을 검증했다.

플랜별 잔여 사용량을 추정하지 않으며 한도 초과를 별도 오류로 반환한다. 자동 로그인·인증 파일 복사·API 키 전환·무한 재시도는 하지 않는다.
