# Phase 2 수집 실행·저장·복구

`collect`는 Python으로 로그와 시스템 지표를 수집한다. **Codex 실행·외부 AI 전송·PostgreSQL 접속은 하지 않는다.** 수집된 로그의 명령문도 실행하지 않는다. 로컬·운영의 동일한 `CollectionService`를 CLI와 이후 API/단일 Worker에서 재사용한다.

## 실행

프로젝트 루트에서 `uv sync --locked` 후 실행한다. 가상환경 별도 활성화는 필요 없다.

```sh
# 기본: 로컬 fixture 로그 4종 + 실행 호스트의 시스템 지표
uv run aiops collect --all

# fixture 중 FastAPI만 (시스템 지표는 프로필 설정에 따라 함께 수집)
uv run aiops collect --service fastapi

# 운영: root 실행 환경을 기준으로 명시적 운영 프로필 사용
uv run aiops collect --config config/log_sources.yaml --service fastapi
uv run aiops collect --config config/log_sources.yaml --all

# .env를 사용할 때는 전역 옵션을 명령 앞에 지정
uv run aiops --env-file .env collect --config config/log_sources.yaml --all
```

운영 검증은 한 서비스로 시작해 `coverage`, 배치 내용과 마스킹을 확인한 다음 전체로 확장한다. 코드가 서비스·원본 로그·DB 권한이나 설정을 변경하지 않는다. PostgreSQL은 로그 파일만 읽으므로 psql 접속 정보가 필요 없다. 이미 합의한 root 실행을 지원하며, 그만큼 설정에 선언된 수집 경로를 좁게 유지한다.

운영 YAML의 경로가 실제 환경과 다르면 해당 값만 수정한다. YAML 상대 경로는 **YAML 파일이 있는 디렉터리 기준**이다. 기본 fixture 파일은 테스트용 오류이며 실제 장애를 의미하지 않는다. fixture와 운영의 이력을 분리하려면 각 실행에 서로 다른 `AIOPS_DATA_DIR`를 사용한다.

## 옵션과 최초 실행

- `--all`: 프로필에 등록된 로그 소스를 모두 대상으로 한다. `enabled: false` 소스는 제외 상태로 기록한다.
- `--service fastapi`: 해당 로그 소스만 수집한다. 여러 서비스는 `--service fastapi --service postgresql`로 지정한다. 시스템·Docker 수집 여부는 YAML의 각 `enabled`를 따른다.
- 최초 발견 파일은 기본 최근 1MiB부터 읽는다. 시작점이 줄 중간이면 그 줄을 버리고 다음 완전한 줄부터 읽으며 `initial_history_skipped`를 남긴다. fixture 프로필은 0.25MiB다.
- 이후에는 저장된 byte offset부터 읽는다. 변화가 없으면 이벤트 0개인 배치가 만들어질 수 있으며 시스템 지표는 새로 수집한다.
- `--backfill`: **아직 체크포인트가 없는 파일**을 처음부터 읽는다. 기존 체크포인트를 되감지 않는다. 소스·전역 읽기 제한은 그대로 적용한다. 제한에 걸렸다면 같은 명령을 다시 실행해 이어 읽는다.
- 이미 수집한 파일을 처음부터 재수집하려면 별도 `AIOPS_DATA_DIR`로 실행한다. 운영 체크포인트 DB를 삭제하지 않는다.
- `--since 24h` (`30m`, `7d`도 가능): 이번 증분 읽기에서 타임스탬프가 있는 레코드만 시간 필터링한다. 과거 24시간 전체를 찾아 읽는 옵션이 아니다. 시각을 파싱하지 못한 로그는 버리지 않고 표시하며, 필터로 제외된 로그 이후까지 체크포인트가 전진한다. 기존 배치를 소급해서 재생성하지 않는다.

```sh
uv run aiops collect --config config/log_sources.yaml --service fastapi --since 24h
uv run aiops collect --config config/log_sources.yaml --service postgresql --backfill
```

## 출력과 데이터 계약

성공 시 stdout에 `batch_id`, `snapshot`, `events_file`, `coverage`, `totals`, `status`를 출력한다.

```text
$AIOPS_DATA_DIR/collection/
├── collect.lock
├── checkpoints.sqlite3
├── .staging/                       # 게시 전 내구성 저장
└── batches/
    └── batch-<uuid>/
        ├── snapshot.json           # AI/후속 단계용 요약과 대표 로그
        ├── metrics.jsonl            # 전체 관측 지표
        └── events.jsonl            # 이번 수집에서 정규화·마스킹한 전체 이벤트
```

`events.jsonl`의 '전체'는 이번에 읽고 정규화한 이벤트를 의미한다. `--since` 제외, 최초 과거 이력 생략, 과대 줄 폐기, 메시지 길이 제한은 적용된다. 원본 로그를 복사하거나 수정하지 않는다.

스냅샷은 기본 90,000바이트 이내로 제한해 Phase 1 기본 입력 상한보다 작게 만든다. 서비스별 대표 오류와 전후 정상 문맥을 선별하고, 크기 초과 시 문맥·대표 이벤트·오류 그룹·지표 일부를 생략한 사실을 기록한다. 집계는 선별 전 이벤트를 기준으로 한다. 전체 스키마는 [batch.schema.json](../config/batch.schema.json), 설명은 [BATCH_CONTRACT.md](BATCH_CONTRACT.md)다.

| 종료 코드 | 의미 |
|---|---|
| 0 | 선택된 활성 소스 수집 완료 |
| 10 | 배치는 저장했지만 일부 소스 누락·실패·한도 도달·보류 발생 |
| 2 | 설정·입력·파일/SQLite 저장 오류; 성공으로 간주하지 않음 |
| 7 | 같은 데이터 디렉터리의 다른 Collector가 실행 중 |

`status: partial`은 전체 실패를 의미하지 않는다. 가능한 소스는 저장하고 `collection_failures`와 `coverage`에 문제를 기록한다. Airflow에서 종료 코드 10을 재시도할지 경고로 처리할지는 DAG에서 명시한다. 재실행은 이미 확정된 범위를 건너뛰며 이후 새 배치를 만들므로 배치 ID로 추적한다.

## 제한과 회전 처리

- 기본 전역 로그 읽기 한도 20MiB, 소스당 최대 5MiB, 실행 60초, 파일 2,000개, 소스별 탐색 항목 10,000개, 물리적 줄 20,000개다. 바이트·물리적 줄·파일 수·실행 시간은 남은 활성 소스에 나누고, 같은 디렉터리 소스의 파일에도 줄 수를 분배한다. 실행마다 시작 서비스를 순환하므로 전체 한도보다 소스가 많아도 특정 서비스가 항상 뒤로 밀리지 않는다. 원래 전역 상한을 늘리지는 않는다.
- 64KiB 이하 청크로 읽고 한 줄은 기본 64KiB를 넘으면 폐기한다. 긴 줄이 여러 실행에 걸치면 폐기 상태를 체크포인트에 보존해 무한 재독을 피한다. 메시지는 16,384자까지 보존하고 잘림을 표시한다.
- 전역/소스 바이트 한도는 로그 payload 읽기에 적용한다. 파일별 fingerprint·첫 줄 경계 확인을 위한 추가 읽기는 약 2KiB 이내이며 별도 고정 오버헤드다. `bytes_read`는 payload 읽기량이다. 실패 도중의 읽기는 남은 예산을 보수적으로 차감한다.
- 파일 크기는 스냅샷 시작 시 고정하며 실행 중 추가된 내용은 다음 실행에서 읽는다. 마지막 미완성 줄은 다음 실행으로 보류한다. 스택 트레이스는 같은 읽기 창 안에서만 결합한다.
- 체크포인트는 서비스 + device/inode를 키로 경로·offset·크기·mtime·앞부분 및 offset 경계 fingerprint·마지막 성공 시각을 기록한다. 잘림·fingerprint 변경·mtime 역행·동일 크기 재기록을 감지하면 처음부터 다시 읽는다. 단순 touch에도 보수적으로 재독할 수 있다.
- rename/create는 새 active 파일과 이전에 추적한 inode의 회전 파일을 함께 후보로 찾는다. 새 파일의 최초 읽기를 우선하고 미수집분이 있는 추적 파일은 마지막 성공 시각 순으로 순환한다. 단일 파일은 같은 이름 접두사의 형제 파일만, 디렉터리는 설정된 탐색 범위 안에서만 추적한다. `include_rotated: false`여도 **이미 추적한 inode의 나머지 내용**은 배출한다. 처음 보는 `.bak`, `.old`, 번호형 회전 파일은 기본 제외다.
- `include_rotated: true`는 인식 가능한 비압축 회전 파일을 추가로 포함한다. gzip/bz2/xz/zip은 항상 제외한다. 압축 해제 수집은 미구현이다.
- inode·크기·mtime·경계가 모두 같도록 재작성하거나, 두 실행 사이 삭제·교체되어 탐색 범위 밖으로 사라진 파일의 내용을 완전 복구할 수는 없다. 읽는 도중의 교체/잘림은 가능한 경우 실패로 남기고 다음 실행에 재시도한다.
- 디렉터리 심볼릭 링크는 따라가지 않고 파일도 `O_NOFOLLOW`로 연다. 소스 설정 자체와 그 상위 디렉터리는 신뢰하는 운영 설정이다. FIFO·소켓·장치 파일을 읽지 않는다. 넓은 루트·Docker 저장소·Kafka KRaft 데이터와 PostgreSQL 로그 외 데이터 경로는 거부한다.
- 시간 제한은 파일 탐색·청크 사이에 확인한다. 로컬 일반 파일을 대상으로 하며, 커널에서 정지된 디스크 I/O 자체를 강제로 중단하는 제한은 아니다. 영속 저장·fsync 시간은 제한 밖이다. Docker는 별도 프로세스의 강제 타임아웃을 사용한다.

## 탐색 이어가기와 경고 해석

디렉터리 탐색은 현재 상대 경로·마지막 확인한 파일명·남은 상위 경로를 SQLite에 보존한다. 깊이 우선으로 최대 64단계까지 탐색하며 심볼릭 링크는 따라가지 않는다. 한도에 도달하면 다음 실행에서 이어간다. 이미 추적한 파일도 제한된 순환 창으로 별도 재확인하고, 단일 파일 소스의 정확한 active 경로는 디렉터리 순서와 무관하게 직접 확인한다.

탐색 상태와 서비스 순환 위치는 배치·체크포인트와 같은 트랜잭션에서 확정한다. 저장 실패 시 탐색 위치만 앞서 나가지 않는다. 탐색 기준 이름이 삭제됐으면 해당 디렉터리부터 다시 확인하며 `scan_cursor_reset`을 남긴다. 디렉터리 항목 순서 변경·파일 삭제를 완전한 파일시스템 스냅샷처럼 보장하는 것은 아니다.

기존 경고 `scan_limit`은 새 수집부터 아래처럼 구분한다.

| 경고 | 의미 |
|---|---|
| `scan_entry_limit` | 이번 탐색의 새 확인 항목 상한 도달; 위치를 저장하고 다음 실행에 계속 |
| `scan_timeout` | 소스의 탐색 시간 몫 소진; 이어 읽기에 쓸 시간은 별도로 남김 |
| `scan_depth_limit` | 64단계보다 깊은 하위 경로 제외 |
| `scan_cursor_reset` | 이전 탐색 기준 이름이 없어 해당 디렉터리 재탐색 |
| `source_record_limit` / `file_record_limit` | 소스/파일에 배분된 줄 수 한도 도달 |
| `source_file_limit` | 소스에 배분된 파일 수 한도 도달 |
| `source_timeout` | 소스에 배분된 탐색+읽기 시간 소진 |
| `run_limit` | 전체 읽기량·줄 수·파일 수 한도 소진 |

`max_scan_entries`는 새로 처리하는 탐색 항목과 추적 파일 확인의 상한이다. 이전 위치를 찾기 위한 이름 순회는 별도 오버헤드이며 소스별 탐색 시간으로 제한한다. 탐색에는 소스 시간 몫의 최대 40%를 사용해 발견한 파일을 읽을 시간도 남긴다. `partial` 배치가 계속 나오면 일부 한도 도달이 예상되지만, 시간이 지나도 특정 소스가 전혀 진행하지 않는지는 확인해야 한다.

## 시각 없는 로그와 운영 형식

FastAPI 접근 로그의 ` - 0.011s` 같은 끝부분 응답 시간을 밀리초로 변환한다. Next.js의 오류 표시·예외 클래스·스택 꼬리(`digest`, 닫는 중괄호 등)를 같은 오류로 결합한다. Redis 날짜/기호형 수준, PostgreSQL 명시적 UTC/KST와 LOG/NOTICE 등 정보성 수준, Nginx 슬래시 날짜와 ANSI 제어문자도 처리한다. NPM 접근 로그의 단일 상태 코드 및 upstream이 없는 상태 코드도 집계하고 접근 URL 안의 ERROR/WARNING 문자열로 수준을 정하지 않는다. Airflow health의 명시적 [OK]/실패 표시와 구분선은 별도로 인식한다.

Next.js의 `[2026-10-28 10:45:06 +0900]` 접두사와 시각이 추가된 Uvicorn WebSocket/연결 로그를 지원한다. 괄호 안 공백 뒤의 명시적 오프셋을 우선하며, 시각이 매 줄에 붙은 스택도 본문 들여쓰기·예외 꼬리로 결합한다. 원문에 시각이 없는 이전 Next.js stdout·Uvicorn 로그는 시각을 만들어 넣지 않는다. `timestamp=null`을 유지하고 metadata의 `timestamp_status`를 `absent`로 표시한다. 시각으로 보이는 선두 형식이 있지만 해석하지 못하면 `unrecognized`다. 레벨 미인식은 `level_unrecognized`로 별도 표시한다. 배치 totals에도 `timestamp_absent`, `timestamp_unrecognized`, `level_unrecognized` 건수를 추가한다. 기존 `parse_failed`는 이전과 같이 시각 또는 수준 미인식 전체 건수다.

시각 없는 로그를 정확한 일일 보고서에 넣으려면 앞으로 애플리케이션/로그 출력 단계에서 타임스탬프를 기록해야 한다. 파일 mtime이나 수집 시각을 실제 발생 시각으로 간주하지 않는다. 이번 변경은 서비스 설정·실행 스크립트·원본 로그를 바꾸지 않으며 기존 배치도 다시 쓰지 않는다.

## 체크포인트·중단 복구·백업

초기 SQLite를 선택한 이유는 수집만으로 운영 DB 설정이나 네트워크가 필요하지 않기 때문이다. 단일 호스트의 동일 데이터 디렉터리에서 OS 파일 잠금으로 Collector 실행을 직렬화한다. SQLite는 DELETE journal과 `synchronous=FULL`을 사용한다. DB는 새로 만들 때 0644, 디렉터리는 0755이다. 네트워크 파일시스템이나 여러 호스트 공유 DB를 전제로 하지 않는다.

저장 순서는 다음과 같다.

1. `.staging/<batch_id>/`에 마스킹된 이벤트·스냅샷 및 전체 관측 지표 파일을 쓰고 파일·디렉터리를 fsync한다.
2. SQLite 단일 트랜잭션에서 배치 카탈로그와 모든 성공 파일 체크포인트를 함께 확정한다.
3. 디렉터리를 `batches/<batch_id>/`로 원자적으로 이동하고 상위 디렉터리를 fsync한다.

트랜잭션 전 중단이면 체크포인트가 전진하지 않으며, 다음 실행은 미등록 staging을 제거하고 재수집한다. 커밋 후 게시 전 중단이면 다음 실행이 등록된 staging을 게시한 뒤 수집을 계속한다. 후속 소비자는 `batches/`만 읽고 `.staging/`을 읽지 않는다. 등록된 배치가 staging·batches 양쪽에 모두 없으면 체크포인트만 믿고 진행하지 않고 오류를 반환한다.

백업은 Collector를 멈춘 상태에서 **`collection/` 전체**를 일관되게 복사한다. 복구도 배치와 SQLite를 함께 수행한다. DB만 되돌리면 재수집이 발생하고, 배치만 지우면 복구 오류가 발생한다.

현재 보관 정책은 자동 삭제 없이 유지다. Phase 3의 분석 완료 상태를 아직 모르므로 완료 후 자동 삭제도 하지 않는다. 운영에서는 디스크 사용량을 감시하고, 분석 이력·보존 기간과 카탈로그 정리를 함께 처리하는 삭제 기능을 Phase 3 이후 추가한다. 배치 디렉터리만 수동 삭제하지 않는다. 장기 고빈도 운영 전 보관 정책을 확정해야 한다.

## 시스템·Docker

시스템은 psutil로 CPU 0.1초 샘플, RAM·swap, load, 설정된 디스크 사용량을 수집한다. swap 값은 순간 사용량이며 swap-in/out 속도를 추정하지 않는다. CPU·RAM 수치로 장애를 자동 확정하지 않는다. [psutil 문서](https://psutil.readthedocs.io/stable/)를 기준으로 구현했다.

Docker는 기본 비활성이다. 활성화하려면 별도 검토한 실제 컨테이너 이름을 YAML에 명시한다. init 컨테이너는 이름 목록에 넣지 않는다.

```yaml
docker:
  enabled: true
  containers: ['actual-airflow-scheduler', 'actual-npm']
  timeout_seconds: 5
```

Docker SDK는 지정한 컨테이너의 상태와 `stats(stream=False, one_shot=True)`만 조회한다. 이전 CPU 샘플이 없으면 CPU 비율을 꾸며내지 않고 생략한다. 상태가 running이 아니면 상태만 남긴다. 컨테이너 stdout·환경변수·설정 원문·이미지 inspect 결과는 배치에 넣지 않는다. Docker 로그는 파일 로그와의 중복 방지를 위해 현재 수집하지 않는다. [Docker SDK 문서](https://docker-py.readthedocs.io/en/stable/containers.html)를 참고한다.

Docker socket은 높은 권한을 제공한다. 현재 root 실행에서도 프로그램은 조회 API만 사용하며 서비스 시작·중지·재시작을 하지 않는다. 테스트는 실제 daemon 없이 대역으로 검증한다.

## Airflow 연결 설계

Phase 2는 호스트 CLI와 Python 서비스 경계를 제공한다. Docker 내부 Airflow에서 호스트의 전용 실행 스크립트를 SSH로 호출하는 방식을 우선 검토한다. 컨테이너의 localhost가 호스트라는 가정을 하지 않는다. `uv`, 프로젝트, 설정, 데이터 경로는 절대 경로로 지정하고 동일한 `AIOPS_DATA_DIR`를 사용한다. 기본 fixture 프로필이 운영에서 실수로 실행되지 않도록 운영 wrapper에는 `--config`를 고정한다.

stdout에서 배치 ID와 스냅샷 경로만 작업 메타데이터로 전달한다. 호스트 파일 경로를 다른 컨테이너가 바로 읽을 수 있다고 가정하지 않고 후속 단계도 호스트에서 실행하거나 승인된 전용 배치 저장 경로를 공유한다. 전체 호스트 파일시스템이나 Docker socket을 Airflow에 마운트하지 않는다.

일일 입력 통합과 주간·월간 결과 의존 관계는 [REPORTING.md](REPORTING.md)를 따른다. 실제 DAG·SSH 연결·스케줄 활성화·상시 단일 Codex Worker·FastAPI 조회 API는 Phase 3 이후 구현한다. 현재 `collect`는 수집만 수행하므로 Airflow 수집 재시도와 AI 분석 재시도도 분리할 수 있다.
