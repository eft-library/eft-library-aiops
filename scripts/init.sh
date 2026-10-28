#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

if [[ ! -f .env ]]; then
  echo '.env 파일에 기존 DB 접속 정보와 데이터 경로를 설정해 주세요.' >&2
  exit 2
fi

uv sync --locked

echo 'DB 테이블 초기화 및 기존 보고서·수집 배치 등록'
.venv/bin/aiops --env-file .env setup

echo 'DB 연결 및 초기화 상태 확인'
.venv/bin/aiops --env-file .env db doctor

echo '초기 준비 완료. API·웹 시작: ./restart.sh'
