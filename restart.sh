#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")"
action="${1:-restart}"
case "$action" in
  restart)
    uv sync --locked
    export AIOPS_SKIP_SYNC=1
    echo '=== API ==='
    ./scripts/restart-api.sh restart
    echo '=== WEB ==='
    ./scripts/restart-web.sh restart
    echo 'API와 웹 재시작 완료'
    ;;
  status|stop)
    result=0
    # On shutdown stop the viewer before its upstream API.
    if [[ "$action" == stop ]]; then
      echo '=== WEB ==='
      ./scripts/restart-web.sh "$action" || result=1
      echo '=== API ==='
      ./scripts/restart-api.sh "$action" || result=1
    else
      echo '=== API ==='
      ./scripts/restart-api.sh "$action" || result=1
      echo '=== WEB ==='
      ./scripts/restart-web.sh "$action" || result=1
    fi
    exit "$result"
    ;;
  *) echo '사용법: ./restart.sh [restart|stop|status]' >&2; exit 2 ;;
esac
