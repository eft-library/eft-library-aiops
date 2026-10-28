#!/usr/bin/env bash
set -euo pipefail
cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
export AIOPS_PROCESS_KIND=api
exec ./scripts/manage-service.sh "$@"
