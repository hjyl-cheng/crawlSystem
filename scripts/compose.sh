#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:-}"

if [[ -z "${ENVIRONMENT}" || ! "${ENVIRONMENT}" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "usage: $0 <environment> <docker compose arguments...>" >&2
  exit 2
fi
shift

if (( $# == 0 )); then
  echo "docker compose arguments are required" >&2
  exit 2
fi

RUNTIME_ROOT="${QY_RUNTIME_DIR:-${ROOT_DIR}/runtime/${ENVIRONMENT}}"
ENV_FILE="${RUNTIME_ROOT}/env/runtime.env"
if [[ ! -f "${ENV_FILE}" ]]; then
  echo "runtime environment is missing: ${ENV_FILE}" >&2
  echo "run ./scripts/bootstrap.sh ${ENVIRONMENT} first" >&2
  exit 1
fi

export QY_RUNTIME_ROOT="${RUNTIME_ROOT}"
exec docker compose \
  --env-file "${ENV_FILE}" \
  -f "${ROOT_DIR}/deploy/compose.yml" \
  "$@"
