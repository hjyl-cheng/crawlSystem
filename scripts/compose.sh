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
DEPLOYMENT_MODE="${QY_DEPLOYMENT_MODE:-}"
if [[ -z "${DEPLOYMENT_MODE}" ]]; then
  DEPLOYMENT_MODE="$(awk -F= '$1 == "QY_DEPLOYMENT_MODE" { print $2; exit }' "${ENV_FILE}")"
fi
DEPLOYMENT_MODE="${DEPLOYMENT_MODE:-bundled}"

COMPOSE_FILES=(-f "${ROOT_DIR}/deploy/compose.yml")
case "${DEPLOYMENT_MODE}" in
  fresh-migration)
    COMPOSE_FILES+=(-f "${ROOT_DIR}/deploy/compose.fresh-migration.yml")
    ;;
  bundled)
    ;;
  shared-qy)
    COMPOSE_FILES+=(-f "${ROOT_DIR}/deploy/compose.shared-qy.yml")
    ;;
  shared-qy-workers)
    COMPOSE_FILES+=(
      -f "${ROOT_DIR}/deploy/compose.shared-qy.yml"
      -f "${ROOT_DIR}/deploy/compose.shared-qy-workers.yml"
    )
    ;;
  *)
    echo "unsupported QY_DEPLOYMENT_MODE: ${DEPLOYMENT_MODE}" >&2
    exit 1
    ;;
esac

exec docker compose \
  --env-file "${ENV_FILE}" \
  "${COMPOSE_FILES[@]}" \
  "$@"
