#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:-}"
IMAGE_TAG="${2:-}"
if [[ -z "${ENVIRONMENT}" || -z "${IMAGE_TAG}" ]]; then
  echo "usage: $0 <environment> <immutable-image-tag> [compose build arguments...]" >&2
  exit 2
fi
shift 2

if [[ "${IMAGE_TAG}" == "latest" || "${IMAGE_TAG}" == "local" ]]; then
  echo "use an immutable image tag, not ${IMAGE_TAG}" >&2
  exit 1
fi

if ! git -C "${ROOT_DIR}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "image builds must run from the committed pachongsys Git repository" >&2
  exit 1
fi

SOURCE_STATUS="$(git -C "${ROOT_DIR}" status --short --untracked-files=all)"
if [[ -n "${SOURCE_STATUS}" ]]; then
  echo "refusing to build from an uncommitted pachongsys worktree:" >&2
  echo "${SOURCE_STATUS}" >&2
  exit 1
fi

QY_VCS_REF="$(git -C "${ROOT_DIR}" rev-parse HEAD)"
QY_VCS_SHORT="$(git -C "${ROOT_DIR}" rev-parse --short=7 HEAD)"
EXPECTED_TAG_PREFIX="pachongsys-${QY_VCS_SHORT}"
if [[ "${IMAGE_TAG}" != "${EXPECTED_TAG_PREFIX}" && "${IMAGE_TAG}" != "${EXPECTED_TAG_PREFIX}-"* ]]; then
  echo "image tag must be ${EXPECTED_TAG_PREFIX} or ${EXPECTED_TAG_PREFIX}-<variant>" >&2
  exit 1
fi

export QY_IMAGE_TAG="${IMAGE_TAG}"
export QY_BUILD_VERSION="${IMAGE_TAG}"
export QY_VCS_REF
export QY_BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
export QY_SOURCE_URL="${QY_SOURCE_URL:-local:pachongsys}"

# One representative Compose role per image. Building every role would export
# the same large QYBullMQ image many times under one tag.
BUILD_SERVICES=(
  qybullmq-api
  rota-core
  rota-dashboard
  feature-ingest
  feature-dispatch
  dashboard
  auth
)

exec "${ROOT_DIR}/scripts/compose.sh" "${ENVIRONMENT}" build "$@" "${BUILD_SERVICES[@]}"
