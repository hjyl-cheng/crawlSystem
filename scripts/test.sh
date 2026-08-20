#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${ROOT_DIR}/.venv/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  echo "missing ${PYTHON_BIN}; run ./scripts/setup-dev.sh first" >&2
  exit 1
fi

(
  cd "${ROOT_DIR}/services/qybullmq"
  YTDLP_PYTHON_BIN="${PYTHON_BIN}" \
    FINGERPRINT_PYTHON_BIN="${PYTHON_BIN}" \
    npm test
)
npm --prefix "${ROOT_DIR}/services/dashboard" test
npm --prefix "${ROOT_DIR}/services/auth" test
npm --prefix "${ROOT_DIR}/services/feature-dispatch" test
(
  cd "${ROOT_DIR}/services/local-agent"
  "${PYTHON_BIN}" -m pytest -q
)
(
  cd "${ROOT_DIR}/services/feature-engine"
  "${PYTHON_BIN}" -m pytest -q
)

if command -v go >/dev/null 2>&1; then
  (cd "${ROOT_DIR}/services/rota/core" && go test ./...)
elif command -v docker >/dev/null 2>&1; then
  docker run --rm \
    --name "qy-pachongsys-go-test-$$" \
    -v "${ROOT_DIR}/services/rota/core:/src:ro" \
    -v qy-pachongsys-go-mod-cache:/go/pkg/mod \
    -w /src \
    golang:1.25.3-alpine \
    go test ./...
else
  echo "Go or Docker is required for Rota tests" >&2
  exit 1
fi

echo "all pachongsys tests passed"
