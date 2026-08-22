#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

for module in qybullmq dashboard auth feature-dispatch; do
  npm --prefix "${ROOT_DIR}/services/${module}" ci
done

python3 -m venv "${ROOT_DIR}/.venv"
"${ROOT_DIR}/.venv/bin/pip" install \
  -r "${ROOT_DIR}/services/qybullmq/requirements.txt" \
  -e "${ROOT_DIR}/services/local-agent[dev]" \
  -e "${ROOT_DIR}/services/feature-engine[postgres]" \
  pytest

echo "development dependencies are ready"
