#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

bash -n \
  scripts/bootstrap.sh \
  scripts/build-images.sh \
  scripts/compose.sh \
  scripts/setup-dev.sh \
  scripts/test.sh \
  ops/adopt-qy-shared-runtime.sh \
  database/init/10-crawler.sh \
  database/init/10-business.sh
python3 scripts/verify_model_bundle.py

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git ls-files | grep -Eq '(^|/)(node_modules|__pycache__|\.pytest_cache|[^/]+\.egg-info)(/|$)'; then
    echo "generated dependency or cache directory is tracked by Git" >&2
    exit 1
  fi
fi

if find . \
  -path './runtime' -prune -o \
  -path './backups' -prune -o \
  -type d \( -name .git -o -name .venv -o -name node_modules \) -prune -o \
  -type f \( -name '.env' -o -name '*.pem' -o -name 'id_rsa' -o -name 'id_ed25519' \) \
  -print -quit | grep -q .; then
  echo "credential-shaped file found outside runtime/backups" >&2
  exit 1
fi

if rg -l --hidden \
  --glob '!runtime/**' \
  --glob '!backups/**' \
  --glob '!.venv/**' \
  --glob '!**/node_modules/**' \
  --glob '!database/reference-snapshots/**' \
  --glob '!scripts/verify.sh' \
  -- '-----BEGIN (RSA |EC |OPENSSH )?PRIVATE KEY-----' . | grep -q .; then
  echo "private key material found in the source tree" >&2
  exit 1
fi

if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  if git ls-files runtime backups | grep -Ev '(^|/)(README\.md|\.gitkeep)$' | grep -q .; then
    echo "runtime or backup material is tracked by Git" >&2
    exit 1
  fi
fi

if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  QY_RUNTIME_ROOT="${ROOT_DIR}/runtime/verify-template" \
    docker compose --env-file .env.example -f deploy/compose.yml config --quiet
  QY_RUNTIME_ROOT="${ROOT_DIR}/runtime/verify-template" \
  QY_DEPLOYMENT_MODE=shared-qy \
  QY_SHARED_BUSINESS_AUDIT_DATABASE_URL_FILE=/tmp/qy-business-audit-dsn \
    docker compose --env-file .env.example \
      -f deploy/compose.yml \
      -f deploy/compose.shared-qy.yml \
      config --quiet
else
  echo "docker compose unavailable; skipped Compose validation" >&2
fi

echo "pachongsys source verification passed"
