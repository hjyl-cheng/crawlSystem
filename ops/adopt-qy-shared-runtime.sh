#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:-}"
if [[ -z "${ENVIRONMENT}" || ! "${ENVIRONMENT}" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "usage: $0 <environment>" >&2
  exit 2
fi

RUNTIME_ROOT="${QY_RUNTIME_DIR:-${ROOT_DIR}/runtime/${ENVIRONMENT}}"
RUNTIME_ENV="${RUNTIME_ROOT}/env/runtime.env"
if [[ ! -f "${RUNTIME_ENV}" ]]; then
  echo "runtime environment is missing: ${RUNTIME_ENV}" >&2
  exit 1
fi

CRAWLER_CONTAINER="${QY_SOURCE_CRAWLER_CONTAINER:-bullmq-crawler-qy-app}"
WORKER_CONTAINER="${QY_SOURCE_WORKER_CONTAINER:-bullmq-crawler-migration-worker-channel-1}"
ROTA_CONTAINER="${QY_SOURCE_ROTA_CONTAINER:-youtube-rota-qy-core}"
BUSINESS_AUDIT_FILE="${QY_SOURCE_BUSINESS_AUDIT_FILE:-/etc/publication/secrets/business-auditor-database-url}"

container_env() {
  local container="$1"
  local key="$2"
  local line
  while IFS= read -r line; do
    if [[ "${line}" == "${key}="* ]]; then
      printf '%s' "${line#*=}"
      return 0
    fi
  done < <(docker inspect "${container}" --format '{{range .Config.Env}}{{println .}}{{end}}')
  echo "missing ${key} in ${container}" >&2
  return 1
}

if [[ ! -r "${BUSINESS_AUDIT_FILE}" ]]; then
  echo "business audit database secret is unreadable: ${BUSINESS_AUDIT_FILE}" >&2
  exit 1
fi

declare -a UPDATE_KEYS=(
  QY_DEPLOYMENT_MODE
  QY_SHARED_CRAWLER_NETWORK
  QY_SHARED_BUSINESS_NETWORK
  QY_SHARED_ROTA_NETWORK
  CRAWLER_DB_HOST
  CRAWLER_DB_PORT
  CRAWLER_DB_USER
  CRAWLER_DB_PASSWORD
  CRAWLER_DB_NAME
  CRAWLER_REDIS_HOST
  CRAWLER_REDIS_PORT
  CRAWLER_S3_ENDPOINT
  MINIO_ROOT_USER
  MINIO_ROOT_PASSWORD
  MINIO_BUCKET
  ROTA_PROXY_BASE_URL
  ROTA_PROXY_CONTROL_URL
  ROTA_PROXY_CONTROL_TOKEN
  ROTA_BULLMQ_PROXY_PASSWORD
  ROTA_WORKLOAD_SCOPE
  QY_NGINX_ROTA_API_UPSTREAM
  QY_NGINX_MINIO_CONSOLE_UPSTREAM
  QY_SHARED_BUSINESS_AUDIT_DATABASE_URL_FILE
  BUSINESS_AUDITOR_ROLE
)

declare -A UPDATES=(
  [QY_DEPLOYMENT_MODE]="shared-qy"
  [QY_SHARED_CRAWLER_NETWORK]="bullmq-crawler"
  [QY_SHARED_BUSINESS_NETWORK]="yewu_business_network"
  [QY_SHARED_ROTA_NETWORK]="youtube-rota-qy-internal"
  [CRAWLER_DB_HOST]="$(container_env "${CRAWLER_CONTAINER}" POSTGRES_HOST)"
  [CRAWLER_DB_PORT]="$(container_env "${CRAWLER_CONTAINER}" POSTGRES_PORT)"
  [CRAWLER_DB_USER]="$(container_env "${CRAWLER_CONTAINER}" POSTGRES_USER)"
  [CRAWLER_DB_PASSWORD]="$(container_env "${CRAWLER_CONTAINER}" POSTGRES_PASSWORD)"
  [CRAWLER_DB_NAME]="$(container_env "${CRAWLER_CONTAINER}" POSTGRES_DB)"
  [CRAWLER_REDIS_HOST]="$(container_env "${CRAWLER_CONTAINER}" REDIS_HOST)"
  [CRAWLER_REDIS_PORT]="$(container_env "${CRAWLER_CONTAINER}" REDIS_PORT)"
  [CRAWLER_S3_ENDPOINT]="$(container_env "${CRAWLER_CONTAINER}" S3_ENDPOINT)"
  [MINIO_ROOT_USER]="$(container_env "${CRAWLER_CONTAINER}" S3_ACCESS_KEY)"
  [MINIO_ROOT_PASSWORD]="$(container_env "${CRAWLER_CONTAINER}" S3_SECRET_KEY)"
  [MINIO_BUCKET]="$(container_env "${CRAWLER_CONTAINER}" S3_BUCKET)"
  [ROTA_PROXY_BASE_URL]="$(container_env "${WORKER_CONTAINER}" ROTA_PROXY_BASE_URL)"
  [ROTA_PROXY_CONTROL_URL]="$(container_env "${WORKER_CONTAINER}" ROTA_PROXY_CONTROL_URL)"
  [ROTA_PROXY_CONTROL_TOKEN]="$(container_env "${WORKER_CONTAINER}" ROTA_PROXY_CONTROL_TOKEN)"
  [ROTA_BULLMQ_PROXY_PASSWORD]="$(container_env "${WORKER_CONTAINER}" ROTA_BULLMQ_PROXY_PASSWORD)"
  [ROTA_WORKLOAD_SCOPE]="$(container_env "${ROTA_CONTAINER}" ROTA_WORKLOAD_SCOPE)"
  [QY_NGINX_ROTA_API_UPSTREAM]="youtube-rota-qy-core:8001"
  [QY_NGINX_MINIO_CONSOLE_UPSTREAM]="bullmq-crawler-qy-minio:9001"
  [QY_SHARED_BUSINESS_AUDIT_DATABASE_URL_FILE]="${BUSINESS_AUDIT_FILE}"
  [BUSINESS_AUDITOR_ROLE]="business_publication_auditor"
)

for key in "${UPDATE_KEYS[@]}"; do
  if [[ -z "${UPDATES[${key}]}" || "${UPDATES[${key}]}" == *$'\n'* ]]; then
    echo "refusing invalid runtime value for ${key}" >&2
    exit 1
  fi
  if [[ ! "${UPDATES[${key}]}" =~ ^[A-Za-z0-9._~:/@+=,-]+$ ]]; then
    echo "runtime value for ${key} is not safe for an unquoted dotenv file" >&2
    exit 1
  fi
done

TEMP_ENV="$(mktemp "${RUNTIME_ENV}.tmp.XXXXXX")"
trap 'rm -f "${TEMP_ENV}"' EXIT
declare -A SEEN=()
while IFS= read -r line || [[ -n "${line}" ]]; do
  key="${line%%=*}"
  if [[ "${key}" =~ ^[A-Z0-9_]+$ && -v "UPDATES[${key}]" ]]; then
    printf '%s=%s\n' "${key}" "${UPDATES[${key}]}" >> "${TEMP_ENV}"
    SEEN["${key}"]=1
  else
    printf '%s\n' "${line}" >> "${TEMP_ENV}"
  fi
done < "${RUNTIME_ENV}"

for key in "${UPDATE_KEYS[@]}"; do
  if [[ ! -v "SEEN[${key}]" ]]; then
    printf '%s=%s\n' "${key}" "${UPDATES[${key}]}" >> "${TEMP_ENV}"
  fi
done

chmod --reference="${RUNTIME_ENV}" "${TEMP_ENV}"
mv "${TEMP_ENV}" "${RUNTIME_ENV}"
trap - EXIT

echo "configured ${ENVIRONMENT} for shared QY state; no credential values were printed"
