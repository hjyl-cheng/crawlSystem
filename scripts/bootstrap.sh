#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENVIRONMENT="${1:-local}"
if [[ ! "${ENVIRONMENT}" =~ ^[a-z0-9][a-z0-9_-]*$ ]]; then
  echo "environment must contain only lowercase letters, numbers, underscore, or hyphen" >&2
  exit 1
fi

RUNTIME_DIR="${QY_RUNTIME_DIR:-${ROOT_DIR}/runtime/${ENVIRONMENT}}"
ENV_DIR="${RUNTIME_DIR}/env"
ENV_FILE="${ENV_DIR}/runtime.env"
EXAMPLE_FILE="${ROOT_DIR}/.env.example"
SECRETS_DIR="${RUNTIME_DIR}/secrets"
COOKIES_DIR="${RUNTIME_DIR}/cookies"
PROXY_DIR="${RUNTIME_DIR}/proxy"

umask 077

command -v openssl >/dev/null 2>&1 || {
  echo "openssl is required" >&2
  exit 1
}

mkdir -p "${ENV_DIR}" "${SECRETS_DIR}" "${COOKIES_DIR}" "${PROXY_DIR}"
chmod 700 "${RUNTIME_DIR}" "${ENV_DIR}" "${COOKIES_DIR}" "${PROXY_DIR}"
chmod 700 "${SECRETS_DIR}"

if [[ ! -f "${ENV_FILE}" ]]; then
  cp "${EXAMPLE_FILE}" "${ENV_FILE}"
  chmod 600 "${ENV_FILE}"
fi

replace_marker() {
  local marker="$1"
  local value
  if ! grep -q "${marker}" "${ENV_FILE}"; then
    return
  fi
  if [[ "${marker}" == "CHANGE_ME_PUBLICATION_STREAM_ID" ]]; then
    local uuid_hex
    uuid_hex="$(openssl rand -hex 16)"
    value="${uuid_hex:0:8}-${uuid_hex:8:4}-4${uuid_hex:13:3}-8${uuid_hex:17:3}-${uuid_hex:20:12}"
  else
    value="$(openssl rand -hex 32)"
  fi
  sed -i "s/${marker}/${value}/g" "${ENV_FILE}"
}

while IFS= read -r marker; do
  replace_marker "${marker}"
done < <(grep -o 'CHANGE_ME_[A-Z0-9_]*' "${ENV_FILE}" | sort -u)

environment_value() {
  local name="$1"
  sed -n "s/^${name}=//p" "${ENV_FILE}" | tail -n 1
}

safe_url_component() {
  local name="$1"
  local value="$2"
  if [[ ! "${value}" =~ ^[A-Za-z0-9._~-]+$ ]]; then
    echo "${name} must contain only URL-safe letters, numbers, dot, underscore, tilde, or hyphen" >&2
    exit 1
  fi
}

write_secret() {
  local name="$1"
  local value="$2"
  printf '%s\n' "${value}" > "${SECRETS_DIR}/${name}"
  chmod 444 "${SECRETS_DIR}/${name}"
}

crawler_db_name="$(environment_value CRAWLER_DB_NAME)"
crawler_db_name="${crawler_db_name:-newcrawler_crawler}"
crawler_db_user="$(environment_value CRAWLER_DB_USER)"
crawler_db_user="${crawler_db_user:-bullmq}"
crawler_db_password="$(environment_value CRAWLER_DB_PASSWORD)"
feature_db_password="$(environment_value FEATURE_DB_PASSWORD)"
publication_db_password="$(environment_value PUBLICATION_DB_PASSWORD)"
business_db_name="$(environment_value BUSINESS_DB_NAME)"
business_db_name="${business_db_name:-newcrawler_business}"
business_db_user="$(environment_value BUSINESS_DB_USER)"
business_db_user="${business_db_user:-business}"
business_db_password="$(environment_value BUSINESS_DB_PASSWORD)"
business_publication_ingress_db_password="$(environment_value BUSINESS_PUBLICATION_INGRESS_DB_PASSWORD)"
business_publication_reconciler_db_password="$(environment_value BUSINESS_PUBLICATION_RECONCILER_DB_PASSWORD)"
business_publication_projector_db_password="$(environment_value BUSINESS_PUBLICATION_PROJECTOR_DB_PASSWORD)"
publication_ingress_token="$(environment_value BUSINESS_PUBLICATION_INGRESS_TOKEN)"
feature_ingest_token="$(environment_value FEATURE_INGEST_TOKEN)"
migration_db_host="$(environment_value MIGRATION_POSTGRES_HOST)"
migration_db_host="${migration_db_host:-migration-postgres}"
migration_db_port="$(environment_value MIGRATION_POSTGRES_PORT)"
migration_db_port="${migration_db_port:-6432}"
migration_db_name="$(environment_value MIGRATION_POSTGRES_DB)"
migration_db_name="${migration_db_name:-bullmq_crawler_migration}"
migration_db_user="$(environment_value MIGRATION_POSTGRES_USER)"
migration_db_user="${migration_db_user:-migration_reader}"
migration_db_password="$(environment_value MIGRATION_POSTGRES_PASSWORD)"

for entry in \
  "CRAWLER_DB_NAME:${crawler_db_name}" \
  "CRAWLER_DB_USER:${crawler_db_user}" \
  "CRAWLER_DB_PASSWORD:${crawler_db_password}" \
  "FEATURE_DB_PASSWORD:${feature_db_password}" \
  "PUBLICATION_DB_PASSWORD:${publication_db_password}" \
  "BUSINESS_DB_NAME:${business_db_name}" \
  "BUSINESS_DB_USER:${business_db_user}" \
  "BUSINESS_DB_PASSWORD:${business_db_password}" \
  "BUSINESS_PUBLICATION_INGRESS_DB_PASSWORD:${business_publication_ingress_db_password}" \
  "BUSINESS_PUBLICATION_RECONCILER_DB_PASSWORD:${business_publication_reconciler_db_password}" \
  "BUSINESS_PUBLICATION_PROJECTOR_DB_PASSWORD:${business_publication_projector_db_password}" \
  "MIGRATION_POSTGRES_HOST:${migration_db_host}" \
  "MIGRATION_POSTGRES_PORT:${migration_db_port}" \
  "MIGRATION_POSTGRES_DB:${migration_db_name}" \
  "MIGRATION_POSTGRES_USER:${migration_db_user}" \
  "MIGRATION_POSTGRES_PASSWORD:${migration_db_password}"; do
  safe_url_component "${entry%%:*}" "${entry#*:}"
done

write_secret crawler_admin_database_url \
  "postgresql://${crawler_db_user}:${crawler_db_password}@crawler-postgres:5432/${crawler_db_name}"
write_secret crawler_publication_database_url \
  "postgresql://publication_publisher:${publication_db_password}@crawler-pgbouncer:6432/${crawler_db_name}"
write_secret feature_database_url \
  "postgresql://feature_user:${feature_db_password}@crawler-pgbouncer:6432/${crawler_db_name}"
write_secret business_database_url \
  "postgresql://${business_db_user}:${business_db_password}@business-postgres:5432/${business_db_name}"
write_secret business_admin_database_url \
  "postgresql://${business_db_user}:${business_db_password}@business-postgres:5432/${business_db_name}"
write_secret business_publication_ingress_database_url \
  "postgresql://business_publication_ingress:${business_publication_ingress_db_password}@business-postgres:5432/${business_db_name}"
write_secret business_publication_reconciler_database_url \
  "postgresql://business_publication_reconciler:${business_publication_reconciler_db_password}@business-postgres:5432/${business_db_name}"
write_secret business_publication_projector_database_url \
  "postgresql://business_publication_projector:${business_publication_projector_db_password}@business-postgres:5432/${business_db_name}"
write_secret business_publication_audit_database_url \
  "postgresql://${business_db_user}:${business_db_password}@business-postgres:5432/${business_db_name}"
write_secret migration_database_url \
  "postgresql://${migration_db_user}:${migration_db_password}@${migration_db_host}:${migration_db_port}/${migration_db_name}"
write_secret business_publication_ingress_token "${publication_ingress_token}"
write_secret feature_ingest_token "${feature_ingest_token}"

auth_username="${QY_AUTH_USERNAME:-$(environment_value QY_AUTH_USERNAME)}"
auth_username="${auth_username:-admin}"
printf '%s\n' "${auth_username}" > "${SECRETS_DIR}/qy_auth_username"
chmod 444 "${SECRETS_DIR}/qy_auth_username"

if [[ ! -s "${SECRETS_DIR}/qy_auth_password_hash" ]]; then
  initial_password="${QY_AUTH_INITIAL_PASSWORD:-$(openssl rand -base64 24 | tr -d '\n')}"
  if (( ${#initial_password} < 16 )); then
    echo "QY_AUTH_INITIAL_PASSWORD must contain at least 16 characters" >&2
    exit 1
  fi
  printf '%s\n' "${initial_password}" > "${SECRETS_DIR}/qy_auth_initial_password"
  chmod 600 "${SECRETS_DIR}/qy_auth_initial_password"
  echo "Initial dashboard login: ${auth_username}"
  echo "Initial dashboard password: ${initial_password}"
  echo "Record it now. The plaintext file is removed after auth-secret-init succeeds."
fi

echo "Bootstrap complete: ${ENV_FILE}"
echo "Runtime directory: ${RUNTIME_DIR}"
echo "Next: follow docs/NEWCRAWLER_FRESH_DATABASE_MIGRATION.md for staged preflight and startup."
echo "Do not run a blanket Compose up before the database, role, and Publication gates pass."
