#!/usr/bin/env bash

set -Eeuo pipefail

readonly ACTION="${1:-}"
readonly EVIDENCE_DIR="${PUBLICATION_RECOVERY_EVIDENCE_DIR:-/tmp/qy-publication-recovery-20260817-lJA52g}"
readonly CRAWLER_DB_CONTAINER="${PUBLICATION_RECOVERY_CRAWLER_DB_CONTAINER:-bullmq-crawler-migration-postgres}"
readonly BUSINESS_DB_CONTAINER="${PUBLICATION_RECOVERY_BUSINESS_DB_CONTAINER:-yewu-business-postgres}"
readonly CRAWLER_ROLE="qy_publication_recovery_crawler_20260817"
readonly BUSINESS_ROLE="qy_publication_recovery_business_20260817"
readonly CRAWLER_URL_FILE="${EVIDENCE_DIR}/crawler-recovery-database-url"
readonly BUSINESS_URL_FILE="${EVIDENCE_DIR}/business-recovery-database-url"
readonly ROLE_MANIFEST_FILE="${EVIDENCE_DIR}/temporary-recovery-roles.json"

usage() {
  cat <<'EOF'
Usage: managePublicationRecoveryRoles.sh <provision|deprovision|status>

Creates or disables the two temporary, least-privilege PostgreSQL roles used by
the QY Publication dead-letter recovery. Passwords are never printed. Provision
writes mode-0600 database URL files into PUBLICATION_RECOVERY_EVIDENCE_DIR.
EOF
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'required command is unavailable: %s\n' "$1" >&2
    exit 1
  }
}

admin_psql() {
  local container="$1"
  docker exec -i "$container" sh -lc \
    'exec psql -X -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"'
}

provision_crawler_role() {
  local secret="$1"
  {
    printf "\\set role_secret '%s'\n" "$secret"
    cat <<'SQL'
BEGIN;
SELECT format(
  'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT '
  'NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 6 PASSWORD %L VALID UNTIL %L',
  'qy_publication_recovery_crawler_20260817',
  :'role_secret',
  current_timestamp + interval '1 day'
) \gexec
ALTER ROLE qy_publication_recovery_crawler_20260817 SET statement_timeout = '10min';
ALTER ROLE qy_publication_recovery_crawler_20260817 SET lock_timeout = '30s';
ALTER ROLE qy_publication_recovery_crawler_20260817
  SET idle_in_transaction_session_timeout = '2min';
GRANT CONNECT ON DATABASE bullmq_crawler_migration
  TO qy_publication_recovery_crawler_20260817;
GRANT USAGE ON SCHEMA publication
  TO qy_publication_recovery_crawler_20260817;
GRANT SELECT ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state,
  publication.domain_current,
  publication.revision,
  publication.outbox
  TO qy_publication_recovery_crawler_20260817;
GRANT INSERT ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state,
  publication.domain_current,
  publication.revision,
  publication.outbox
  TO qy_publication_recovery_crawler_20260817;
-- PostgreSQL requires UPDATE privilege for SELECT ... FOR SHARE.
GRANT UPDATE ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state
  TO qy_publication_recovery_crawler_20260817;
COMMIT;
SQL
  } | admin_psql "$CRAWLER_DB_CONTAINER"
}

provision_business_role() {
  local secret="$1"
  {
    printf "\\set role_secret '%s'\n" "$secret"
    cat <<'SQL'
BEGIN;
SELECT format(
  'CREATE ROLE %I LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT '
  'NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 6 PASSWORD %L VALID UNTIL %L',
  'qy_publication_recovery_business_20260817',
  :'role_secret',
  current_timestamp + interval '1 day'
) \gexec
ALTER ROLE qy_publication_recovery_business_20260817 SET statement_timeout = '10min';
ALTER ROLE qy_publication_recovery_business_20260817 SET lock_timeout = '30s';
ALTER ROLE qy_publication_recovery_business_20260817
  SET idle_in_transaction_session_timeout = '2min';
GRANT CONNECT ON DATABASE yewu_business
  TO qy_publication_recovery_business_20260817;
GRANT USAGE ON SCHEMA publication, result
  TO qy_publication_recovery_business_20260817;
GRANT SELECT ON
  publication.stream,
  publication.channel_ownership,
  publication.consumer_cursor,
  publication.revision,
  publication.inbox,
  publication.quarantine,
  publication.activation,
  publication.activation_item,
  publication.projection_outbox,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  TO qy_publication_recovery_business_20260817;
GRANT INSERT ON
  publication.stream,
  publication.consumer_cursor,
  publication.quarantine,
  publication.activation,
  publication.activation_item,
  publication.projection_outbox,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  TO qy_publication_recovery_business_20260817;
-- PostgreSQL requires UPDATE privilege for SELECT ... FOR SHARE.
GRANT UPDATE ON
  publication.stream,
  publication.channel_ownership,
  publication.consumer_cursor,
  publication.revision,
  publication.quarantine,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  TO qy_publication_recovery_business_20260817;
COMMIT;
SQL
  } | admin_psql "$BUSINESS_DB_CONTAINER"
}

write_credentials() {
  local crawler_secret="$1"
  local business_secret="$2"
  local generated_at
  generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

  umask 077
  mkdir -p "$EVIDENCE_DIR"
  chmod 700 "$EVIDENCE_DIR"
  if [[ -e "$CRAWLER_URL_FILE" || -e "$BUSINESS_URL_FILE" ]]; then
    printf 'refusing to overwrite existing recovery credential files\n' >&2
    exit 1
  fi
  printf 'postgresql://%s:%s@bullmq-crawler-migration-postgres:5432/bullmq_crawler_migration\n' \
    "$CRAWLER_ROLE" "$crawler_secret" >"$CRAWLER_URL_FILE"
  printf 'postgresql://%s:%s@yewu-business-postgres:5432/yewu_business\n' \
    "$BUSINESS_ROLE" "$business_secret" >"$BUSINESS_URL_FILE"
  chmod 600 "$CRAWLER_URL_FILE" "$BUSINESS_URL_FILE"
  cat >"$ROLE_MANIFEST_FILE" <<EOF
{
  "generated_at": "${generated_at}",
  "temporary": true,
  "passwords_recorded": false,
  "crawler": {
    "role": "${CRAWLER_ROLE}",
    "database": "bullmq_crawler_migration",
    "attributes": ["LOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOINHERIT", "NOREPLICATION", "NOBYPASSRLS", "CONNECTION_LIMIT_6"],
    "operations": ["SELECT", "INSERT", "UPDATE"]
  },
  "business": {
    "role": "${BUSINESS_ROLE}",
    "database": "yewu_business",
    "attributes": ["LOGIN", "NOSUPERUSER", "NOCREATEDB", "NOCREATEROLE", "NOINHERIT", "NOREPLICATION", "NOBYPASSRLS", "CONNECTION_LIMIT_6"],
    "operations": ["SELECT", "INSERT", "UPDATE"]
  }
}
EOF
  chmod 600 "$ROLE_MANIFEST_FILE"
}

provision() {
  local crawler_secret business_secret
  crawler_secret="$(openssl rand -hex 32)"
  business_secret="$(openssl rand -hex 32)"
  if ! provision_crawler_role "$crawler_secret"; then
    return 1
  fi
  if ! provision_business_role "$business_secret"; then
    deprovision_crawler_role
    return 1
  fi
  if ! write_credentials "$crawler_secret" "$business_secret"; then
    deprovision_crawler_role
    deprovision_business_role
    return 1
  fi
  crawler_secret=''
  business_secret=''
  printf 'temporary recovery roles provisioned; credential files are mode 0600\n'
}

deprovision_crawler_role() {
  cat <<'SQL' | admin_psql "$CRAWLER_DB_CONTAINER"
BEGIN;
REVOKE UPDATE ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state
  FROM qy_publication_recovery_crawler_20260817;
REVOKE INSERT ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state,
  publication.domain_current,
  publication.revision,
  publication.outbox
  FROM qy_publication_recovery_crawler_20260817;
REVOKE SELECT ON
  publication.stream,
  publication.channel_stream_state,
  publication.channel_delivery_state,
  publication.domain_current,
  publication.revision,
  publication.outbox
  FROM qy_publication_recovery_crawler_20260817;
REVOKE USAGE ON SCHEMA publication
  FROM qy_publication_recovery_crawler_20260817;
REVOKE CONNECT ON DATABASE bullmq_crawler_migration
  FROM qy_publication_recovery_crawler_20260817;
ALTER ROLE qy_publication_recovery_crawler_20260817
  NOLOGIN PASSWORD NULL CONNECTION LIMIT 0;
COMMIT;
SQL
}

deprovision_business_role() {
  cat <<'SQL' | admin_psql "$BUSINESS_DB_CONTAINER"
BEGIN;
REVOKE UPDATE ON
  publication.stream,
  publication.channel_ownership,
  publication.consumer_cursor,
  publication.revision,
  publication.quarantine,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  FROM qy_publication_recovery_business_20260817;
REVOKE INSERT ON
  publication.stream,
  publication.consumer_cursor,
  publication.quarantine,
  publication.activation,
  publication.activation_item,
  publication.projection_outbox,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  FROM qy_publication_recovery_business_20260817;
REVOKE SELECT ON
  publication.stream,
  publication.channel_ownership,
  publication.consumer_cursor,
  publication.revision,
  publication.inbox,
  publication.quarantine,
  publication.activation,
  publication.activation_item,
  publication.projection_outbox,
  result.entity_current,
  result.agent_current,
  result.video_current,
  result.content_current
  FROM qy_publication_recovery_business_20260817;
REVOKE USAGE ON SCHEMA publication, result
  FROM qy_publication_recovery_business_20260817;
REVOKE CONNECT ON DATABASE yewu_business
  FROM qy_publication_recovery_business_20260817;
ALTER ROLE qy_publication_recovery_business_20260817
  NOLOGIN PASSWORD NULL CONNECTION LIMIT 0;
COMMIT;
SQL
}

remove_credential_file() {
  local path="$1"
  if [[ -f "$path" ]]; then
    chmod 600 "$path"
    : >"$path"
    unlink "$path"
  fi
}

deprovision() {
  deprovision_crawler_role
  deprovision_business_role
  remove_credential_file "$CRAWLER_URL_FILE"
  remove_credential_file "$BUSINESS_URL_FILE"
  printf 'temporary recovery roles disabled and credential files removed\n'
}

status() {
  cat <<SQL | admin_psql "$CRAWLER_DB_CONTAINER"
SELECT rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,
       rolbypassrls,rolconnlimit,rolvaliduntil
FROM pg_roles WHERE rolname='${CRAWLER_ROLE}';
SQL
  cat <<SQL | admin_psql "$BUSINESS_DB_CONTAINER"
SELECT rolname,rolcanlogin,rolsuper,rolcreatedb,rolcreaterole,rolreplication,
       rolbypassrls,rolconnlimit,rolvaliduntil
FROM pg_roles WHERE rolname='${BUSINESS_ROLE}';
SQL
  [[ ! -e "$CRAWLER_URL_FILE" ]] || stat -c '%a %n' "$CRAWLER_URL_FILE"
  [[ ! -e "$BUSINESS_URL_FILE" ]] || stat -c '%a %n' "$BUSINESS_URL_FILE"
}

require_command docker
case "$ACTION" in
  provision)
    require_command openssl
    provision
    ;;
  deprovision)
    deprovision
    ;;
  status)
    status
    ;;
  *)
    usage >&2
    exit 2
    ;;
esac
