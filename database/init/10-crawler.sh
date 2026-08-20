#!/bin/sh
set -eu

: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${FEATURE_DB_PASSWORD:?FEATURE_DB_PASSWORD is required}"
: "${PUBLICATION_DB_PASSWORD:?PUBLICATION_DB_PASSWORD is required}"

psql -X -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --file /bootstrap/crawler.sql

psql -X -v ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=feature_password="$FEATURE_DB_PASSWORD" \
  --set=publication_password="$PUBLICATION_DB_PASSWORD" <<'SQL'
SELECT format('CREATE ROLE feature_user LOGIN PASSWORD %L', :'feature_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='feature_user')
\gexec

ALTER ROLE feature_user LOGIN PASSWORD :'feature_password';
GRANT CONNECT ON DATABASE :DBNAME TO feature_user;
GRANT USAGE ON SCHEMA crawler,feature_clock TO feature_user;
REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA crawler FROM feature_user;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA feature_clock TO feature_user;
GRANT USAGE,SELECT,UPDATE ON ALL SEQUENCES IN SCHEMA feature_clock TO feature_user;

ALTER DEFAULT PRIVILEGES IN SCHEMA crawler
  REVOKE ALL ON TABLES FROM feature_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA feature_clock
  GRANT SELECT,INSERT,UPDATE,DELETE ON TABLES TO feature_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA feature_clock
  GRANT USAGE,SELECT,UPDATE ON SEQUENCES TO feature_user;

SELECT format(
  'CREATE ROLE publication_publisher LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE '
  'NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 8 PASSWORD %L',
  :'publication_password'
)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='publication_publisher')
\gexec

ALTER ROLE publication_publisher WITH LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
  NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 8
  PASSWORD :'publication_password';
GRANT CONNECT ON DATABASE :DBNAME TO publication_publisher;
GRANT USAGE ON SCHEMA publication TO publication_publisher;
GRANT SELECT,UPDATE ON TABLE publication.outbox TO publication_publisher;
GRANT SELECT ON TABLE publication.revision TO publication_publisher;
SQL
