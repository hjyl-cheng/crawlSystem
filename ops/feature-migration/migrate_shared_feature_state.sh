#!/bin/sh
set -eu
set -o pipefail

source_host="${LEGACY_FEATURE_DATABASE_HOST:-feature-postgres}"
source_database="${LEGACY_FEATURE_DATABASE_NAME:-feature_clock}"
target_host="${FEATURE_DATABASE_HOST:-bullmq-crawler-migration-postgres}"
target_database="${FEATURE_DATABASE_NAME:-bullmq_crawler_migration}"
database_user="${FEATURE_DATABASE_USER:-feature_user}"
expected_channels="${EXPECTED_FEATURE_CHANNEL_COUNT:-1553}"
expected_events="${EXPECTED_FEATURE_EVENT_COUNT:-6212}"

counts_sql="SELECT concat_ws('|',(SELECT count(*) FROM feature_clock.channel_feature_state),(SELECT count(*) FROM feature_clock.channel_clock_state),(SELECT count(*) FROM feature_clock.crawler_event_inbox),(SELECT count(*) FROM feature_clock.bootstrap_channel_receipts),(SELECT count(*) FROM feature_clock.clock_decision_log),(SELECT count(*) FROM feature_clock.recalculation_runs),(SELECT count(*) FROM feature_clock.daily_channel_plans),(SELECT count(*) FROM feature_clock.dispatch_outbox));"
policy_sql="SELECT string_agg(policy_version || ':' || checksum,',' ORDER BY policy_version) FROM feature_clock.rule_policy_definitions;"

source_counts="$(psql -h "$source_host" -U "$database_user" -d "$source_database" -X -Atc "$counts_sql")"
target_counts="$(psql -h "$target_host" -U "$database_user" -d "$target_database" -X -Atc "$counts_sql")"
source_policy="$(psql -h "$source_host" -U "$database_user" -d "$source_database" -X -Atc "$policy_sql")"
target_policy="$(psql -h "$target_host" -U "$database_user" -d "$target_database" -X -Atc "$policy_sql")"

case "$source_counts" in
  "$expected_channels|$expected_channels|$expected_events|$expected_channels|"*) ;;
  *)
    echo "legacy Feature state does not match expected coverage" >&2
    exit 1
    ;;
esac

if [ "$source_policy" != "$target_policy" ]; then
  echo "source and target Feature policies differ" >&2
  exit 1
fi

if [ "$target_counts" = "$source_counts" ]; then
  echo "shared Feature state already matches legacy source"
  exit 0
fi

if [ "$target_counts" != "0|0|0|0|0|0|0|0" ]; then
  echo "shared Feature target is not empty; refusing a partial overwrite" >&2
  exit 1
fi

pg_dump \
  -h "$source_host" \
  -U "$database_user" \
  -d "$source_database" \
  --data-only \
  --schema=feature_clock \
  --exclude-table-data=feature_clock.rule_policy_definitions \
  --no-owner \
  --no-privileges \
| psql \
  -h "$target_host" \
  -U "$database_user" \
  -d "$target_database" \
  -X \
  --single-transaction \
  -v ON_ERROR_STOP=1

migrated_counts="$(psql -h "$target_host" -U "$database_user" -d "$target_database" -X -Atc "$counts_sql")"
if [ "$migrated_counts" != "$source_counts" ]; then
  echo "shared Feature state coverage differs after migration" >&2
  exit 1
fi

echo "shared Feature state migration completed: $migrated_counts"
