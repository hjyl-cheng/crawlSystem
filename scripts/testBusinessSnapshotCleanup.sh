#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_container="qy-snapshot-cleanup-test-$$"
test_image="postgres:18.4-alpine"
test_files=(test/businessSnapshotCleanup.postgres.integration.test.js)
if [[ "${SNAPSHOT_REPACK_TEST:-0}" == 1 ]]; then
  test_image="qy-maintenance/postgres-repack:18.4-1.5.3"
  test_files+=(test/businessSnapshotRepack.postgres.integration.test.js)
fi
cleanup() { docker rm -fv "$test_container" >/dev/null 2>&1 || true; }
docker run -d --name "$test_container" --label qy.purpose=isolated-snapshot-cleanup-test \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=business_cleanup_test \
  -p 127.0.0.1:0:5432 "$test_image" >/dev/null
trap cleanup EXIT
ready=false
for attempt in {1..30}; do
  if docker exec "$test_container" pg_isready -U postgres -d business_cleanup_test >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo 'test PostgreSQL did not become ready' >&2; exit 1; fi
docker exec -i "$test_container" psql -qX -v ON_ERROR_STOP=1 -U postgres -d business_cleanup_test \
  < "$repo_root/database/bootstrap/business.sql" >/dev/null
test_port="$(docker port "$test_container" 5432)"
test_port="${test_port##*:}"
cd "$repo_root/services/qybullmq"
BUSINESS_SNAPSHOT_CLEANUP_TEST_URL="postgresql://postgres@127.0.0.1:${test_port}/business_cleanup_test" \
BUSINESS_SNAPSHOT_REPACK_TEST_CONTAINER="$test_container" \
  node --test --test-isolation=none --test-concurrency=1 "${test_files[@]}"
