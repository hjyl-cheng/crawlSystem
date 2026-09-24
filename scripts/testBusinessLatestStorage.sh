#!/usr/bin/env bash
set -euo pipefail
repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
test_container="qy-latest-storage-test-$$"
cleanup() { docker rm -fv "$test_container" >/dev/null 2>&1 || true; }
docker run -d --name "$test_container" --label qy.purpose=isolated-storage-test \
  -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=business_latest_test \
  -p 127.0.0.1:0:5432 postgres:18.4-alpine >/dev/null
trap cleanup EXIT
ready=false
for attempt in {1..30}; do
  if docker exec "$test_container" pg_isready -U postgres -d business_latest_test >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [[ "$ready" != true ]]; then echo 'test PostgreSQL did not become ready' >&2; exit 1; fi
docker exec -i "$test_container" psql -qX -v ON_ERROR_STOP=1 -U postgres -d business_latest_test \
  < "$repo_root/database/bootstrap/business.sql" >/dev/null
test_port="$(docker port "$test_container" 5432)"
test_port="${test_port##*:}"
cd "$repo_root/services/qybullmq"
BUSINESS_LATEST_POSTGRES_TEST_URL="postgresql://postgres@127.0.0.1:${test_port}/business_latest_test" \
  node --test --test-isolation=none --test-concurrency=1 \
  test/businessLatestStorage.postgres.integration.test.js \
  test/businessLatestPublication.postgres.integration.test.js
