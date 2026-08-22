# QY BullMQ Crawler

This directory is the single Crawler implementation used by Query discovery,
Migration/full crawl, Incremental crawl, Data API fallback, Agent, Finalize, and
Publication roles. Roles use the same image and select work through
`WORKER_QUEUES`; there are no separate Query or Migration crawler copies.

## Entrypoints

```text
src/server.js      management interface and Bull Board
src/worker.js      all BullMQ queue consumers
src/controller.js  backpressure, repair, and batching control
src/pipelineV2.js  full-channel crawl and persistence pipeline
src/incrementalChannelRunner.js  incremental execution
```

Additional `run*.js` entrypoints own Feature relay and Publication processes.
Operational migration, recovery, backfill, and diagnostic commands live under
`scripts/`; they are not automatically executed by a worker.

## Primary Flows

```text
Query -> Discover -> youtube-channel-crawl -> Local Agent -> Finalize -> Publication
Migration input --^ 

Feature Clock -> Feature Dispatch -> youtube-channel-incremental
              -> Local Agent incremental -> Finalize -> Publication
```

Uploads provides video discovery and ordering. Per-video watch/player detail
provides authoritative `canonical`, `isShortsEligible`, and live-broadcast
signals. Content type is never inferred from duration and is never delegated to
the YouTube Data API.

Workers use `RotaSlotAdapter` and the Proxy Control interface. Rota owns proxy
inventory and atomic route replacement; the worker owns the YouTube session,
cookies, fingerprint, locale, parser state, and logical-job retry budget.

`src/schema.sql` is idempotent runtime DDL. Fresh portable deployments initialize
from `database/bootstrap/crawler.sql`; production workers set
`SKIP_SCHEMA_MIGRATION=true` so replicas do not compete to execute DDL.

Run the tests with:

```bash
cd services/qybullmq
npm test
```

## Business Publication DSM Diagnostics

Inventory all 11 audit plans without executing them or changing database state:

```bash
INC009_DSM_MODE=audit-plans \
INC009_DSM_DATABASE_URL='<database-url>' \
INC009_DSM_EXPECTED_DATABASE='<database-name>' \
npm run diagnose:business-publication-dsm
```

Run the calibrated 64 MiB or 256 MiB Parallel Hash matrix only against an
isolated database whose name ends in `_test`:

```bash
INC009_DSM_MODE=reproduce \
INC009_DSM_DATABASE_URL='<test-database-url>' \
INC009_DSM_CONTAINER='<postgres-container>' \
INC009_DSM_EXPECTED_SHM_MIB=64 \
INC009_DSM_PREPARE=1 \
npm run diagnose:business-publication-dsm
```

`INC009_DSM_PREPARE=1` creates the calibrated unlogged probe tables when they
do not exist. It never truncates or replaces an existing fixture. The joint
runtime load requires separate Crawler and Business `_test` databases. Use
`INC009_SOAK_MODE=steady` for the one-hour stability gate or `capacity` for the
300-Channel saturated throughput gate:

```bash
INC009_CRAWLER_TEST_DATABASE_URL='<crawler-test-database-url>' \
INC009_BUSINESS_TEST_DATABASE_URL='<business-test-database-url>' \
INC009_SOAK_MODE=capacity \
npm run soak:business-publication-runtime
```
