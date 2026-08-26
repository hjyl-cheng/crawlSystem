# Newcrawler Fresh Database Migration

This runbook is the only approved procedure for moving selected legacy QY
Migration channels into fresh newcrawler Crawler and Business databases. It
does not authorize a deployment. Every production command below requires a
separate change window and operator approval.

## 1. Invariants

- The legacy `bullmq_crawler_migration` database is a read-only Migration
  Source. Never apply repository schema SQL, run a migration, truncate a
  table, or use a Writer credential against it.
- The Crawler Writer must connect to `newcrawler_crawler` and rejects
  `bullmq_crawler_migration`.
- The Business Writers must connect to `newcrawler_business` and reject
  `yewu_business`.
- Source reads and target writes use separate PostgreSQL connections and
  separate transactions. There is no cross-database SQL or transaction.
- A Migration intent is unique by `(source_id, channel_id)` and by
  `(source_id, source_candidate_id)`. Its source snapshot is immutable.
  Repeated clicks and BullMQ retries reuse the same target Candidate.
- Authoritative Migration list membership originates in the legacy Source and
  is copied into the narrow `crawler.migration_channel_inventory` table before
  the API becomes healthy. Normal list requests, including filtering, sorting,
  counting, and pagination, read only the fresh Crawler database. Source
  connections remain limited to inventory refresh, detail, and dispatch
  snapshots. The inventory's `search_subscriber_count` is a legacy discovery
  snapshot, so the Dashboard labels it as `源库订阅`; it is not a fresh About
  metric.
- Full Crawl, future Incremental Crawl, and future Query work share the fresh
  Crawler database. Incremental, Query, Discover, Daily Clock, and Feature
  Dispatch remain disabled throughout this canary.
- Redis and MinIO are fresh, isolated state. The single existing QY Rota Core
  remains the only Rota instance.
- Never run `docker compose down -v` or reuse a legacy newcrawler volume.

## 2. Audited Starting State

The 2026-08-24 read-only runtime audit found this live connection topology:

```text
Controller/API/Dashboard/Full/Incremental/Agent/Finalize/Feature/Publisher
  -> bullmq-crawler-migration-pgbouncer:6432/bullmq_crawler_migration

Dashboard Migration Source
  -> the same bullmq_crawler_migration database

Redis
  -> bullmq-crawler-migration-redis

MinIO
  -> bullmq-crawler-qy-minio / crawler-raw-migration

Ingress/Reconciler/Projector
  -> yewu_business

Rota
  -> youtube-rota-qy-core
```

The shared Redis contains legacy queue state, including 17 entries under
`bull:youtube-channel-incremental:paused`. An empty Crawler database must not
consume that Redis. A fresh Redis is therefore a hard gate, not an optional
optimization.

The legacy MinIO bucket contains about 2.0 GiB. Reusing it would mix evidence
whose database ownership differs. The fresh runtime uses a new volume and
`newcrawler-raw-20260824-v1` bucket.

Two older, stopped PostgreSQL volumes were mounted read-only, copied to
temporary audit volumes, and inspected. The originals were not changed or
deleted:

| Preserved volume | PostgreSQL | Database | Business rows | Missing fresh contract |
| --- | ---: | --- | ---: | --- |
| `qy-newcrawler_crawler-postgres-data` | 16 | `bullmq_crawler_migration` | 0 | `crawler.database_identity`, `crawler.migration_channel_intents` |
| `qy-newcrawler_business-postgres-data` | 18 | `yewu_business` | 0 | `publication.database_identity`, current Publication schema |

Their database names are forbidden by the new Writer gates and their schemas
are stale. Preserve them but do not reuse them. The approved fresh names are:

```text
qy-newcrawler-crawler-postgres-20260824-v1
qy-newcrawler-business-postgres-20260824-v1
qy-newcrawler-redis-20260824-v1
qy-newcrawler-minio-20260824-v1
```

## 3. Final Linear Topology

```text
Legacy Migration PostgreSQL: bullmq_crawler_migration
  role=migration_reader
  default_transaction_read_only=on
  BEGIN ... REPEATABLE READ READ ONLY
  database name + OID + user identity gate
       |
       | Startup inventory sync and immutable dispatch/detail snapshots
       v
Fresh Crawler PostgreSQL: newcrawler_crawler
  migration_channel_inventory
       |
       | Dashboard WHERE -> ORDER BY -> LIMIT 50
       | QYBullMQ immutable, idempotent Migration intent
       v
  Candidate -> Full Run -> Channel/Video/Comment -> Local Agent -> Finalize
       |
       | Publication stream, revision, outbox
       v
Publication Publisher
       |
       | authenticated HTTP transport
       v
Fresh Business PostgreSQL: newcrawler_business
  Ingress -> Reconciler -> Projector -> Business Current
```

Supporting state is deliberately separate:

```text
Full/Data API/Agent/Finalize queues -> fresh newcrawler Redis
Raw Full Crawl evidence             -> fresh newcrawler MinIO bucket
Full Crawl proxy leases             -> existing youtube-rota-qy-core only
```

Connection ownership is explicit:

| Component | Migration Source | Fresh Crawler | Fresh Business | Redis/MinIO/Rota |
| --- | --- | --- | --- | --- |
| Dashboard | read-only detail | read inventory/status/result | audit read | fresh Redis/MinIO, shared Rota UI |
| QYBullMQ API | read-only inventory/dispatch snapshot | write inventory/intent/Candidate | none | fresh Redis |
| Controller, Full, Data API | none | writer | none | fresh Redis/MinIO, shared Rota |
| Local Agent, Finalize | none | writer with Crawler identity gate | none | fresh Redis |
| Feature Bridge | none | Crawler/feature schema | none | fresh Redis |
| Publication Publisher | none | `publication_publisher` | HTTP only | none |
| Business Ingress | none | none | `business_publication_ingress` | none |
| Business Reconciler | none | none | `business_publication_reconciler` | none |
| Business Projector | none | none | `business_publication_projector` | none |

## 4. Source Read-Only Gate

The DBA must provide an existing login whose role setting is
`default_transaction_read_only=on` and whose only application table grants are
`SELECT`. Creating or altering that role is outside this rollout because it
changes the legacy cluster. Do not proceed until it already exists.

Connect using the exact Source URL that will be written to
`runtime/<environment>/secrets/migration_database_url`, then run:

```sql
BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

SELECT current_database() AS database_name,
       (SELECT oid::text FROM pg_database WHERE datname=current_database()) AS database_oid,
       current_user AS database_user,
       current_setting('default_transaction_read_only') AS role_default_read_only,
       current_setting('transaction_read_only') AS transaction_read_only,
       to_regclass('crawler.channel_candidates') IS NOT NULL AS candidates_ready,
       to_regclass('crawler.channels') IS NOT NULL AS channels_ready,
       has_table_privilege(
         current_user,'crawler.channel_candidates','INSERT,UPDATE,DELETE,TRUNCATE'
       ) AS candidate_write,
       has_table_privilege(
         current_user,'crawler.channels','INSERT,UPDATE,DELETE,TRUNCATE'
       ) AS channel_write;

COMMIT;
```

Required result:

```text
database_name              = bullmq_crawler_migration
database_oid               = the approved, recorded OID
database_user              = migration_reader
role_default_read_only     = on
transaction_read_only      = on
candidates_ready/channels_ready = true
candidate_write/channel_write   = false
```

Set `MIGRATION_POSTGRES_DATABASE_OID` to that exact OID. A restore or database
replacement changes the OID and intentionally blocks reads until re-approved.
Also verify `MIGRATION_POSTGRES_DB` differs from `CRAWLER_DB_NAME`.

Before every cohort, export an ordered `(candidate_id, channel_id,
md5(to_jsonb(row)::text))` fingerprint for the selected Source Candidate rows
and matching Channel rows from a read-only transaction. Repeat the same query
after the cohort and require byte-identical output. Store both artifacts with
the change record. Do not create a helper table in the Source.

## 5. Configuration Gate

Bootstrap an ignored runtime, edit it, and render the deployment without
starting a service:

```bash
./scripts/bootstrap.sh production
./scripts/compose.sh production config --quiet
./scripts/compose.sh production config --services
./scripts/verify.sh
```

Required values include:

```text
QY_DEPLOYMENT_MODE=fresh-migration
QY_FRESH_CRAWLER_NETWORK=qy-newcrawler-crawler-runtime
QY_FRESH_BUSINESS_NETWORK=qy-newcrawler-business-database
CRAWLER_DB_NAME=newcrawler_crawler
BUSINESS_DB_NAME=newcrawler_business
MIGRATION_POSTGRES_DB=bullmq_crawler_migration
MIGRATION_POSTGRES_DATABASE_OID=<approved OID, never 0>
MIGRATION_INVENTORY_SYNC_BATCH_SIZE=5000
MIGRATION_INVENTORY_FORCE_SYNC=false
MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS=10000
MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS=120000
EXPECTED_MIGRATION_INVENTORY_ROW_COUNT=0
CRAWLER_POSTGRES_VOLUME_NAME=qy-newcrawler-crawler-postgres-20260824-v1
BUSINESS_POSTGRES_VOLUME_NAME=qy-newcrawler-business-postgres-20260824-v1
CRAWLER_REDIS_VOLUME_NAME=qy-newcrawler-redis-20260824-v1
CRAWLER_MINIO_VOLUME_NAME=qy-newcrawler-minio-20260824-v1
MINIO_BUCKET=newcrawler-raw-20260824-v1
PUBLICATION_BOOTSTRAP_PROJECTION_MODE=online
```

Pin `QYBULLMQ_IMAGE_TAG`, Dashboard, Feature Engine, Feature Dispatch, and Auth
to the approved immutable release. Replace the generated Publication release
digests with the actual immutable Writer and four-process runtime image
digests. `local`, `latest`, a mutable tag, OID `0`, or either legacy Writer
database name blocks the rollout.

The fresh main Compose, standalone Feature Bridge, and standalone Publication
Runtime must resolve the same two fresh network names above. Neither may use
the legacy `QY_SHARED_CRAWLER_NETWORK` or `QY_SHARED_BUSINESS_NETWORK` values.
The Source endpoint must be reachable only on the external
`newcrawler-migration-source` network. Only API and Dashboard join that
network. Full, Agent, Finalize, Feature, and Publication processes must not.
The external `youtube-rota-qy-internal` network must contain the single
`youtube-rota-qy-core`; do not start the bundled Rota services.

## 6. Fresh Infrastructure Bootstrap

Create and start only the stateful services. Do not start applications or
workers yet:

```bash
./scripts/compose.sh production up -d --no-build \
  crawler-postgres crawler-pgbouncer business-postgres redis minio minio-init
```

Check the resolved mounts with `docker inspect`. Each PostgreSQL identity table
must match its current database, and all channel, Candidate, Run, Publication,
Business Current, Redis queue, and MinIO object counts must be zero. If a named
volume already contains data or lacks the identity marker, stop. Allocate a
new versioned volume name. Never delete or repair the old volume in place.

Before starting any API process, publish the Migration inventory tables and
page index through the manual Schema publisher. For a fresh Target the expected
inventory row count is `0`; for an existing Target, first record the exact
current count and use that value instead. The publisher connects directly to
`crawler-postgres:5432`, verifies the Crawler database identity, runs in one
transaction, and refuses to commit if the inventory row count changes:

```bash
EXPECTED_MIGRATION_INVENTORY_ROW_COUNT=0 \
./scripts/compose.sh production \
  --profile manual-migration-inventory-schema run --rm \
  -e CONFIRM_MIGRATION_INVENTORY_SCHEMA_APPLY=newcrawler_crawler \
  migration-inventory-schema-publisher
```

Archive the publisher JSON and require `ok=true`, the exact approved database
name, and the expected row count. The running API never creates or alters these
objects. It performs a read-only PostgreSQL catalog contract check and exits
before health becomes ready when the controlled Schema publication is missing
or invalid. The check verifies the exact column types, nullability and defaults,
primary/unique/foreign-key/CHECK constraints, and the complete page-index
contract.

Run the dedicated runtime-role administrator as a plan:

```bash
./scripts/compose.sh production \
  --profile manual-publication-runtime-role-admin run --rm \
  publication-runtime-role-admin
```

Archive the JSON. Apply only with its exact confirmation:

```bash
./scripts/compose.sh production \
  --profile manual-publication-runtime-role-admin run --rm \
  -e CONFIRM_PUBLICATION_RUNTIME_ROLES='<exact plan value>' \
  publication-runtime-role-admin \
  node scripts/managePublicationRuntimeRoles.mjs --apply
```

Rerun the plan and require zero pending role changes. Then plan the fresh
Publication stream bootstrap:

```bash
./scripts/compose.sh production \
  --profile manual-fresh-publication-bootstrap run --rm \
  fresh-publication-bootstrap
```

Apply its exact confirmation:

```bash
./scripts/compose.sh production \
  --profile manual-fresh-publication-bootstrap run --rm \
  -e CONFIRM_FRESH_PUBLICATION_BOOTSTRAP='<exact plan value>' \
  fresh-publication-bootstrap \
  node scripts/bootstrapFreshPublication.mjs --apply
```

This command intentionally commits Business first and Crawler second in two
independent transactions. If it stops after the first commit, the next plan
reports `business_committed`. Repeat the same confirmed apply. Never delete the
Business stream to simulate rollback. A completed rerun must report zero
stream inserts and phase `complete`.

Verify the fresh Redis has no BullMQ keys and the fresh MinIO bucket is empty.
Any legacy queue key, object, or volume mount is a hard stop.

## 7. Controlled Service Startup

Start the Publication destination before any crawl worker can create an
outbox entry:

```bash
./scripts/compose.sh production up -d --no-build \
  business-publication-ingress business-publication-reconciler \
  business-publication-projector publication-publisher
```

Start the Crawler control path with one Full worker and no automatic producer:

```bash
./scripts/compose.sh production up -d --no-build \
  --scale worker-channel=1 \
  local-agent-config controller worker-channel worker-data-api \
  worker-agent worker-finalize feature-ingest feature-relay \
  crawler-outbox-publisher
```

Start only QYBullMQ API first. Its first startup performs the real, read-only
legacy Source inventory sync in batches of `5000`; each Source statement has a
`120000` ms timeout. The API cannot become healthy until the Target inventory
is atomically marked `ready`:

```bash
./scripts/compose.sh production up -d --no-build qybullmq-api
```

This first sync is a deployment gate, not background warm-up. Record its
elapsed time and final `migration_channel_inventory_sync_ready` event. Query
fresh Crawler and require `status='ready'`, the pinned Source database name and
OID, and exact equality between `eligible_count` and the inventory row count.
Any timeout, Source identity mismatch, failed status, count mismatch, or API
restart blocks Dashboard/Nginx startup. The synthetic two-database regression
test with 410,292 rows must also pass before the change window:

```bash
cd services/qybullmq
MIGRATION_INVENTORY_SOURCE_POSTGRES_ADMIN_TEST_URL='<local Source admin test URL>' \
MIGRATION_INVENTORY_SOURCE_POSTGRES_TEST_URL='<local read-only Source test URL>' \
MIGRATION_INVENTORY_TARGET_POSTGRES_TEST_URL='<different local Target test URL>' \
node --test test/migrationInventorySync.postgres.integration.test.js
```

After the initial inventory gate passes, start the controlled UI path:

```bash
./scripts/compose.sh production up -d --no-build \
  auth-secret-init auth dashboard nginx
```

On later starts, a `ready` inventory with the same Source database/OID and an
exact stored row count skips the Source connection. To intentionally refresh
membership or Source snapshot fields, set `MIGRATION_INVENTORY_FORCE_SYNC=true`
for one API restart, repeat the same ready/count gate, then immediately restore
it to `false` and recreate the API container. Leaving it true would repeat a
full Source scan on every restart. The force value accepts only `true` or
`false`; misspellings stop startup. Normal Source detail and dispatch reads use
`MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS` (10 seconds by default), while only
the full inventory scan uses
`MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS` (120 seconds by default).
Change either timeout or `MIGRATION_INVENTORY_SYNC_BATCH_SIZE` only through an
approved change; invalid values stop startup instead of silently falling back.

Confirm the rendered and running service lists exclude:

```text
worker-incremental
worker-discover
worker-query-quality
feature-scheduler-daily
feature-dispatch
rota-db
rota-core
rota-dashboard
```

The API and Dashboard must report `CONTROLLED_MIGRATION_ONLY=true`. Do not
enable Daily Clock, Incremental, Query, Discover, or Feature Dispatch during
the 100/1000/2000 validation.

## 8. One-Channel Functional Gate

Before a batch, migrate one explicitly reviewed Channel from the Dashboard.
Require all of the following:

1. The Source identity and read-only preflight passes and its before/after
   fingerprint is identical.
2. One immutable Migration intent and one target Candidate exist. Repeating
   the click does not create another intent, Candidate, or Full job.
3. The initial Full Run reaches `done`, Local Agent reaches `done`, and
   Finalize records `ready_auto` or an explicitly accepted `ready_partial`.
4. Crawler automatic onboarding creates one stream owner, one online Business
   delivery, and exactly one bootstrap revision for each of `channel`,
   `video`, and `agent`.
5. Publisher delivers all three revisions. Ingress, Reconciler, and Projector
   have no conflict, quarantine, dead letter, or retry backlog.
6. Business contains exactly one matching channel in entity, video, agent,
   snapshot, and creator-search Current views.
7. The job exists only in the fresh Redis and raw evidence exists only in the
   fresh MinIO bucket.

## 9. 100, 1000, And 2000 Channel Gates

Use three disjoint Dashboard cohorts in order: 100, then 1000, then 2000.
The list, its Target lifecycle joins, filtering, stable ordering, counts, and
`LIMIT 50/OFFSET` pagination all run in fresh Crawler PostgreSQL against the
published inventory. The legacy Source is not queried by normal list requests;
it is used only for controlled inventory refreshes and immutable detail or
dispatch snapshots. Do not start the next cohort until the previous gate is
signed off.

For each cohort, archive the batch response, Source fingerprints, target
queries, queue counts, MinIO object summary, Publication summary, Business
summary, container restarts, and image digests. Acceptance is exact:

- Requested Source Candidate count, distinct Source Candidate count,
  Migration intent count, distinct intent Channel count, and target Candidate
  count equal the cohort size. Duplicate counts are zero.
- Every enqueued job is terminal before evaluation. First-pass successful
  Full + Agent + Finalize rate is at least 95%; one controlled retry must bring
  it to at least 98%. Every remaining failure has a classified external or
  data-quality reason and no duplicate intent.
- For every successful Channel, Crawler has one active Channel, one terminal
  promotion Full Run, one finalized profile, three current Publication
  domains, three delivered bootstrap revisions, and no pending/dead outbox.
- For every successful Channel, Business has one active ownership and one
  matching entity/video/agent Current row, snapshot, and creator-search row.
  The exported successful Channel ID sets from Crawler and Business are
  byte-identical. Compare exports outside PostgreSQL; do not use a cross-db
  query.
- Source fingerprints are byte-identical before and after. Source role write
  privileges remain false and role/transaction read-only settings remain on.
- Fresh Redis has no stalled, failed-without-classification, or unexpected
  Incremental/Query jobs. Legacy Redis key counts do not move because no fresh
  service connects to it.
- All raw object keys use the fresh bucket. The legacy bucket object count and
  size do not move because of this migration.
- Publication conflict, quarantine, and dead-letter counts are zero. Database
  identity gates, health checks, and container restart counts remain clean.

After the 2000 cohort, hold the system in controlled Migration-only mode. A
separate decision is required before scaling workers or enabling any automatic
schedule.

## 10. Exact Rollback Order

Rollback preserves all fresh evidence and never writes to the legacy Source.

1. Disable external access to the Dashboard/API migration actions, then stop
   `nginx`, `dashboard`, and `qybullmq-api`. This stops new intent creation.
2. Stop `controller` so no fresh database state can produce additional work.
3. Record fresh Redis queue counts. Allow already-running Full, Data API,
   Agent, and Finalize jobs to finish. For an emergency, stop those workers
   and record their active job IDs before continuing.
4. Let `publication-publisher`, Ingress, Reconciler, and Projector drain all
   committed outbox/inbox/projection work. Record final Crawler and Business
   Channel ID exports and hashes.
5. Stop `worker-channel`, `worker-data-api`, `worker-agent`,
   `worker-finalize`, `crawler-outbox-publisher`, `feature-relay`, and
   `feature-ingest`.
6. Stop Publication Publisher, Ingress, Reconciler, and Projector only after
   their backlogs are zero or explicitly recorded.
7. Leave both PostgreSQL containers, Redis, MinIO, all four fresh volumes, and
   the Publication stream rows intact for diagnosis and resume. Do not run
   `down -v`, truncate a table, reuse a stream UUID, or rename a fresh volume
   onto a legacy name.
8. Disconnect the new project from the Migration Source network if required
   by the security change. Do not stop or modify the legacy database.
9. If the pre-existing shared QY runtime must be restored, restart only its
   previously approved immutable images against its original
   `bullmq_crawler_migration`, `yewu_business`, legacy Redis, and legacy MinIO.
   Never point an old image at either fresh database and never let old and new
   consumers share one Redis concurrently.
10. To resume the fresh canary, verify identities and fingerprints, rerun both
    role and fresh Publication plans, require phase `complete`, start services
    again in the order in section 7, and reconcile recorded active jobs before
    creating a new cohort.

## 11. Implementation Files

The fresh topology is owned by these source-controlled surfaces:

```text
.env.example
database/bootstrap/crawler.sql
database/bootstrap/business.sql
database/init/10-crawler.sh
database/init/10-business.sh
deploy/compose.yml
deploy/compose.fresh-migration.yml
scripts/bootstrap.sh
scripts/compose.sh
scripts/verify.sh
services/qybullmq/src/databaseIdentity.js
services/qybullmq/src/migrationInventorySchema.js
services/qybullmq/src/migrationInventorySchema.sql
services/qybullmq/src/migrationInventorySync.js
services/qybullmq/src/migrationSource.js
services/qybullmq/src/manualMigrationDispatch.js
services/qybullmq/scripts/applyMigrationChannelInventorySchema.mjs
services/qybullmq/src/freshPublicationBootstrap.js
services/qybullmq/scripts/bootstrapFreshPublication.mjs
services/dashboard/src/migrationCompletion.js
services/dashboard/src/migrationInventory.js
services/dashboard/src/migrationTopology.js
services/dashboard/src/server.js
```

Local Agent, Feature Bridge, Finalize, and all three Business roles have
separate identity tests. Publication Publisher reads only fresh Crawler
Publication tables; Business Ingress, Reconciler, and Projector each use their
own least-privilege Business credential.
