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
