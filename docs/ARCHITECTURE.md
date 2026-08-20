# Architecture

## Source Lifecycle

`pachongsys` is the editable source of truth. Every self-built image carries
the Git revision in OCI labels. Runtime credentials and identities are mounted
from `runtime/<environment>` and never enter a build context.

```text
source commit -> immutable images -> environment runtime mounts -> containers
```

## Runtime Boundaries

```text
Browser
  -> Nginx + QY Auth
      -> QY Dashboard
      -> BullMQ API / Bull Board
      -> Rota Dashboard and API
      -> MinIO Console

Query terms
  -> Query Quality
  -> Discover
  -> youtube-channel-crawl
      -> Full Crawl / Migration
      -> local Agent branch
      -> Data API fallback when permitted
      -> Finalize
      -> Publication Outbox
      -> Business Projection

Daily Feature Clock
  -> Feature Dispatch
  -> youtube-channel-incremental
      -> Incremental crawl
      -> local Agent incremental
      -> Finalize / Publication
```

## One Crawler Codebase

`services/qybullmq/src/worker.js` is the only queue worker implementation.
Compose changes only `WORKER_QUEUES`, concurrency, and the Rota identity role.
Query and Migration do not own separate Full Crawl implementations:

```text
Query Discover ----+
                   +--> youtube-channel-crawl --> processChannelCrawlV2()
Migration input ---+
```

This prevents a bug fix from reaching Migration while Query or Incremental
continues to run an older copy of the pipeline.

## Data Stores

- Crawler PostgreSQL 16 stores crawler, publication, and feature-clock state.
- PgBouncer is the only application entry point to the Crawler database.
- Redis stores BullMQ jobs and authenticated dashboard sessions.
- MinIO stores raw crawler evidence and finalized payloads.
- Rota owns a separate TimescaleDB/PostgreSQL 17 database.
- Business PostgreSQL 18 stores the published projection.

The local Agent is not a network service. The Agent worker starts the Python
processor as a child process. It inherits the worker's PgBouncer connection
settings and reads the immutable model bundle from the same image.

## Network Identity

Network workers claim a Rota slot before consuming work. A slot is a stable
worker assignment; Rota may atomically replace the underlying proxy when that
proxy cools down. Cookie, browser fingerprint, locale, timezone, and route
generation remain part of the worker execution identity.

Rota owns proxy inventory, health checks, cooldown, reserve selection, and slot
replacement. Workers own YouTube.js, yt-dlp, cookies, visitor data, and parser
state. A failed route is reported to Rota and retry policy decides whether the
same logical job may switch route.

Rota also owns the canonical identity-policy catalog. The qybullmq image copies
that exact file during its build and verifies policy scope, role, locale, and
hash before a network worker consumes jobs.

## Publication Boundary

Crawler state is never written directly into business result tables. Finalize
creates versioned publication revisions, the publisher sends authenticated
shards to Business Ingress, and the reconciler/projector applies ordered
revisions. This preserves gap detection, idempotency, and retraction history.

The Compose topology still enforces three distinct paths:

```text
Crawler database <- publication_publisher -> Publication transport
                                             -> Business Ingress -> Business database
Dashboard audit -------------------------------------------------> Business database
```

Publisher has no Business database network, while Ingress has no crawler
network. Database URLs and transport tokens are mounted as file-backed secrets.
