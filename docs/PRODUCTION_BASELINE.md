# Production Baseline

This document records the initial recovery evidence. It does not make any old
workspace tree or running container an editable source location; ongoing work
belongs only in `pachongsys`.

## Capture Date

The source bundle was reconciled against the running QY containers on
2026-08-19 UTC. Online containers were inspected read-only; no online service,
queue, database row, or object was modified during recovery.

## Qybullmq Reconciliation

The online deployment was not using one uniform image. Representative roles
were running these image tags:

- Channel: `url-mailto-guard-channel-20260819-v2`
- Incremental: `url-mailto-guard-incremental-20260819-v3`
- Finalize: `url-mailto-guard-finalize-20260819-v2`
- Controller: `url-mailto-guard-controller-20260819-v2`
- Discover and Query Quality: `rota-slot-v2-20260814-candidate8`
- Data API: `data-api-public-replay-20260818-v1`
- Publication: `publication-contract-sync-20260817-v4`
- Local Agent: `bullmq-crawler-qy-local-profile:20260814-v5`

The recovered QYBullMQ inventory contained 185 source files. At capture time,
183 matched at least one active role image byte-for-byte. The remaining two
files, `commentFirstPageBackfill.js` and `contentRepair.js`, are tested merges of
behavior that was split across active role images.

The canonical tree contains 175 source files. It removes nine legacy Worker-side
Proxy Guard/Reconciler files that were not started by the current Compose and
whose responsibilities now belong to Rota. It also removes the Worker's copied
identity-policy JSON; the qybullmq image now copies Rota's canonical catalog at
build time. Current Worker adapters, Rota control clients, identity handling,
and retry policies remain in QYBullMQ. All queue roles build from this one tree.

## Other Services

- Dashboard source was extracted from the running `crawler-dashboard-qy`
  container because no known local tree matched its `server.js` hash.
- Feature Dispatch source was extracted from the running publisher container.
- Feature Engine uses the running Ingest contract variant, which includes
  unresolved-video compatibility fields. Its canonical copy also includes the
  tested counter-overlap correction: persisted-success and current-failure
  counts are bounded independently because they can refer to the same video.
- Local Agent Python source matched the online container's 32 source files.
- Auth source matched the running gateway and its known source tree.
- PgBouncer is pinned to the live `1.25.2` image digest instead of a mutable
  tag.
- Rota uses the source tree associated with the live Compose labels and includes
  the 2026-08-18 task-lease repair. Its Go binary was not reverse-engineered.

## Schema Snapshots

`database/bootstrap/*.sql` and `database/reference-snapshots/*.sql` were
produced with `pg_dump --schema-only --no-owner --no-privileges`. The files
contain no table data. `business.sql` includes the live
`idx_business_publication_projection_predecessor` index. `rota.sql` may show
TimescaleDB internal objects and is reference-only for fresh deployment.
