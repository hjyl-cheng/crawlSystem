# Incident Ledger

## INC-20260821-001: Daily Clock Plan Was Not Generated

- Status: fixed in source; deployment pending
- Symptom: `2026-08-21` had eligible Channels but zero Daily Plans.
- Root cause: `shared-qy-workers` took over QYBullMQ Controller and Workers but
  left both `feature-scheduler-daily` and `feature-dispatch` disabled. The
  corresponding legacy processes had already been stopped.
- Prevention: the shared takeover overlay now owns both Clock roles, and a
  regression test requires them in the merged runtime topology.

## INC-20260821-002: New Videos Were Crawled But Not Published

- Status: fixed, deployed, and publishable historical repair completed
- Example: Lisa Barcelos (`UCElNAGclgkuKa0bKRtUg3Ig`).
- Symptom: Crawler Current contained the `2026-08-16` videos while Business
  Current remained at `2026-08-05`.
- Root cause: a legacy comment backfill wrote comment count/status fields
  without refreshing `publication_item_hash`. The integrity gate correctly
  rejected the resulting Video Current as `video_item_hash_mismatch`.
- Scope at diagnosis: 8,541 Channels and 77,396 mismatch issues.
- Prevention: current comment backfill persists only the unhashed first-page
  evidence and tests forbid it from mutating count/status fields. Existing
  stale hashes must be refreshed with the guarded
  `backfillVideoPublicationItemHashes.mjs` command, followed by normal
  Publication reconciliation. Historical Revisions and Business tables must
  not be edited directly.
- Repair tooling: a Video-only reconciliation must validate the Business
  database identity, Channel count, and active watermark, but it must not load
  or compare a Channel preservation baseline. Preservation is required only
  when the requested Domains include `channel`. A non-empty active watermark is
  revalidated during Apply, but normal watermark advancement invalidates a plan
  only when Channel preservation was requested.
- Repair outcome: the guarded reconciliation classified 8,447 successfully
  evaluated Channels as 7,032 `revised`, 147 `no_change`, and 1,268
  `not_ready`. All 7,032 exact Video Revisions reached the Crawler Outbox
  `delivered` state with no error. Business Video Current is at the exact
  repaired Revision for 6,630 Channels and at a newer Video Revision for 402;
  none are behind or conflicting. Of the 7,032 repair effects, 7,017 produced
  an exact delivered Projection Outbox row, while 15 were already superseded
  before a separate Projection row was required. The 1,268 evidence-incomplete
  `not_ready` Channels remain isolated for evidence repair and were not
  force-published. Historical Revisions and Business result tables were not
  edited directly.

## INC-20260821-003: Shared Feature Services Used a Bundled Database Alias

- Status: fixed in source; deployment validation pending
- Symptom: the restored Scheduler could not resolve `crawler-pgbouncer`, and
  Feature Dispatch restarted after the same DNS failure.
- Root cause: shared-runtime adoption updated `CRAWLER_DB_HOST` but left the
  file-backed `feature_database_url` using both the disabled bundled service
  name and the isolated database's different password.
- Prevention: adoption now copies the complete credential from the working QY
  Feature service, rewrites its endpoint with a structured URL parser,
  preserves file mode, and rejects an unexpected database role before
  replacing the file atomically.

## INC-20260821-004: Large Publication Shards Timed Out During Repair Drain

- Status: mitigated at runtime; fixed in source; permanent deployment pending
- Symptom: the Video Repair burst produced 70-90 item Shards that repeatedly
  entered `retry_wait` with `fetch failed`, while smaller retries succeeded.
- Root cause: the Publisher claimed 100 Revisions per batch and allowed a Shard
  up to 4 MiB, but its authenticated Ingress request timeout remained 15
  seconds. Large Business transactions exceeded the client deadline even
  though Publisher, Ingress, Reconciler, and Projector stayed healthy with zero
  restarts and no OOM event.
- Prevention: the Compose default is a configurable 10-Revision Publisher
  batch. The bounded batch was verified through the real Outbox and Ingress
  path before taking over the drain, and a Compose regression test freezes the
  default.

## INC-20260821-005: An Already Absent Retraction Blocked Its Projection Batch

- Status: fixed and deployed
- Symptom: a Business Projection batch repeatedly failed the
  `creator_search_changes_check` constraint. One already removed Channel caused
  24 unrelated active Channels in the same batch to roll back and retry.
- Root cause: the target Channel Revision was a valid retraction, while the
  Channel was already absent from `creator_search_live`. The Projector treated
  that achieved target state as a new remove operation and attempted to record
  a change with neither a before nor an after document.
- Prevention: before creating a Projection batch, the Projector now proves the
  exact target Channel Revision is a `retract_channel` Revision and the search
  row is already absent. It then marks that Projection covered by the current
  state. The database history constraint remains strict, and a regression test
  prevents no-op removals from creating a batch.
- Production verification: the original retraction Channel
  `UCCQPs-PkhIiWKhSQreD6xmA` and an unrelated upsert Channel from the same
  failing batch, `UCCBSukVTHzh-eo_vbaX7xZg`, both reached `delivered` with no
  remaining error after the fixed Projector took over.

## INC-20260821-006: Channel Detail Displayed Only One Run's Content

- Status: fixed, deployed, and fleet-verified
- Symptom: Channel detail pages displayed an older latest video even when both
  Crawler Publication Current and Business Current contained newer videos.
  Fleet audit found 15,569 active Channels whose displayed latest video lagged
  the authoritative Crawler Current, plus 4 active Channels with an empty
  displayed list.
- Root cause: `crawler.contents` is a Channel-level canonical catalog with one
  row per source video. Its `run_id` records the last Run that touched each row;
  it is not a snapshot boundary. The Dashboard filtered the catalog by
  `channels.latest_run_id`, hiding every current video last touched by another
  Run, and ordered those remaining rows by a Run-local position.
- Prevention: Channel content statistics and the current-content table now read
  the complete Channel catalog and order it by publication time. Run-specific
  candidate diagnostics remain scoped to `latest_run_id`. A regression test
  forbids a Run filter in the current-content queries.
- Deployment: Dashboard image
  `qy-allpachong/dashboard:pachongsys-9a4b664-channel-current`, built from full
  Git revision `9a4b664fa0df14c50c322b7f98c802bc9e2640a6`, replaced only the
  `qy-newcrawler-dashboard-1` service. It became healthy with zero restarts;
  Workers, Rota, queues, and databases were not replaced.
- Production verification: Lisa Barcelos now displays video `hO1VuSXgweM`
  published on `2026-08-16`. The same online-page assertion passed for 50
  deterministically sampled formerly stale Channels. A read-only fleet audit
  over 23,563 active Channels found zero canonical Dashboard catalogs behind
  Publication Current and zero canonical catalogs incorrectly displayed as
  empty. The previous `latest_run_id` query still reproduced 15,576 stale
  Channels, proving that this was a population-wide query defect rather than a
  Channel-specific data repair.

## INC-20260821-007: Audit Script Polluted Shared PgBouncer Backends

- Status: fixed, fully deployed, and production-verified
- Symptom: Incremental Workers intermittently failed PostgreSQL writes with
  SQLSTATE `25006` (`cannot execute INSERT/UPDATE in a read-only transaction`).
  Successful and failed writes were interleaved across multiple Workers.
- Root cause: an external audit script executed session-level
  `SET default_transaction_read_only=on` through the transaction-pooled shared
  PgBouncer. The modified PostgreSQL backend sessions were returned to the
  shared pool and subsequently reused by write Workers. The first write error
  followed the audit script timestamp by approximately 0.27 seconds.
- Prevention: production code relies on the PostgreSQL role's normal default
  permissions. Read-only audits use a transaction-scoped `BEGIN ... READ ONLY`
  only when needed. Dashboard and migration tooling no longer request a
  session-level read-only default, and a repository test rejects equivalent
  settings in production source and runtime configuration.
- Deployment: Dashboard and all 53 running QYBullMQ roles use immutable image
  tag `pachongsys-3e65734-pgbouncer-readonly`, Git revision
  `3e657340a2df72fcbfe430d7bbf9314e0a13dbe5`. This includes API, Controller,
  20 Channel Workers, 20 Incremental Workers, five specialist Workers,
  Feature Relay, Crawler Outbox Publisher, and the four Publication roles.
  All have zero restarts. Three old QYBullMQ repair containers whose only
  command was `infinity` were stopped without deletion. Feature Engine,
  Feature Dispatch, Auth, Rota, PostgreSQL, PgBouncer, and Nginx contained no
  matching setting and were not rebuilt for this incident.
- Publication ownership: commits `ab927ec` and `6e65178` moved the four shared
  Publication roles to a source-controlled Compose topology under this
  repository. The deployment launcher pins the project name and immutable
  QYBullMQ tag, so a copied `/tmp` Compose file cannot silently retake them.
- Pool cleanup: `RECONNECT bullmq_crawler_migration` was issued through the
  PgBouncer admin console after the application rollout. Sixteen immediate
  samples reached two new backends and reported both
  `default_transaction_read_only=off` and `transaction_read_only=off`.
- Write verification: the initial temporary-table DML probe was rejected as
  insufficient because PostgreSQL permits temporary-table writes in a
  read-only transaction. The final probes used each runtime role to execute a
  zero-row `UPDATE` against a normal table inside a rolled-back transaction.
  `bullmq`, `publication_publisher`, `business_publication_ingress`,
  `business_publication_reconciler`, and `business_publication_projector` all
  passed with `off/off`. A 1,000-transaction, 16-concurrency PgBouncer stress
  run rotated across 17 PostgreSQL backend PIDs with zero read-only errors and
  zero persistent row changes.
- Recovery: the final population was 12 Incremental Runs affected by the
  polluted pool. Eleven had already completed through bounded BullMQ retries.
  The remaining Job for `UCLGNJYRIp1fY2l7KQq-uAxA` had exhausted all five
  attempts; a guarded BullMQ retry resumed the same Plan and Run, completed
  both About and Video, and moved the Daily Plan to `succeeded`. Three attempt
  rows whose failure finalization had itself been blocked were closed as
  `failed` only after a successful replacement attempt was proven. Their
  `result_json` records controlled recovery operation
  `inc-20260821-007-attempt-close-v1`. All 12 Runs and Daily Plans are now
  successful and no affected attempt remains `running`.
- Post-rollout log verification: all 54 affected running containers were
  scanned from the rollout boundary; no SQLSTATE `25006` or read-only
  transaction error was present.

## INC-20260821-008: Runtime Environment Overrode Publication Compose Project

- Status: fixed, deployed, and regression-tested
- Symptom: the first source-controlled Publication rollout created
  `qy-newcrawler-business-publication-ingress-1` instead of replacing the
  existing `bullmq-publication-runtime` Ingress.
- Root cause: `COMPOSE_PROJECT_NAME=qy-newcrawler` in the ignored runtime
  environment has higher precedence than the Compose file's top-level `name`.
  The Publication launcher did not explicitly override it.
- Containment: the original Publication Ingress remained healthy throughout.
  The duplicate stateless container was gracefully stopped and removed; no
  database or volume was removed.
- Prevention: `scripts/publication-compose.sh` now passes
  `--project-name bullmq-publication-runtime`. A regression test requires that
  fixed project name and rejects legacy or `/tmp` deployment paths. All four
  running Publication roles now report
  `deploy/compose.qy-publication-runtime.yml` in their Compose labels.

## INC-20260821-009: Business PostgreSQL Dynamic Shared Memory Exhaustion

- Status: open; isolated from INC-20260821-007
- Symptom: Business Publication Reconciler iterations repeatedly fail with
  `could not resize shared memory segment ... No space left on device` while
  Ingress, Publisher, and Projector remain running with zero restarts.
- Evidence: the Business PostgreSQL container has the Docker default 64 MiB
  `/dev/shm`, PostgreSQL uses `dynamic_shared_memory_type=posix`, and
  `max_parallel_workers_per_gather=2`. The failure existed before the BUG-8
  rollout and continued unchanged after it. From `11:03 UTC` through the audit
  boundary, the new Reconciler logged 1,367 matching failures. Idle `/dev/shm`
  usage is low; the error is a concurrent parallel-query burst, not host disk
  exhaustion.
- Required follow-up: reproduce the exact Reconciler query plans and measure
  peak dynamic shared memory before choosing between a role-scoped parallelism
  limit, lower Reconciler fan-out, or a larger Business PostgreSQL `shm_size`.
  Do not attribute this incident to PgBouncer or change database permissions.
