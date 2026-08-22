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

- Status: fixed in source, reproducible, and soak-tested; deployment pending
- Symptom: Business Publication Reconciler iterations repeatedly fail with
  `could not resize shared memory segment ... No space left on device` while
  Ingress, Publisher, and Projector remain running with zero restarts.
- Root cause: one audit iteration launched all 11 inspection queries with
  `Promise.all`. Queries 0, 1, and 6 selected Parallel Hash plans and query 8
  selected a Parallel Seq Scan, each with two workers. Their overlapping POSIX
  dynamic shared-memory growth exceeded the Business PostgreSQL container's
  64 MiB `/dev/shm`. The Reconciler's four-channel activation concurrency was
  not the direct cause; it completed before the audit fan-out started.
- Isolation evidence: the Business PostgreSQL container has the Docker default 64 MiB
  `/dev/shm`, PostgreSQL uses `dynamic_shared_memory_type=posix`, and
  `max_parallel_workers_per_gather=2`. The failure existed before the BUG-8
  rollout and continued unchanged after it. From `11:03 UTC` through the audit
  boundary, the new Reconciler logged 1,367 matching failures. Idle `/dev/shm`
  usage is low; the error is a concurrent parallel-query burst, not host disk
  exhaustion.
- Controlled reproduction: `npm run diagnose:business-publication-dsm` now
  preserves the exact one-million-row, 128-byte Parallel Hash fixture, starts
  concurrent queries behind a PostgreSQL advisory-lock barrier, reads the
  target container's configured `shm_bytes`, and refuses non-`_test`
  databases. On PostgreSQL 18.4 with POSIX DSM, `work_mem=4MB`, two workers per
  Gather, and 64 MiB `/dev/shm`, concurrency 1 succeeded 1/1, concurrency 2
  failed 1/2 with SQLSTATE 53100, and concurrency 4 failed 3/4. Four concurrent
  transaction-local serial plans succeeded 4/4. The identical fixture at 256
  MiB succeeded 1/1, 2/2, and 4/4, proving that capacity only moves the failure
  threshold. Parallel and serial-plan p50 were about 958 ms and 1,883 ms on the
  original production-shaped query before SQL optimization.
- Prevention: audits now use a dedicated one-connection Pool whose startup
  options set both `max_parallel_workers_per_gather=0` and
  `debug_parallel_query=off`. Every audit also opens one `REPEATABLE READ READ
  ONLY` transaction, applies both settings again with `SET LOCAL`, and runs all
  11 unnamed, tagged queries serially from one snapshot. Single-flight
  admission blocks overlapping audits. A failed audit is contained and retried
  on a bounded 30/60/120/300 second schedule without failing core
  reconciliation. Channel activation concurrency remains four; PostgreSQL
  parallelism for every other role remains enabled; neither Compose file
  increases `shm_size`.
- Query performance: all inspections compute exact counts from narrow rows and
  construct JSON only for top-N samples; the former full-result `row_number()`
  sorts are gone. On a 100,000-row synthetic Projection dead-letter set, query
  8 improved from 704 ms and 4,932 temporary write blocks to 141 ms and zero
  temporary writes. The complete small integration audit improved from 37/45
  ms p50/p95 to 36/39 ms.
- Verification: the final query set completed 156 consecutive real audits in
  25.234 seconds with p50/p95 51/67 ms, zero failures, zero DSM errors, and zero
  final issues. An earlier 211-audit gate on the same isolation design also had
  zero failures. A separate 3,600.177-second joint Publisher, Ingress,
  Reconciler, Auditor, and Projector load on PostgreSQL 18.4 with 64 MiB
  `/dev/shm` processed 720 Channels and 2,160 Revisions. All 120 audits
  succeeded, all queues drained, no dead letter was created, every final row
  count matched exactly, and the final audit reported zero issues. Publisher,
  Reconciler, and Projector p50/p95 loop latency was 7/13 ms, 6/11 ms, and
  10/16 ms; audit p50/p95 was 135/184 ms as the dataset grew.
- Throughput non-regression: a saturated 300-Channel run against equal-size
  test databases measured `origin/main` versus the candidate at 47.608/48.042
  Publisher Revisions/s, 90.459/90.461 Ingress Revisions/s, 29.557/31.713
  Reconciler Channels/s, and 4.423/4.409 Projector Channels/s. Both runs
  delivered all 900 Revisions, matched every cursor and final search row, and
  created one delivered Projection per Activation with no dead letters. The
  committed capacity mode enforces a non-flaky 90% floor of the baseline for
  each role (42.8, 81.4, 26.6, and 3.98/s); a separate final run measured
  47.073, 88.896, 29.302, and 4.792/s and passed all four gates.
- Runtime evidence: every audit reports query labels and durations, failed
  query, failure kind, last attempt/success, next attempt, consecutive
  failures, and cumulative attempts, successes, failures, and DSM failures.
  The load gate independently reports Publisher, Ingress, Reconciler, and
  Projector productive capacity and p50/p95 latency, rather than treating the
  configured arrival rate as throughput.
- Guarantee boundary: core PostgreSQL SELECT plans cannot enter Parallel Query
  DSM after both audit-local settings are applied, and the Auditor uses no
  named prepared statements that could retain an earlier generic plan. This
  guarantees that this Auditor does not create Parallel Query DSM. It does not
  claim that unrelated sessions, extensions, or parallel maintenance can never
  exhaust DSM for the whole PostgreSQL cluster.
- Production safety: diagnosis used read-only transactions only. No production
  configuration, data, container, or deployment was changed or restarted.
- Rollout: build an immutable image, run the schema-free Reconciler canary,
  confirm its ready event reports audit pool 1, Gather workers 0, and debug
  parallel off, then observe audit failure counters and all four throughput
  rates for 30 minutes before normal release. Roll back only the application
  image if any gate regresses; this fix has no schema migration, PostgreSQL
  restart, or `shm_size` change.

## INC-20260822-010: Stale Channel Pressure Permanently Paused Query Discover

- Status: fixed and regression-tested; production rollout in progress
- Symptom: the final Query canary completed Query Quality and created its first
  managed Discover Page, but `youtube-discover-page` remained globally paused.
  Controller reported a Channel failure rate of 87% and a proxy cooldown ratio
  of 41.8% despite zero current Channel failures and Rota reporting 833 active
  exits with zero exits in cooldown.
- Root cause: Channel pressure selected the last 200 terminal task events with
  no time boundary. The sample therefore retained 174
  `BUSINESS_RUN_KEY_CONFLICT` failures emitted during one migration burst on
  2026-08-20, plus only 26 later completions. Independently,
  `proxyUnavailableRatio` preferred `(total-active)/total` even when Rota
  supplied an explicit cooldown count, so archived and other non-running
  inventory was incorrectly classified as cooling.
- Prevention: Channel pressure now uses at most 200 terminal samples from the
  last 900 seconds, backed by the existing `(queue_name, created_at DESC)`
  index. Rota pressure now computes `cooldown/(active+cooldown)` whenever both
  lifecycle counts are present and retains the total-based calculation only
  for legacy payloads that omit cooldown. Controller telemetry reports the
  active sample window.
- Verification: the original production data returns zero terminal failures
  inside the corrected 15-minute window, and the live Rota payload computes a
  cooldown ratio of zero. Dedicated regression tests cover stale-event expiry,
  explicit lifecycle counts, and the legacy fallback. The complete repository
  suite passed: 1,015 QYBullMQ tests, Dashboard, Auth, Feature Dispatch, 312
  Python tests plus subtests, and all Rota Go packages. Source verification
  also passed.

## INC-20260822-011: Deferred Video Repair Was Re-enqueued Before Its Due Time

- Status: fixed and regression-tested; production canary pending
- Symptom: the final Query canary repeatedly failed the same logical Content
  Repair Job eight times in about four minutes. Several Video Candidates had
  already been classified as `deferred` with `next_attempt_at` around
  2026-08-22 19:45 UTC, but the Controller immediately re-enqueued them around
  13:45 UTC instead of waiting for their scheduled retry.
- Root cause: the Content Completeness selector and the independent Final
  Repair candidate policy checked missing fields and retry state but ignored
  the persisted Video disposition schedule. Resetting Candidate attempts and
  replacing a terminal BullMQ Job with the same deterministic ID amplified the
  loop, but neither behavior was the primary defect: the Candidates should not
  have been eligible before `next_attempt_at` in the first place.
- Prevention: one shared disposition policy now serves both repair paths.
  Normal and stored Candidates remain immediately eligible, while `deferred`
  and `terminal_excluded` Candidates are eligible only when a valid
  `next_attempt_at` is due. The JavaScript policy fails closed when that
  timestamp is missing or invalid; the PostgreSQL predicate has equivalent
  null-safe behavior.
- Verification: focused Content Repair, Final Repair, and Video disposition
  tests cover a future schedule, a due schedule, and missing or malformed
  timestamps. The complete repository suite passed with 957 QYBullMQ tests,
  Dashboard, Auth, Feature Dispatch, 312 Python tests plus subtests, and all
  Rota Go packages. Source verification also passed.
- Production safety: `youtube-channel-crawl` was paused only after it had no
  active, waiting, or delayed Jobs. The obsolete Controller and Channel Worker
  canaries were then stopped gracefully. Queue history, failed Jobs, database
  state, and all other services were preserved.
- Rollout: build an immutable QYBullMQ image from the committed source, replace
  the stopped canaries, resume the preserved Query cycle, and prove that a
  future-scheduled Candidate is not dispatched before its due time. Promote
  the image only after Query reaches completion and Crawler Current, local
  Agent, Finalize, Publication, and Business Current agree for an accepted
  Channel.
