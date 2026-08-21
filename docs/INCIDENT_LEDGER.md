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
