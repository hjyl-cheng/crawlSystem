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

- Status: current writer path protected; historical repair pending
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
