# Business latest storage

Business publication can now keep one current channel record and one last-known
record per video, instead of copying complete video/profile/metric snapshots for
each About or Video update. Collection, remote execution and API fallback are
unchanged. The default remains `BUSINESS_PUBLICATION_STORAGE_MODE=snapshots`.

## Data and delivery contract

- `latest` reads the accepted `result.*_current` state while holding the existing
  channel ownership lock. It acknowledges pending projection vectors covered by
  that state in the same transaction. It does not bypass validation or acknowledge
  an unaccepted collection result.
- Each adopted channel gets a new, deterministic `publication_current_snapshot_*`
  ID. Existing immutable snapshots are not overwritten. Existing business SQL
  table names, columns and DTOs remain usable.
- Channel/video identities, latest snapshot rows, Search and delivery receipts
  commit together. Unchanged rows are not updated. SQL maintenance is batched
  rather than issuing a separate set of statements for every channel.
- Partial video updates have already been merged by the activator before they
  reach this writer. Videos leaving the accepted window retain their last-known
  row with `is_recent=false`; only active-window rows participate in the live
  display and publication metrics. Leaving a window is not evidence of deletion.
- `publication.latest_projection_state` stores one publication/version pointer
  per channel and rejects backwards versions and inconsistent same-sequence data.
- `publication.channel_metric_history` retains up to 100 observation points per
  adopted channel (subscription count, total views and their statuses), not full
  video/profile payloads. Video-only publications do not duplicate About points.
- Existing immutable snapshots stay available during transition. The history
  reader combines old observations and lightweight points until cleanup.

## Reader deployment is required

The associated business application is a separate repository:
`/root/workspace/agent/kolfront/KOL-proje`.

Its `CreatorsService` changes are part of this rollout: search and detail use one
`REPEATABLE READ READ ONLY` transaction for all constituent queries. Reusing a
current snapshot ID without this change would allow a detail response to combine
old Search fields with newly committed video data. Trend reads use
`creator_channel_metric_history_v1`; the API falls back to its legacy query until
the additive migration is installed. Single-statement contact queries already
have statement-level consistency.

External consumers making several related SQL queries must likewise use a
consistent read transaction. They must not treat a current snapshot ID as an
immutable historical observation.

## Rollout sequence

These steps are the deployment procedure. The 2026-09-24 production rollout and
validation results are recorded in `reports/business-latest-storage-rollout-20260924.md`.

1. Build the new publisher and business API images. Install the additive schema
   using `manageBusinessLatestStorage.mjs install --apply`. This leaves the mode
   at `snapshots`, preserves existing data and grants the existing publisher the
   additional narrowly scoped writes. Role reprovisioning detects the new schema.
   Run each statement in `businessLatestPublicationIndexes.sql` separately outside
   a transaction. The two concurrent indexes avoid scanning delivered history or
   sorting the full queue for every claim. Check `pg_index.indisvalid` after the
   build; an interrupted concurrent build can leave an invalid index that must be
   removed before retrying. Leave the existing historical indexes in place.
2. Deploy and validate the business API reader changes while writes are still in
   snapshot mode. Check search, details, contacts, metrics and trend parity.
3. Gracefully stop only the business publication projector, letting its current
   transaction finish. Collection and durable ingress may continue buffering work.
   Read the current Search watermark. Run `enable --apply --reader-ready` with
   that exact watermark, actor/reason and expected database identity.
4. Start the new projector with `BUSINESS_PUBLICATION_STORAGE_MODE=latest`.
   Pin its dedicated `BUSINESS_LATEST_PROJECTOR_IMAGE`; both Compose definitions
   honor it independently of collection Worker images. The optional
   `deploy/compose.business-latest-storage.yml` also makes the cutover explicit.
   Both definitions allow five minutes for graceful publication shutdown.
   A legacy writer is rejected by
   database triggers after cutover; an unenabled latest writer fails startup.
5. Verify delivered counts, pending work, sampled field parity and storage growth.
   Existing channels adopt latest records on their next publication. First
   adoption temporarily adds a current copy alongside retained old snapshots.

Configuration, supplied via the existing secret/environment mechanism:

```text
BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
EXPECTED_BUSINESS_DATABASE=<verified database>
CONFIRM_BUSINESS_LATEST_STORAGE=<same database>  # install/enable only
EXPECTED_BUSINESS_WATERMARK=<current Search watermark>  # enable only
BUSINESS_STORAGE_ACTOR=<operator>                         # enable only
BUSINESS_STORAGE_REASON=<reason>                         # enable only
```

`node scripts/manageBusinessLatestStorage.mjs inspect` is read-only by default.
Installation and enablement are separate; `--reader-ready` attests that the API
consistency change has been deployed. This is an operator precondition, not an
automatic remote API verification.

## Recovery and historical cleanup

Transaction rollback, duplicate delivery, worker retry and accepted-version
ordering continue to work. Old Search watermark replay/rollback is deliberately
rejected in latest mode: a current snapshot ID cannot reconstruct its old payload.
Recover incorrect business data by a verified new publication, rather than
repointing Search to an old watermark. Do not switch back to an old image or flip
the mode without a separately reviewed recovery procedure.

This change does **not** delete old snapshots, original crawler data or publication
revision chains. Cleanup is a separate operational step after read/write parity:
inventory adopted channels and retain live references, required rollback evidence,
manual classification evidence, metric baselines and source-time repair references.
Copy needed trend points before deleting their old sources. Delete in bounded
batches and measure database maintenance/space reuse before scheduling any physical
table rewrite. Updating current rows still produces temporary PostgreSQL dead
tuples; monitor autovacuum and disk growth after cutover.

Publication inbox/revision/audit tables still retain delivery/recovery evidence.
These are not safe to truncate: activation, auditing and reconciliation reference
them. This implementation stops full business snapshot duplication; it does not
claim to bound every operational journal or reclaim existing disk allocation.

### Bounded cleanup of superseded payloads

`services/qybullmq/scripts/pruneBusinessSnapshotPayloads.mjs` is an operator tool,
not part of the publisher or collection path. Its default is read-only. Supply a
fixed JSON array of unique channel IDs using `SNAPSHOT_CLEANUP_CHANNELS_FILE` and
the existing `BUSINESS_DATABASE_URL[_FILE]` secret mechanism. It verifies
`EXPECTED_BUSINESS_DATABASE` against `publication.database_identity` and requires
both latest storage and live/incremental Search. Writes additionally require
`--apply` and `CONFIRM_SNAPSHOT_PAYLOAD_CLEANUP` equal to that database name.
`--apply --rollback` exercises the actual deletion and verification without
committing it. Save JSONL output to an operator audit file.

The cleanup removes only old `content_snapshots`, `channel_metric_values`,
`channel_profile_facts` and `channel_links` payloads for successfully adopted,
currently visible channels. It retains:

- every channel snapshot header, observation time and trend value;
- the current payload and newest complete immutable payload for each channel;
- any payload referenced by retained Search releases, classification claims or
  runs, metric baselines, or source-time repair records;
- crawler source data, content identities, publication revisions, batches and
  delivery evidence.

Thus old publication identifiers and all existing trend points remain resolvable,
while their unneeded historical video/profile/metric copies can be removed. The
retained immutable payload is comparison evidence; historical Search replay
remains disabled in latest mode. Full historical details for a pruned snapshot
are intentionally no longer available. No header is deleted and no artificial
empty current payload is published.

Each transaction locks at most ten channels using the publisher's ownership lock
order, skips busy owners, rechecks references under snapshot row locks, and removes
at most 100 old payload sets. Unknown incoming foreign keys reject the operation.
Before committing, it compares current/retained payloads, all channel headers,
live Search and trends. A mismatch, timeout or error rolls back the entire batch.
The CLI repeats full batches and pauses 250 ms between transactions; skipped busy
channels are reported for a later pass. Repeating a completed cleanup is harmless.
No service restart or collection task mutation is involved.

DELETE creates dead tuples until PostgreSQL vacuums them; it does not imply that
the relation files or `df` usage immediately shrink. Do not run `VACUUM FULL` or a
large table rewrite as part of this tool. Measure reuse and I/O first, and plan
physical disk reclamation separately.

`bash scripts/testBusinessSnapshotCleanup.sh` validates the tool against a new,
disposable PostgreSQL 18 database, including real foreign keys, classification
references, busy ownership, rollback after a partial DELETE, trend/current parity,
and repeat execution. It does not use a production URL.

## Validation

`scripts/testBusinessLatestStorage.sh` creates a dedicated temporary PostgreSQL 18
database from `database/bootstrap/business.sql`; it never uses production URLs.
The real ingress, activator, projector, Search functions and restricted publisher
permissions run against it. Tests cover About-only updates, one changed video
with another preserved, duplicate delivery, backlog coalescing, stale versions,
SQL rollback, window exits, stable row counts, unchanged-row writes, legacy
snapshot preservation, migration reapplication and 100-point trend retention.

Run the business API's latest-storage, creator mapping and data-source tests and
TypeScript check in its repository before deploying its image.
