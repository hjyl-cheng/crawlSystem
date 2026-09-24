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

No command below has been run against production as part of implementation.

1. Build the new publisher and business API images. Install the additive schema
   using `manageBusinessLatestStorage.mjs install --apply`. This leaves the mode
   at `snapshots`, preserves existing data and grants the existing publisher the
   additional narrowly scoped writes. Role reprovisioning detects the new schema.
2. Deploy and validate the business API reader changes while writes are still in
   snapshot mode. Check search, details, contacts, metrics and trend parity.
3. Gracefully stop only the business publication projector, letting its current
   transaction finish. Collection and durable ingress may continue buffering work.
   Read the current Search watermark. Run `enable --apply --reader-ready` with
   that exact watermark, actor/reason and expected database identity.
4. Start the new projector with `BUSINESS_PUBLICATION_STORAGE_MODE=latest`.
   Both Compose definitions expose this variable. A legacy writer is rejected by
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
