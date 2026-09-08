# Migration batch controls — 2026-09-08

The migration page supports 100/200/500/1000/2000 and All, live progress, graceful pause/resume, and graceful end. All means the entire eligible migration inventory, not only the restored 23,507 channels and not the current filtered page.

## Execution model

- A request creates a persisted `preparing` batch. The controller freezes eligible channel IDs in one SQL snapshot, excluding existing migration intents, crawler channels, and candidates. IDs arriving later belong to a future batch.
- Admission is bounded to 20 unfinished channels. Lightweight `migration-channel-start` jobs materialize migration intents and candidates under scheduler/batch locks; actual collection reuses the existing YouTubeJS Full Crawl pipeline and shared video API fallback.
- Pause changes status to `pausing`: no additional channel may be admitted. Started channels continue through details, recovery, finalization and publication. After draining, the batch becomes `paused`. Resume retains the frozen list and prior outcomes.
- End changes status to `stopping`, releases pending IDs and drains started work before `ended`. Released IDs remain eligible. Ended batches cannot resume.
- Shared queues are not paused by these controls. The legacy scheduler stays in `finishing` for admitted work. This also prevents starting another batch while the controlled batch is paused.
- Progress counts channel outcomes once, excluding individual retry attempts. `ready_partial` is displayed as dormancy / partial completion. Runtime excludes fully paused periods; throughput and ETA are estimates after sufficient observations.
- Exact pending manual-retry fences represent failed attempts requiring intervention, not automatic recovery still in flight. Active `retrying`/`dispatched` recovery blocks settlement. Unfinished publication blocks draining and appears in the progress panel.
- Request versions reject stale actions. The end confirmation retains the selected batch/version even if polling sees another batch.

## Schema and deployment

Additive tables: `crawler.migration_control_batches`, `crawler.migration_control_items`. Canonical SQL is `services/qybullmq/src/migrationBatchControlSchema.sql`, also included in bootstrap schemas.

Apply with the identity-checked `services/qybullmq/scripts/applyMigrationBatchControlSchema.mjs --apply`; `CONFIRM_MIGRATION_BATCH_CONTROL_SCHEMA_APPLY` must equal the configured `EXPECTED_CRAWLER_DATABASE`. The script does not connect to the legacy source database.

Runtime flag: `MIGRATION_BATCH_CONTROL_ENABLED=true`. Install schema first, then update all channel workers, controller, API and dashboard. Preserve existing worker proxy identities and `MIGRATION_RESTORED_SOURCES_ENABLED=true` in source-reading services. Incremental workers and the daily scheduler need no changes.

For rollback, keep the new coordinator available until its batch is ended or completed; do not disable the flag or restore an old worker while a controlled batch is active. Additive tables and frozen IDs should be retained.

## Validation

- 54 backend regression tests passed, including a real PostgreSQL instance with full crawler/publication schema. Cases cover All freezing, capacity races, pause/drain/resume/end, late jobs, release/reselection, empty batches, controls before preparation, duplicate starts, publication drain, configured failure limits, and automatic versus manual retry settlement.
- Dashboard existing tests passed after moving selector assertions to the new rendered panel.
- Isolated browser fixture tested All submission, live progress, pause, drain, resume, end cancellation/confirmation, ended batch controls and mobile layout. No production batch was started by these tests.

Deployment status will be recorded after runtime verification.
