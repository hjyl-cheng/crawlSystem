# Migration batch controls — 2026-09-08

The migration page supports 100/200/500/1000/2000 and All, live progress, graceful pause/resume, and graceful end. All means the entire eligible migration inventory, not only the restored 23,507 channels and not the current filtered page.

## Execution model

- A request creates a persisted `preparing` batch. The controller freezes eligible channel IDs in one SQL snapshot, excluding existing migration intents, crawler channels, and candidates. IDs arriving later belong to a future batch.
- The controller maintains a buffer of up to 100 pending `channel-snapshot` jobs in `prioritized`. A worker atomically admits its channel under scheduler/batch locks, persists the normal snapshot payload, and immediately enters the existing collector. Actual concurrency remains the existing 20 workers; downstream Agent/publication work does not consume crawl admission capacity. The legacy launcher remains supported only for rolling upgrades.
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

## Production verification

Deployed on 2026-09-08 around 08:00 UTC:

- 20 channel workers, controller and API: `qy-allpachong/qybullmq:pachongsys-28fa83a-migration-control`.
- Dashboard: `qy-allpachong/dashboard:pachongsys-deb46c6-migration-control` (includes the display fix for ending before list preparation).
- API and dashboard health checks passed; all upgraded services had zero restarts. Controller ticks continued normally.
- 20 incremental workers and the daily scheduler remained running on their previous versions.
- Inventory rows: 401,325. Eligible unstarted channels for All: **398,192**. No controlled migration batch was created during validation.
- Public URL continues to require administrator login. The actual deployed page was verified through the server's internal network: All present, start enabled, progress HTTP 200, no JavaScript errors. Public authentication was not changed.
- Backend regression log: `/tmp/migration-control-tests7.log`; dashboard regression: `/tmp/migration-dashboard-tests-final.log`; isolated browser screenshots: `/tmp/migration-ui-desktop.png`, `/tmp/migration-ui-mobile.png`; deployed-page check: `/tmp/migration-ui-production-internal.log`.

Live containers preserve their original network aliases, proxy identities, mounts and resource limits. For a future Compose recreation, retain `MIGRATION_BATCH_CONTROL_ENABLED=true`, `MIGRATION_RESTORED_SOURCES_ENABLED=true` for source readers, and the image tags above (`QYBULLMQ_IMAGE_TAG` / `QY_DASHBOARD_IMAGE_TAG`). These flags default off in the generic Compose template so older images are not accidentally enabled before schema/worker rollout.

## Incident and repair: missing controller source configuration

The first real controlled batch, `migration-a3ccb9ee-e130-4ae2-aa88-d5da6154e128` (100 channels), exposed a deployment gap: the controller had not previously read migration source snapshots, so it lacked `MIGRATION_SOURCE_ID`, the three `EXPECTED_MIGRATION_DATABASE*` identity fields and the read-only migration connection secret. The initial integration tests injected a snapshot loader and did not test that runtime dependency. Collection code itself had not run: source hydration failed with `EXPECTED_MIGRATION_DATABASE is required` before candidate/intent creation or queue admission.

The old source-error handler incorrectly consumed three attempts per item and labeled those unstarted items as final failures. The batch was paused for repair. Commit `e583823` adds the controller Compose reader configuration/secret and changes source-read failures to pause admission while preserving pending items, with an error visible on the page. Resume clears the control error. A real-reader integration regression went red on the original behavior and passed after the fix; the 54 backend regressions and dashboard regressions passed again.

Controller, API and dashboard were deployed as `pachongsys-e583823-migration-control`. The controller's read-only mount and exact source identity settings were copied from the already verified API; worker images remained `28fa83a` because their collector/admission logic did not change. In the actual controller, all 100 source snapshots were read and identities checked successfully before resuming the batch.

Recovery reset only 80 affected items with the exact configuration error, no candidate ID and no started timestamp. The frozen 100-channel selection was preserved. At **08:12:39 UTC**, the original batch had **20 started / 80 pending / 0 final failures**; its 20 candidates were accepted, 20 real Runs were in `waiting_detail`, and the channel queue had **20 active / 0 failed**. No additional batch was created. This verifies actual production admission and execution, rather than only the progress display; completion of all 100 was still in progress at this observation.


## Continuous queue admission repair

The first control implementation unintentionally limited entire unfinished channel lifecycles to 20 and dispatched a separate launcher before the actual snapshot. That depleted the prioritized queue while Agent/publication work was still running and added controller-tick latency. This was a dispatch regression; the YouTubeJS collector, feed pagination, video details, proxy policy, API fallback and incremental executor files were unchanged.

The corrected controller prepares up to 100 pending **real** `channel-snapshot` Jobs. Once a worker picks one, it checks the batch state, creates the candidate/intent, reserves the existing candidate execution fence for that exact Job, writes the canonical snapshot payload and immediately enters the original worker processing path. A database commit preceding an interrupted Redis payload write can be replayed using that exact owner. The standard snapshot job ID, retry/backoff, retention and source priority are retained.

Paused/ended placeholders never create candidates or use the collector and are removed on completion, allowing resume to requeue the same IDs. Already admitted channels and their retries still drain completely. The `max_in_flight` column remains only for compatibility with old launchers during rollout; direct snapshots rely on BullMQ worker concurrency.

Validation: 56 regressions passed with an isolated full PostgreSQL schema and real Redis. The production-scale fixture put 100 Jobs in prioritized, ran 20 concurrent fetches, and verified that the 21st starts immediately after one fetch ends even though downstream work remains unfinished. Pause prevented all remaining queued channels from starting; resume and end preserved/released the correct IDs. The same test covers an interrupted payload write and the original candidate execution fence. The previously requested real 100-channel batch had already completed with 97 ready_auto and 3 ready_partial before this queue repair rollout; no extra real batch was requested for validation.

Continuous admission repair deployed as **`qy-allpachong/qybullmq:pachongsys-9c207da-migration-control`** to all 20 channel workers, controller and API. Workers were updated before switching the controller. All upgraded services ran with zero restarts; API health was healthy. The controller's actual source-read probe passed after the upgrade. Dashboard remains `e583823`; the 20 incremental workers and daily scheduler were not redeployed. Final read-only audit showed the original batch completed (97 success / 3 dormant), no active batch, and progress HTTP 200. No additional production collection batch was created. Validation log: `/tmp/migration-prefetch-20-tests.log`; runtime audit: `/tmp/migration-prefetch-runtime-verified.log`.

## All-scale and lifecycle API repair (2026-09-08)

The real All batch `migration-94c0e7ac-db1f-43d0-b208-5aaf47a3363c` froze 397,392 IDs but its controller stalled in outcome reconciliation before admission. Bulk insertion left tiny-table statistics in place. An isolated 400k fixture reproduced poor planner choices: per-ID snapshot hydration chose the pending-state index and filtered channel ID afterward, scanning the large pending set repeatedly. An already running production settlement query remained active for over 17 minutes despite subsequent auto-analyze.

Changes:
- Explicitly ANALYZE the control items table after freezes of 10,000+ IDs, before the batch becomes running.
- Materialize started IDs and settled outcomes before the UPDATE join, so pending inventory is not pulled into outcome resolution.
- Extract the existing controlled-write middleware and narrowly allow POST batch pause/resume/stop. Those endpoints were previously rejected with HTTP 423; unrelated writes remain restricted.
- Preserve the existing 100-pending-ID admission window and deterministic BullMQ job IDs. No changes to collector, concurrency, or worker dispatch payloads.

Verification:
- 6 tests passed including the HTTP middleware, ordinary lifecycle SQL, and a 400,000-ID PostgreSQL fixture with autovacuum disabled. First admission improved from approximately 24 seconds to 327 ms. Repeated ticks retained only 100 distinct queued IDs. Large-batch end took 19,973 ms; all 400,000 IDs were eligible for a subsequent All batch.
- An additional real Redis/PostgreSQL integration test passed: 20 active workers continuously refill from prioritized jobs, pause drains active work, resume continues, and stop prevents queued-but-unstarted channels from being admitted.
- Validation logs: `/tmp/all-migration-after.log`, `/tmp/all-migration-redis.log`.

Deployment: API and controller now use `qy-allpachong/qybullmq:pachongsys-all-control-fix-20260908`. This is a targeted overlay on the previously deployed `9c207da` image containing only `server.js`, `controlledMigrationGuard.js`, and `migrationBatchControl.js`; uncommitted country-recheck collection changes were not deployed. Workers and dashboard were not redeployed.

During rollout, the old API exceeded its 30-second stop window and exited 137 (not OOM); it was replaced with the new healthy API. The old controller received SIGTERM, its specifically identified stuck statement was cancelled, and it exited cleanly. Old containers remain stopped as rollback backups with suffix `-before-all-control-fix`.

The user's previously requested stop was submitted through the repaired production API and returned HTTP 200. Final audit: batch ended, all 397,392 items released, zero candidate IDs, all 397,392 still eligible under the actual migration inventory filters. Progress HTTP 200, active batch null. API healthy, both updated containers running with zero restarts. No replacement production migration batch was started.

## Ended-batch display repair

The main progress panel now renders only an active batch. When no batch is active it clears stale progress and displays “当前无运行批次，可以开始迁移”. Completed/ended records remain in the collapsed recent-batch history; ended rows show the number retained for later migration. No batch records or inventory are deleted.
