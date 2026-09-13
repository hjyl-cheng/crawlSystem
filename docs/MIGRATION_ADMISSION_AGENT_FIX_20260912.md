# Migration admission and Agent dispatch, 2026-09-12

This change removes full-batch recounts from controlled migration admission and gives Agent dispatch an independent controller cycle. It does not change YouTubeJS collection, API retry rules, execution ownership, or incremental collection.

## Evidence before repair

The production batch is `migration-19eff406-4ec3-442e-a448-882b14caae69`. At approximately 04:20–04:25 UTC, all 40 crawl consumers were occupied and more than 100 prioritized tasks remained. Video volume was stable: the 03:15–03:30 UTC window averaged 27.01 processed details/channel versus 27.32 at 04:00–04:15. Completed jobs fell from 737 to 653 per 15 minutes while average job duration rose from 46.04 to 51.61 seconds.

`startControlledMigrationChannel` holds the scheduler/batch admission locks, then calls `materializeControlledMigrationChannel`. The latter called `refreshBatchCounts`, scanning all candidates twice for every new channel. The batch already had approximately 177,000 candidates. `pg_blocking_pids` identified transactions doing these counts as owners of the scheduler lock, with other admissions/settlement waiting 5–7 seconds. At 04:00–04:15 UTC, 178 of 180 settlement cycles failed with a lock timeout.

Agent dispatch was in the main controller tick, after the slow publication onboarding backlog scan and other maintenance. A snapshot found 1,507 ready channels awaiting Agent with the Agent queue empty. This differs from the separate Finalize backlog (approximately 200–240 jobs).

## Implementation

- Controlled admission updates batch/page candidate totals by one inside the existing transaction, only after a genuinely new candidate is inserted. Replays and occupied channels return before the increment; rollbacks undo it. Ordinary manual migration keeps its existing recount behavior.
- `agentBatchDispatch.js` contains the existing Agent selection/queue logic with the same eligibility, recovery exclusions, configured worker capacity and batch sizing.
- Agent tail eligibility uses an indexed existence check for open candidates, instead of recounting the whole batch on each dispatch.
- With independent controller loops enabled, `agent_dispatch` runs every 2 seconds on its own database pool. The old main tick does not also dispatch Agent jobs. A separate 30-second `batch_counts` cycle reconciles authoritative status totals and validation completion.
- The legacy controller mode retains its main-tick dispatch and full validation reconciliation.
- Scheduler/config readers accept an optional database query dependency; existing callers retain their default.

The delta counters remain operational statistics. Periodic reconciliation remains authoritative for totals; task admission, pause/end checks, and ownership continue to use their existing records and locks.

## Validation

The new admission regression failed on the old production path with `admission must not scan all prior candidates while holding the scheduler lock`, then passed after repair. The same real PostgreSQL test covers frozen All selection, replay/fences, pause/drain/resume/end.

The 400,000-channel All test passed on the unchanged refill/control path. The repaired 40-consumer PostgreSQL/Redis integration test passed while a separate SQL query was blocked for 120 seconds (cancelled after the test), including pause admission fencing and real controller startup/shutdown with both new cycles.

A new real PostgreSQL/BullMQ test fills three Agent consumers while maintenance is blocked, enqueues 60 unique channels in three batches, then waits for validation before sending the remaining five. It checks capacity, duplicate avoidance and tail eligibility.

Related Agent policy, retry exclusions, migration dispatch/control/completion, throughput and lifecycle tests passed. Production Node 20 image imports were verified.

## Deployment

Worker image: `qy-allpachong/qybullmq:migration-admission-20260912`, based on the existing migration image and replacing only `manualMigrationDispatch.js`.

Controller image: `qy-allpachong/qybullmq:migration-agent-dispatch-20260912`, based on the existing production controller image and replacing the five relevant source modules.

Compose override: `deploy/compose.migration-admission.yml`. Private runtime configuration backups and deployment timestamps are under ignored `runtime/remote-center-production/migration-admission-20260912/`.

Deployment drains the controller, pauses only crawl queue intake, waits for active jobs to reach zero, then replaces the 40 idle containers. It verifies the replacement consumers before starting the new controller and resuming intake. Existing incremental node containers and database services are not restarted.

Production resumed at **04:54:55 UTC**. The controller exited cleanly, and the queue reached zero active jobs before any migration Worker was stopped. All 40 replacement consumers registered before intake resumed. The incremental center kept its original 03:24:01 UTC start time.

Startup checks held intake longer than planned: Worker 22 retried eight times while a previous Rota lease was still live; Workers 25–28 each retried once after an initial Rota timeout. Read-only Rota records confirmed the previous conflicting lease expired at 04:48:03 UTC, a new unique lease was acquired and renewed, and Docker start times/restart counts remained unchanged for several minutes. Intake was resumed only after those checks. These startup retries occurred with the migration queue paused, before any replacement Worker received a channel. At 04:59:21 UTC, all 40 were still running, the controller had zero restarts, and none of those startup restart counters had increased.

Initial production observation: 36 of 37 settlement cycles succeeded, averaging 539 ms, compared with 178/180 lock timeouts in the pre-repair window. This removes the sustained lock blockage; it does not claim all database contention is eliminated. A separate, slow publication onboarding scan remains outside this patch's scope.

The faster Agent dispatcher exposed existing incomplete profile inputs for Nicollas Jesus and Jeffart Studio (country/language/audience estimates unavailable). Their original validation and delayed retry policy are retained; no fields were fabricated to make those tasks pass.

## Post-deployment throughput and remaining limit

The first complete window, 04:55–05:00 UTC, completed 197 crawl jobs for 197 distinct channels (48.66 seconds/job, 25.72 details/channel), 34 successful Agent batches covering 680 channels, and 216 Finalize jobs. Two crawl jobs failed with the existing Rota route-budget error; two Agent batches reported incomplete profile inputs. These are queue-stage completions, not a count of fully published migrations.

A second observation, 05:00–05:04:40 UTC (280.386 seconds), completed 241 crawl jobs for 241 distinct channels, approximately 51.6 channels/minute or 3,094/hour at that short-window rate. Average duration was 46.93 seconds with 27.73 details/channel. All 40 crawl consumers were active and 109 prioritized jobs remained. The window had no new crawl, Agent, or Finalize failures. Agent completed 44 successful batches covering 880 channels; Finalize completed 230 jobs. Ready/pending Agent work fell from 1,602 just after resumption to 212, with 45 running and four failed at the final snapshot. No scheduler-lock waiters were observed in that snapshot. This is a short measurement, not a sustainable daily throughput estimate.

Finalize queued work grew to 1,685 while the independent Agent dispatcher cleared old backlog. A 05:01:45 UTC sample contained 1,141 queued jobs with 1,141 distinct run IDs: 853 Agent-complete, 108 channel-full-fetch-complete, 109 migration-activity-dormant, and 71 controller-finalize-source-change. This supports Agent catch-up as the main source of the new backlog; it does not establish duplicate enqueueing. The existing single Finalize consumer is processing work, but end-to-end publication throughput is not yet demonstrated to match the catch-up rate. Finalize concurrency and publication onboarding SQL were not changed in this repair.

The final container check found all 40 migration Workers running, zero controller restarts, and unchanged restart counters for the startup incidents described above. The temporary drop to two active crawl jobs occurred during the deployment drain, before replacement; it was not the post-deployment concurrency.
