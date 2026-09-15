# Remote node intake control contention

Saving intake from 45 to 10 and preparing deployment from 45 to 46 could time out. Idle claim transactions held node/Worker row locks while queuing for the same allocation advisory lock. Claim and status queries also scanned completed task history. Deployment administration then waited for those locks and exceeded its lock timeout.

## Changes

- Take the allocation lock without waiting, before taking node/Worker row locks. Retry contention with jitter outside the transaction.
- Check for claimable work without a locking transaction. Notify the intended slot, retaining a timeout read to recover lost notifications.
- Index pending, live-slot and live-lease lookups separately from completed history; retain generation, scope, lease and replay checks in the authoritative claim.
- Persist desired intake in `remote_ingestion.node_intake_requests`. Reconcile only differing Worker flags with `SKIP LOCKED`; the supervisor also checks desired intake before starting a new channel.
- Keep active channel draining unchanged. Report a committed setting as saved even if the subsequent status read fails.
- Avoid locking existing immutable Worker registrations during additive deployment. Preserve deployment identity validation and serialization.

No collection policy, Clock generation policy, migration dispatcher or migration Worker implementation changes are part of this patch.

## Validation

The original code reproduced both the blocked intake save and blocked additive deployment in isolated PostgreSQL. Regression tests cover busy Worker rows, 45→10→0→45, stale edits, configuration persistence, post-commit observation failure, 120,000 completed task records, replay, occupied-slot exclusion, and additive registration while allocation is busy. Actual PostgreSQL notifications and NATS tests cover targeted wakeup, rollback, idle waits and contention retry. Existing execution-control, deployment, capacity and activation integration tests were also verified. Dashboard execution/runtime/capacity tests passed.

Run the PostgreSQL test files sequentially (`--test-concurrency=1`); they share the guarded isolated fixture database. Never point these tests at production.

## Deployment

Run `scripts/applyRemoteIntakeUpgrade.mjs` with an explicit database URL and `EXPECTED_CRAWLER_DATABASE`; it defaults to a dry run and requires `--apply`. The four indexes build concurrently before the small metadata transaction. An interrupted invalid index must be inspected and removed individually before retrying. Old snapshots can delay concurrent index validation; do not terminate unrelated collection work to accelerate it.

Deploy the center and Dashboard only after the upgrade completes. Existing remote collector images need no replacement. Keep the metadata and valid indexes on an application rollback; older applications ignore the new table. Restore the prior notification function if rolling back the center, to recover its node-wide wakeup behavior.

During this deployment, the daily Plan generator held a coordination transaction for over five hours and prevented index validation. It was stopped under the user's existing request to pause daily Clock execution. Committed Plans are retained. The incremental queue and dispatcher remain paused; migration must remain running.

## Production verification

On 2026-09-15, all four indexes became valid and the targeted metadata upgrade completed. The center and Dashboard were deployed from commit `329e4ee`. A long background publication scan also delayed index validation; it ended naturally before the guarded cancellation request matched any query.

- The actual Dashboard control client saved 45→10 in 188 ms and 10→45 in 221 ms. Final allowed intake was restored to 45.
- Before this deployment, `incremental-30` had stopped heartbeating at 01:34 UTC despite its container remaining up. It had no pending, leased or received task. Restarting only that container restored its connection; its underlying hang is not diagnosed by this intake patch.
- Retried the user's failed desired deployment of 46 through the Dashboard deployment coordinator. All deployment steps completed in 20 seconds and `appliedCount` became 46. Existing containers were preserved by the additive deployment path; the allowed intake count was not automatically increased.
- The migration queue remained unpaused with 40 active tasks. The incremental queue remained paused with 0 active and 62 paused jobs. The daily generator and dispatcher remained stopped.
- A final database sample found zero remote-ingestion lock waiters. This is a point-in-time verification, not a long-duration throughput benchmark.
