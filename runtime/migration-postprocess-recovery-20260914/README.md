# Migration retry post-processing recovery

Batch: `migration-19eff406-4ec3-442e-a448-882b14caae69`.

The 125,488-channel Rota recovery cohort remains in its original batch. The old
standalone retry dispatcher must stay stopped: its cursor is a handoff boundary,
not live completion progress.

## Cause and change

Ordinary Agent dispatch excludes active system retries. Their Agent/Finalize
handoff and settlement depended on the Controller main tick, which could wait
minutes on unrelated maintenance SQL. Throughput mode now runs that same fenced
reconciler in a dedicated, non-overlapping work loop with its own bounded DB
pool. Main-tick mode retains the previous behavior. New Agent and Finalize
recovery jobs are limited by queue high-water marks (100 and 250 respectively).
Scanning first selects bounded IDs using separate active/legacy cursors before
joining channel payloads; the existing fairness and execution fences remain.

## Validation

- Real PostgreSQL/Redis Controller regression failed before the change: blocking
  the main tick prevented the recovery Agent job from appearing.
- The same regression passes after the change, also checking a full queue blocks
  additional enqueue and freeing capacity resumes enqueue without main-tick progress.
- Real PostgreSQL/Redis integration verifies the old Content Detail incarnation
  is fenced and system retry ownership survives Agent, Finalize and settlement.
- Controller work-loop and recovery unit tests: 15 passed, no skips.
- A read-only production baseline records 300 recently collected channels:
  134 waiting for Agent, 156 dormant awaiting publication, 10 already published.

## Finalize queue fairness

The production smoke test found that historical Finalize compensation kept its
200-job queue budget full (its 40-job refill can reach 239 queued jobs). Sharing
that threshold starved system retry handoffs even after Agent completed. The
system retry high-water mark is 250, leaving bounded room above that historical
producer. A real Controller regression reproduces starvation at 239 queued jobs
and verifies handoff at 239 while still refusing additional work at 250.

## Online index preparation

Use CREATE INDEX CONCURRENTLY outside a transaction for these schema indexes:

```sql
CREATE INDEX CONCURRENTLY IF NOT EXISTS migration_system_retry_active_scan
ON crawler.migration_system_retry_items(system_retry_id)
WHERE status IN ('retrying','dispatched');
CREATE INDEX CONCURRENTLY IF NOT EXISTS migration_system_retry_legacy_scan
ON crawler.migration_system_retry_items(system_retry_id)
WHERE status='resolved' AND resolution='job_completed';
```

Check `pg_index.indisvalid` before accepting scan timings. A concurrent build can
wait for the daily Scheduler's transaction-scoped coordination lock snapshot;
do not terminate that live plan-generation transaction just to finish the index.

Only the migration Controller needs rollout. Do not restart collection Workers
or reset running/successful channels. Environment backups, baseline IDs and raw
probe outputs remain ignored runtime artifacts and must not be committed.
