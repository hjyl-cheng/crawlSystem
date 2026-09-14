# Return unstarted migration failures to normal execution

This rollout is limited to the frozen 125,488-item Rota incident manifest and the
original migration batch. It does not restart remote incremental services.

## Cause

The full-batch candidate census dates to 40b78bc (2026-08-31); the YouTubeJS
Full Crawl wrapper retained a per-channel census in 7dee060 (2026-09-07).
The 2026-09-12 admission optimization did not remove this completion-path work.
A 2026-09-14 snapshot found 30 concurrent copies, oldest 39 seconds. The existing
Controller already refreshes counts/validation every 30 seconds.

The Rota failure cohort had entered the exceptional recovery pipeline, where
Agent jobs contain one channel and stage transitions wait for historical scans.
This change selects normal execution only for still-pending cohort items. The
same Candidate/Intent locks allocate a durable G+1 Outbox before atomically
resolving the old retry as `handed_to_normal_migration`. This means handoff, not
successful collection. The batch item becomes started and later settles from
its real result. Old generation fences, source data and failure evidence remain.
Running, queued, successfully completed and already-collected executions are
not reallocated. Their existing recovery continues.

## Performance boundary

Per-channel reconciliation checks the controlled batch by primary key and
returns without scanning Candidates. The Controller's authoritative
`closeValidation: true` reconciliation and ordinary Query batch behavior remain.
This requires no new schema. The independent publication backlog SQL remains a
separate known performance issue; this change does not claim to fix that query.

## Verification and operation

The PostgreSQL regression reproduces 40 blocked completion calls under a held
Candidate relation lock. After the fix all 40 return, while background counts
still correctly record failures and close validation. Admission integration
covers pending/accepted failures, pause, newer generations, duplicate refill,
allocation rollback, terminal-item exclusion and generic Finalize eligibility.
Existing recovery and controlled migration tests must also pass.

`services/qybullmq/scripts/returnMigrationFailuresToNormal.mjs --manifest PATH`
validates and describes the manifest. `--execute` marks eligible pending members
in bounded transactions; `--limit N` permits a bounded live sample. Replaying is
idempotent. The script never enqueues the entire manifest. The original
Controller supplies the normal queue through its existing bounded refill.

Deploy the Controller changes and the census helper in migration Workers only.
Pause migration intake, let active migration jobs drain before replacing any
Worker, preserve old containers/configuration with restart disabled, verify the
new consumers, then resume the existing batch. Do not reset active results,
clear leases, restart the retired standalone dispatcher, or restart databases.
The exact production samples and private deployment snapshots stay ignored.

## Coexisting normal/recovery Agent work

The ordinary producer previously subtracted all queued recovery jobs from its
three-Worker capacity and could never enqueue a normal batch behind recovery.
Count only ordinary outstanding batches against that producer's bounded window.
Both classes still share BullMQ's unchanged global execution concurrency and
FIFO. The PostgreSQL/Redis regression holds three recovery executions with seven
more queued, requires three normal batches to queue without exceeding three
active Workers, then releases recovery and verifies normal batch/tail completion
without duplicate channels. This fails before the producer accounting change.

## Agent eligibility scan under the real recovery backlog

The live normal producer hit its 15-second SQL timeout despite having queue
room. A read-only comparison limited to selecting 20 channels found the old
query timed out at 12 seconds. Reordering a CASE predicate also timed out;
materializing eligible channel/run IDs before reading their Run JSON returned
20 rows in 5.7 seconds in that sample. Do not describe CASE ordering as the fix.
The final producer preserves its original criteria and rechecks them in the
locking query after the materialized shortlist. Existing real PostgreSQL/Redis
normal/recovery coexistence and tail tests pass on this query as well.

## Stop reading Run payloads once one Agent batch is full

Materializing eligible identities alone still let the outer priority sort read
all remaining Run JSON values before LIMIT. Under live load the Agent producer
continued to hit its 15-second timeout. An ordered identity subquery followed by
a correlated, locking LIMIT 1 checks each Run only as needed; the outer LIMIT
stops after one full batch. Priority, current Run identity, authoritative Run
batch metadata, recovery exclusions and channel/Run SKIP LOCKED all remain.
A read-only comparison returned 20 rows in 9.2 versus 4.7 seconds and reduced
read blocks from 31,558 to 16,450; this is a sample, not a throughput guarantee.
The actual PostgreSQL dispatch regression fails before the change: selecting 20
inspects all 65 fixture Run payloads. It passes after the change at 20. Tests
also verify skipping another batch, a locked channel, a locked Run and a future
retry while still selecting the next 20 eligible channels in priority order,
as well as legacy pipeline_cycle_id ownership, normal/recovery coexistence,
shared concurrency, no duplicate channels and a five-channel tail.

The first bounded-query production rollout still timed out: its read-only probe
had used three parallel participants, while the UPDATE executes serially.
Rechecking with max_parallel_workers_per_gather=0 reproduced the 12-second
SELECT timeout. Materializing the active recovery candidate IDs once replaces
thousands of random recovery index lookups with a set anti-join; this returned
20 rows in 10.0 seconds in that same serial comparison. The final locking check
still queries the real recovery table and preserves the original exclusions.
Real PostgreSQL tests cover pending/retrying/dispatched exclusion and
resolved/cancelled eligibility, in addition to the prior concurrency/lock cases.
