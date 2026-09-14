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
