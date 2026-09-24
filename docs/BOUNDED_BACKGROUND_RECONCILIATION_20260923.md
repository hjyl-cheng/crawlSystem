# Bounded publication and terminal Run reconciliation

The former publication backlog query joined and sorted historical channel and
delivery tables before applying LIMIT. Terminal Run repair similarly used an
unpaged UPDATE. Both shared the crawler database with incremental collection.

Publication recovery now reads at most 200 channel IDs by primary-key cursor.
It applies cheap eligibility predicates to those IDs and processes at most the
existing publication batch limit (25 by default). The unchanged publication
writer rechecks online delivery, ownership, capture boundaries and the complete
Initial Package. Delivery aggregation is no longer part of backlog discovery.
Normal Finalize commits still synchronize Run state and reconcile publication.

The cursor advances atomically with each successful publication transaction.
When more candidates remain than the processing budget, the next page starts
after the last processed candidate, not after the whole input page. A failed
candidate stops that page without advancing past it; previously committed work
is retained. Persistent failures must be alerted and repaired, not silently
skipped. Repeated not-ready results do not block page progress.

An additive, small `crawler.background_reconciliation_scans` table stores scan
positions, round upper bounds, ownership leases and completed-round times. It
is not a publication request queue. A fixed upper bound lets a round finish
under continuous inserts; the next round revisits changes and inserts behind
the cursor. Database row locks and lease tokens fence stale scanners. Business
ownership checks remain authoritative. Each publication candidate uses its own
transaction; no database transaction spans a network operation.

Terminal repair checks at most 100 current channel/Run pairs per page. Active
cycles use the existing dispatch-batch/channel index; a separate global audit
covers historical records and Runs outside candidate membership. The original
latest-Run, channel-status, finalized-status and optional cycle predicates are
preserved. A pipeline cannot complete on a partial audit, and the existing open
work checks still run after the audit wraps.

With independent Controller recovery enabled, publication pages run every
`PUBLICATION_ONBOARDING_SCAN_INTERVAL_MS` (default 2000 ms). The existing
`PUBLICATION_ONBOARDING_RECONCILE_INTERVAL_MS` is now the pause between complete
rounds (default 60000 ms), not between pages. The global terminal audit runs one
100-channel page every 2 seconds and pauses one hour between complete rounds.
These intervals are starting budgets, not completion-time guarantees. The
actual audit duration must be monitored, especially if publication processing
or a persistent failure slows a page. No hot-table index or trigger is added.

## Deployment

Apply the additive cursor schema before replacing the Controller:

```
CONFIRM_BACKGROUND_RECONCILIATION_SCHEMA_APPLY=<expected crawler database> \
  node scripts/applyBackgroundReconciliationSchema.mjs --apply
```

The command also requires `EXPECTED_CRAWLER_DATABASE` and the standard database
connection configuration. It has transaction-local 1-second lock and 10-second
statement timeouts. `schema.sql` contains the same table for new databases.

Build the committed QYBullMQ source and replace only the Controller after a
graceful drain. The worker image need not change: normal publication code is
unchanged apart from the backlog-only selector. Keep the old immutable image
and container configuration. Rollback restores that Controller image; retain
the cursor table and its contents for a future retry.

Watch `controller_work_cycle` for `publication_onboarding` and `terminal_runs`:
examined, scanned/updated, failed, wrapped and after_channel_id. Inspect cursor
age and completed_rounds to detect stalled historical coverage. Measure SQL
blocks, database physical reads, incremental success throughput and publication
age separately; a lower SQL cost does not by itself prove higher crawl throughput.

## Validation before release

- PostgreSQL integration tests cover concurrent claims, expired ownership,
  rollback, process restart, fixed upper bounds, changes behind the cursor,
  65 candidates with a 25-item processing budget, failure recovery, scoped and
  global Run repair, partial-round completion and idempotent replay.
- Existing publication eligibility tests still cover online routes, immutable
  promotion evidence, capture timing, ownership and complete Initial Packages.
- A 20,000-history-channel isolated benchmark compared actual old/new SELECTs:
  old 201,754 shared block hits and 418–590 ms; bounded 2,411 shared block hits
  and 5–7 ms over three warm-cache runs. These are synthetic candidate-selection
  measurements with different result counts (25 old, 199 prefiltered new), not
  end-to-end production performance or physical-read savings.
- Production EXPLAIN without ANALYZE confirms the 200-channel prefilter and
  100-channel terminal UPDATE use indexed lookups; the only remaining sequential
  scan in the publication prefilter is the small publication stream catalog.
- Isolated Controller startup observes the two new loops and existing migration,
  API, Agent and Finalize loops, with a clean shutdown and no cycle/tick errors.

Evidence and release observations are kept in
`runtime/background-reconciliation-release-20260923/` and the corresponding
release report. No production EXPLAIN ANALYZE was used.
