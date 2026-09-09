# Creator Search incremental storage verification

The existing `shadow/legacy` to `incremental/live` storage transition eliminates
full Search release copies during normal publication. Production was switched
using the guarded administrator on 2026-09-09 at 07:46:57 UTC after isolated
tests and a publication drain. Historical data was not pruned.

## Production evidence

At 2026-09-09 06:12:53 UTC, a single `REPEATABLE READ READ ONLY` transaction
compared the active Legacy release with Live, excluding only `watermark`:

- 19,879 channels, zero missing or different documents.
- 100 leased and 2,730 pending Projection Outbox records.
- All 22,709 ownership records were online/active.

These are point-in-time observations, not parameters for a future cutover.
The earlier storage survey found about 26.4 GiB in Legacy Search and indexes,
with 1,235 retired releases. Historical pruning was not executed.

The deployed `kollavo-api-1` Creator service reads `creator_search_live` for
searches, totals, details and facets. During cutover, its real service and data
source were invoked inside the deployed container against `newcrawler_business`
with read-only sessions. This exercises deployed queries, but is not an
authenticated browser or HTTP end-to-end acceptance test.

## Compatibility fix

`publicationReadinessReport.js` previously always joined the active watermark
to `creator_search_current`. Incremental publication removes its temporary
Legacy rows, so the report incorrectly saw zero Business channels. The real
PostgreSQL regression reproduced `0 !== 1000` after a successful incremental
publish. The report now selects Legacy or Live according to the storage read
mode, within one SQL statement, including scoped channel filters. No publication
eligibility rules, crawlers, queues or network settings changed.

## Isolated verification

PostgreSQL 18.4 was initialized from `database/bootstrap/business.sql` and the
current `businessCreatorSearchIncrementalSchema.sql`. The test creates 1,000
synthetic channels and snapshots, then exercises the real publication and
cutover functions. Business schema constraints and triggers remain enabled.
All fixture changes roll back at the end.

| Measurement | Legacy | Incremental |
| --- | ---: | ---: |
| Channels in search | 1,000 | 1,000 |
| Channels changed in one publish | 10 | 10 |
| Rows inserted into Legacy Search by that publish | 1,000 | 10 |
| Retained rows for the new Legacy release | 1,000 | 0 |

The 99% reduction applies to Legacy Search inserts in this fixture, not total
database growth. Live updates, change history, source snapshots and indexes
still require storage. The final run measured 7,953 ms versus 4,978 ms for the
publication calls in a CPU-limited test container; these are not production
throughput predictions.

Coverage:

- Full document equality against the Legacy control after the same update.
- Keyword search, country/language and subscriber filters, sorting and paging.
- Snapshot/detail joins and unchanged Live rows retaining their physical tuple.
- Three subsequent update batches and channel removal without full copies.
- Stale watermark and equal-count data mismatch rejecting cutover.
- Readiness reports before and after cutover, including channel filtering.
- Rollback restoring the original documents and search results, and reactivation.
- Existing administrator, Projector, runtime and publish-lock concurrency checks.

Final results: 32 management/Projector tests, 7 readiness tests and 7 integration
test entries passed; zero skipped. Logs for this session are in
`/tmp/search-storage-regression.log`, `/tmp/search-readiness-unit.log`,
`/tmp/search-storage-integration-green.log`. The failing compatibility regression
is recorded in `/tmp/search-storage-readiness-red.log`.

To repeat the integration test, bootstrap an empty isolated PostgreSQL 18
database ending in `_test`, set `CREATOR_SEARCH_INCREMENTAL_POSTGRES_TEST_URL`
to its localhost connection, and run from `services/qybullmq`:

```sh
node --test test/businessCreatorSearchIncremental.postgres.integration.test.js
```

## Production transition

The existing guarded administrator documented in
[CURRENT_SYSTEM_OPTIMIZATION_PLAN.md](CURRENT_SYSTEM_OPTIMIZATION_PLAN.md#101-creator-search-存储管理)
was used without relaxing its zero-open-projection guard. Image
`qy-allpachong/qybullmq:pachongsys-b36c362` contains the readiness fix and was
also deployed to the crawler API. The fresh plan showed 22,709 Live and active
Legacy rows, zero parity differences, zero open projections, and zero abnormal
ownership records. Apply completed with no warnings.

Before and immediately after cutover, the full Live document fingerprint
(excluding only watermark) was `d089dd1850d86a36b2501aafdb58b9eb`, with all 22,709
channels retained. Five deployed API searches (default, keyword, BR/pt filters,
second-page average views sorting, and engagement sorting), detail, and facets
had identical response hashes. Search totals were respectively 22,709, 80,
22,349, 22,709 and 22,709. Individual queries took 39–160 ms before and
40–136 ms after; these are smoke observations, not a throughput benchmark.

The original Projector and Reconciler were restarted with their original
configuration. Both reported ready, running with zero restarts and no new errors.
The Projector/Reconciler source files are unchanged between their retained image
revision `60a7314` and `b36c362`; the SQL storage mode selects the new write path.
At 07:51:36 UTC, the database remained incremental/live, all 29,673 projection
records were delivered, and all Live rows joined their channel snapshots.
The updated readiness report's real Business query also passed for 100 channels.
Its separate full-dataset diagnostic had exceeded a 30-second timeout; full-report
performance remains a limitation, not a failed storage parity check.

No new projection inputs had arrived after resumption at that observation, so
no post-cutover incremental release or production publication-speed improvement
was measured. Changed-row-only writes, subsequent releases, exact results and
rollback were verified in the isolated integration test above. Observe the next
normal publication for production timing and zero retained Legacy rows rather
than generating synthetic production records. The cutover Legacy release is
preserved for rollback. Historical pruning and disk reclamation are separate work.


## Production drain observations (2026-09-09)

The guarded administrator requires no open Projection Outbox records. Reconciler
and Projector were stopped cleanly, and a temporary Projector using the same real
transaction, version-vector and retry code drained existing publications. Crawling
workers and result ingress continued running. These are business/search publication
records, not new channel crawls or mere task dispatches.

Eight batches of 250 committed between 06:42 and 07:14 UTC, taking 262–289 seconds
each. The next 250 and a smaller 100-record attempt rolled back on the legacy
search statement's 300-second timeout. No task was manually marked delivered.
430 records remained. A temporary container then used a 900-second statement
limit and matching 1,200-second lease, with JIT disabled; the normal services'
timeout and lease configuration were unchanged. Its first 250 committed at
07:39:08 UTC in 328,842 ms; the final 180 committed at 07:44:22 UTC in 313,394 ms.
The drain then reported zero remaining records and exited successfully.

The prolonged window reflects the cost of draining through the old full-copy
search publisher, including the conservative drain-before-cutover workflow. It
is not evidence that dispatching 1,000 IDs should take minutes. Search Current
and indexes occupied about 28 GB during this window, versus about 62 MB for Live.
Neither historical pruning nor physical disk reclamation was performed.
