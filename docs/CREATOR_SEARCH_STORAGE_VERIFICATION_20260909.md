# Creator Search incremental storage verification

The existing `shadow/legacy` to `incremental/live` storage transition eliminates
full Search release copies during normal publication. This change prepares and
tests that transition; production storage mode and historical data were not
changed during this verification.

## Production evidence

At 2026-09-09 06:12:53 UTC, a single `REPEATABLE READ READ ONLY` transaction
compared the active Legacy release with Live, excluding only `watermark`:

- 19,879 channels, zero missing or different documents.
- 100 leased and 2,730 pending Projection Outbox records.
- All 22,709 ownership records were online/active.

These are point-in-time observations, not parameters for a future cutover.
The earlier storage survey found about 26.4 GiB in Legacy Search and indexes,
with 1,235 retired releases. Historical pruning was not executed.

The deployed `kollavo-api-1` compiled Creator service reads
`creator_search_live` for searches, totals, details and facets. This is code-path
inspection, not an authenticated browser or HTTP end-to-end acceptance test.

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

Use the existing guarded administrator documented in
[CURRENT_SYSTEM_OPTIMIZATION_PLAN.md](CURRENT_SYSTEM_OPTIMIZATION_PLAN.md#101-creator-search-存储管理).
It requires zero in-flight Projection records. The production observation above
does not yet satisfy that condition. Arrange a publication drain window, deploy
the readiness compatibility fix, obtain a fresh read-only plan and execute the
exact guarded transition. Verify subsequent releases keep Live current without
retaining full new Legacy snapshots; verify the consumer-facing queries and
readiness report after publication. Preserve the cutover Legacy release for
rollback. Historical pruning and physical disk reclamation are separate work.
