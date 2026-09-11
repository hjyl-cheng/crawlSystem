# Bounded cleanup of retired search snapshots

The user authorized deleting expired search snapshots after inventory, while
retaining rollback releases and all channel, video, comment and source data.
This operation only deletes rows from `public.creator_search_current` and inserts
matching records into `publication.creator_search_legacy_prune_audit`.
Release metadata and `publication.creator_search_changes` remain intact.

## Retention and inventory

The production inventory contained 5,913,681 Legacy search rows. The fixed plan
selects 1,253 retired pre-cutover releases containing 5,846,164 rows for deletion.
It preserves the three most recent complete releases (22,279, 22,529 and 22,709
rows; 67,517 total), the initialized empty release, and any applied cohort
cutover rollback references. The active Live search contains 22,709 channels.

The existing all-at-once prune function was not used: its single transaction
does not satisfy bounded cleanup, and its nonempty-retained-release guard does
not support this deployment's valid initialized baseline with zero rows.
The new manual tool explicitly validates that baseline against its recorded
initialized count, including zero, while requiring full retained snapshots.
No production SQL function or crawler service is replaced by this operation.

## Execution safeguards

`services/qybullmq/scripts/pruneBusinessCreatorSearchLegacy.mjs` produces a
read-only fixed plan by default. `--apply` requires that exact file's SHA256,
the expected Business database, an operator, and an action reason. Its module
uses the same advisory lock order as the existing prune function, acquiring
locks with try-lock calls so a running publisher takes priority.

Each transaction rechecks database identity, incremental/live mode, cutover
identity, current rollback references, retained row counts, retired target
status, and exact remaining target count. It deletes at most 5,000 rows (module
hard maximum 10,000), records an audit in that transaction, and releases locks.
The CLI waits 100 ms between commits, with a 10-second statement timeout and
250-ms lock timeout. It stops on unexpected state/count changes or errors.
SIGTERM stops after the current bounded transaction. After an interrupted run,
generate a fresh plan for the remaining rows; an old plan is deliberately
rejected if its target counts have changed.

Production services remain running. No global timeout, worker count, database
schema or publisher setting is changed. Plain row deletion does not promise
physical disk reclamation; table rewrites and index rebuilds are separate work.

## Isolated validation

The integration test initializes a dedicated PostgreSQL 18 test database with
the real Business bootstrap and incremental search schema. It publishes six
real Legacy releases, activates incremental mode and exercises bounded cleanup.
Five test entries passed with none skipped:

- Protected releases, stale counts and shadow storage mode reject cleanup.
- An independent publisher holding the common lock receives priority.
- Deletion and audit commit together, with exact per-batch and total limits.
- Live documents, retained snapshots, source channel snapshots, release metadata
  and change journals remain identical across cleanup.
- A subsequent real incremental publication succeeds without retaining its
  temporary Legacy rows, followed by successful storage rollback to the oldest
  of the three retained complete releases with exact document equality.

Run against a fresh, bootstrapped local database ending in `_test`:

```sh
CREATOR_SEARCH_PRUNE_POSTGRES_TEST_URL=postgresql://.../creator_search_prune_test \
  node --test services/qybullmq/test/businessCreatorSearchLegacyPruner.postgres.integration.test.js
```

The test writes synthetic fixtures to that dedicated database. The production
execution artifacts for this session are under `/tmp/search-prune-run`.

## Production completion

The fixed plan ran from 08:53:23 to 09:22:23 UTC on 2026-09-09. All 5,846,164
target rows were deleted in 1,969 committed batches, taking 1,739,387 ms (about
29 minutes). Median batch time was 757 ms and maximum batch time was 2,673 ms.
The process exited successfully with no errors. Database audit totals exactly
match the fixed plan and committed batch log; no batch exceeded 5,000 rows.

At 09:23:07 UTC the final read-only verification confirmed:

- Exactly 67,517 Legacy search rows remain across the three retained complete
  versions; their full document fingerprints match the before-cleanup baseline.
- All 22,709 Live search documents have the same complete fingerprint as before.
- Channel count (22,709), channel snapshots (29,673), video/content items
  (471,620), and content snapshots (656,082) are unchanged.
- All monitored source-table deletion counters are unchanged. All 1,257 release
  metadata records and 29,673 search change records remain.
- Five deployed API search responses, detail and facets have identical hashes
  before and after cleanup. During cleanup, sampled searches took 60–283 ms.
- Projector and Reconciler remain running with zero restarts; all 29,673
  projection outbox records are delivered, with no outstanding backlog.

Search Current table/index allocation was 30,554,152,960 bytes before and
30,554,464,256 bytes at verification. Thus row cleanup is complete, but physical
disk allocation has not fallen. PostgreSQL autovacuum was already cleaning up
indexes after scanning/vacuuming the heap. No physical rewrite was part of that row-deletion operation. The user subsequently
authorized the separate online physical reclamation recorded below.


## Subsequent online physical reclamation

The user separately authorized returning the unused allocation to the filesystem.
A local custom-format dump of all retained Legacy search rows was restored into
an isolated PostgreSQL 18.4 database. All 67,517 rows and all three full release
fingerprints matched the production baseline before any physical maintenance.

All 17 search indexes were rebuilt sequentially with PostgreSQL's native
`REINDEX INDEX CONCURRENTLY`, a 2-second lock timeout, and one parallel maintenance
worker. All indexes remained valid and all retained documents remained identical.
Index allocation fell from 22,123,397,120 bytes to about 127 MB. Normal publication
services remained running; no database restart was required.

The remaining heap was compacted with official `pg_repack` 1.5.3, compiled against
the same `postgres:18.4-alpine` base as production. Upstream source commit:
`6902ab313aeb49227867087dca99cf107f7be23b`; downloaded source archive SHA256:
`9439892c0b7c7575677f61133e5692f3e8cfd5c6688c5db4fb202320c668c745`.
The build used a separate maintenance image. Isolation tests verified all restored
rows, that lock conflicts cause skipping without terminating the blocked client,
and that 660 committed concurrent updates survived repacking with the exact final
value and all other fields unchanged.

Production ran only against `public.creator_search_current`, using `--no-order`,
`--jobs=1`, `--wait-timeout=2`, and `--no-kill-backend`. The operation exited zero
without warnings or errors. The temporary extension was removed afterward with
`DROP EXTENSION ... RESTRICT`; maintenance binaries were removed from the production
container, while the verified backup and evidence were retained locally.

Final verification at 2026-09-09 09:59:30 UTC confirmed:

- Total search table/index allocation: 30,554,464,256 → 208,756,736 bytes.
- Physical allocation released: 30,345,707,520 bytes (about 28.3 GiB).
- Heap allocation: 82,591,744 bytes; index allocation: 126,115,840 bytes.
- Filesystem observation: about 271 GiB used / 28 GiB available before, versus
  244 GiB used / 55 GiB available afterward; other running services also use disk.
- All 67,517 retained search rows, their complete per-release fingerprints and
  all 22,709 Live search documents match the original baseline.
- Source counts and source-table deletion counters are unchanged. Search, detail
  and facet API response hashes match the pre-maintenance responses.
- All 17 indexes are ready and valid, constraints remain valid, both existing
  search normalization triggers remain, and no temporary repack tables remain.

This maintenance changes physical storage, not crawl data or publication rules.
Backup and verification artifacts are under `/tmp/search-reclaim-20260909`.
