# Business snapshot cleanup — 2026-09-24

The user requested cleanup of old business snapshots and then explicitly requested
returning their disk allocation to the operating system. This operation targets
redundant business publication payloads, not crawler source data or revision chains.

## Retention and logical cleanup

The fixed manifest contains 4,999 channels already adopted into latest storage,
with valid current snapshot/Search references. Its SHA256 is
`efc6e5c3e910e307b8702b2a3d2ca23b7794b150f85415bd338318cb9a978f2e`.
The executed cleanup script SHA256 is
`fcac2bebfa9633346f843358cab7e7e2700d8ff2f6a31b1f95d734bca86ad01a`.

The default read-only preflight identified 32,113 eligible obsolete payload sets.
Committed totals exactly match that preflight:

| Historical payload | Deleted rows |
| --- | ---: |
| Video/content snapshots | 853,683 |
| Channel metric values | 2,472,701 |
| Channel profile facts | 321,130 |
| Channel links | 63,518 |

All channel snapshot headers, observation times and trend values remain. Each
channel retains its current payload and newest complete immutable payload.
Retained Search releases, classification claims/runs, metric baselines and
source-time repair references protect their complete source payloads. Non-adopted
channels and retracted channels are outside this manifest. Publication receipts,
revisions, content identities and original crawler data were not deleted.

Execution ran from 11:36:32 to 11:58:11 UTC, including the initial committed sample.
There were 505 committed transactions, 503 with payload deletion. Each transaction
covered at most ten channels and 100 old payload sets. Median transaction time was
2,176 ms; maximum was 3,647 ms. No channels were skipped and no batch failed.

Every deleting transaction revalidated references while holding the publisher's
channel ownership lock and source snapshot locks, and checked current/retained
payloads, all headers, live Search and trends before committing. Production first
exercised ten channels / 44 old payloads with a forced rollback, then committed
the same sample before the full pass.

## Validation

`bash scripts/testBusinessSnapshotCleanup.sh` passed against an isolated
PostgreSQL 18 database with real schema and foreign keys. Tests cover current and
fallback preservation, manual/model classification references, metric baselines,
Search and repair references, busy publisher ownership, unknown dependency
rejection, wrong database rejection, rollback after a partial DELETE and repeat
execution.

`SNAPSHOT_REPACK_TEST=1 bash scripts/testBusinessSnapshotCleanup.sh` also passed.
It used the existing PostgreSQL 18.4 / pg_repack 1.5.3 maintenance image, verified
50,007 link rows and 117 concurrent updates, checked exact field fingerprints and
valid indexes, and verified that conflicting readers survive while maintenance
skips the table.

Three deployed business API searches/details remained identical before deletion,
after deletion and during physical maintenance. These were real nonempty searches
using platform-prefixed creator IDs. The final during-maintenance check took
356 ms for the combined smoke check. Runtime database checks reported no broken
current references and no increase from the existing 13 dead-letter publications.

## Physical table reclamation

Three initial online attempts on `channel_links` skipped on lock contention,
without cancelling business connections. The publisher was continually holding
table locks across its publication transactions.

Only `qy-newcrawler-fresh-business-publication-projector-1` was then drained using
SIGTERM with no forced-kill deadline. It completed its current batch and exited
zero. It was stopped from 12:09:00 to 12:18:26 UTC, about 9 minutes 26 seconds.
The operation's `finally` block restarted the same container with its existing
configuration. Collection, remote-node center, durable ingress, the business API
and the crawler and business databases remained running. Incoming results continued queuing for
publication. The resumed publisher passed startup checks and published another
100 channels / 116 delivery acknowledgments.

The two full-table repacks used `--no-order --jobs=1 --wait-timeout=2
--no-kill-backend`. Both completed without warnings or errors in the drained
publication window:

| Table | Baseline allocated bytes | After allocated bytes | Reduction |
| --- | ---: | ---: | ---: |
| `channel_links` | 2,228,830,208 | 1,947,328,512 | 281,501,696 |
| `channel_profile_facts` | 20,550,647,808 | 18,967,986,176 | 1,582,661,632 |

Combined physical reduction at that measurement: **1,864,163,328 bytes (1.736 GiB)**.
Other ongoing writes can increase these tables subsequently.

Complete historical row counts and PostgreSQL full-record hash sums with two
different seeds matched exactly before and after repacking: 2,161,374 link rows and
11,504,840 profile rows. Current API details also remained identical. Every index
on both tables was ready and valid.

The temporary extension was removed with `DROP EXTENSION pg_repack RESTRICT`,
after confirming zero temporary repack tables. Only the three installed maintenance
files were removed from the production container, after SHA256 verification.
No database restart, full vacuum, schema rewrite of business columns, or worker
deployment was performed.

The video snapshot table is about 114 GiB. The available filesystem space was
about 123 GiB, below pg_repack's conservative recommendation of twice the target
table/index size. No full rewrite of that table or the approximately 65 GiB
metric table was attempted.

## Index reclamation

A separate native `REINDEX INDEX CONCURRENTLY` of
`public.idx_channel_metric_values_snapshot` started at 12:23:07 UTC with the
publisher running. The original allocation was 14,389,256,192 bytes. Its session
used 256 MiB maintenance memory, one parallel maintenance worker and a 60-second
lock timeout to let existing writers finish. It completed at 12:33:41 UTC in
633,757 ms, without errors. Allocation fell to 9,706,323,968 bytes, returning
**4,682,932,224 bytes (4.361 GiB)**. The original index definition was unchanged;
the replacement was ready and valid. No temporary `ccnew` or `ccold` indexes
remained. Ongoing publication had subsequently grown this index by about 4.5 MiB
at final inspection, which is normal new allocation.

## Final outcome

Measured table/index reductions total **6,547,095,552 bytes (6.097 GiB)**.
Filesystem available space increased from **122.437 GiB to 126.414 GiB**, a net
increase of **3.977 GiB**, while the rest of the system continued writing data.
This is physical file reclamation, in addition to reusable pages remaining in
the two large tables that were not rewritten. Filesystem usage is not expected
to fall by the exact targeted relation reduction during ongoing writes.

At 12:35:53 UTC:

- Business database size was 258,412,713,663 bytes.
- 11,900 channels had adopted latest storage; there were zero broken current
  snapshot/Search/version references.
- Publication had 23,359 pending and 100 leased receipts. The 13 pre-existing
  dead-letter receipts were unchanged. Publication was progressing again.
- There were no replication slots or transactions older than two minutes.
- All checked payload indexes were valid/ready, and no temporary repack schema,
  extension or transient reindex objects remained.
- Three real API search results and all three current details matched their
  pre-cleanup values. The combined final API smoke check took 269 ms.

This is a completed, fixed-cohort cleanup, not deletion of all historical
business rows. Other channels' old payloads remain until adoption and reference
checks make them eligible for a later bounded pass. Large video/metric heap
compaction remains outside this run's verified disk-space budget.

## Artifacts

Operator artifacts are under `/tmp/business-snapshot-cleanup-20260924`: fixed
manifest, preflight and per-batch logs, exact verified totals, API comparisons,
full-data fingerprints, repack logs, physical sizes and maintenance progress.
These files contain no copied connection password. Credentials were read through
the existing production secret file and not printed. The prior verified repack
build provenance is in `docs/CREATOR_SEARCH_LEGACY_CLEANUP_20260909.md`.
