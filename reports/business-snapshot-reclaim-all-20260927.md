# Physical reclamation after all ten snapshot-cleanup manifests

## Final verified outcome

All four payload tables completed physical reclamation and data verification.
The last table passed at **2026-09-27 12:34:14 UTC**; publication resumed
immediately, and temporary extension/file cleanup completed at **12:34:59 UTC**.

| Table | Before, GiB | After, GiB | Reclaimed, GiB | Retained rows verified |
| --- | ---: | ---: | ---: | ---: |
| `channel_links` | 1.861 | 0.757 | 1.104 | 894,906 |
| `channel_profile_facts` | 17.914 | 7.721 | 10.193 | 5,060,280 |
| `channel_metric_values` | 55.888 | 24.038 | 31.850 | 39,595,325 |
| `content_snapshots` | 112.945 | 35.241 | 77.704 | 8,800,550 |

**Total measured relation-file reduction: 129,762,902,016 bytes
(120.851 GiB / 129.763 decimal GB).** Each row uses its run's initial inventory
and verified final allocation, including preparatory indexes exactly once.
This excludes the earlier September 25 reclamation and the failed index copy
removed on September 27. Earlier intermediate totals used immediate pre-repack
readings for the first two tables; the final inventory basis differs by 131,072
bytes because publication continued before their drain.

At the supervisor's **12:34:49 UTC** final filesystem reading, **197.419 GiB**
was free, versus 85.504 GiB at the initial 05:09 baseline: a host-wide net gain
of **111.915 GiB**. Other services continued writing, so this differs from the
120.851 GiB reduction in the selected relation files. These are timestamped
readings, not a claim that free space remains fixed after publication resumes.

The independent receipt audit passed for **54,351,061 retained rows** across
the four tables, matching both full-record hash sums, table/index/constraint
definitions and API results within each stopped-publication window. The final
health check found zero broken current references and the unchanged 13 dead
letters. The 12:34:59 read-only inspection found no repack relations, residual
concurrent-rebuild indexes, invalid payload indexes or maintenance extensions.

The independent **12:35:25 UTC** closeout again found zero broken references
and 13 dead letters, and confirmed publication had advanced latest state to
**12:34:54 UTC**, after the publisher restart. Pending/leased receipts were
9,805, down from 9,933 in the supervisor's final check; the accumulated
publication backlog was being processed. All three installed maintenance files
were independently confirmed absent. The metrics and video publication-stopped
windows were respectively 31 minutes 57 seconds and 1 hour 45 minutes 49 seconds.

Final artifacts in `/tmp/business-reclaim-resume-20260927` include
`result.json`, `status.json`, `verified-four-table-summary.json`, `closure.json`,
the per-table receipts and `observations/20260927T123507Z.json`. The earlier links/profile
receipts remain in `/tmp/business-reclaim-all-20260927`.

## Execution scope and earlier observations

The user requested physical disk reclamation after all ten manifests had completed
logical cleanup and verification. The fixed 199,487-channel scope removed
803,690 redundant payload sets / 91,803,788 rows in total. No further logical
data deletion is part of this physical maintenance operation.

The remaining nine manifests completed at 2026-09-25 15:51:03 UTC. Their receipts
are in `/tmp/business-cleanup-remaining-20260925`; every manifest was verified,
with no unresolved skipped owners. The first manifest has its separate receipt.

## Fresh inventory and execution

At the September 27 inventory, the filesystem had 91,915,866,112 available bytes
(85.6 GiB). The business database was PostgreSQL 18.4 in verified latest storage
mode, with no existing maintenance extension or concurrent index-maintenance
operation. All existing payload indexes were valid and ready.

| Table | Allocated bytes before maintenance |
| --- | ---: |
| `channel_links` | 1,997,996,032 |
| `channel_profile_facts` | 19,234,807,808 |
| `channel_metric_values` | 60,009,193,472 |
| `content_snapshots` | 121,268,387,840 |

These are allocated file sizes, not predicted reclaim amounts. The video table
also contains substantial TOAST storage. Current row counts are much smaller
than when these files grew, following the completed logical cleanup.

The systemd service `qy-business-reclaim-all-20260927.service` runs
`/tmp/business-reclaim-all-20260927/reclaim.py`. Its order is links, profile facts,
metrics, then video snapshots. It uses the PostgreSQL 18.4 / pg_repack 1.5.3 image
and the same checksum-verified extension files validated in the earlier physical
reclamation. The image ID observed before startup was
`sha256:40122c0220b1edf168c45bd0be6cbe1e169aaf5a96e5fbe0d72a27263df18269`.

## Bounds and verification

- Initially require twice the table-plus-index allocation plus 20 GiB reserve.
- For larger tables that do not meet that conservative original-size budget,
  scan the main and TOAST heaps with `pgstattuple` to measure live tuple lengths
  and counts. Include 16 bytes per tuple, add 50% packing allowance, then budget
  twice that live storage plus twice the currently allocated indexes and the
  separate 20 GiB reserve. This is a measured estimate, not a hard guarantee;
  live disk monitoring also stops this operation's own container if the reserve
  is breached. The scans are exact rather than sample extrapolations.
- If needed, rebuild indexes individually with `REINDEX INDEX CONCURRENTLY`,
  checking sufficient workspace and unchanged definitions/validity each time.
- Use one repack job, no sorting of the table, a two-second lock wait, and
  `--no-kill-backend`. A warning or skipped table is a failure, even if the tool
  returns zero. No unrelated backend is cancelled or terminated.
- Drain only the business publication projector without a forced-kill deadline
  for each table. Capture full-table counts and two independent full-record hash
  sums before and after repack while that publisher is stopped. Check table OID,
  ownership/permissions, constraints and index definitions remain unchanged;
  require a changed physical file identifier as evidence that repack took place.
- Compare deployed API search/detail results during the same publication window.
  Restart the same publisher in `finally`, including on failure. Crawlers,
  durable ingress, database servers and the business reader remain running.
- Stop later tables on errors. Remove temporary extensions with `RESTRICT` and
  remove only the installed checksum-matching maintenance files, after checking
  no repack relations or triggers remain.

The budget guard was exercised without production writes for both accepted
original-size/live-size budgets and rejection before writes when disk is
insufficient. The deleting module and collection code are not modified.

Artifacts: `/tmp/business-reclaim-all-20260927`.

- `status.json` and `events.jsonl`: active phase and audit transitions.
- `baseline.json`: initial file sizes, disk accounting and business health.
- `density-*.jsonl`: exact live main/TOAST storage measurements where required.
- `fingerprint-before/after-*.jsonl`: full retained-row checks.
- `completed-<table>.json`: one successfully verified table.
- `result.json`: final four-table result; check `status.json` for completion of
  extension/file cleanup too.

No completion or saved-space amount is claimed by this initial execution record.
Relation-file reductions and the host-wide filesystem delta must be reported
separately, since other services continue writing throughout maintenance.

## Startup observations

Two preflight attempts stopped before any table rewrite or publisher drain. The
first exposed multiline `json_agg` output in the metadata reader; complete-object
parsing was fixed and checked against the real saved output, including rejection
of truncation. The second exceeded the maintenance-only 45-second full-reference
check timeout during disk I/O. Query plans showed scans of current Search/state
and snapshot indexes. The maintenance check limit was raised to 180 seconds;
an independent rerun took 25.82 seconds and found zero broken references and the
unchanged 13 dead letters. Both attempts' outputs remain in numbered subfolders.
The temporary extensions from the second attempt were removed successfully.

On the continuing attempt, the first table passed its space guard at
**05:09:51 UTC**: about 91.75 GB free versus a 25.47 GB required budget. The
publisher finished its current transaction and exited cleanly by **05:10:29 UTC**.
Only that publication service was drained; collection and durable ingress were
left running. Later phase transitions and results are recorded by the supervisor.

## First verified physical result

The links table completed verification at **05:12:02 UTC**. Its 894,906 retained
rows matched both full-record hash sums exactly; all table/index/constraint
metadata and the deployed API results matched. Allocated files decreased from
1,998,168,064 to 812,548,096 bytes, a reduction of **1,185,619,968 bytes
(1.104 GiB)**. The publisher resumed at **05:12:03 UTC**, after a 94-second stopped
window. The tool waited for an existing reader transaction to finish; it did not
cancel or terminate that reader. Later tables remain managed by the same serial
supervisor; their completion must be read from their own receipts.

## Second table and interrupted index preparation

Profile facts completed at **05:20:43 UTC**, preserving all 5,060,280 rows and
both full-record hash sums, table/index/constraint definitions and API results.
Its allocated files decreased from 19,235,340,288 to 8,290,844,672 bytes:
**10,944,495,616 bytes (10.193 GiB)** reclaimed. The publisher resumed immediately.
Combined verified reductions from the two repacks are **12,130,115,584 bytes
(11.297 GiB)**; measuring from the initial per-table baselines gives
12,129,984,512 bytes, with the small difference due to writes before draining.

The preparatory concurrent rebuild of `idx_channel_metric_values_snapshot`
subsequently hit its 60-second lock wait. The supervisor stopped at
**05:29:26 UTC** and removed its temporary extensions/files. The publisher
continued running; the metrics/video tables were not repacked. The exact old
blocking transaction was not captured at failure and is not attributed to a
particular service here.

The 10:01 UTC inspection found the original index still valid/ready and an
invalid, ready `idx_channel_metric_values_snapshot_ccnew` (OID 43881471), with no
constraint attached. Such an index is still maintained by writes even though it
cannot serve queries. A dedicated PostgreSQL 18 test reproduced the same
timeout/residual-copy pattern using an older repeatable-read snapshot, and then
verified successful cleanup/reindex with unchanged row counts/sums after that
reader committed. No production transaction was killed for this test.

At **10:05:50 UTC**, only the verified invalid copy was dropped concurrently,
returning its **4,653,604,864 bytes (4.334 GiB)** allocation. Exact OIDs, original
index validity, matching definitions, lack of constraints and absence of an
active index build were checked first; all original structures matched afterward.
This is removal of a failed maintenance allocation, not additional compacted
business data, and must not be added to the 11.297 GiB table savings as if it were
an independent original-data reduction.

The remaining two tables continue under
`qy-business-reclaim-resume-20260927.service`, with artifacts in
`/tmp/business-reclaim-resume-20260927`. The revised operator script drains the
publisher before preparatory index work as well as before the table rewrite, and
allows old readers up to ten minutes to finish at an index lock wait. It preserves
the original disk, structure and fingerprint guards and never kills other
backends. The earlier two verified tables are not reprocessed. The new directory
has its own `status.json`, per-table receipts and final result; the original
directory retains the failed run's audit unchanged.

## Resumed run: verified index reductions

At **10:10:13 UTC**, the publisher drained cleanly for the metrics maintenance
window. Both preparatory concurrent index rebuilds then completed with unchanged
definitions and valid/ready state:

| Index | Completed UTC | Relation allocation reduction |
| --- | --- | ---: |
| `idx_channel_metric_values_snapshot` | 10:15:43 | 6,221,217,792 bytes (5.794 GiB) |
| `channel_metric_values_channel_snapshot_id_metric_key_scope_key` | 10:21:00 | 5,525,168,128 bytes (5.146 GiB) |

The two rebuilds reduced metrics table-plus-index allocation from 60,009,193,472
to 48,262,807,552 bytes. Together with the two earlier verified table repacks,
completed maintenance steps had reclaimed **23,876,501,504 bytes (22.237 GiB)**.
This is an intermediate amount; the metrics table rewrite and video table had
not yet completed and are not counted as additional savings here.

The metrics rewrite began at **10:23:41 UTC** with **39,595,325 retained rows**.
The 10:32 observation showed the row copy complete and index construction in
progress without blocking transactions. At 10:31:24 UTC the host had
89,309,061,120 bytes (83.176 GiB) available. The ongoing rewrite's temporary
allocation and other services' writes mean this host-wide free-space reading
is distinct from the completed relation-file reduction.

Read-only progress observations and an independent receipt-audit script are in
`/tmp/business-reclaim-resume-20260927/observations` and `summarize.py` in that
same parent directory. The audit refuses to produce a final four-table summary
until the supervisor reports successful verification and temporary-file cleanup.

## Metrics table verified

At **10:42:09 UTC**, metrics completed full verification. All **39,595,325 rows**,
both full-record hash sums, table/index/constraint definitions and deployed API
results matched. The table rewrite itself reduced allocation from 48,262,807,552
to 25,810,763,776 bytes, reclaiming **22,452,043,776 bytes (20.910 GiB)**.
Including the two preparatory indexes exactly once, the metrics operation reduced
allocation by **34,198,429,696 bytes (31.850 GiB)**.

Combined with the earlier links/profile rewrites, completed steps had reclaimed
**46,328,545,280 bytes (43.147 GiB)**. The publisher resumed at **10:42:10 UTC**,
and the post-table health check found zero broken current references and the
unchanged 13 dead letters. Filesystem free space at restart was 130,278,440,960
bytes (121.331 GiB). Video live-storage measurement began at **10:43:21 UTC**;
video savings are not included in this intermediate result.

## Video preparation

Exact scans measured 8,799,983 live main-table tuples and 11,706,074 live TOAST
tuples. Free space represented 67.96% of the main heap and 69.16% of the TOAST
heap. The publisher drained cleanly at **10:48:26 UTC**. Four preparatory
concurrent index rebuilds completed, preserving their definitions and validity:

| Index | Completed UTC | Allocation reduction, bytes |
| --- | --- | ---: |
| `content_snapshots_id_snapshot_channel_key` | 10:52:40 | 2,743,910,400 |
| `idx_content_snapshots_metric_scope` | 10:56:37 | 2,900,434,944 |
| `idx_content_snapshots_one_canonical` | 11:00:40 | 2,899,173,376 |
| `idx_content_snapshots_snapshot_date` | 11:04:23 | 2,898,935,808 |

These four steps reclaimed **11,442,454,528 bytes (10.657 GiB)**, bringing the
intermediate total for completed steps to **57,770,999,808 bytes (53.803 GiB)**.
At **11:04:24 UTC**, the video rewrite passed its measured-live-storage budget:
140,780,277,760 bytes available against 137,965,783,576 required, including
the separate 20 GiB reserve. The full-record baseline fingerprint then started;
it was still executing without blockers at the 11:11:30 UTC observation.
