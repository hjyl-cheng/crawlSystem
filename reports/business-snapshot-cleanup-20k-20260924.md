# Business snapshot cleanup in 20,000-channel batches

The user requested further disk reclamation and selected batches of 20,000
channels. This expands the existing operator cleanup, without changing collection,
publication scheduling, remote Workers, accepted source data or revision chains.

## Scope and safeguards

The optional `--include-legacy-current` allows cleanup of superseded immutable
payloads even before a visible channel's next publication adopts latest storage.
It preserves the visible payload, its newest preceding immutable payload, all
snapshot headers/trend observations and every reference already protected by the
original tool. Newer-than-current snapshots and snapshots created since the latest
storage cutover are excluded. Removed/inconsistent latest pointers, non-visible
channels and non-active/non-online ownership are not eligible.

The manifest cap is 20,000 channels, while each committing transaction still
covers at most ten channel ownership locks and 100 old payloads. Busy ownership
is skipped and reported. The cleanup checks current/retained payload, Search and
trend signatures before committing each transaction; exceptions roll it back.

No latest-state adoption, channel re-crawl or publication outbox insertion is
needed for this cleanup. Existing channels adopt the new storage format through
their next normal publication.

## Validation and manifests

`bash scripts/testBusinessSnapshotCleanup.sh` passed both real PostgreSQL
integration tests (no skips). The added test exercises legacy-current opt-in,
current/previous/newer retention, Search/baseline references, ownership contention,
removed/inconsistent latest pointers, mid-delete rollback, idempotency and later
latest publication. The original test still covers classification/repair evidence,
unknown dependencies and wrong-database rejection.

At 12:54:13 UTC, a read-only inventory fixed **199,487** currently visible eligible
channel IDs after excluding the prior run's 4,999 IDs. It produced nine manifests
of 20,000 IDs and a tenth of 19,487. This is an inspection scope, not a claim that
every channel has removable payloads. No later-arriving channel is automatically
added to these files.

First manifest SHA256:
`e56beaf01a265c33c5a6365161d3ce25ef6714d086c0371746d8906d735d7a4b`.

Frozen cleanup module SHA256:
`6e4c07ba2b826836bc27e49cb733196dd8550df49df90273c4d289a486194ece`.

Production validation on ten manifest channels at 12:56:16 UTC deleted and
**rolled back** 39 old payload sets: 1,055 video rows, 3,003 metric rows, 390
profile rows and 31 link rows. All signatures passed, with no skipped owners.
These are validation counts and do not represent committed removal.

The deployed business API returned three nonempty searches/details in 305 ms at
the baseline check. Pending projection receipts were 23,890, leased receipts 100
and existing dead-letter receipts 13 at inventory time.

## Execution status

The first manifest's read-only preflight completed at 13:01:57 UTC:

| Item | Eligible count |
| --- | ---: |
| Inspected channels | 20,000 |
| Channels with eligible payloads | 13,814 |
| Old payload sets | 72,902 |
| Video rows | 1,875,048 |
| Metric rows | 5,613,454 |
| Profile rows | 729,020 |
| Link rows | 140,847 |

The ten-channel production sample committed at 13:02:17 UTC. Its counts exactly
matched the rollback test above. Three real legacy-current API searches/details
were identical before and after deletion (303 ms after deletion).

The first complete manifest began committing at 13:04:28 UTC and finished at
13:59:36 UTC. Twelve skipped busy owners were retried successfully by 13:59:51.
Verification finished at 14:09:19 UTC with no remaining busy channels.
Preflight counts are not final committed counts: normal publication can advance
a channel between inspection and its locked cleanup transaction. Every actual
deletion is recorded with snapshot IDs and per-table counts.

At 13:08:49 UTC, 1,520 channels had been inspected across 152 committed small
transactions, removing 5,623 old payload sets (plus the separately committed
39-set sample). Two busy channel owners were skipped for a later pass. Median
transaction time was 1,431 ms and maximum 2,751 ms; no transaction errors were
recorded. The 13:06 health check showed publication progressing, zero broken
current references, no transactions older than two minutes and the unchanged 13
pre-existing dead-letter receipts.

The first-manifest container was
`qy-business-snapshot-cleanup-20k-01-20260924`. A detached completion monitor
`finish-first-batch.py` waits for its completion footer, retries only this
manifest's skipped owners once, verifies every deleted payload is absent while
its header survives, checks current references and runs the deployed API smoke
checks. It records `completed-01.json` on successful verification, or
`finish-first-batch-error.json` on failure. It does **not** start another manifest
or perform physical compaction. Remaining busy owners are explicitly recorded.
The monitor PID is recorded in the artifact directory's `finisher.pid`.

Final committed totals, including the ten-channel sample and busy-owner retry:

| Item | Removed |
| --- | ---: |
| Old payload sets | 73,298 |
| Video rows | 1,883,969 |
| Metric rows | 5,643,946 |
| Profile rows | 732,980 |
| Link rows | 141,561 |

The 8,402,456 removed payload rows were checked absent and all their snapshot
headers remained. Current-reference violations were zero. Three control and
three affected legacy-current API searches/details matched the pre-cleanup
hashes (334 ms and 202 ms respectively). No subsequent manifest was started;
179,487 channel IDs remain across the other nine manifests.

Physical reclamation has not been performed in this operation. Deleting old rows
does not itself imply that filesystem space has been returned; compaction will be
assessed separately, without rewriting the large tables after every
20,000-channel batch.

Artifacts: `/tmp/business-cleanup-20k-20260924` contains numbered manifests,
their hashes, the frozen module, baseline health/API outputs, preflight logs and
rollback validation. Credentials remain in the existing mounted secret file and
are not copied into the artifacts.
