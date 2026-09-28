# Physical reclamation after the first 20,000-channel cleanup

The user explicitly requested reclaiming actual disk allocation after the first
20,000-channel cleanup. Its completed audit is in
`business-snapshot-cleanup-20k-20260924.md`. No subsequent channel-cleanup manifest
is included in this operation.

At 03:10 UTC on 2026-09-25, the business database had zero broken current
references, 74 pending and 59 leased projection receipts, the unchanged 13
pre-existing dead-letter receipts, and no transactions older than two minutes.
The filesystem had approximately 118.6 GiB free. Table/index allocations were:

| Relation | GiB |
| --- | ---: |
| Video snapshots | 115.70 |
| Metric values | 62.08 |
| Profile facts | 18.52 |
| Channel links | 1.89 |

## Selected operations and guards

Four indexes are rebuilt sequentially with native `REINDEX INDEX CONCURRENTLY`:

- `channel_metric_values_channel_snapshot_id_metric_key_scope_key`
- `channel_metric_values_pkey`
- `content_snapshots_id_snapshot_channel_key`
- `idx_content_snapshots_source_key`

The connection uses 256 MiB maintenance memory, one parallel maintenance worker,
a 60-second lock timeout and a 30-minute statement timeout. It requires the
verified first-manifest receipt, correct database identity/latest mode, no other
index-maintenance task and free space exceeding three times the selected index
allocation plus a 25 GiB reserve. Every completed rebuild must preserve the exact
definition, table association, primary/unique properties and valid/ready state.

Next, the two smaller payload tables are eligible for full repack using the
previously validated PostgreSQL 18.4 / pg_repack 1.5.3 maintenance build. The plan
uses `--no-order --jobs=1 --wait-timeout=2 --no-kill-backend`, checks at least twice
the relation size plus a 20 GiB disk reserve, drains the business publisher
gracefully, and restarts it in `finally`. Crawlers, durable ingress, business
readers and databases keep running. Immutable retained payloads are fingerprinted
before/after with two independent record-hash seeds and exact counts. Temporary
extension files are installed/removed only after SHA256 verification.

Full video/metric table rewrites are outside the currently verified conservative
temporary-space budget. No `VACUUM FULL`, source-data removal, index-definition
change, collection deployment or database restart is part of this operation.

## Execution

All four indexes completed between 03:11:45 and 03:45:41 UTC without errors.
Native rebuilds retained the exact definitions and valid/ready state:

| Index | Before bytes | After bytes | Reduction |
| --- | ---: | ---: | ---: |
| Metric snapshot/key/scope unique | 14,339,432,448 | 9,505,923,072 | 4,833,509,376 |
| Metric primary key | 9,381,511,168 | 6,767,771,648 | 2,613,739,520 |
| Video ID/snapshot/channel unique | 6,009,176,064 | 4,261,978,112 | 1,747,197,952 |
| Video source-key unique | 5,043,978,240 | 3,336,134,656 | 1,707,843,584 |

Total index reduction: **10,902,290,432 bytes (10.154 GiB)**.

After baseline fingerprints, the publisher was drained without a forced-kill
deadline and exited zero. It was stopped from 03:50:19.617 to 03:59:04.336 UTC,
**524.72 seconds (8 minutes 45 seconds)**. The same container restarted with its
existing configuration. No database, crawler or ingress service was restarted.

Both full-table repacks completed without warnings or errors:

| Table | Before bytes | After bytes | Reduction |
| --- | ---: | ---: | ---: |
| Channel links | 2,035,826,688 | 1,907,204,096 | 128,622,592 |
| Profile facts | 19,965,624,320 | 18,597,240,832 | 1,368,383,488 |

The **2,019,813 retained immutable link rows** and **10,771,860 retained immutable
profile rows** matched their exact pre-maintenance counts and both full-record
hash sums. Three deployed API searches and details also matched their baseline
hashes; the post-maintenance smoke check took 412 ms. All payload indexes were
valid/ready. The temporary extension and its three checksum-verified server files
were removed; no temporary repack tables or `ccnew`/`ccold` indexes remained.

The post-maintenance check reported zero broken current references and the
unchanged 13 pre-existing dead-letter receipts. The publisher resumed accepting
work and advanced latest state from 60,347 channels to 60,540 by 04:02:39 UTC.
Some ownership-lock waits during index maintenance were traced to another active
publication transaction; the index maintenance connection was not their blocking
owner. These waits subsequently cleared. No business backend was terminated.

## Physical outcome

Measured relation-file reduction totals **12,399,296,512 bytes (11.548 GiB)**.
At the 04:06:32 UTC filesystem comparison, free space was **120.630 GiB**, versus
**118.508 GiB** before maintenance: a host-wide net increase of **2.121 GiB**.
The host continued writing during this operation; the net filesystem delta must
not be presented as equal to the sum of the selected relation reductions.

Post-maintenance checks ruled out large leftover maintenance allocations:
business WAL was 1 GiB, archiving was off, there were no replication slots,
temporary SQL files totaled about 5.8 MB, and open deleted files across the host
totaled 32 MiB. No additional deletion or forced checkpoint was performed to
alter these figures. The remaining host-wide growth was not attributed to a
specific service by this operation.

The four-index and two-table reclamation is complete. Large video/metric heaps
have not been rewritten, and the other nine logical-cleanup manifests have not
been started. Original collection data, revision chains and current business
data remain intact.

Artifacts: `/tmp/business-reclaim-20k-20260925`, including before/after physical
sizes, filesystem observations, index phase logs, API checks and table-repack
fingerprints. Passwords are read from the existing secret mount and are not
copied into artifacts.
