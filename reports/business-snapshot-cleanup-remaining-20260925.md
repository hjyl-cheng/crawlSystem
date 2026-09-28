# Remaining nine business snapshot cleanup batches

The user authorized continuing the remaining nine fixed manifests after batch 01
was cleaned and verified and its separate physical reclamation was completed.
This operation covers batches **02–10: 179,487 channel IDs**. Batches 02–09 have
20,000 IDs each; batch 10 has 19,487. The original 20,000-channel batch and the
earlier separate 4,999-channel cleanup are not counted again.

All ten original manifest hashes, uniqueness, cross-batch disjointness and
exclusion of the prior 4,999-channel set were rechecked before starting. The
frozen deleting module is identical to the tested batch-01 implementation:

`6e4c07ba2b826836bc27e49cb733196dd8550df49df90273c4d289a486194ece`

## Execution and safeguards

At **2026-09-25 04:20:46 UTC**, the systemd service
`qy-business-snapshot-cleanup-remaining-20260925.service` started the serial
supervisor. Batch 02 entered read-only preflight at 04:21:07 UTC after database,
service and deployed business API checks passed. This timestamp records the start
of work; it is not a claim that all nine batches have completed.

Batch 02 preflight finished at **04:28:16 UTC**: 14,697 channels had eligible
payloads, comprising 78,120 old snapshot payload sets and 8,935,625 rows
(1,989,970 video, 6,015,240 metric, 781,200 profile and 149,215 link rows).
These are preflight counts, not final deletion counts; publication continues.
Actual cleanup started at **04:28:24 UTC**. By 04:28:50, committed transactions
had visited 140 channels and removed 567 old payload sets / 64,957 rows with
no recorded error. The remaining manifests are queued for automatic serial
advancement after their preceding manifest's verification.

At 04:29:33 UTC, the committed log had reached 400 channels, 1,502 old payload
sets and 171,102 removed rows. The independent 04:29:19 health observation showed
zero broken current references, no long transactions and the unchanged 13
pre-existing dead letters. Latest-adopted channels increased from 61,939 at
startup to 63,039, confirming publication continued during cleanup.

The operation uses the existing `--include-legacy-current --apply` cleanup.
Its database writes are unchanged: at most ten channel ownership locks and 100
superseded snapshot payload sets per transaction, with a 250 ms pause between
transactions. Current and preceding retained payloads, Search, snapshot headers,
trend observations, required evidence, crawler originals and revision chains
remain protected by the already tested transactional checks.

For every manifest, the supervisor:

1. Checks services, free disk, current references and publication dead letters.
2. Checks three real business API searches/details, then inventories candidates
   and their payload-row counts without writing.
3. Runs the fixed-manifest cleanup, validates every committed transaction against
   its ten-channel scope, verifies its completion footer and checks summed counts.
4. Retries only skipped owners, with a bounded five retries. Persistent contention
   is recorded and stops advancement to the next manifest.
5. Checks every removed payload is absent and its snapshot header still exists;
   checks current references, the deployed API and continuing publication.
6. Writes an atomic `completed-NN.json` before advancing to the next manifest.

The supervisor uses an exclusive process lock and named Docker containers.
An interrupted step with incomplete audit output is not silently replayed.
Errors, unexplained additional dead letters, missing current references or failed
checks stop later batches. The minimum free-disk reserve at step boundaries is
20 GiB. No business service is stopped or restarted for this logical cleanup.

The orchestration audit was checked against the actual completed batch-01 log
and its busy-owner retry. Deliberately incomplete and out-of-manifest logs were
rejected. JavaScript syntax checks passed. The deleting SQL module was unchanged;
its real PostgreSQL integration test results remain recorded in the batch-01
report.

## Audit and status

Artifacts: `/tmp/business-cleanup-remaining-20260925`.

- `status.json`: current batch/phase, or a stopped/completed state.
- `events.jsonl`: durable step and batch transitions.
- `channels-NN.json` and `manifest.json`: fixed input scope and hashes.
- `applied-NN.jsonl`, `busy-retry-NN-M.jsonl`: actual committed removals.
- `verification-NN.jsonl`: deleted-payload/header/current-reference checks.
- `completed-NN.json`: one successfully verified manifest.
- `completed-all.json`: emitted only after all nine manifests are verified.

Read local progress without adding database load:

```bash
python3 /tmp/business-cleanup-remaining-20260925/progress.py
systemctl status qy-business-snapshot-cleanup-remaining-20260925.service
```

Initial database checks found zero broken current references and the same 13
pre-existing dead-letter receipts. Publication continued normally. Host available
space at 04:21 UTC was 137,648,160,768 bytes, about 128.2 GiB.

This supervisor removes redundant rows; it does not rewrite the large tables or
claim immediate filesystem shrinkage. Physical reclamation of batch 01 is recorded
separately in `business-snapshot-reclaim-20k-20260925.md`. Further compaction needs
fresh relation-size and temporary-space measurements after these removals.
