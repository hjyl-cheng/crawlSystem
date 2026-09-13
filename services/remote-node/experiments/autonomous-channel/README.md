# Autonomous channel execution trial

The original trial below remains isolated. Its production-module integration now lives in `services/qybullmq/src/remoteNodes/wholeChannel*.js`; see [integration status and validation](../../../../docs/WHOLE_CHANNEL_REMOTE_EXECUTION.md). The integration has passed a single production Worker rollout; see the linked production validation before changing deployment scope. The historical trial measurements below are not rollout evidence.

Status: isolated backend feasibility trial. **Not connected to production dispatch, NATS, Rota, API-batch ingestion or Finalize.** No production entry point imports this directory. YouTube responses and dispatch snapshots are fixtures; journal writes, process termination, chunk assembly and restart recovery are real.

The trial tests whether a node can select and collect a channel's video targets from a frozen input, persist progress locally and submit one logical result without a central command or PostgreSQL transaction between videos. It does not establish production throughput or complete the production migration.

## Shared behavior

`services/qybullmq/src/incrementalVideoBatchPlan.js` extracts the existing target preparation from `createCheckpointBatch`. The production caller still runs it inside the same fenced transaction, using the original database loaders and ContentEnrich reservations, in the original order. First-seen/deferred filtering, recent scoring, capacity handling and phase deduplication remain shared. No migration dispatch or collection code was edited for this trial.

The trial reuses the existing scan normalization, `fetchIncrementalYoutubeJsVideoDetail`, detail validation, failure classification and `createVideoDetailApiFallback`. The raw YouTube adapter is injected. The API persistence adapter writes a local outbox; it never uses real API credentials or issues an API request. A pending API item ends this collection pass, retains partial evidence and unfinished targets, and preserves the first-seen barrier. A later API result and continuation are not yet implemented in this trial.

## Durability model

- The input includes Plan, generation, observation time, anchors, policy and immutable snapshot data. Reusing a journal with different input fails.
- Checkpoints append to a checksummed journal and fsync before progress advances. An interrupted final append is discarded; corruption of a complete record fails closed. Disk space is bounded. Each directory requires one owning process; cross-process ownership enforcement is not implemented here.
- SIGKILL after a completed checkpoint is tested in a child process. A fresh owner reuses the saved About, scan, targets and video evidence. A request interrupted before its checkpoint may still need repeating; this is not exactly-once network execution.
- The final logical result is split into at most 512 KiB binary chunks, below the existing message limits after base64 encoding. This tests encoding and a durable local inbox, not JetStream itself.
- A receipt is issued only after complete result integrity and identity checks. It means **received**, not business data applied or Plan finalized. Exact completed receipts can be replayed after a lost ACK; unreceived old-generation results cannot be completed after handoff.
- Lease cancellation is tested through an abort signal. Heartbeats, managed YouTube session recovery, graceful intake pause and node fleet supervision still need the production execution wrapper.

The inbox's `assertOwner` is a synchronous isolated-test predicate. It is not a substitute for the production business fence/transaction. Receipt creation and business ownership checks must be atomic in the real center store.

## Validation on 2026-09-12

- 15 trial tests: autonomous target discovery; first-seen/full and recent/metrics detail modes; SIGKILL/resume; parser/API handoff; exhausted and unexhausted network budgets; About-only; dormant and incomplete scans; deferred/live entries; cancellation; input generation conflicts; oversized multipart results; lost ACK/restart; stale generation; corrupt chunks; journal truncation/corruption/capacity.
- 68 existing planner, detail, probe and API fallback regression tests. Together with the trial, **83/83 passed**, without skips, on host Node 26 and the existing production image's Node 20 runtime. The Node 20 container had networking disabled and a read-only source mount.
- **7/7 real PostgreSQL regressions passed**, without skips, on a separate temporary PostgreSQL 16 instance: Full Crawl → ordinary incremental metrics; pending content repair; dormant incremental preserving existing data/anchors; dormant Full Crawl; incremental API checkpoint replay; newer Full Crawl attempt replay; checkpoint schema/claim/phase constraints. These exercise the production caller after extraction; they do not test applying the new whole-channel envelope to production tables.

A local fixture measurement collected 30 new targets and one recent target, with zero center calls during collection and one result submission (33,917 bytes). Local collection/validation/journaling took 145.40 ms; encoding and durable local reception took 14.81 ms. Responses return immediately in this fixture. These numbers measure local trial overhead only, **not YouTube collection speed, NATS latency or expected production gain**.

Run from the repository root:

```sh
node --test --test-concurrency=1 services/remote-node/experiments/autonomous-channel/channel.test.mjs
node services/remote-node/experiments/autonomous-channel/measure.mjs
```

The existing PostgreSQL tests require separate initialized/empty disposable databases respectively. Never point them at a production database.

## Required next integration

1. Build an authoritative dispatch snapshot under the existing business fence. Current eligibility uses live ContentEnrich state and sometimes publication evidence learned from the scan. The trial only accepts an isolated snapshot with prequalified recent rows whose publication is known. It does not export production snapshots. Extract and parity-test the remaining eligibility projection, due-entry positioning and pending-first-seen reconciliation before supporting arbitrary channels.
2. Reserve/fence ContentEnrich ownership without per-video center calls. A no-op reservation is allowed only by the explicitly isolated fixture adapter. Preserve coordination with other repair tasks and reject conflicting snapshot revisions.
3. Wrap a complete node pass in the managed Rota/YouTube session lifecycle. Reuse the same collectors, client retries, country handoffs, route budgets, heartbeat abort and pause/drain semantics. The injected fixture adapter has not exercised a live remote node or actual YouTube requests.
4. Add a versioned whole-channel command/result to the existing NATS transport. Persist complete receipts in the center store before acknowledging/releasing collection ownership. Add transactional business application and idempotent reconciliation for received-but-unapplied results; leave publication and Finalize on the center.
5. Import API outbox requests into the shared center API-batch module, merge verified results using the existing policy, and issue only unfinished work under an authorized continuation. Test the complete path through Plan/Clock completion, including duplicate deliveries and center restart.
6. Only then measure the same real channels through old/new paths with equivalent Clock masks, target counts and routes. The trial alone is insufficient evidence to switch the running fleet.
