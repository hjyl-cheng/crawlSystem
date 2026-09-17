# Worker fleet release — 2026-09-17

The September 14 incremental worker outage was caused by startup recovery retrying results whose durable NATS receipts had permanently rejected their business fence. Commit `69b717c035b3014ccee87e2e622dfd313f7e045c` archives an authoritative HTTP 409 `INCREMENTAL_BUSINESS_FENCE_STALE` through the existing stale-result path, preserves the rejected bytes and allows new claims. Transient failures and unrelated conflicts still retain unacknowledged results.

## Deployed scope

| Role | Count | Image |
| --- | ---: | --- |
| Local full-crawl workers | 40 | `qy-allpachong/qybullmq:pachongsys-69b717c` |
| Local incremental workers | 20 | same |
| Local content-enrich worker | 1 | same |
| Local Agent workers | 3 | same |
| Local Finalize and Data API workers | 2 | same |
| QYBullMQ API and full-crawl Controller | 2 | same |
| Remote execution center | 1 | `qy-allpachong/remote-node-center:pachongsys-69b717c` |
| Node01 remote incremental workers | 47 | fixed collector digest below |
| Node02 remote incremental workers | 10 | fixed collector digest below |

Collector image: `newcrawdashboard.137-175-93-199.nip.io/qy-node-incremental@sha256:a1736b34c23108f2e26582d068ae992cb2b0bd98e563864806fb8c2f327019ea`.

The Dashboard retained its existing application image and was recreated to load the new default collector image. Both nodes' central deployment records and Dashboard registration were updated atomically, and persistent center/Dashboard environment files and remote Compose manifests now use the fixed collector digest. This supersedes the earlier five-worker-only rollout.

The local incremental intake setting remains disabled with a configured capacity of 20, as it was before this deployment. Remote intake remains enabled for 47 and 10 workers. Existing worker identities, spool mounts, commands, environment values and resource limits were preserved.

## Validation and cutover

- The original stale-fence regression failed before the fix; its recovery/protocol/runtime suite passed 26 tests after the fix.
- The complete QYBullMQ test suite ran in an isolated Node20 image with a clean source snapshot: **1,768 passed, 0 failed, 0 cancelled, 240 skipped**. Skipped tests require external integration fixtures.
- Release validation corrected three stale test fixtures: four Agent retryability guards now exist; center capacity refresh includes two batched maintenance transactions; the mocked YouTube transport must keep the process alive while awaiting an unreferenced timeout.
- `scripts/verify.sh` and `git diff --check` passed. The repository-wide `scripts/test.sh` could not start because the host `.venv/bin/python` is absent; the QYBullMQ suite above used the built image's Python dependencies. This is not a claim that every repository component's suite ran.
- An idle local incremental worker was replaced first and verified before promotion.
- Queues were paused and all target queue active counts reached zero before the fleet replacement. Both remote spools had no pending result/network/session pointers at cutover.
- The five previously updated remote workers retained their current image; the other 52 remote workers were replaced. All local target processes exited normally during replacement.
- Verification covered 70 local target containers and 57 remote workers: all running, zero unexpected restarts, and unchanged runtime identities/configuration. Center, API and Dashboard health checks passed. All 57 remote workers reconnected and became enabled before queue intake resumed.

## Operational evidence and rollback

Private evidence is stored under `runtime/fleet-worker-upgrade-20260917/`, including local before/create manifests, queue states, rollout logs, verification results and remote configuration hashes. Remote root-only backups are under `/var/lib/qy-node/recovery/fleet-upgrade-20260917/`. The five original blocked-worker spool backups remain under `/var/lib/qy-node/recovery/stale-fence-20260917/`.

Local `*.create.json` manifests in that release directory preserve the exact deployed configuration for roles whose older Compose labels point to retired temporary worktrees. Do not redeploy those roles from an old temporary worktree or a stale runtime manifest.

Rollback pauses intake, waits for current tasks and network ownership to settle, then recreates the affected role with its recorded previous image/configuration. Never restore an old spool over completed work or revive old leases. Rollback of the collector also requires corresponding default-image and deployment-registration changes; the previous collector contains the stale-fence bug.

Proxy transport errors, route budget exhaustion and delayed center attempt cleanup are separate operational issues. This release fixes permanent rejected-result recovery; it does not claim to eliminate every retry or all causes of worker stalls.
