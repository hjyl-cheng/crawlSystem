# Remote node health without periodic collector imports

## Problem and evidence

On node 01 (4 CPU cores, 30 incremental containers), Docker started `node scripts/runRemoteNodeIncremental.mjs --healthcheck` every 10 seconds per container. That entrypoint statically imports the collecting runtime and YouTube dependencies before reading a small local status file. A 15.59-second read-only `/proc` sample attributed 37.86 CPU seconds to healthcheck processes, equivalent to 2.43 cores. Short-lived processes exiting between samples are undercounted. Several existing Docker health log entries exceeded the five-second timeout even though the node continued returning results.

The node had 30 connected/allowed Workers but 29 eligible queue consumers, with 10–12 tasks waiting throughout a 35-second observation and no expired capacity markers. Slot 28's Rota lease expired separately at 05:28:30 UTC; this healthcheck change does not repair the underlying Rota recovery policy. Center CPU pressure and publication SQL also remain separate concerns.

## Change

- Both remote connection and collecting Dockerfiles use `HEALTHCHECK NONE`.
- Generated node Compose services explicitly set `healthcheck.disable=true`, including when deploying a previously published image with an inherited healthcheck. Existing containers need a drained recreation; normal additive deployment retains `--no-recreate` and does not interrupt older Workers.
- The fixed remote installer verifies running, non-paused, non-restarting containers, exact slot coverage, expected image and expected node/deployment labels. It does not require Docker's Health property or accept a running container as sufficient proof of completed deployment.
- The existing next deployment step still requires a live, matching center registration/heartbeat for every slot. Only after this succeeds is the installed count recorded; intake activation retains its existing readiness and ownership checks.
- Connection heartbeats, local readiness records, task leases, progress, timeouts, execution fences, spool replay and the shared incremental collector are unchanged. Idle connected Workers need not be collecting to count as connected. Docker's `unless-stopped` process restart policy remains.

## Verification

`node --test --test-isolation=none services/dashboard/src/nodeWorkerHealth.test.js services/dashboard/src/nodeConnectionDeployment.test.js` initially failed on both missing disable configuration and rejection of a running container without Docker Health. After repair all four tests passed. The actual Python installer's verify path is exercised with simulated Docker inspection: no Health passes; legacy unhealthy does not override the later center heartbeat gate; stopped, paused, restarting, duplicate slots and wrong node identity fail.

The PostgreSQL Dashboard deployment integration passed, including a new case where containers are started but center heartbeats are missing: deployment fails at connection verification, the installed count is not increased, and intake is not enabled. Restoring heartbeat and retrying succeeds. Existing installation, count expansion, durable state and concurrent intake controls also passed.

The complete real SSH/SFTP/sudo fixture passed all 11 tests, including first deployment, private registry cleanup, additive deployment and spool preservation. Its Docker inspection now omits Health. An initial fixture run failed because mounting the repository's non-executable mock file bypassed the image's executable permission; rebuilding the isolated fixture with the original chmod step fixed the setup. No production installer change was made for that fixture issue.

## Deployment

Dashboard image: `qy-allpachong/dashboard:node-heartbeat-health-20260912`, based on the current production Dashboard image, overlaying only `connectionDeployment.js` and `deployWorkers.py`. Compose override: `deploy/compose.node-heartbeat-health.yml`.

Private configuration backups are in ignored `runtime/remote-center-production/node-health-20260912/` and the node's root-only deployment directory. Remote Workers retain their existing image digest and spool bind mounts; only their Docker healthcheck setting changes.

The first Dashboard stop reached its 30-second timeout and exited 137 (not OOM); the old Dashboard was promptly restarted. There were no ongoing node deployment operations. The npm entrypoint did not forward the stop to the actual server; sending the existing Node server its normal SIGTERM allowed a clean stop and deployment. The replacement's live preview verified all 30 generated services disable healthcheck at 05:52:20 UTC. No collection container was stopped during that Dashboard retry.

Node 01 intake was set from 30 to 0 with an expected-count guard at 05:52:50 UTC. Active work is allowed to finish before recreating containers. The migration fleet and center incremental coordinator are outside this rollout.

At 05:54:23 UTC the center reported active=0, draining=0 and all 30 standby; a separate node inspection found no claim/network/pending files. A browser POST to the node's execution endpoint at 05:54:52 UTC then resumed all 30 Workers. The remote apply preflight subsequently detected unfinished spool files and refused before writing configuration or replacing any container. Follow-up inspection confirmed genuine new leases (not just idle claim intentions). The existing containers remain unchanged pending coordination of a new drain window; the new Dashboard deployment template is already live. Slot 28 was ready again after this pause/resume, which does not by itself establish a fix for its earlier lease-recovery issue.

## Completed rollout and production verification

The user confirmed they had clicked resume and explicitly requested another pause. Intake was set to zero again at **05:59:16 UTC**. The rollout was changed to inspect each slot's spool and local heartbeat and recreate only drained slots with `compose up --no-deps` and explicit service names, preserving any active sibling's container ID/start time. When that final preflight ran, the last two tasks had also drained, so all 30 qualified. A bare idle claim intention is retained; network state, pending result or a claim with a task lease prevents replacement.

At **06:03:26 UTC**, all 30 replacements were running with `Healthcheck.Test=["NONE"]`, using the exact previous image digest and bind mounts. Center status subsequently confirmed active=0, draining=0 and all 30 connected. Intake was restored to **30 at 06:04:35 UTC**. At 06:04:59 all 30 were connected, ready and actively processing; every registered heartbeat was later than the replacement startup.

The same read-only `/proc` method measured 15.40 seconds after resumption: **zero healthcheck processes**, 9.04 observed CPU seconds for collectors, 4.58 for fingerprint gateways and 1.52 for other processes. The prior healthcheck category alone had consumed 37.86 CPU seconds in 15.59 seconds. Five two-second vmstat intervals reported **56–80% CPU idle** with the 30-Worker fleet enabled. This establishes removal of the repeated-import overhead, not a fixed whole-system speedup ratio or daily throughput prediction.

By 06:05:47, 44 Plans had succeeded since resumption and video results were returning. Existing Rota route-budget execution retries appeared for three slots; no claim is made that this healthcheck change fixes network budgets or all remote recovery issues. NATS reported no pending messages and no RPC rejections/timeouts in the sampled status. The center incremental coordinator retained its 03:24:01 start time and zero restarts; the migration queue remained unpaused with 40 active jobs.

The isolated SSH/PostgreSQL test services were stopped after verification. No collected data was deleted, no task ownership fence was bypassed, and no commit or push was made as part of this change.
