# Restore the deployed node NATS transport, 2026-09-12

## Diagnosis

Node 01 expanded from 30 to 40 Workers at approximately 06:45 UTC. All 40 were connected, allowed and executing; the BullMQ queue still had waiting jobs. In equal windows, successful daily Plans were 140 (06:35–06:40), 124 (06:40–06:45), and 115 (06:46–06:51). Video-detail command receipts also decreased: 793, 750, 634 respectively. These are short observations, not a controlled workload benchmark.

All 40 actual containers lacked `REMOTE_NODE_NATS_URL`. Sampled startup logs explicitly reported `transport: https`. The center's NATS counters were zero before a diagnostic connection. Dashboard's runtime environment lacked `SERVER_NODE_NATS_URL`, although its saved `runtime/remote-center-production/dashboard.compose.json` contained the intended WSS endpoint. The two retained predecessor Dashboard containers also lacked this environment variable (including the container created at 03:41 UTC). The exact first deployment that lost it was not established.

The recipe made NATS optional; missing configuration silently generated HTTP workers. The installer used `--no-recreate`, then checked container state/labels/image but not its transport environment. Thus a desired NATS recipe could falsely pass against a reused HTTP container. These two defects were reproduced by failing tests before the fix.

Other observed pressure: node CPU idle 78–93%, ~3 GiB available memory and zero swap use. Center service limited to 1 CPU; over 15.024 seconds it used 12.482 CPU seconds and hit throttling in 36 of 150 quota periods. Center host CPU was also busy, with a long publication backlog query. Detail result-to-next-command gaps grew from 1.4–1.7 seconds to ~2.3 seconds. There were 28 STALE_LEASE and 15 EXECUTION_ROUTE_BUDGET_EXHAUSTED failed attempts in 06:46–06:51; these are not counts of final failed channels. The exact cause of every lease failure is not established by these observations.

## Changes

- Page-managed deployment requires an explicit NATS endpoint. Missing configuration yields an unavailable preview and disables deployment; lower-level HTTP fixtures remain supported.
- Python installer verifies the actual container's NATS environment against the intended recipe. A reused HTTP container cannot pass a NATS deployment check.
- `deploy/compose.dashboard-node-nats.yml` makes the public endpoint a required deployment input, independent of image changes.
- Restore the endpoint in the live Dashboard and both saved production Compose/environment files. Private rollback copies are retained.
- Switch each existing node container only after its current work drains, with no network or result spool pending. Preserve image, slot identity, config hash, credentials, collector and task fences. Keep Docker health probes disabled.

## Validation

Eight focused tests passed, including both new regression failures, 40-worker NATS recipes, existing 150-worker count support and the actual Python verifier. Isolated WSS NATS integration with PostgreSQL and Redis passed, exercising SQL notification delivery, durable original receipts and node gates. The disposable stack was removed afterward.

Before changing containers, a real authenticated WSS connection from node 01 returned the expected NOT_FOUND for a nonexistent receipt; this confirmed public TLS, broker authentication and center RPC handling without claiming any task.

## Dashboard throughput interpretation

The fixed 115-success observation is 06:46–06:51 UTC. The page uses a rolling five-minute window, cached for 30 seconds (stale results may be served during refresh), and effective completion time `GREATEST(plan.completed_at, run.finished_at)`. It includes success/partial/recovered outcomes. Therefore it is not exactly the same statistic as a historical succeeded-Plan query. 17.6/min means 88 eligible completions in the page's sampled window; remaining time is remaining channels divided by that rate. At generatedAt 06:56:35, the live page reported 18.8/min and 32,151 remaining. No statistics code was modified.

## Production rollout

- Paused node 01 intake at 07:02:29 UTC with 40 active tasks.
- Dashboard `node-nats-required-20260912` deployed at 07:04:12. Correct endpoint also persisted in `dashboard.compose.json` and `dashboard.env`.
- Switched 30 drained slots first and the remaining 10 after their tasks finished. Center service and migration Workers were not restarted. Allowed count restored to 40 at 07:08:36.
- All 40 container startup logs reported `transport: nats`; all used digest `416f6327ca742a4ce542419d71292a8f3d53e37ebf38db32cb6dd5047839429b`, with zero restarts/OOM kills. At 07:09:58 all 40 were connected, ready and executing.
- Center live image setting, Dashboard live image setting, saved Dashboard Compose image setting, Dashboard env and center env all agree on that collector digest. A focused test also verifies new-server deployment and existing-server expansion choose the same released image and NATS endpoint.

The patched installer was also executed in `verify` mode against the real 40-node container set and passed (no writes to collector configuration). At 07:12:25, JetStream transport receipts had 863 successful SQL acknowledgements spanning 119 tasks and no error receipts. First complete minute counts after restore were 27, 26 and 25. Logs since restore at that check showed three route-budget failed attempts and no STALE_LEASE events; this is a short observation, not proof all network/recovery problems are solved. Center CPU throttling and database pressure remained visible.

## Full five-minute observation and remaining bottleneck

07:09–07:14 UTC after NATS restoration: 127 succeeded Plans, 25.4/min, compared with 115 (23/min) during 06:46–06:51 using HTTP, a 10.4% increase. Received video-detail commands increased from 634 to 1,052 (65.9%). Video-bearing succeeded Plans were 37 versus 55; changing workload and retries mean this is not an isolated protocol benchmark. By 07:14:39 only one leased task was on a retry generation; no retired binding or >30-second active binding without a command was observed.

Final check 07:15:36: all 40 connected/ready/executing. NATS had pending=0, unacknowledged=3 (in flight), no admission rejections or timeouts. A diagnostic attempt to open a new direct PostgreSQL connection failed with SQLSTATE 53300. Through the existing transaction pool, PostgreSQL reported max_connections=100, reserved=3, clients=100, idle_clients=73. Of these, 40 direct sessions held remote supervisor locks and 21 sessions listened for notifications. Gateway/results/heartbeats and publication also occupied connections. This is an independently confirmed database capacity bottleneck. No database limits, task locks, notification topology or collection policies were changed in this NATS configuration fix; further scaling requires addressing this capacity, not declaring transport restoration a complete performance fix.
