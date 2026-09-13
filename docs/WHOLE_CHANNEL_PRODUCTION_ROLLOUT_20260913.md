# Whole-channel production rollout — 2026-09-13

## Scope and current state

One production Worker (`incremental-30` on node 01) now advertises
`youtubejs-incremental-whole-v1` and receives complete Clock execution snapshots
through NATS. The other 29 admitted Workers continue the previous command path.
The operator's configuration remains 45 deployed / 30 allowed; 20 central local
incremental Workers remain paused. This is a single-Worker rollout, not fleet
acceptance or a change to Dashboard installation defaults.

The center has `REMOTE_NODE_WHOLE_CHANNEL=true`. Both whole-result tables and the
expanded command constraint were explicitly applied to `newcrawler_crawler`.
No schema is automatically applied at service startup. Collection policy,
YouTubeJS/Rota, API-batch, Agent and publication retain their existing ownership.

The incremental queue was paused at 02:59:41 UTC and active jobs naturally drained
to zero by 03:02:58. An existing historical-recovery defect discovered during
center replacement prolonged the maintenance window. Intake resumed at 03:23:07;
no active channel was killed. All 40 migration containers retained their original
IDs and start times. The final 03:32:35 sample had 30 active incremental jobs and
40 active migration jobs.

## Recovery defect found during rollout

An applied task's completion writer did not clear its coordinator, and the outer
transaction renewed that coordinator again after the result was applied. About
22,186 historical applied tasks still had coordinator timestamps at diagnosis;
all associated attempts were already finished. The replacement supervisor
classified them as unsettled. Recovery scanned history in batches of 32 and
refused to change older executions when newer attempts existed. These identical
recovery routines were present in the previously deployed image; this was not a
new collection-policy change.

The fix:

- Completion clears coordinator identity and expiry. Normal and API replay
  transactions do not renew terminal tasks afterward.
- A historical applied result with a finished attempt and matching channel,
  Plan, dispatch generation, result run and registered Worker is no longer
  treated as active solely because a coordinator timestamp remains. The timestamp
  need not have expired: completed execution evidence, not elapsed time, is the
  reason it is safe. Unknown/mismatched records remain quarantined.
- Running attempts and non-retired network bindings independently prevent intake.
  Older results are not rewritten and newer executions are not modified.
- The task target index and a partial index for non-retired network slots avoid
  scanning unrelated historical tasks/routes. Both indexes are in the schema
  sources and were explicitly applied to production.

A real read-only assertion for the drained slot initially returned unsettled=true;
the fixed query returned false. EXPLAIN ANALYZE on an idle slot found a route
scan reading 50,547 historical rows and taking about 723 ms. With both indexes,
the same complete check took 0.594 ms and used the partial route index. These are
individual SQL samples, not a fleet throughput estimate. Concurrent index builds
hit bounded lock timeouts while waiting for existing transactions; incomplete
indexes were removed and the bounded ordinary builds completed. Both final
indexes were verified valid. The route index build took about 1.1 seconds.

The final audit found zero newly applied tasks retaining coordinator expiry
since intake resumed. Old historical rows remain preserved.

## Real Clock and publication validation

At 03:32 UTC, 13 distinct Plans handled by the selected Worker had succeeded,
including three Plans with video enabled. Two more were not yet terminal.

One fully checked sample:

- Channel `UCDpq09nHKe4QDaQ-QrWm03g`
- Plan `c17b6440-e744-5759-98c0-14640b87e77f`
- Command `ebdb7fd9-db71-40c4-8d34-8731103e7d9e`
- About + video enabled; Agent not due.
- One `collect_channel` command, one 34,156-byte logical result, nine captured
  video items. No per-video center commands were created.
- Command created 03:27:57.351, durable result received 03:28:15.938, applied
  03:28:23.067, original execution finished successfully 03:28:28.122 UTC.
- All nine records matched returned title, description, publication time,
  duration, views, likes, comment count and disabled-comment state. Returned
  first-page comment IDs also matched (two stored comments across this sample).
  The items included Shorts and live replays.
- Original About and Video observations each completed once; both observation
  outbox events were published and the Clock became `succeeded`.
- Publication revisions `82175ce9-1af1-4630-9c51-4da7db2511d2` (channel) and
  `f65ee093-8d88-43a5-acc5-46cec021d3e7` (video) were `delivered` to `business`.
- The task was applied with both coordinator fields NULL. The original run was
  `done`, and the original attempt was `success`.

Logical observation timestamps and the Clock's `finished_at` can precede the
receipt's wall-clock application time. They are not used as upload-duration
measurements above.

Proxy failures occurred during the rollout: `FINGERPRINT_PROXY_TRANSPORT`, curl
35 (SSL) and 56 (connection/receive). Some selected-Worker executions retried before
succeeding. This was not an error-free networking run. The incremental BullMQ
terminal failed count remained 37 between pause and the final sample; job failure
**events** also include recoverable executions and are not distinct failed Plans.
The two preexisting transport receipts awaiting central handling were retained.

## Tests

- 34 real PostgreSQL/NATS/Redis fence and mixed-supervisor tests passed, including
  regressions for terminal coordinator renewal, finished historical records,
  newer attempts, active network bindings and mismatched identities.
- 28 original Rota/queue/API/country/recovery tests passed, including five center
  SIGKILL boundaries and both original/whole execution modes. An earlier run hit
  the harness's 180-second overall timeout after 23 passing tests; the final run
  used a 360-second harness budget and had no failures, cancellations or skips.
- Production images imported the center/node modules under Node 20.20.2.
- The deployed real nine-video sample was compared against stored rows and
  publication delivery, not just queue completion.

These checks do not establish fleet throughput or production Agent performance:
Agent was not due in the fully checked sample. API-batch interruption/replay is
covered by the isolated tests. More fleet rollout should follow measured
production observation, retaining graceful drain and the existing task fences.

## Deployment records and rollback

Center image: `qy-allpachong/remote-node-center:whole-4e5788cf00d3`.
The selected node pins the private-registry manifest
`sha256:43e14a8bd36c537eba62ab5a4dc2ae5d1ea01423371ca60385d3bcef5abc0241`.
The recovery fixes are central; node collector code was not changed by that fix.
The final route-index schema addition was applied explicitly after the center
image build; the running runtime JavaScript matches the build manifest.

The center image/opt-in are persisted in the ignored production environment
files. The selected node's actual Compose service persists its image digest and
whole-channel flag; restart retains the mode. Other service specifications and
the deployment-wide default image were not switched.

Private snapshots, source manifests, exact image references and validation
artifacts are under `runtime/remote-center-production/whole-channel-20260913/`.
The node's pre-change Compose/container snapshots are in
`/etc/qy-node/runtime/rollouts/whole-channel-20260913/`. They may contain credentials
and must stay private. Registry credentials were sent through SSH into a temporary
root-only Docker config and removed after pulling; no global node login was saved.

To revert the execution mode, drain the selected Worker, verify its task/network
and result spool have settled, and restore its previous service specification.
Keep the compatible fixed center while reverting a node; do not remove durable
whole-result tables or stop a node holding unacknowledged results. Do not blindly
restore the old center image, which still contains the historical-recovery defect.
