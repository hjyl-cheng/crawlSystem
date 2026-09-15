# Paused remote Worker CPU and deployment intake

A paused fleet still consumed nearly four CPU cores. Each idle executor pass
reopened all historical whole-channel journals, fsynced them, parsed their JSON
and recomputed every record hash before noticing that a journal was terminal.
The measured fleet read 321 MiB in three seconds with all 45 Workers disabled.
Separately, deployment could turn an old `syncIntake` form value into a request
that enabled the entire fleet, including an explicitly paused node.

## Changes

- Move conclusively expired whole-channel journals to `whole-archive`, retaining
  evidence and counting it against the existing spool limit. Sync both parent
  directories after the rename. Unacknowledged results remain in recovery.
- Reconcile journals at executor startup and after an execution error. Successful
  idle passes no longer replay historical journals.
- A paused executor waits for the existing readiness heartbeat to enable intake.
  Heartbeats continue; shutdown wakes the waiter. Recovery and result delivery
  run before the idle wait, so pause does not strand undelivered results.
- Worker deployment never changes intake. Remove automatic intake from the page;
  old callers sending `syncIntake` remain compatible but cannot enable Workers.
  First deployment also waits for an explicit intake action.

## Verification

- A synthetic 19 MiB archive repeatedly used 988 ms CPU and read 57 MiB across
  three idle passes before the fix. Afterwards: 47 ms CPU, zero journal reopens
  and zero historical payload reads (timings are local samples).
- Real SIGKILL/restart, lost ACK, stale generation, receipt cleanup and retained
  evidence tests pass. Paused runtime tests cover sleep, activation and shutdown.
- Isolated PostgreSQL deployment/removal tests and 13 channel Plan integration
  tests pass, including API handoff and recovery. Protocol spool tests pass.
- A browser test confirms adding a Worker sends no auto-intake request.
- The first upgraded production Worker sampled 0.00% Docker CPU while paused;
  an unchanged comparison Worker sampled 23.09%. Its 19 MiB archive was retained.

## Production result

All 45 remote containers now use the new digest and NATS whole-channel mode;
all 45 heartbeats are present, and allowed intake remains zero. A three-second
host CPU sample after rollout measured 1.51% busy and 0.08% I/O wait. Historical
journal payloads were retained. The nine centrally received API-wait results
still account for seven draining slots; they were not cancelled or recollected.
The migration queue retained 40 active tasks; incremental remained paused.

## Deployment

Only the remote collector and Dashboard node management code need updating.
The center retains its application image, but must reload its permitted collector
digest. After verifying the node is drained and all containers use that digest,
synchronize both deployment registries in a guarded transaction; otherwise the
next page-managed expansion correctly rejects the image mismatch. Migration
Workers and database schema do not need changes. Preserve
node spool mounts and intake settings; verify no live collection lease before
graceful replacement. An old spool lease may refer to an already applied task;
confirm its identity and central state rather than deleting it.

`deploy/compose.paused-worker-recovery.yml` pins the Dashboard and private
collector digest for subsequent page-managed deployments. Apply it after the
existing Dashboard overlays. Apply `compose.paused-worker-center.yml` to the
independent center stack. The node's Compose manifest must use the same
digest. Never use a bulk forced restart of actively collecting Workers.
