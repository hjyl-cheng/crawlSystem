# Add Workers and toggle intake

The manager accepts a Worker type and an **additional** count. Its two type
options are incremental and Query; Query is visibly disabled until its remote
deployment contract is implemented. A preview states installed + additional =
total. Intake count remains independently editable. Pausing no longer removes
the action: the same button becomes Start. The browser remembers positive intake
counts in localStorage; resume clamps that preference to the installed count.
If no browser preference exists, the installed count is used and shown in the
button tooltip. The authoritative admission state remains on the server.

## Failure diagnosis

Node `43.172.83.170` retried target 20 at 2026-09-11 08:13:19 UTC and failed at
the files step before container start. Read-only SSH returned MemTotal 7806160
KiB (7.44 GiB). The unchanged installer rule requires count × 768 MiB + 512 MiB,
so 20 requires 15.50 GiB and this node permits at most 9 by that rule. This is
the installer's admission calculation, not a measurement of actual Worker usage.

The original installer path reproduced the generic `NODE_DEPLOYMENT_INVALID`
error for a valid 20-slot recipe and this memory size. A regression expecting an
explicit memory reason failed before the fix. A read-only SSH capacity check now
runs before center registration; insufficient memory is stored as a clear error
with actual/required capacity and maximum count. The installer retains its own
memory check. The browser previews the same requirement from monitor data, while
the server reads actual `/proc/meminfo` before installation.

## Failed targets and additive retries

The API accepts `role`, `additionalCount`, and `expectedInstalledCount`. It
computes the total from the verified installed count, validates registry version
and installed count, and freezes that total in the existing deployment operation.
Replaying an addition cannot add again. Legacy absolute-count calls remain valid.

A failed target 20 must not prevent an installed fleet of 5 from adding only 1.
Existing unused center registrations remain reserved rather than being deleted.
Center prepare rejects excluding any connected, admitted, draining or unsettled
slot. The remote installer additionally checks all node-labeled containers,
including stopped containers, and rejects a recipe that omits one. Existing
credentials, slot identities, containers and spool data are retained. Connection
verification permits the extra unused registrations but requires all intended
slots to be connected before updating installed count.

The dashboard allows count-based start/pause within the verified installed
capacity even when expansion failed. Center admission checks still enforce live
connections, readiness, deployment identity and expected current count.

## Verification and deployment

- Real PostgreSQL/Redis tests cover unused registrations, refusal to exclude
  connected/admitted/draining slots, original queue consumption and graceful drain.
- Dashboard PostgreSQL tests cover failed target 20 followed by a smaller
  addition, stale/repeated submissions, unsupported type rejection, intake
  synchronization, and memory failure before any new center registration.
- Real SSH/SFTP/sudo fixture tests (11 passed) execute the fixed installer against
  a simulated Docker daemon, preserving credentials/spool and rejecting omission
  of existing containers. Capacity and route tests also pass.
- Browser tests cover exactly two types, additional-count arithmetic, memory
  rejection before submission, failed20→add1, pause/reload/start retaining 5 even
  after installed count grows, center intake toggle, first deployment and mobile.

Only Dashboard and center deployment management were replaced with
`worker-add-20260911`; both old processes exited 0 and are retained for rollback.
No collector was deployed or replaced. Runtime and checked-in compose image
references were updated. Production verification found local 20 connected and
admitted, remote 5 connected and **0 admitted**, preserving the user's pause.
Both queues remained unpaused and Full Crawl had 40 active jobs. A read-only
execution of the new capacity check against the real node rejected 20 with the
explicit 7.44/15.50 GiB message and accepted 9; it started no containers.
