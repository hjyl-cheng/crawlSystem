# Per-server incremental Worker admission

The server page separates installed Worker count from allowed intake count. A
server may admit 0 through its installed count. Reducing admission preserves
active channel work; increasing it reuses existing Workers. There is no new
cross-server/global limit and no change to channel/Clock collection contracts.

## Remote nodes

`setExecution` accepts `allowedCount` and `expectedAllowedCount` alongside the
registered deployment identity/count. The existing boolean operation remains
compatible. The transaction validates installed capacity and concurrent edits,
then selects stable slots, retaining previously admitted slots before adding
connected slots. Newly deployed slots remain unrequested.

The existing supervisor drains removed slots and releases their Rota identities
only after active processors finish. Added slots use the existing automatic
network-capacity provisioner and become eligible when their route is ready.
Admission is stored in `worker_connections.activation_requested`, so it survives
center and collector restarts.

## Existing center Workers

This does not migrate the center fleet to the remote collector deployment model.
The current direct incremental processor is wrapped by an optional admission
controller (`LOCAL_INCREMENTAL_INTAKE_CONTROL=true`, incremental-only,
concurrency 1). It pauses/resumes the local BullMQ Worker, keeps the original
job/lock/processor, and checks authorization at the processor boundary.
Control-read failures defer jobs without consuming ordinary failure attempts.

`remote_ingestion.local_incremental_workers` stores stable Worker identities,
instance fencing, requested admission and short-lived runtime telemetry. New
registrations default to standby. Adoption of the existing 20 running Workers
seeds exactly those identities as admitted, preserving production behavior.
Restart preserves requested admission; the previous instance must relinquish or
expire before replacement registration. Clean shutdown keeps heartbeats until
the active processor finishes.

Apply `src/remoteNodes/localIntakeSchema.sql` explicitly before enabling local
control. It is also included in `scripts/applyRemoteNodeSchema.mjs`. It creates
only the new control table; channel/video/Clock schemas are unchanged. The local
Worker database role needs schema USAGE and table SELECT, INSERT, UPDATE.

The dashboard exposes a protected `local-center` card when
`SERVER_NODE_LOCAL_INTAKE_CONTROL=true`. Its count API uses the same authenticated
center deployment-control endpoint. The card does not expose SSH initialization,
container deployment, editing or deletion for this existing local fleet.

## Capacity and display

Named controlled consumers publish short-lived admission markers in their own
BullMQ queue namespace (`intake:<worker-name>`, TTL 15 seconds). The dispatcher
counts only admitted live named consumers; older unnamed Workers remain counted
for rollout compatibility. Missing/expired markers fail closed. Active draining
jobs still contribute queue pressure, while standby Workers contribute no new
capacity. The previous future-delay exclusion remains in effect.

Page counts distinguish installed, admitted, connected, running, draining, idle
and standby Workers. Count editing uses a dialog so background refresh cannot
replace typed input. Saving includes the displayed admission count for optimistic
concurrency protection.

## Validation

- Selection tests retain admitted slots, prefer connected additions, reject
  counts outside installed capacity and distinguish partial drain states.
- Real local PostgreSQL/Redis/Worker test covers 0→2→1→0→3, active drain, no next
  channel on removed Workers, capacity accounting, restart at zero and stale edits.
- Real remote supervisor test covers shared local/remote job consumption, active
  drain, 0→2→3→1→0, deployment growth, restart at zero and stable slot choice.
- Dashboard route tests include controlled migration policy and the protected
  center endpoint. A real browser with isolated mock controls exercised center
  20→5 and remote 5→0, automatic status updates and mobile overflow checks.

## Rollout

Images use `intake-count-20260911`; service overrides are recorded in
`deploy/compose.intake-count.yml`. They layer only the affected modules onto the
previous running images. No remote collector image or Full Crawl Worker changes
are needed. Before/create private configurations are preserved in
`runtime/remote-center-production/intake-count-rollout`, with old containers
retained stopped for rollback. Rollback must restore both Worker images and the
previous dispatcher so old unnamed consumers are interpreted consistently.

Production targets remain center 20 and remote node 5; the user can change them
on the server page after rollout. Testing reduced counts only in isolated
fixtures; production count verification uses idempotent saves of existing values.

### Production result

All 23 target services were replaced after incremental jobs drained; their old
processes exited with code 0. Incremental intake was resumed. The local control
table contains exactly 20 adopted Worker identities, all connected/admitted;
the remote node remains 5 connected/admitted. Both production count endpoints
accepted idempotent saves (20 and 5) through the Dashboard's controlled-write
policy. No production allocation reduction was used as a test.

The deployed dispatcher identified all 25 admitted consumers. At verification,
the current day's eligible unscheduled plan count was zero, so idle workers were
expected. Full Crawl remained unpaused with 40 active jobs. The private current
center env/image and Dashboard compose configuration were updated alongside the
checked-in service override. Dedicated test containers and their anonymous
volumes were removed after all integration tests passed.

### Failed expansion boundary

The remote node subsequently attempted expansion from 5 to 20, but file
preparation failed. Registration contained 20 slots while only the original 5
were installed and connected. Dashboard capacity now uses the verified
`appliedCount`, exposes registration separately, and allows reductions, pause
and idempotent saves while that expansion is failed. Increasing admission
requires a successful deployment. The center accepts the verified installed
prefix within the larger registration and never admits the uninstalled slots.

An additional real PostgreSQL/Redis supervisor regression and Dashboard route
tests passed for this case. Center and Dashboard were updated to
`intake-count-20260911-r2`; existing collection Workers and the dispatcher retain
the first intake-count image. Both replaced services exited cleanly. Incremental
intake was resumed; production original-value saves returned HTTP 200 for local
20 and remote 5. Remote status reports deployed 5, registered 20, connected 5.
Full Crawl remained unpaused with 40 active jobs. The failed expansion was not
retried by this rollout.
