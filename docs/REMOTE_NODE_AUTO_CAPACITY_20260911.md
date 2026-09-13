# Automatic network capacity for remote Worker deployment

## Behavior

Dashboard's deployment preparation now registers the frozen Worker configuration, then asks the center to ensure enough Rota channel slots. Starting a node also checks capacity before changing desired activation. Pausing does not depend on Rota availability.

The center computes an absolute target from the configured local fleet plus all active registered remote deployments. Production's local baseline is 61 (40 migration, 20 incremental, one auxiliary). Three remote Workers require 64 slots; five require 66. Additional nodes contribute their own registered counts. Browser-provided counts are not used for provisioning.

Deployment failures retain the original node/deployment IDs and credentials. Retrying reuses the same absolute target. New Workers remain paused until explicitly started; provisioning network capacity does not authorize collection.

## Rota contract

`POST /api/v1/proxy-control/capacity/ensure` uses the existing private control-token authentication and strict JSON decoding:

```json
{"role":"channel","minimum_slots":66}
```

Only channel capacity from 1 through 500 is accepted. The response reports provisioned slots; it does not promise healthy proxy availability. Existing assignment/reconciliation still decides when Workers have a usable route.

Migration 1015 creates `proxy_control_capacity_targets`, scoped by workload and role. A transaction under the existing Rota resource lock persists the maximum requested target and creates missing slots. The online path skips existing pools, users, credentials, routes and tasks. Failure rolls back both the target and partially created resources. Concurrent requests and lower/stale retries cannot reduce capacity.

Startup and periodic resource synchronization use the maximum of `ROTA_CHANNEL_SLOTS` and the persisted target. No process-local options are mutated and no Rota restart is needed for subsequent growth. Pausing, deleting or reducing a deployment does not automatically shrink network slots.

## Deployment configuration

Center configuration:

```dotenv
REMOTE_NODE_AUTO_CAPACITY=true
REMOTE_NODE_LOCAL_CHANNEL_SLOTS=61
```

The center uses its existing Rota control credential, never a remote node's token. If the separately managed local fleet changes, update the local baseline accordingly. Remote deployments are counted automatically.

Images:

- `qy-allpachong/rota-core:auto-capacity-20260911`
- `qy-allpachong/remote-node-center:auto-capacity-20260911`
- `qy-allpachong/dashboard:auto-capacity-20260911`

Append `deploy/compose.rota-auto-capacity.yml` after existing Rota image overrides on subsequent Compose deployments. Existing remote-center manifests and private runtime configuration remain in use. Rota's new binaries are built from this workspace; the release image retains the prior runtime filesystem, CA certificates and GeoIP data.

## Verification

- Real PostgreSQL Rota tests cover growing capacity with an active execution, concurrent requests, repeated/lower targets, restart resource synchronization, new-slot claims, invalid bounds and atomic rollback on resource creation failure.
- Rota proxy-control, API and database package regression suites passed. The new API rejects missing/wrong control tokens and unknown/trailing request fields.
- Center PostgreSQL + HTTP integration verifies 3 to 5 Workers, multiple nodes, stable credentials, retry after provisioning failure, unchanged activation on failed start, and pause independent of capacity service.
- Existing deployment and execution-control integration tests passed.
- Dashboard suite: 66 passed, eight environment-dependent skips.

## Production verification

Migration 1015 applied online as the only pending migration. During the initial binary upgrade, collection intake was paused and all active collection/Rota tasks were allowed to drain. The dispatch controller exited cleanly (0). Rota and the central gateway were updated; the prior Rota container is retained stopped as `youtube-rota-qy-core-before-auto-capacity-20260911`. Original queue pause flags and the dispatch controller were restored afterward.

An idempotent request through the live Dashboard start endpoint invoked the center's automatic calculation and persisted 64 slots for the existing local baseline and three remote Workers. During resumed collection, the live authenticated Rota interface then received targets 66, 66 and 64. Every response after growth reported 66. The Rota container ID/start time stayed unchanged during this online growth test, and 41 tasks active across both snapshots retained the same lease, proxy and route generation.

Final verification found 40 active migration Workers, three connected/ready remote Workers, and all 66 preexisting local Worker containers running with unchanged IDs/start times. The durable capacity minimum is now 66, leaving two network slots available for the user's next expansion. No extra remote Workers were deployed for this test.

Capacity snapshots, private configuration backups and the live-check result are in the ignored runtime directory. A rollback to the old Rota binary must configure its static slot floor to at least the persisted target (currently 66), because that old binary does not read the new table.

No changes to YouTubeJS extraction, migration policy, Clock generation or the shared incremental queue are part of this feature. Source changes have not been committed or pushed by this task.
