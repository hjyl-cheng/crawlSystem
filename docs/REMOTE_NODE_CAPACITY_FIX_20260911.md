# Remote Worker startup blocked by Rota capacity

## Cause and change

The three page-authorized remote incremental Workers were connected and requested activation, but none could obtain a Rota channel slot. Production had 61 provisioned slots, all leased by 40 migration Workers, 20 local incremental Workers, and one auxiliary Worker. Healthy reserve proxies did not imply available Worker slots.

`deploy/compose.migration-rota-scale.yml` now specifies 64 slots for this fleet. Apply this override alongside the existing Rota configuration on subsequent deployments. Future fleet expansion must include remote Worker slots in the capacity budget; this change does not introduce automatic Rota resizing or a hot configuration API.

The center reports Workers waiting for a network assignment and a cached, read-only capacity summary. Capacity requests occur outside the application database transaction, are shared across status requests, and are cached for ten seconds, including unavailable responses. Dashboard distinguishes **等待网络名额** from **等待可用网络线路**, shows occupied/total slots when full, and leaves pause available. No shared YouTubeJS extraction or Plan processing policy changed.

## Validation

The PostgreSQL/Redis execution-control regression failed before the change because `preparation` was absent, then passed with the real supervisor, queue consumers, and a deliberately pending route. It also verifies cached capacity reads, ordinary shared-queue consumption, graceful pause, resume, and pause during startup. Deployment administration regression also passed (two tests, no skips).

Dashboard suite passed: 66 passed, eight environment-dependent skips. A browser fixture verified full slots, available slots awaiting routes, and ready states, including the pause button. Sandbox-local HTTP binding was unavailable, so the HTTP/browser tests were rerun with local network permission.

## Production cutover

- Images: `qy-allpachong/remote-node-center:node-capacity-20260911` and `qy-allpachong/dashboard:node-capacity-20260911`.
- Temporarily stopped the dispatch controller and paused only network collection queues. Preserved initial pause flags; API batch and postprocessing queues remained available. Controller stop exceeded its 60-second grace period and exited 137 without OOM; its existing container was subsequently restarted and dispatch recovered.
- Waited for all collection queue active counts and all Rota active tasks to reach zero before recreation. Existing collection Worker containers were not restarted.
- First Rota shutdown reported a deadlock while flushing six proxy usage records, so the protection script restarted the original container and stopped the cutover. These are proxy usage statistics, not channel collection records. A second guarded shutdown/recreation succeeded.
- Recreated Rota with the same `qy-allpachong/rota-core:pachongsys-3e1a849` image, mounts, ports, resource limits, and network aliases; changed channel capacity from 61 to 64. Original container retained stopped as `youtube-rota-qy-core-before-node-capacity-20260911`.
- Verified channel capacity: desired/provisioned/assigned/ready/claimed all 64. All three remote Workers became enabled and ready while retaining the user's existing start request.
- Restored queue pause flags and restarted the dispatch controller. Migration returned to 40 active Workers. All three remote slots received real incremental work; the first two observed completed results returned `status: done` and had no task errors. These were existing queued Plans, not duplicate test submissions.
- Final observation: remote slots 1/2/3 had respectively 2/2/4 applied `done` tasks (eight total), with one further task leased to each slot.

Private rollback configuration and queue snapshots are in the ignored `runtime/remote-center-production` directory. No credentials are recorded in this document. This task has not committed or pushed source changes.
