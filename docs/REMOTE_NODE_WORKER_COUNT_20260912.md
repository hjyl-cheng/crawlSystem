# Remote Worker counts, 2026-09-12

## Behavior

Remove the fixed 32-worker deployment limit and the default 32-consumer limit at the remote center. Worker counts are positive safe integers; allowed intake remains 0 through the number actually deployed. This upgrade does not itself install or activate additional workers. Existing additive deployment, optional intake synchronization and graceful intake reduction remain in place.

The Dashboard form, node registry validation, deployment recipe, Python installer, center deployment API and database constraints all accept counts above 32. Existing node `max_leases` had a separate upper bound of 100, also removed. No new tables or collection data migrations are involved.

The supervisor grows its dedicated advisory-lock connection pool to the registered slot count. Each executing slot still owns its own PostgreSQL session lock. Existing task fences, shared incremental collection and migration collection are unchanged by this change.

This removes arbitrary count limits, not infrastructure capacity: existing Rota capacity, database connection resources and deployment payload size protections still apply. Allowed intake cannot exceed installed workers. No automatic increase of existing counts is performed.

## Validation

- Node configuration and recipes accept 150; the original 30 slots retain identical configuration. Invalid counts are rejected.
- Real Dashboard PostgreSQL deployment integration adds workers to 50 and sets allowed intake to 50.
- Real center PostgreSQL integration accepts 150, reduces intake to 50 and 0, and rejects intake above deployed count. Previous slot credentials remain stable.
- Real PostgreSQL and Redis integration runs 40 BullMQ consumers with 40 distinct advisory-lock backends. Of 50 queued jobs, 40 run concurrently; pausing drains these 40 and leaves 10 queued, with no duplicate execution.
- Existing node configuration, execution-control route, installer capacity and heartbeat-health tests pass. New center modules import successfully using the image's Node 20.20.2.

## Rollout

Dashboard override: `deploy/compose.node-worker-count.yml`.
Independent center stack override: `deploy/compose.remote-center-worker-count.yml`.
Apply `services/qybullmq/src/remoteNodes/workerCountSchema.sql` after the worker activation schema. The normal schema installer now includes it. Existing-production upgrade uses a transaction with a 3-second lock timeout and 15-second statement timeout, touching only the two count constraints.

Remote collection images and containers need no update for this count change. Pause intake and drain active work before replacing the center. Replace Dashboard after schema and center readiness, then restore the previous allowed count.

Production completed (UTC): intake paused 06:21:39; all slots drained before center replacement. Center deployed 06:26:41, Dashboard deployed 06:27:17, allowed intake restored to 30 at 06:27:51. At 06:28:45 all 30 slots were connected, ready and executing. Both updated containers were healthy with zero restarts; all 40 migration Worker containers were running. All 30 remote container IDs remained identical across this rollout.

Both live database constraints now enforce only `>= 1`. Live Dashboard HTTP `/assets/server-nodes.js` matches repository SHA-256 `6eb8cfc0533623fc886d7abc5bd4aca3f00260cc466a6db822165d79f0acf197`. Unauthenticated public checks redirect to login; they were not treated as authenticated UI validation. Isolated PostgreSQL and Redis test containers were stopped after validation.
