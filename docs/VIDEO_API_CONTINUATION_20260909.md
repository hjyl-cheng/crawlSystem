# Durable video API continuation

The shared Full Crawl and incremental video fallback previously blocked its channel worker for up to 180 seconds. Pending/deferred API results then used `VIDEO_API_FALLBACK_UNRESOLVED`, which was treated as a terminal parser failure. Subsequent API completion updated the durable request but did not resume its channel.

Pending requests now yield with `VIDEO_API_PENDING`. The original BullMQ Job stores its request identity and enters delayed state without consuming a failure attempt. A lightweight gate checks the durable request every 15 seconds before entering candidate ownership or Rota. Terminal API results resume normal validation; an unavailable/invalid result remains a real failure.

On resumption, stored evidence is consumed under the original candidate/run/detail fences without another network Task. If unfinished videos require network access, an explicit replay guard returns control to normal managed execution. Incremental Item claims are released as pending; the frozen Plan hash, video target batch, and completed domain checkpoints remain intact.

Rota acknowledges `api_continuation` only for a successful, quiesced Task with unfinished business work, no failure observations, and no country switch. Its completion receipt excludes this handoff from network retry budgets while keeping Task sequence numbers monotonic. Ordinary failure budgets and completion idempotency still apply. Deploy Rota before the updated workers. No database schema migration is required.

## Verification

- Real PostgreSQL + Redis, concurrency 1: another channel completes while Full and incremental requests are pending. Restart the worker, hold API completion for 185 seconds, deliver one shared Task result, and verify both original Jobs finish with no additional YouTubeJS calls or network Tasks.
- Full Crawl PostgreSQL replay preserves About/uploads and rejects the old attempt fence.
- Incremental PostgreSQL replay releases the pending Item, emits no premature failed Observation, and finalizes the original batch exactly once.
- Shared API batching, quota deferral, result replay and terminal failures.
- Rota PostgreSQL: 20 API handoffs do not exhaust the network budget; nine actual failed Tasks do. Completion replay cannot change budget accounting.
- Existing channel lifecycle, fallback, frozen Plan, Rota adapter, proxy-control and API regression suites.

## Historical failed snapshots

The 15 affected channels have 20 durable API results. Worker trigger logs identify 13 video requests from 11 channels as `youtube_challenge` and seven requests from four channels as `proxy_transport`. All stored API evidence passes the current full-detail validator. The channels still have unfinished videos and must resume, not be marked successful manually.

`scripts/recoverPendingVideoApiSnapshots.mjs --candidate-ids <explicit list>` checks the latest Run, original failed Job, candidate generation, publication state and stored API evidence. `--apply` records the original timeout in Run audit metadata and retries the same snapshot with monotonic attempt fences; it does not start new migration inventory.

Historical Rota Tasks predate the handoff receipt. Before replay, inspect each affected Run's last Task and its timeout timestamp. If that failed Task consumed the final budget solely at the API wait boundary, an audited one-time allowance of one Task compensates that historical handoff; do not reset network history or change the global failure limit. Preserve the explicit Run/task list and before/after values in the deployment audit.
