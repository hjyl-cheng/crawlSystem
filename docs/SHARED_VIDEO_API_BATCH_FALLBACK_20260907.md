# Shared video detail API fallback

Full Crawl v3 and the Incremental checkpoint executor share the existing
`youtube-data-api-batch` queue, Task table, Batch worker, API keys and daily quota.
Channel/About and Uploads pagination remain YouTubeJS-only. No yt-dlp is added.

## Behavior

- New migrations default to `youtubejs_full_v3`. Historical v1/v2 hashes and
  recovery contracts remain frozen. Incremental enables the adapter with
  `YOUTUBEJS_VIDEO_API_BATCH_FALLBACK=true`.
- YouTubeJS remains the first source. Detail attempts are bounded at three;
  parser failures retry locally, while transport failures retain existing
  Job/Rota retry behavior. The final Bull attempt or authoritative remaining
  Rota business budget can trigger fallback earlier than the per-video cap.
- API requests are durable and keyed by consumer/checkpoint/video. Full and
  Incremental subscribers deduplicate against the existing per-video API Task.
  Resuming an existing request consumes its result without scraping again.
- The controller flushes partial batches (maximum 50 videos). This demand is
  independent of the query scheduler and the legacy scrape-failure circuit.
  It still uses the existing global API mode and daily quota.
- The API worker delivers evidence inside its Batch execution fence. Consumers
  then validate and commit through the existing Full/Incremental checkpoints.
  The API worker does not publish or settle these consumers itself.
- API metadata never invents Shorts classification. Observed YouTubeJS type
  evidence is retained; missing type or missing API items remain unresolved.
  Incremental metrics require metrics only; first-seen details retain the full
  validation contract. Required comment pages use the existing comment API.
- Stale ownership, database failures and cancellation never trigger fallback.
  Unresolved API evidence never inherits earlier proxy failures or rotates Rota.

## Failure boundaries

Subscriber API failures retry with the existing worker, at most three Task
attempts, with a five-second eligibility delay. Concurrent subscribers do not
reset an exhausted Task's attempts. Quota checks that perform no API call do not
consume a Task attempt.

Consumers wait up to 180 seconds, with cancellation and Incremental claim
heartbeats. A longer deferral (including exhausted daily quota) surfaces an
explicit unresolved result and retains the durable request/evidence. Batch
completion does not automatically publish a failed consumer: recovery must
restore its original fenced checkpoint. API fallback does not promise successful
publication when the API is unavailable or cannot supply required evidence.

## Rollout

Apply the additive request table before starting updated services:

```sh
CONFIRM_VIDEO_API_SCHEMA_DATABASE=newcrawler_crawler \
  node scripts/applyVideoApiBatchRequestSchema.mjs --apply
```

Upgrade the 20 Full workers, 20 Incremental workers, API Batch worker, API server,
Finalize worker and controller. Set the Full default to `youtubejs_full_v3` and
enable `YOUTUBEJS_VIDEO_API_BATCH_FALLBACK`. Keep ordinary queue names and existing
Rota identities. Do not replay the 100 already-settled migration channels.

## Verification

- 172 unit/regression tests passed; 13 integration tests skipped without their
  dedicated database environments.
- A dedicated PostgreSQL scenario exercised the real API Batch processor:
  shared Full/Incremental subscribers, partial batching, one quota charge,
  missing video evidence, duplicate delivery fences, exhausted attempts,
  required comments, and daily-quota deferral.
- Five real PostgreSQL/Redis Worker/Controller recovery tests passed, covering
  stalled ownership takeover, stale scopes, partial commits and orphan recovery.
- Pre-rollout baseline: 2,954 completed Runs, 1,093 finalized Incremental
  checkpoints, 5,636 delivered publication outbox rows; no active Full or
  Incremental Jobs.
