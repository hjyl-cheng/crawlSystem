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

## Production verification

Deployed application `f576c05`, image
`qy-allpachong/qybullmq:pachongsys-f576c05-video-api-batch`. The additive table
was applied to `newcrawler_crawler` before rollout. All 44 services were updated:
20 Full, 20 Incremental, API Batch, Finalize, controller and API server. Previous
containers are retained with `-before-f576c05` for rollback.

Controlled validation used the already repaired video `1xobqeOzsFE`. The scraper
failure was injected; the downstream controller, queue, API worker and Google
API request were real. Three bounded failures entered fallback. Full and
Incremental subscribers shared Task 62, and the controller dispatched it while
the query scheduler was stopped. One `videos.list` request fetched one video.
Both subscribers received complete metadata with authoritative retained Shorts
evidence. No stored content was updated or republished by this probe.

The real Incremental `first_seen` and `recent` adapters also consumed the same
evidence successfully. API playback/like counts have exact statuses, including
when the injected partial evidence was estimated/unresolved. The API reported
comments disabled; this probe therefore did not require a live commentThreads
call. These checks reused cached evidence and added no API requests. Request
identifiers explicitly carry `deployment-proof` to distinguish probes from
ordinary checkpoint consumers.

Post-rollout verification retained the same 2,954 completed Runs, 1,093 finalized
checkpoints and 5,636 delivered outbox rows. The original 100-channel batch remains
71 ready-auto, 28 dormant ready-partial, one rejected channel, and 1,278 contents
with no missing core values, negatives or out-of-window contents. TropaTaspio
has 30 and Maite kids six contents in both business and search projections.

Final-image Node 20 checks passed: 40 integration-point unit tests before the
count-status follow-up, and 24 targeted tests after it. Temporary PostgreSQL,
Redis and schema-application containers were removed.
