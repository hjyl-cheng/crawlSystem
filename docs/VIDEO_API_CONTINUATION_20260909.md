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

## Deployment follow-up

Revision `3e1a849` was deployed to Rota, API, controller, 40 Full Crawl workers,
20 incremental workers, the API batch worker, three Agent workers, Finalize and
content enrichment. The Node 20 image passed 92 tests; the real Redis/PostgreSQL
continuation test held API delivery for 185 seconds across a worker restart.

Automatic final repair can replace the original API timeout with a budget-exhausted
terminal Business Run Binding. The explicit recovery script accepts the saved
validated dry-run plan for the same candidate/Run/Job/request, and restores only
that budget-exhausted binding in the repair transaction. A later, already completed
or published Run is not retried. The audited 15 historical Runs each received one
additional Rota allowance for the old API timeout boundary; global limits and Task
history were preserved.

Psicose (`UCnptMlmRB4HiS7F9IRP0v7Q`) and Darek BR
(`UCM-UsGS5XNIOvcxAadYuYWQ`) reproduce HTTP 200 with a structured YouTube ERROR
alert, `The playlist does not exist.` Their About country is Brazil; all three
content tabs are absent. The shared uploads loader now passes that exact verified
response to the existing country/dormancy policy. Transport failures, missing raw
evidence, and responses containing unparsed video IDs still throw. Real managed
probes from GB requested BR, received `unavailable`, and returned dormant with
`no_country_reserve` for both channels. This proves policy execution, not that
regional restriction was the cause of the missing playlists.

The Finalize recovery contents revision lookup now specifies both channel_id and
run_id, matching the existing index. On the production stopped batch, the fixed
full read-only query returned 200 candidates in 19,566 ms; the original exceeded
10 seconds and the live controller query ran for minutes. The whole query remains
substantial; this is not a measured migration throughput claim.

A fresh official videos.list request for RECEBA video `rGxicmKEPCY` returned
likeCount=801, commentCount=17, and no viewCount. Missing view count was not caused
by dropping a returned API field. Do not manufacture a zero or mark that detail
complete without evidence.

## Narrow replay view estimate

The user approved a fixed 2.5% like/view ratio for completed live replays only
when YouTubeJS has no view count and a successful official videos.list response
also omits it. The API must confirm a public ended live, with a valid end timestamp,
not live/upcoming, and a positive safe-integer like count from API statistics.
No estimate is made for ordinary videos, failed/unverified API responses, malformed
counts, zero/missing likes, private content, or an existing view count (including 0).

The merged detail records `view_count_status=estimated`,
`view_count_source=youtube_data_api_likes_estimate`, and the method/rate/input in
`view_count_estimation`; the raw API response is preserved. A later real API count
replaces the estimate and removes its estimation metadata. For `rGxicmKEPCY`,
801 / 0.025 = 32,040. Replaying the stored production evidence passes both Full
Crawl and incremental metrics validation. This ratio is a user-selected heuristic,
not a measured platform benchmark.

## Pending snapshot recovery handoff

Andreza receitas (`10731`, `run:fceb63e7-a827-49c9-806c-04b8e2103ba0`)
retained pending retry evidence for original snapshot attempt 3. Final Repair
requeued that same Job as attempt 4 without changing the pending record. The
inline detail fence correctly rejected pending recovery ownership before details
could resume. Rota Task 9 (`464d4880-5248-4aa3-8ebe-15bf58556da6`) lasted under
half a second, recorded no network failure observations, and consumed the last
business allowance. Attempt 5 subsequently terminated on budget exhaustion.

Candidate activation now atomically transitions matching pending retry evidence
to retrying, before allocating a Rota Task. It requires the exact original Job,
candidate, generation, batch and current materialized Run, a newer attempt, and
no separately allocated recovery generation or Run. The detail fence is unchanged.
This also handles a crash/retry between queue dispatch and worker activation.
The PostgreSQL regression reproduces the pending mismatch, accepts the resumed
Job, rejects the old writer, and rejects six mismatched recovery identities.
The fix passed 41 regression tests and three PostgreSQL concurrency tests, and
image `pachongsys-b457385` was deployed to the API, 40 Full Crawl workers and
20 incremental workers.

Before the proposed original-Job repair could run, the existing checkpoint
repair completed all 30 details at 2026-09-09 03:50:45 UTC. The repair script's
preconditions stopped it from resetting that completed Run or its binding.
The unused one-Task allowance was withdrawn, restoring the original limit of 9;
no additional Task was allocated and route history was preserved.

Historical retry 637 still remained pending. A one-time guarded transaction
verified the exact candidate, generation, batch, original failed Job, latest Run
and all 30 completed details, then moved only that retry to `retrying`, recording
the checkpoint handoff in its failure evidence. The existing recovery reconciler,
scoped to retry 637, then ran the fenced Agent and Finalize jobs. The original
Run published as `ready_auto` at 2026-09-09 03:59:36 UTC with 30/30 details done,
Agent done and no Run error. This live completion used checkpoint recovery;
the original-Job activation fix is verified by the PostgreSQL regression.
The scoped reconciler subsequently resolved retry 637 from the verified
publication outcome, with zero stale fences or queue conflicts.
