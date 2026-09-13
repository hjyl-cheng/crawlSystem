# API waiting and incremental dispatch capacity

## Incident evidence

At 2026-09-11 06:29 UTC, 26 incremental jobs were waiting for shared video API results.
All 26 API tasks had `attempts=0`, `status=pending`, and `next_retry_at=2026-09-12T00:05:00Z`.
Their batches recorded `daily_request_limit_reached`. The configured system allowance of
500 requests had been consumed by 00:09:27 UTC. This did not establish exhaustion of
YouTube's own quota. The user subsequently raised the shared allowance to 10,000.

The original fallback trigger was `youtube_challenge` for 24 videos and
`proxy_transport` for 2. The retained BullMQ failure reason was route budget
exhaustion (two failed attempts per job). Hundreds of subsequent starts were
15-second API status checks, not additional YouTube/API requests.

The incremental dispatcher targeted 35 queued/executing jobs for 25 workers while
Full Crawl had a backlog. Counting 26 future retries as pressure left about 9
ordinary jobs. Workers had been released, but replacement work was suppressed.

## Changes

- The incremental capacity probe uses Redis `ZCOUNT` on BullMQ's delayed deadline
  index. Only due delayed jobs contribute to incremental dispatch pressure.
  Raw delayed counts remain available; older telemetry without the new count
  stays conservative. Full Crawl's competing-pressure calculation is unchanged.
- This applies to future incremental retry deadlines generally, without changing
  retry limits, task identity, queue ownership, clocks or collection policy.
- The shared local/remote API gate joins its durable request to the API task and
  delays until the later of its normal poll interval and `next_retry_at`.
  Completed/failed/unavailable API results still enter the existing fenced replay.
- No schema migration, manual replay, failed-job deletion, or early rescheduling
  of the existing quota-deferred API requests is required.

## Validation

The original implementation failed the new tests (35 units of pressure instead
of 9; tomorrow's API request rescheduled for 15 seconds later). The fix passed
70 related unit/contract tests. One optional remote-entry integration test was
not configured and was skipped.

Two real PostgreSQL/Redis integration tests passed in a dedicated temporary
database and unique Redis prefixes, including:

- 26 future delays excluded while 3 due retries still count;
- Full Crawl and incremental API continuations survive Worker restart;
- a future API deadline prevents repeated Worker starts;
- an ordinary channel completes during the wait;
- late API evidence completes both consumers without additional network work.

The temporary database and Redis fixtures were removed after testing.

## Deployment

Patch tag: `api-wait-capacity-20260911`. Images replace only the relevant module
on each previously deployed base image:

| Service | Base | Replaced module |
| --- | --- | --- |
| qybullmq | migration-resume-20260909 | videoApiContinuation.js |
| remote-node-center | auto-capacity-20260911 | videoApiContinuation.js |
| feature-dispatch | pachongsys-60a7314-rota-recovery | dynamicDispatcher.js |

The rollout targets 20 local incremental Workers, the remote center (which gates
the 5 remote Workers), and the incremental dispatcher. It leaves migration
Workers and remote collector containers running. The shared gate fix is present
in source for Full Crawl, but existing Full Crawl containers are not restarted
in this rollout.

Pinned service overrides are in `deploy/compose.api-wait-capacity.yml`. Private
before/create container specifications and retained stopped containers provide
rollback evidence under `runtime/remote-center-production/api-wait-rollout`.
The current remote-center environment file must reference the new image.

## Production verification

The rollout completed after pausing only incremental intake and waiting for its
active jobs to finish. All 22 replaced processes exited with code 0. Intake was
resumed and the current remote-center environment was updated to the new tag.

At 06:54:40 and 06:56:15 UTC, incremental execution was 25/25, with 5 and 9
ordinary waiting jobs respectively. The dashboard reported remote execution
5/5 active. All 27 API quota waits (26 original plus one added before rollout)
had their BullMQ deadline equal to their durable API deadline. Their start
counters and deadlines were unchanged across the 94-second observation.
Completed incremental jobs increased by 252; failed-job count stayed at its
pre-rollout value of 5. Full Crawl stayed unpaused with 40 active jobs in both
snapshots.

The live dispatcher reported raw delayed=27, delayed_ready=0, active=25,
waiting=8, incremental_pressure=33, target=35 and available dispatch budget=2.
This confirms the waiting API jobs no longer consume the runnable-work budget.
