# Video fallback migration incident

Affected batch: `legacy-results-canary-1788785030266-4ab36f0d` (100 channels).

The first audit found 62 channel records with `BUSINESS_RUN_NOT_FOUND`, one
nonexistent-channel rejection, and 56 video fallback requests without content
type evidence. These 56 videos consumed six batch API calls. The earlier
deployment probe consumed one additional call, making seven calls for 57 videos
that day. Successful API Task delivery did not mean channel completion.

## Root causes

1. The new budget client percent-encoded the colon in `run:<uuid>`. Rota's route
   parameter was passed to its database lookup without unescaping. A real Run
   returned 404 for `run%3A...` and 200 for `run:...`, with four of nine network
   attempts used. These control-plane failures interrupted proxy retry recovery.
2. The adapter treated the final Bull Job attempt and cumulative Detail claims
   as exhausted network retries. Control-plane errors had consumed those counts.
   A bot challenge then entered API fallback before the Rota budget was exhausted.
3. No authoritative type evidence had been observed when the bot challenge hit.
   `videos.list` cannot determine Shorts. Metadata delivery therefore failed the
   full-detail validation and terminated the snapshot instead of repairing it.

The original production probe injected a parser error with valid type evidence.
It verified batching and consumers, but did not exercise a real Rota budget
lookup or the interaction between control-plane retries and bot challenges.

## Fix

Application `6ba7824` preserves the colon in Rota Run lookup paths. Managed
network fallback checks the authoritative business budget, regardless of Bull
attempt count or cumulative Detail claims. Full-detail requests without
authoritative type evidence do not spend API quota. Existing API requests without
type evidence re-enter YouTubeJS acquisition during checkpoint recovery instead
of repeatedly consuming an unusable cached result.

Recovery retries each eligible original snapshot Job using the existing
`retryFullCrawlSnapshotJob` identity/contract checks. It preserves checkpoints,
generation, attemptsStarted and actual Rota budget consumption. It does not
rebuild the migration or erase already fetched contents.

Thirty-four affected-module regression tests pass in host Node and the formal
Node 20 image, including the exact colon-bearing lookup, remaining network
budget despite final Bull attempt, and zero API calls when Full type is absent.

## Recovery outcome

All 44 formal services run `6ba7824`, with zero restarts. Fifty-six failed original
snapshot Jobs were retried. At 13:06:44 UTC, all 99 valid Runs were done; one
nonexistent channel was rejected. Publication readiness was 64 ready-auto and
35 ready-partial. The 1,272 ready-auto contents had no missing core values or
negative counters. All 2,873 detail targets settled. Recovery added no API calls:
daily usage remained seven calls for 57 videos, including the earlier probe.

One Run, `run:292d0d01-bee8-42a0-94e8-da9b75e9ec42`, required additional state
repair. Legacy repair had filled all details without closing the Full fetch
receipt. Its original snapshot had exhausted nine attempts, including the two
failed budget lookups at 12:45:25 and 12:45:36. Only those two attempts were
credited (limit 9 to 11); counters and history were retained. Its binding was
restored to materialized with an operator audit in the Run result.

The first closing attempt encountered a migration retry-state fence. Recovery
item 272 was then advanced from pending to retrying after confirming its exact
failed Job/attempt 19 and generation. The failed candidate ownership was released
with an exact compare; the Detail fence was not reset. The original snapshot
resumed at a higher attemptsStarted, wrote the normal fetch receipt and completed
publication readiness. No synthetic receipt or content-type evidence was written.

All 35 ready-partial channels were confirmed dormant. Forty-one stale candidate
error messages remained after successful Run completion; they were archived in
`source_json.video_api_incident_recovery` and cleared only after verifying a
completed Run, complete Full fetch receipt and no active candidate owner.
