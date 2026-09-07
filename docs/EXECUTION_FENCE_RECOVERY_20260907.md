# Execution Fence Recovery

Application revision: `7dd94b0`.
Image: `qy-allpachong/qybullmq:pachongsys-7dd94b0-fence-recovery`.

The earlier `a631d8c` repair restored Full Crawl using the original snapshot Job,
frozen candidate generation, and monotonically increasing `attemptsStarted`.
This follow-up closes the remaining error-handling paths that could waste
network attempts after a stale execution rejection.

## Behavior

- `CONTENT_DETAIL_EXECUTION_FENCE_STALE` is recognized as an internal fence
  failure. Candidate and migration-intent stale fences also stop automatic
  BullMQ retries via `discard()`.
- A stale fence overrides earlier network diagnostics in the same execution;
  those diagnostics cannot cause Rota to switch routes after ownership is lost.
- Failure recording is still conditional on the exact candidate owner. A stale
  attempt rejected by this check does not overwrite the newer owner or turn the
  rejection into a generic persistence error that triggers another retry.
- An unfinished YouTubeJS snapshot accepts only its original snapshot Job.
  Incompatible legacy repair Jobs are rejected before candidate activation and
  before a Rota Task is allocated. Existing generation and detail-owner checks
  remain authoritative for writes.
- Legacy content-completeness repair no longer selects YouTubeJS Full runs or
  resets their checkpoints. Those runs belong to the snapshot recovery path.
- Stopped Job history and scoped system-recovery evidence are retained. This
  change does not bypass ownership checks or invent a new valid owner.

No Data API fallback or proxy change is used to resolve a fence conflict.
The separately requested shared video-detail API fallback is not implemented
by this revision; channel information and pagination remain YouTubeJS-only.

## Verification

Three new regression scenarios failed before the fix: stale detail errors were
unclassified, old network evidence requested another route, and the current
attempt was not settled as a terminal system failure.

After the fix: 82 related unit tests, 8 PostgreSQL tests, and 2 Redis tests pass.
The real Redis test configures three attempts and verifies exactly one execution
and no waiting/delayed retry after a stale detail fence. PostgreSQL verifies legal
original-Job takeover, rejection of late writers, and idempotent system-failure
recording. The older PostgreSQL fixtures were updated to supply `attemptsStarted`,
which has been the production fence clock since before this change.

## Deployment

The explicit rollout targets 20 Full workers, 20 Incremental workers, and the
Controller. It preserves runtime configuration and retains old containers with
suffix `-before-7dd94b0`. API and Finalize do not need replacement. No schema
migration or channel-data reset is required.

Before rollout, the 100-channel batch was already complete: 71 published,
28 dormant, one terminated-account rejection. The two recovered channels had
30 and 6 published contents; the 24 older Maitê kids targets were excluded by
the 90-day policy. All 2,954 runs were done, and all 5,636 publication events were
delivered. No repeat crawl was submitted for this deployment.

To roll back, stop dispatch and drain work before stopping replacements and
restoring their corresponding retained containers. Do not run both generations
with the same worker identity, and preserve the repaired data.
