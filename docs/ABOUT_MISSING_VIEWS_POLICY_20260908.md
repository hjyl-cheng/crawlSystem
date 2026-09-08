# Missing About views and migration finishing recovery

## Confirmed behavior

CNPqOficial (`UCXs3LHxIwbfr5_HrySsGXTA`) returned a successful YouTube About response with 17,500 subscribers and 38 videos, but no `viewCountText` in the raw `aboutChannelViewModel`. The user independently confirmed the missing count on the page.

One official `channels.list` diagnostic request at 2026-09-08 03:12:04 UTC returned `viewCount=0`, `videoCount=0`, and `subscriberCount=17500`. It was charged to the request counter, but its values were not written to the channel. The API video count must not overwrite the observed About value of 38.

The user subsequently requested that successful About observations lacking lifetime views use zero directly during collection, without requiring an API call for that case.

## Collection policy

Commit `d85607b` changes the shared `normalizeAboutMetrics` used by Full Crawl and incremental About collection:

- Successful About observation, absent view-count text and numeric value: store zero with source `youtube_about_missing_view_count`.
- Preserve the absent display text as null. The source distinguishes the product default from an explicit YouTube zero; the numeric status is `exact` under the existing count contract.
- Failed About request, malformed nonempty view-count text, or invalid numeric value: do not substitute zero.
- Subscriber and video counts retain their existing rules and values.

The new regression failed with `null !== 0` before implementation. All 67 tests covering About metrics/current/store, incremental About, initial Full observations, Full Crawl executor, About-only repair, and migration completion passed afterward.

## Why the previous batch remained finishing

Batch `legacy-results-canary-1788831661581-a7d1264e` had 498 accepted and two rejected candidates, with all channel runs finished. The remaining blocker was publication of CNPqOficial because its initial About metric observation was partial.

Two independent recovery bugs were fixed before the zero policy:

- `b070576`: explicit About-only repairs now fetch fresh About before the Full Crawl completed-checkpoint replay path, stage the observation under the current Candidate fence, and reuse the completed video/agent work.
- `e60dffc`: final repair dispatch reads the Candidate's actual dispatch generation instead of using the repair-round number. Original rounds 1 and 3 were rejected by the Candidate fence; round 2 used generation 2 but only replayed the completed fetch in approximately 60 ms.

After those fixes, a targeted About repair made real successful HTTP requests but still found no views. It correctly failed the then-existing complete-metrics requirement. No fake successful observation or metric was written. The user's zero policy resolves this specific absence at collection time.

The earlier proposed policy to finish a batch while deferring an exhausted publication gap was not implemented. This channel is instead being recovered through the existing About-only repair and normal publication path.

## Deployment and recovery verification

The final worker image is `qy-allpachong/qybullmq:pachongsys-5d2d0c0-about-zero`.
Commit `5d2d0c0` also prevents an exhausted About-only repair from marking already completed video work failed. Its regression reproduced the incorrect Run update before the fix. The combined regression run passed 72 tests.

The previous failed About repair had already changed the target Run's status/detail status to failed. A rollback preview verified all 30 video Candidates were done/stored, all 30 corresponding contents existed, and the actual Full Crawl store accepted the frozen hashes as a completed handoff checkpoint after restoring the two statuses. The same transaction was then committed, preserving before-values under `result_json.about_failure_state_repair`.

Task `about-zero-policy-5d2d0c0-cnpq` completed at 03:26:52 UTC. The Run became `done` / `ready_auto`, with `total_view_count=0`, source `youtube_about_missing_view_count`, 38 videos and 17,500 subscribers. At 03:27:39, both business current data and the active search snapshot matched these values and contained 30 published contents. No additional API requests were made; the daily count remained one, from the earlier explicit diagnostic query.

A separate controller issue remained after the publication gap was resolved: the three previously repaired external-link channels were repeatedly given finalize Jobs without a dispatch source revision. They were skipped as `dispatch_revision_stale` and recreated at every tick. Commit `1b8a227` routes reconciliation through the existing `dispatchFinalizeForRun` function, which supplies the source revision and deterministic Job identity. All 16 finalize-dispatch, source-fence, recovery-policy and migration-completion tests passed. Only the controller needs the additional `pachongsys-1b8a227-finalize-reconcile` image; the 20 Full Crawl and 20 incremental workers retain the final About-policy image.

The controller automatically completed the batch at 03:35:24.838 UTC. Verification at 03:36:13 found scheduler `stopped` with reason `pipeline_complete`, batch `completed`, all three finalize blocker counts zero, and `schedulerConflict` returning null for the next batch. Statistics are 500 total, 498 accepted, two rejected, zero failed. The outcome remains `completed_with_system_failures` because earlier system failures are retained in history; it does not indicate remaining failed channels. No forced scheduler status update was used.
