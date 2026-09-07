# Shared YouTubeJS Integration

Release branch: `agent/incremental-migration-release`.

## Integrated Sources

- Base: `87417d2`, also the local `main` at integration time.
- Full Crawl: `7dee060` from `agent/query-incremental-refactor`.
- Incremental: `d126ca6` from `agent/incremental-script-refactor`.
- Merge commits: `01ec156` (Full Crawl), `7f8be7f` (Incremental).

The shared extractor retains alternate-client recovery, zero-count evidence,
comment sorting fallback, optional Full Crawl comments, and cancellation.
Both branches' tests are retained, with equivalent tests consolidated.

## Shared Implementation

| Responsibility | Module |
| --- | --- |
| Channel, uploads and video requests; client recovery | `youtubeJs.js` |
| Comment parsing, sorting, total-count evidence | `youtubeCommentPage.js` |
| Locale-aware dates and failure classification | `localizedTime.js`, `youtubeFailurePolicy.js` |
| Video observation validation, access and content classification | `youtubeJsVideoDetailContract.js` |
| Detail values, statuses, sources and checkpoint field evidence | `videoDetailEvidence.js` |
| Full snapshot, first-seen insert, recent refresh/repair and access writes | `videoContentStore.js` |
| Description, keyword/hashtag and comment-page retention | `videoContentMutationPolicy.js` |
| Content action and disposition assembly | `collectedVideoOutcome.js` |
| About observation assembly with explicit locale and execution context | `aboutObservation.js` |
| Publication-window evidence counters | `videoActivityEvidence.js` |
| Executor selection and runtime extractor requirements | `channelExtractorCapabilities.js` |

Full Crawl's `validateFullCrawlYoutubeJsDetail` and Incremental's
`fetchIncrementalYoutubeJsVideoDetail` use the same validation implementation.
The validator returns the original detail object, access facts and content
classification. Errors retain `required_surface` and `partial_detail`, so
checkpoint recovery can preserve the observed evidence.

The callers select their existing policies:

| Caller | Required detail | Comments | Ongoing live |
| --- | --- | --- | --- |
| Full Crawl v1 | Full public fields | Required when applicable | May settle as excluded before complete metadata |
| Full Crawl v2 | Full public fields | Optional | Same exclusion policy |
| Incremental first-seen / pending repair | Full public fields | Required when applicable | Requires public metadata |
| Incremental ordinary recent refresh | Numeric view count | Required when applicable | Metrics policy |

Full Crawl admission, candidate ordering, frozen fetch contracts and Finalize
handoff remain in its executor and store. Incremental anchors, sampling,
first-seen processing, Batch/Item claims, cursors and Clock observations remain
in its executor. Their persisted state and recovery protocols are different.

The shared content store uses the caller's transaction. Candidate settlement,
first-seen ledgers, observation/cursor commits and publication reconciliation
remain at their original transactional points. Metrics refresh retains populated
static fields; explicit repair may update them. Type corrections preserve the
existing content key. Older recent observations cannot overwrite newer ones.
The old `fullVideoContentStore.js` exports remain as compatibility entry points.

Both writers now preserve estimated view counts and explicit unresolved comment
evidence. A numeric count from a failed optional comment surface remains
unresolved and cannot become a published exact count. Zero, policy zero,
disabled, missing and parser failure remain distinct.

Feed traversal shares normalization, deduplication, continuation requests and
parse-gap accounting. Full Crawl keeps a selected-item limit and dense positions;
Incremental keeps raw source positions, ordered anchors and separate first-page
and catch-up counters. Full pagination failures throw; Incremental returns
partial evidence. Parse gaps prevent completeness. No continuation is requested
after the page budget, fixing the old Full loop's unused extra request at that
boundary. The historical Full duplicate-page/terminal-page stop is retained;
Incremental still inspects that terminal page. Persisted document shapes and
contract hashes are unchanged.

## Verification

- QYBullMQ regression run: 1,567 passed, zero failures; 171 optional tests
  skipped without their external environment variables.
- Final targeted run after the field-status projection and page-budget tests:
  113 passed, zero failures, no skips (including the two database chain tests).
- Eight cross-pipeline tests cover required metadata, zero evidence, optional
  comments, metrics-only refresh, pending repair, live exclusions, restricted
  access and cancellation.
- Isolated PostgreSQL/Redis: 18 Full Crawl tests passed, including process-kill
  recovery, duplicate delivery, checkpoint atomicity and publication evidence.
- Isolated PostgreSQL: one Incremental checkpoint schema/claim test and two
  finalization/replay scenarios passed. Both scenarios now create the Channel
  and initial content through the real Full Crawl executor/store before running
  Incremental. They cover ordinary metrics and pending repair, publication
  hashes, observation/outbox identity, cursor advancement and network-free replay.
- Isolated PostgreSQL: two existing Full content-store scenarios and one shared
  writer scenario passed. The latter covers Full snapshot, Incremental refresh,
  stale observation, repair and first-seen type correction on the same content.
- In total, 24 distinct PostgreSQL/Redis integration scenarios were enabled and
  passed. YouTube responses and Agent results are fixtures, not live canaries.
- `scripts/verify.sh` and `git diff --check` passed.

The original Incremental PostgreSQL replay test also failed on the unchanged
source branch: it created a pending repair but expected ordinary metrics-only
metadata preservation. The fixture now covers both modes, supplies publication
field sources, and checks the frozen target's `enrich_pending` value. No storage
policy was changed to satisfy this test.

Schema/claim testing requires an empty database. Runtime integration testing
requires Crawler plus the separately published Publication Current/Capture
schemas. All database tests ran against disposable local infrastructure.

## Deployment Still Required

Source and Compose defaults now select `FULL_CRAWL_FETCH_CONTRACT_DEFAULT=youtubejs_full_v2`,
`INCREMENTAL_VIDEO_EXECUTOR=youtubejs_checkpoint_v1` and
`YOUTUBEJS_EXTRACTOR_MODE=full`. New Full Crawl and Incremental executions acquire
only YouTubeJS and have no yt-dlp fallback. Dedicated new-chain workers do not
warm yt-dlp. Frozen legacy Full contracts, an explicitly selected legacy
Incremental executor, and the separate legacy Content Enrich consumer retain
their resource requirements. Content Enrich remains Clock-owned by default.

This integration does not switch running workers or update `main`. Existing
runtime environment overrides must be checked before release. Deployment still
requires the matching schemas, one immutable image, legacy queue draining and
a combined live canary through publication before scaling out. See
`DEPLOYMENT.md` section 9.1. No business database, running worker or remote
repository was modified by this integration.
