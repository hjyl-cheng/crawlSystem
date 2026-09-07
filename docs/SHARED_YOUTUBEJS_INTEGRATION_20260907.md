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

## Verification

- QYBullMQ regression run: 1,561 passed, zero failures; 169 optional tests
  skipped without their external environment variables.
- Eight cross-pipeline tests cover required metadata, zero evidence, optional
  comments, metrics-only refresh, pending repair, live exclusions, restricted
  access and cancellation.
- Isolated PostgreSQL/Redis: 18 Full Crawl tests passed, including process-kill
  recovery, duplicate delivery, checkpoint atomicity and publication evidence.
- Isolated PostgreSQL: one Incremental checkpoint schema/claim test and two
  finalization/replay scenarios passed.
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

This integration does not switch running workers or update `main`.
Both legacy defaults remain until the runtime cutover is performed.
Deployment needs explicit Full Crawl contract and Incremental executor settings,
the matching schema checks, a single immutable release image, queue draining,
and a combined live canary through publication before scaling out.
