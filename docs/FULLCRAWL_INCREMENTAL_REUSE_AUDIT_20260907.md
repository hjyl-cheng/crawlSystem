# Full Crawl and Incremental Reuse Audit

Reviewed revision: `7daacb6` on `agent/incremental-migration-release`.
This is a source-level design assessment, not a claim that the proposed
refactors are implemented or validated. No runtime was changed for this audit.

Follow-up: the seven areas below have now been implemented in the release
worktree. See `SHARED_YOUTUBEJS_INTEGRATION_20260907.md` for the resulting
modules, preserved policies, source defaults and verification. This audit
retains the original revision's line references and assessment for context.

## Conclusion

The previous integration consolidated the extractor revisions and detail
validation. It did not finish consolidating the workflows' shared behavior.
The highest-value remaining work is video evidence projection and content
mutation policy, followed by feed traversal and About observation assembly.

The intended architecture is two workflow orchestrators using shared domain
modules. Full Crawl and Incremental must retain their persisted identities,
admission/planning rules, transaction ownership and recovery protocols.

## Already Shared

All paths below are relative to `services/qybullmq/src/`.

| Behavior | Existing module or entry point |
| --- | --- |
| Managed network identity, request tracking, route lifecycle | `channelExecutionRuntimeAdapter.js`, `channelExecutionRuntime.js`, `rotaSlotAdapter.js` |
| YouTubeJS channel, player and comment requests, alternate clients | `youtubeJs.js` |
| Raw feed node normalization | `normalizeYoutubeJsFeedItem` in `youtubeJs.js:365` |
| Comment page parsing and count evidence | `youtubeCommentPage.js` |
| Required video detail validation | `youtubeJsVideoDetailContract.js:42` |
| Access, content type, publication time and disposition primitives | `detailPolicy.js`, `youtubeContentType.js`, `publicationTimeEvidence.js`, `videoDisposition.js` |
| About normalization and persistence | `aboutMetrics.js`, `aboutCurrent.js`, `aboutObservationStore.js` |
| Observation idempotency, outbox and publication | `crawlObservationStore.js`, `videoPublicationItemStore.js`, `publicationReconciler.js` |
| Agent execution | `processAgentBatchV2` in `pipelineV2.js:4251` |

Sharing a file does not establish that its implementations are unified.
For example, `youtubeJs.js` still contains two separate pagination loops.
Incremental imports `fullVideoStorageAction`, but does not use the Full Crawl
content writer for its first-seen INSERT or recent-content UPDATE.

## 1. Video Evidence Projection: High Priority

Evidence:

- `fullVideoContentStore.js:128`: `normalizeFullVideoViewCount`.
- `fullVideoContentStore.js:150`: field normalization and SQL parameter mapping
  inside `upsertFullVideoContent`.
- `incrementalYoutubeJsVideo.js:161`: `incrementalYoutubeJsVideoFieldStatus`.
- `incrementalYoutubeJsVideo.js:398`: `detailFacts`.
- `incrementalYoutubeJsVideo.js:1256` and `:2052`: further mappings at write time.

These paths independently derive numeric counts, field status/source,
description observation, publication evidence, access and live timestamps.
The common detail validator only accepts/rejects an observation; it does not
own these projections.

A concrete divergence exists: Full Crawl retains an explicitly `estimated`
view count, while Incremental writes `exact` for a non-null count at lines
1449 and 2157. This is a source-level contract difference, not evidence that
production currently supplies an estimated value to those Incremental paths.

Proposed module: a video evidence projection that keeps each field's value,
status, source and observation presence together. It must distinguish missing,
observed empty, numeric zero, policy zero, disabled, unavailable and parser
failure. Persisted field-status documents become explicit projections of that
evidence, rather than independently inferred interpretations.

Keep required-surface decisions in the existing detail contract. A Full Crawl
v2 optional comment failure must not become fabricated zero, and must not make
Incremental's required comment surface optional.

## 2. Content Mutation Policy and Writer: High Priority

Evidence:

- Full snapshot writer: `fullVideoContentStore.js:150`.
- Incremental first-seen writer: `incrementalYoutubeJsVideo.js:1256`.
- Incremental recent refresh writer: `incrementalYoutubeJsVideo.js:2052`.

All write `crawler.contents`, including overlapping rules for identity/type,
URLs, empty descriptions, keyword retention, publication-time evidence, count
status/source, comment first-page retention and publication hashes.

For example, retaining a stored nonempty comment page appears independently
at `fullVideoContentStore.js:331`, `incrementalYoutubeJsVideo.js:1475` and
`:2172`. Description retention appears at lines 211, 1405 and 2113 respectively.
Publication evidence selection already has shared helpers, but its application
and surrounding write rules remain duplicated.

Evolve the existing content store into a shared writer with explicit mutation
intents such as full snapshot, first-seen, metrics refresh, detail repair and
access-only update. Consolidate common field rules once; preserve each intent's
actual behavior instead of adding many unrelated boolean switches.

Important differences to preserve:

- Ordinary recent refresh retains populated static metadata; pending repair
  can update it. Missing-field repair remains possible under its current rules.
- Incremental records playlist/player/next observation times and observation
  identity; Full Crawl records candidate position and its own raw evidence.
- Full Crawl currently uses database time and sets `is_recent=true`; Incremental
  uses cycle observation time and derives recency. These are not interchangeable.
- Incremental recent UPDATE has a freshness predicate. Preserve this protection,
  conflict keys and row-lock order when consolidating the writer.
- The caller supplies the active transaction. Candidate settlement, first-seen
  ledger, cursor and Batch finalization must not be split across new transactions.
- Retain publication hash updates at their existing transactional points; do
  not add per-video publication delivery or independently commit a content row.

The existing writer is also used by legacy `pipelineV2.js`; its compatibility
is part of the testing surface.

## 3. Feed Traversal and Upload Evidence: High Priority

Evidence in `youtubeJs.js`:

- `collectFeed` at line 401: Full Crawl bounded collection.
- `scanYoutubeJsFeed` at line 445: Incremental anchor-based collection.
- `collectYoutubeJsUploadBundle` at line 567: Full Crawl entry mapping.
- `openYoutubeJsChannel().scanUploads` at line 1317: Incremental wrapper.

The loops repeat continuation fetching, node normalization, deduplication,
parse-gap accounting and completion evidence. Relative publication evidence
is also constructed in both paths.

Share the traversal mechanism and upload evidence normalization. Keep bounded
collection and anchor-based stopping as explicit policies. Preserve these
differences:

- Full Crawl uses a selected-item limit and dense selected positions.
- Incremental counts raw source positions, first-page and catch-up items,
  tracks ordered/crossed anchors and has a distinct catch-up limit.
- Full Crawl continuation failure propagates; Incremental can return an
  incomplete scan with the original pagination error.
- `list_end`, `max_items`, `max_pages`, `anchor_matched`, `catchup_limit` and
  `parse_gap` are different proofs. A partial scan cannot advance a cursor.
- Do not issue an extra continuation request after the stopping condition.
- Frozen Full Crawl documents and hashes must retain their existing shape.

The common entry representation can expose adapters for existing `id` versus
`video_id`, numeric/text count, publication and position fields. Renaming
persisted checkpoint properties requires a separate compatibility decision.

## 4. About Observation Assembly: Medium Priority

`fullCrawlYoutubeJsFactory.js:98` and `incrementalAbout.js:12` both combine
About metrics/current facts, extractor versions, timestamps, execution identity
and observation metadata.

Share a pure About observation builder over the channel snapshot and explicit
execution context. Reuse the existing normalization and observation store.
Pass locale and observation time explicitly instead of relying on different
defaults at each call site.

Full Crawl admission still requires channel/About completeness, qualification
and country handling. Incremental can represent an unsuccessful About fetch
as partial evidence. Full Crawl stages its initial observation for Finalize;
Incremental records through its own transaction. Preserve these choices outside
the common builder, including each path's idempotency key.

## 5. Collected-Video Outcome: Medium Priority

`fullCrawlYoutubeJsStore.js:787` and
`incrementalYoutubeJsVideo.js:1208` combine content classification, access,
storage action, live exclusions and `resolveVideoDisposition` separately.

A shared pure outcome resolver can compose these existing primitives and
return the content action, disposition and evidence. It should take the caller's
scope and prior disposition explicitly. Full Crawl's age-window exclusion,
Incremental's incomplete-discovery deferral, and treatment of a known stored
content row remain explicit inputs.

Persisting a Full Crawl candidate and settling an Incremental first-seen Item
remain separate operations. Share the outcome semantics, not their row shapes.

## 6. Activity Evidence Accumulation: Medium/Low Priority

`migrationActivityPolicy.js:27` and `videoActivityLifecycle.js:288` both count
publication-window relations, recent/uncertain content and unresolved statuses.
Both already use `classifyPublicationWindow`.

The shared opportunity is a normalized evidence accumulator. Keep migration
uploads admission and stored-content lifecycle transitions separate. Inputs
have different type evidence, live/upcoming detection, deduplication and scan
completeness. Incremental also has database scan/time budgets and dormant
recheck scheduling. A single shared `isActive` function would hide these rules.

## 7. Executor Resource Requirements: Medium Priority

The runtime is already shared, but its resource selection is not uniform.
`channelExecutionRuntime.js:59` skips yt-dlp only for a YouTubeJS Full Crawl
contract; an Incremental workload still requires the yt-dlp lease. The Worker
also warms both extractors at `worker.js:1686`.

Derive required extractor capabilities from the selected executor and pass
them into the common runtime. Keep legacy Full Crawl, legacy Incremental and
Content Enrich requirements intact. Do not globally disable yt-dlp because
the two new video executors use YouTubeJS.

## Lower-Value Cleanup and Adjacent Duplication

Canonical JSON hashing is repeated in `fullCrawlYoutubeJsModel.js:10`,
`fullCrawlFetchContract.js:9` and `incrementalYoutubeJsVideo.js:97`. It can be
consolidated only with fixtures proving byte-identical persisted hashes.
Separate target schemas and recovery-cycle identity remain necessary.

The new Incremental executor also duplicates substantial logic from legacy
`incrementalVideo.js`: discovery, first-seen writes, content-enrich ownership,
recent sampling and observation recording. This is a separate reuse axis from
Full Crawl versus Incremental. `worker.js` and `incrementalChannelRunner.js`
still import the legacy module, so deleting it now would break active interfaces.
Shared content/evidence modules can reduce this duplication incrementally.

Existing abort, failure, content-type, publication and observation modules
should be reused directly. A new generic utilities module would add little.

## Workflow Logic That Must Remain Distinct

| Full Crawl | Incremental |
| --- | --- |
| Migration/Query candidate admission and qualification | Clock Plan, task mask and capacity |
| Bounded initial collection and content age window | Anchors, catch-up, gap abandonment and recent sampling |
| Business Run binding and frozen fetch contract | Run/cycle identity and controlled recovery markers |
| Candidate/Run execution fence and receipts | Batch/Item claims, claim tokens, leases and heartbeat |
| Fetch closure and deterministic Finalize handoff | Atomic observation, cursor, first-seen ledger and Batch finalization |
| Initial Agent scheduling and admission closure | Incremental Agent backlog and result ownership |

Full Crawl can fail its attempt on a parser gap; Incremental can settle an Item
as `settled_error` for supported failure classes. A shared detail loop must not
silently erase that distinction. A generic checkpoint engine is not justified
by the fact that both workflows resume after interruption.

## Implementation Order and Verification

1. Define shared video evidence and characterize the existing count/status/source
   differences with cross-caller tests. Make policy changes explicit.
2. Consolidate content mutation rules and the writer, testing against isolated
   PostgreSQL through both real callers, including legacy callers of the store.
3. Consolidate feed traversal and upload normalization, preserving output shape,
   stopping evidence, cancellation and frozen hashes.
4. Consolidate About assembly and selected-executor resource requirements.
5. Extract collected-video outcomes and activity accumulation where the shared
   interface demonstrably removes remaining business-rule duplication.

Required scenarios include zero versus missing counts, estimated versus exact
evidence, optional comments, empty metadata retention, existing content type
correction, older observations, concurrent writes, partial scans, replay after
committed detail, route change, stale claim and duplicate delivery. Publication
and observation/cursor effects must be checked within the original transaction.

The complete release check should create a channel through Full Crawl, then
run Incremental on the same stored content and verify publication, metadata,
evidence, cursor and replay behavior. Separate per-executor tests alone do not
establish the safety of the proposed common writer.
