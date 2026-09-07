# Incremental Engagement Repair, 2026-09-07

## Scope

Branch: `agent/incremental-script-refactor`. No merge into main. No Clock,
Run, cursor, or finalized Batch/Item changes. The frozen September 7 cohort
contains 438 missing likes and 291 missing comment counts, overlapping in
109 videos: 620 unique videos.

## Correctness Rules

- Explicit numeric likes, including zero: `exact`.
- A Like button whose title and accessibility text both say `Like`, without
  a numeric count: user-authorized zero, `zero_from_empty`, source
  `youtubejs_like_count_not_public`. This is not proof of zero actual likes.
- Explicit Next `Message` saying comments are turned off: zero, `disabled`.
  Read the parsed `info.page[1].contents_memo`, not just enumerable properties.
- An empty continuation response alone does not prove disabled comments.
- Public upcoming/live videos with a successfully fetched but absent comment
  surface: policy zero, `zero_from_upcoming` / `zero_from_empty`, with separate
  upcoming/live sources. Do not label these `exact` or `disabled`.
- Request errors, private/deleted/member-only responses do not manufacture counts.
- Preserve the extracted count status through Item, first-seen INSERT/upsert,
  and recent-content UPDATE; do not promote policy zero to `exact`.

## Full Reverification

The actual `fetchYoutubeJsVideoDetail` path ran for all 620 videos. The final
live-comment handling was then rerun for all 116 affected live videos.
Requests used a direct network route with fingerprint transport disabled;
this verifies extraction, not Rota route switching. No official API was used.

| Missing Metric | Final Classification | Count |
| --- | --- | ---: |
| Likes | exact | 260 |
| Likes | zero_from_empty, non-public count | 165 |
| Likes | private / unavailable | 13 |
| Comments | disabled | 158 |
| Comments | zero_from_upcoming | 108 |
| Comments | zero_from_empty, live surface absent | 8 |
| Comments | zero_from_surface | 2 |
| Comments | exact | 1 |
| Comments | private / unavailable / members_only | 14 |

All 606 public videos have numeric results and statuses for the requested
metrics. The other 14 are 8 private, 5 unavailable, and 1 members-only.

Do not reuse the older audit's apparent 273 exact zero comments: that temporary
audit incorrectly used `Number(null)`. The final evidence uses explicit numeric
checks and the production extractor.

## Applied Repair

`scripts/applyIncrementalEngagementRepair.mjs` defaults to transaction rollback.
Apply requires the reviewed evidence SHA-256, exact cohort matching, channel
mutation locks, writer-version guard, and per-row freshness checks. It changes
only the requested metrics and status/source fields, records before-values in
`raw_json.engagement_repair`, refreshes publication hashes, and reconciles once
per affected channel. Reusing the same evidence is idempotent.

- 465 current content records repaired; both counts verified non-null.
- 141 public live/upcoming first-seen candidates have no current content row
  under the existing exclusion policy. Evidence is retained; no forced inserts.
- 14 restricted videos skipped without invented values.
- 76 channels generated repair revisions; 13 had no publication change.
- 1 existing blocked channel: `UCW5g4EWONTc1gtqN1K9hU4w`.
  Its publisher reports `video_item_hash_missing` for `BitGnXLFi98` and
  `video_window_termination_unproven`. Metrics are repaired, but its publish
  barrier is deliberately not bypassed.

Local evidence (not committed to Git):

- `backups/incremental-engagement-final-20260907.ndjson`
- `backups/incremental-engagement-apply-20260907.json`
- SHA-256: `100c3c215cf37c8c66b092f0b09cae90db61c9fddf2a483c900eb1c00c60e05f`

Finalized Items remain historical captures and still show their original
`unobserved`; current content and the separate audit contain the corrections.

## Verification

Targeted YouTubeJS, cancellation/timeout, incremental checkpoint, and publication
test files passed. PostgreSQL integration cases require
`INCREMENTAL_POSTGRES_TEST_URL` and were not executed against production.
The repair itself was exercised through a full rollback preview before apply.
