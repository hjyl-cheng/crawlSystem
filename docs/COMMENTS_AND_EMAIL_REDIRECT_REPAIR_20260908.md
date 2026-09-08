# Comments recovery and YouTube email redirect repair

Production code: `8c6d719` on `agent/incremental-migration-release`.
Image: `qy-allpachong/qybullmq:pachongsys-8c6d719-comments-recovery`.
All times below are UTC, 2026-09-08.

## Email redirect evidence and scope

The 02:37:32 read-only audit inspected `crawler.channels.source_json.channel_header.external_links`, preserving the original YouTube targets. A match requires a YouTube hostname, `/redirect` path, and a decoded `q`/`url` target containing a bare email rather than an HTTP URL. Display text was compared character-for-character, without inferring addresses from descriptions or channel names.

| Scope | Count |
| --- | ---: |
| Channels in crawler database | 2,315 |
| Channels with retained raw external-link arrays | 2,314 |
| Channels with nonempty raw external links | 1,684 |
| Channels with bare-email redirect targets | 17 |
| Email redirect links | 17 |
| Exact target/display matches | 17 |
| Display mismatches | 0 |
| Unresolved external-link channels before repair | 17 |

This is 0.73% of channels with retained raw arrays, or 1.01% of channels with external links. It is a local dataset measurement, not an estimate of all YouTube channels. One channel lacked the retained raw array and is excluded from the raw-data denominator.

The latest 500 migration batch (`legacy-results-canary-1788831661581-a7d1264e`) has 498 channel rows after two removed/rejected channels. Its three matches were Caleen, Premier Crypto, and Fernando Fenwick - Marketing digital. Live About responses at 02:29:20–22 independently confirmed these addresses:

| Channel | Display text = decoded target | Links |
| --- | --- | ---: |
| Caleen | caleen.health@gmail.com | 5 |
| Premier Crypto | pixcryptomoon@gmail.com | 4 |
| Fernando Fenwick - Marketing digital | contato@fernandofenwick.pro | 3 |

Root cause: contact recognition ran before YouTube redirect unwrapping. A bare email discovered afterward was parsed as HTTP userinfo and rejected. One rejected target made the entire link observation unresolved. Recognition now runs inside the bounded unwrapping loop, respecting `allowMailto`/`allowTel` and rejecting unsupported schemes. Original display text is retained; canonical mailto targets continue to use the existing lowercase normalization policy.

## Comments recovery

`HHQNB1X0U70` originally retained 388 likes but unresolved comments after an SSL failure. Full Crawl's optional-comments handling swallowed an actual request failure. A live YouTubeJS observation at 02:29:18.307 returned an exact comment count of 16 and 16 comment rows, with comments enabled and 388 likes.

The shared fetcher now retries transient comment transport/upstream failures once locally, retaining the fetched player data. Persistent failures propagate to managed recovery even when comments are optional. Optional successful empty pages and known disabled comments remain valid.

After the authoritative managed route budget is exhausted, shared API fallback requests comment threads when a real comments error is present, including Full Crawl, incremental first-seen, and incremental metrics paths. API fallback does not bypass the route budget. The batch module coordinates requests; YouTube's commentThreads endpoint still operates per video.

## Validation and deployment

Bug-specific tests failed before the fixes. The production Node 20 image then passed all 140 tests across strict detail, detail contract, API fallback, comment page, cancellation, YouTubeJS, Data API comments, publication links, and redirect-contact suites. Budget tests cover 4/9 (no API) and 9/9 (require API comments) for all three consumer modes. `git diff --check` passed.

After one formal worker was updated and verified, deployment rolled through all 44 formal services. Verification found all 44 running on the new image with zero restarts. Full Crawl and incremental each have 20 workers; channel worker 20, temporarily stopped for the live probe, was restored.

## Data repair and verification

A transaction rollback preview preceded the committed repair at 02:42:47.975. Repair used publication writer-version guards, channel mutation locks, assertions against original values, and normal publication reconciliation. It did not reset completed candidates, tasks, or execution fences.

- All 17 channels now have observed external links: 67 links total, including 17 emails. Global external-link unresolved count became zero.
- Three channels used fresh live About evidence; the other 14 used retained original header evidence. Evidence timestamps and previous values are preserved under `source_json.email_redirect_repair`; historical observations were not relabeled as fresh.
- Thirteen owned channels generated channel repair revisions: 51 links. At 02:44:55, every target URL, display URL, and link type matched both `result.entity_current` and the active search snapshot's `public.channel_links`.
- Four channels lack publication ownership: A Cuban in Brazil, Vanda Souzza, Cesa Santos - Investimentos, and Luis Carreiro. Their 16 links were repaired in the crawler database without forcing ownership or publication.
- `HHQNB1X0U70` was repaired to 16 exact comments with 16 stored rows and comments enabled; 388 likes were preserved. Video item hashes were refreshed and a video repair revision generated. Business current content and active search content both verified 16 comments, 388 likes, and `comments_disabled=false`.
- Repair evidence and previous video values are preserved under `raw_json.comments_recovery_repair`.

The live supplement used YouTubeJS and did not request Data API. The API budget transition was verified by tests, not by consuming live API quota during this repair.

Operational evidence remains in `/tmp/email-redirect-audit.jsonl`, `/tmp/comment-links-live-evidence.jsonl`, `/tmp/comment-links-repair-preview.jsonl`, `/tmp/comment-links-repair-applied.jsonl`, and `/tmp/comment-links-business-verified.jsonl`. These temporary files are not durable audit storage; database repair annotations preserve the source evidence and before-values.

The separately identified CNPqOficial total-view-count/About-only recovery issue is outside this repair and is not claimed resolved.
