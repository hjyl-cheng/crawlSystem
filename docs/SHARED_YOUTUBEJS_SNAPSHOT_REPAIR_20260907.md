# YouTubeJS Snapshot Repair and Backfill

Application commit: `a631d8c`.
Image: `qy-allpachong/qybullmq:pachongsys-a631d8c-snapshot-repair`.
Deployed to 20 ordinary Full Crawl workers, 20 Incremental workers, and the
existing Controller on 2026-09-07. API and Finalize remain on `8a5449a`.
Old containers are retained with suffix `-before-a631d8c` for rollback.

## Fixed Causes

Full YouTubeJS detail could accept IOS metadata before obtaining authoritative
content type. Required full surfaces now try bounded alternate clients including
a final WEB request that can expose Shorts canonical URLs and eligibility.
Metrics-only refresh behavior is unchanged. No yt-dlp or Data API was used for
this backfill.

Automatic final repair previously used repair round as candidate dispatch
generation and created a different Job identity. Snapshot recovery now validates
the frozen identity and retries the original Job, retaining monotonic
`attemptsStarted`, so the new activation can supersede its old detail owner.

Validation: 96 targeted tests; 5 PostgreSQL fence integration tests. The broader
suite passed 1,571 tests with 171 optional skips; its two environment-related
exceptions passed separately on the host (1,573 distinct passing tests).
All four originally failing video IDs also passed real YouTubeJS probes.

## Actual Backfill

Batch: `legacy-results-canary-1788778403004-d0b4c1de`, ordinary Full queue.

| Channel | Candidate | Before | Final stored | Excluded | Publication |
| --- | --- | --- | --- | --- | --- |
| TropaTaspio (`UC9c2R-x7OgGH7xqc3k94lSQ`) | 1443 | 20 | 30 | 0 | Delivered, searchable |
| Maitê kids (`UCG3jvxuenC_WiU1scglJHwA`) | 1519 | 5 | 6 | 24 | Delivered, searchable |

Both original target sets contained 30 items. All 60 candidates finished.
The 24 exclusions have exact player publication evidence outside the 90-day
window; they are not missing or silently discarded items. The 36 stored items
have no missing core fields or duplicate IDs within each channel/type, and have
exact publication time, duration, and views. TropaTaspio has 19 Shorts and
11 videos; Maitê kids has 6 Shorts.

Originally failing IDs `1xobqeOzsFE`, `6CFdIWYkiMY`, `NwOBD2iZKCU`, and
`aGeGcMSLA0g` are stored with authoritative Shorts type and exact core values.
Existing 25 contents were retained; 11 further contents were stored. Publishing
the two completed runs added 36 contents to the previously published 1,242.

At 11:34 UTC the batch has 71 published channels, 28 dormant channels, and one
terminated-account rejection. All 1,278 published contents match business and
canonical search counts, with no missing core values, negative values, or
out-of-window rows. The original 69 published channels remain at 1,242 contents.
All 2,954 crawler runs are done, and all 5,636 publication outbox rows delivered.
The scheduler naturally stopped; ordinary Full queue is paused in that state.
Incremental remains unpaused. No active, waiting, or delayed work remains.

## Retry Accounting and Operator Recovery

Rota's default total is 9 network attempts per Business Run, with up to 2 route
switches per execution. This is separate from BullMQ Job attempts and from the
multiple HTTP requests made within a single route attempt.

Maitê kids consumed its first 9 attempts as follows:

| Rota attempts | Origin | Evidence |
| --- | --- | --- |
| 1-5 | Original snapshot's first three executions: 1 + 3 + 1 | Parser gaps and HTTP 200 login responses |
| 6-8 | Three activations of legacy final-repair round 2 | Detail ownership fence rejection |
| 9 | Original snapshot resumed after deploying the fix | Alternate clients exhausted on the next video |

The next route was blocked by the 9-attempt budget. A separate legacy
content-completeness repair then returned `skipped` and left the run waiting for
detail. This path was not fixed by `a631d8c` and must not be represented as
successful automatic budget recovery.

For the user-authorized backfill, the Controller was briefly stopped and only
this Business Run's budget was extended from 9 to 18. `next_attempt_number` and
`budget_exhausted_at` were preserved. Its exhausted binding was reopened with
compare-and-set checks; `result_json.operator_recovery` records the old budget,
new budget, reason, time, and original exhaustion evidence. The original snapshot
Job was retried again without resetting activation history. Global defaults
were not changed.

Rota attempts 10-16 completed the remaining target set. Attempt 10 timed out;
11-12 received ambiguous HTTP 200 player responses; 13-15 received login
requirements; 16 completed. Routes actually changed: channel-3 used generations
7, 8, 9; channel-2 used 15, 16, 17, 18. These records show routing changes, not
proof that every response problem was caused by an IP. The final count is 16
network attempts including the historical failures, not 16 full channel crawls.
Progress persisted between attempts.

## Operational Notes

Controller was restarted and normal publication completed. No global budget,
schema, or successful-channel data reset occurred. Temporary PostgreSQL test
containers created/started for this repair were removed/restored to stopped.
Historical failures were not manually purged; normal queue cleanup subsequently
reported zero failed Full jobs. Durable execution history retains the evidence.

For rollback, stop dispatch and drain work, then stop replacements before
restoring their matching `-before-a631d8c` containers. Never run both generations
with the same worker identity. Preserve repaired and published data. Future
Compose recreation must carry forward the explicit image and existing 20+20
worker configuration.
