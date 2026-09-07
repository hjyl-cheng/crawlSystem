# Age-Gated Comments Repair, 2026-09-07

## Cause

Video `NGOT1hCseGU` in migration batch
`legacy-results-canary-1788786614981-7b804c36` was stored with an unresolved
comment count. The user confirmed that its page says comments are turned off.

The incremental refactor's existing age-gate rule was present in release,
including its parsed Next Message handling. However, its empty-comments rule
only recognized the old parsed Comments API error, `Comments page did not have
any content`. The shared raw continuation fetch now returns a successful empty
response instead of that exception.

Managed live probes reproduced `LOGIN_REQUIRED / Sign in to confirm your age`,
`is_family_safe=false`, no parsed Next Message, and a successful continuation
containing only response context and tracking data. The disabled-state assertion
failed before the fix.

## Change and Verification

Commit `fdf3077` preserves the existing age-gate semantics for a successfully
fetched, normalized absent comment surface with no count or rows. Ordinary empty
responses and network failures remain unresolved. Both Full Crawl and Incremental
use the corrected shared function. No API fallback policy changed.

- 103 related tests passed in the production Node 20 environment.
- Managed live verification at 13:28:19 UTC returned `0 / disabled / true`,
  source `youtubejs_comments_age_gate_empty`.
- All 44 formal services run
  `qy-allpachong/qybullmq:pachongsys-fdf3077-comments-fix`, including 20 Full Crawl
  and 20 Incremental workers. Deployment retained the previous containers.

## Data Repair

Only the current content row for this video was corrected. The transaction used
the channel mutation lock, writer-version guard, expected old-state checks and
observation freshness checks. It preserved old values and probe evidence in
`raw_json.disabled_comment_repair`, refreshed the publication item hash and
reconciled the video domain. A rollback preview succeeded before apply.

Repair revision `61695c2a-c8e4-43b5-8986-660e3c724b15` was delivered. Crawler and
business result payloads contain `0 / disabled / true`; the active search snapshot
contains `0 / exact / true`, as required by the business projection contract.
Historical completed candidate captures were preserved. Daily API usage remained
7 calls / 57 requested videos; the repair used no Data API calls.
