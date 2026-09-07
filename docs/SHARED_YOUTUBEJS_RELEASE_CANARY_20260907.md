# Shared YouTubeJS Production Canary

Observed on 2026-09-07, through 10:38 UTC. This records actual production
deployment and migration, not an isolated database test.

## Release and Deployment

- Branch: `agent/incremental-migration-release`; application commit: `8a5449a`.
- Image: `qy-allpachong/qybullmq:pachongsys-8a5449a-shared-canary`.
- Image config digest: `sha256:5032300f8c84601a0e652a954da8f8ee38e865cecbde18a5901001621148fd3b`.
- Manifest list digest: `sha256:a0408f3b96c88780348a6ffa45c2a91dd2cd8755ecf1230b10a6a1a6ba896189`.
- Built using `services/qybullmq/Dockerfile` from the clean application commit.
  The standard image script required absent Publication bootstrap variables;
  direct Docker build avoided introducing synthetic bootstrap configuration.
- Image-local Node 20 verification: 111 tests passed, zero failed or skipped.
  Broader pre-deployment verification is in
  [the integration report](SHARED_YOUTUBEJS_INTEGRATION_20260907.md).

Only these workers were replaced; both are running with zero restarts:

| Container | Slot | Concurrency | Previous image tag |
| --- | --- | --- | --- |
| `qy-newcrawler-fresh-worker-fullcrawl-canary-1` | `bullmq-channel-10` | 1 | `fullcrawl-youtubejs-canary-20260907-6` |
| `qy-newcrawler-fresh-worker-incremental-1` | `bullmq-channel-32` | 1 | `pachongsys-d126ca6-incremental-engagement-canary` |

Full work uses the isolated `bull-fullcrawl-youtubejs-v1` prefix. Both workers
set `YOUTUBEJS_EXTRACTOR_MODE=full`; Incremental selects
`INCREMENTAL_VIDEO_EXECUTOR=youtubejs_checkpoint_v1`. Automatic schema migration
is disabled. Existing networking and runtime configuration were retained.

During this canary, the operational containers explicitly retain
`FULL_CRAWL_FETCH_CONTRACT_DEFAULT=legacy_full_v2` for ordinary intake. The
canary batch selector freezes `youtubejs_full_v2`, independently of that default.
This is not a fleet-wide transition of legacy Full Crawl contracts.

Controller, Finalize, API, the remaining 19 incremental workers, and ordinary
Full workers were not replaced. No production schema or runtime env file was
changed. The local `main` remains at `87417d2`.

## Actual Migration Results

Each source was rechecked against its frozen snapshot and dispatched separately.

| Field | Kleinner Almeida | AZZY |
| --- | --- | --- |
| Channel | `UCk8XFH5iBlzrgy5Kkk795Tw` | `UC_AfmhisedgE465QRCGZjDw` |
| Source candidate | 33841 | 33842 |
| Target candidate / intent | 1432 | 1433 |
| Batch | `fullcrawl-youtubejs-canary-shared-8a5449a-20260907` | `fullcrawl-youtubejs-canary-shared-8a5449a-20260907-2` |
| Run | `run:665e2f84-fc26-4ab9-973d-600c4f2dd973` | `run:4bb80291-401d-4858-af9d-e42a2cb7d213` |
| Fetch finished UTC | 10:33:06 | 10:36:33 |
| Details processed | 30 | 30 |
| Stored / excluded / deferred | 0 / 30 / 0 | 6 / 24 / 0 |
| Run / detail | done / done | done / done |
| Finalize | ready_partial | ready_auto |
| Outcome | Dormant, no content within 90 days | Activated and published |

Job IDs are `channel-snapshot__<batch>__<channel>__g1`. Both jobs completed
with `attemptsMade=1`. Both execution audits report `ytdlp_session=null` and
YouTubeJS 17.2.0, enabled in full mode. Their frozen executor is
`youtubejs_full`, version 2, with contract hash
`sha256:cdd2d1843c89057feb0638f6d6fc4b801dd76832c6effde0a551ca3103fcf911`.

Frozen source hashes:

- 33841: `2b72056e126328c0a1654e7d7e7df44632003f0e1f186b8ae69b099c7ef9527d`.
- 33842: `204473fa667ee27393b0b3ef4ee5cde8811bfe0df9be71d8e67e1ed33a342b8e`.

Kleinner Almeida correctly entered dormancy and produced no publication.
Its scheduled dormancy recheck day is 2026-10-18.

AZZY's six stored rows all have exact publication time, duration, view count,
and comment count, with six non-null comment first pages. Its `agent`,
`channel`, and `video` publication outbox entries were delivered at sequence 1,
each on the first attempt.

At 10:38:03 UTC, read-only verification of `newcrawler_business` confirmed:

- Consumer cursors for all three domains at active sequence 1.
- Six active rows in `result.content_current`, all with exact duration.
- `public.creator_search_current` resolves AZZY and six canonical contents.
- Business projection outbox delivered on attempt 1, with no last error.

The first one-off dispatch process exited 1 after successful enqueue because
its assertion expected the worker-prepared contract to exist immediately on
job data. The actual job was not retried or duplicated. Run/audit evidence
confirmed the correct v2 executor. The second dispatcher validated the expected
contract through `newFullCrawlFetchContractForJob` and completed normally.

## Final State and Remaining Validation

At 10:38:41 UTC, Full canary and Incremental queues had no active, waiting,
prioritized, delayed, or failed jobs. The migration scheduler was stopped after
the second batch. The ordinary Full queue remained paused with one pre-existing
failed job; this rollout did not alter it.

The new Incremental worker is deployed and ready, but has not processed a real
new Plan. Preflight found no eligible video-due channel without existing or
already-covered plans; 1,093 checkpoint batches were finalized. No Clock dates
or Plans were fabricated to force execution. Its next genuine due Plan remains
a production validation requirement before wider promotion. The isolated
Full-to-Incremental tests passed, but do not substitute for this live evidence.

## Rollback

Both previous workers remain stopped with exit code 0:

- `qy-newcrawler-fresh-worker-fullcrawl-canary-1-before-shared-8a5449a`
- `qy-newcrawler-fresh-worker-incremental-1-before-shared-8a5449a`

To roll back a worker, stop further dispatch for that lane, let active work
settle, gracefully stop the new container, rename it to an audit name, restore
the previous container's original name, and start the previous container. Do
not run both generations on the same proxy slot. Preserve completed migration
and publication records; container rollback does not undo business data.
Avoid a blanket Compose recreation, which would replace this explicit canary
configuration. Wider rollout is not part of the completed two-worker canary.
