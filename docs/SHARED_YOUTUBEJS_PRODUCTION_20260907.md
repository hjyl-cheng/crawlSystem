# Shared YouTubeJS Production Deployment

Deployed on 2026-09-07 from application commit `8a5449a`, following the
two-channel production validation. The user will initiate migration from the
existing page; no migration was dispatched during this fleet deployment.

## Running Fleet

| Role | Containers | Queue | Concurrency each |
| --- | --- | --- | --- |
| Full Crawl | `qy-newcrawler-fresh-worker-channel-1` through `-20` | `youtube-channel-crawl`, ordinary prefix | 1 |
| Incremental | `qy-newcrawler-fresh-worker-incremental-1` through `-20` | `youtube-channel-incremental` | 1 |

All 40 consumers are registered in BullMQ and hold live Rota leases. All use
`YOUTUBEJS_EXTRACTOR_MODE=full`, `FULL_CRAWL_CANARY_WORKER=false`,
`FULL_CRAWL_FETCH_CONTRACT_DEFAULT=youtubejs_full_v2`, and
`INCREMENTAL_VIDEO_EXECUTOR=youtubejs_checkpoint_v1`.
New migration snapshots use YouTubeJS. Historical frozen legacy contracts are
not rewritten; the pre-existing failed ordinary Full job was not retried.

The API, existing Controller, and Finalize were also updated to `8a5449a`.
The Controller retains its historical container name
`qy-newcrawler-fresh-controller-fullcrawl-canary-1`, but manages the ordinary
queues. The isolated `worker-fullcrawl-canary-1` is stopped and its queue paused.

Image config digest:
`sha256:5032300f8c84601a0e652a954da8f8ee38e865cecbde18a5901001621148fd3b`.
The running containers retain image reference
`qy-allpachong/qybullmq:pachongsys-8a5449a-shared-canary`; the same immutable image
is also tagged `qy-allpachong/qybullmq:pachongsys-8a5449a-shared-production`.
The retained tag text does not select a canary queue. The initial deployment
continued after the conversation interruption and completed before the tag
change; a second deployment attempt stopped at its existing-backup guard.
No second fleet replacement occurred.

## Proxy Capacity

An existing content-enrichment worker also needs a channel slot. Rota was kept
on `qy-allpachong/rota-core:pachongsys-87417d2` and its sole configuration change
was `ROTA_CHANNEL_SLOTS=40` to `41`. The service was recreated with its original
ports, addresses, aliases, and volume, while no proxy task was active.

Final database verification: 20 Full leases, 20 Incremental leases, and one
content-enrichment lease, all live. Incremental worker 20 restarted three times
during the Rota restart and subsequently acquired `bullmq-channel-41` and
registered its BullMQ consumer. Other deployed workers had zero restarts.

## Measurement Baseline

Baseline at 2026-09-07 10:50:52 UTC:

| Metric | Value |
| --- | --- |
| Channel runs | 2,855, all done |
| Candidates | 1,421 accepted; 11 rejected; 1 historical failed |
| Incremental checkpoint batches | 1,093, all finalized |
| Crawler publication outbox | 5,423, all delivered |
| Ordinary Full completed / failed Bull jobs | 202 / 1 |
| Prior canary completed / failed Bull jobs | 33 / 0 |
| Incremental completed / failed Bull jobs | 1,434 / 0 |
| Finalize completed / failed Bull jobs | 139 / 0 |

There were no active, waiting, prioritized, or delayed jobs in these queues.
The scheduler remains stopped. The Controller automatically pauses the ordinary
Full queue in that state. The page's manual migration endpoint activates the
scheduler with the selected batch; the Controller then resumes ordinary Full
consumption. This behavior was checked in `manualMigrationDispatch.js` and
`controller.js`, without submitting a migration request.

For the user's next batch, measure its own Run/attempt outcomes, duration and
phase timings, stored/excluded/deferred counts, publication delivery, and
business/search projection. Do not count prior canary samples as results of
the 20-worker production batch. Verify Incremental processing separately when
real due Plans execute; deployment readiness alone is not execution evidence.

The API is healthy and its `/health` endpoint was successfully reached from
the existing page gateway. No production schema or runtime env file changed.

## Rollback and Future Recreation

Replaced containers are retained under their original name plus
`-before-fleet-8a5449a`. Full workers 1 and 2 were newly added. The previous Rota
container is retained as `youtube-rota-qy-core-before-fleet-41-slots`.

Stop dispatch and drain active work before rollback; stop each replacement
before restoring its predecessor. Never run both generations with the same
worker identity. Restore 40 Rota slots only after reducing channel consumers
accordingly. Preserve migrated and published records.

This was an explicit Docker deployment preserving each service's runtime
configuration. A future Compose recreation must carry forward the image,
ordinary Full queue, YouTubeJS executor settings, 20+20 replicas, and 41 Rota
channel slots; older runtime defaults do not represent this deployment.
