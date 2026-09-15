# Independent intake limit and start/pause control

Saving a positive count previously selected that many Workers immediately. The
UI's start button was another count update, so there was no durable distinction
between a concurrency limit and authorization to start. A paused node could
therefore resume through the count form.

The shared intake control module now persists `configured_count` separately from
`intake_enabled`, for both remote nodes and the existing local incremental fleet.
Count updates preserve the switch; explicit start/pause updates preserve the
count. Both changes use the existing per-node transaction lock, so concurrent
pause and resize cannot undo one another. Effective selected slots still drive
the existing claim gates, heartbeat admission and graceful drain behavior.

`allowedCount` in status continues to report effective admission for existing
consumers. `configuredCount` and `intakeEnabled` describe the independent operator
settings. A count request's `expectedAllowedCount` now checks the saved limit.
Paused count zero is preserved; starting with zero requires a positive limit.
Saving zero while enabled reduces admission to zero without changing the switch.

Dashboard buttons submit explicit `enabled` requests. The count form only submits
a limit; browser local storage is no longer the source of resume settings. The
page separates actual collection from centrally pending results, which previously
made seven Workers with nine API-wait results appear to be collecting.

## Upgrade

Run `scripts/applyIntakeControlUpgrade.mjs --apply` from the center environment,
with `EXPECTED_CRAWLER_DATABASE` explicitly matching `REMOTE_NODE_DATABASE_URL`.
This creates one small control table and initializes settings from existing
selected slots; it never enables a Worker or changes tasks/results. Re-running
the upgrade retains settings already saved. Deploy the center control code before
the Dashboard. Keep existing collector images, node containers and queue states.
Old center code must not resume serving control writes after the new table is in
use, because it does not preserve the independent switch.

## Tests

PostgreSQL tests cover paused saves, explicit start/pause, resize while running,
concurrent pause/save, busy Worker row contention, deployment and retirement.
The local PostgreSQL/Redis test exercises real Worker admission, completion of
in-flight work, zero capacity and restart. A browser test covers save, refresh,
start and pause, and shows pending results separately from collection.

## Production verification

The existing node was initialized with limit 47 and intake disabled; the local
fleet retained limit 20 and intake disabled. After deploying the center and page,
saving limit 47 through the production Dashboard endpoint and fetching status
again both returned `configuredCount=47`, `intakeEnabled=false`, `allowedCount=0`.
All 47 node connections remained online, collecting=0 and awaiting=7. No remote
collector container needed restarting. Migration remained active with 40 running
tasks; incremental remained globally paused with zero active queue jobs.
