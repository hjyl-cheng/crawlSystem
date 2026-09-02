# QY V16 Feature Runtime

This Compose project runs the Feature calculation module against the existing
`bullmq_crawler_migration` PostgreSQL database. Crawler facts remain in
`crawler.*`; Rolling Feature State, three Clocks, decisions, plans, and dispatch
state live in `feature_clock.*`.

`feature_user` owns only `feature_clock`. Startup validation requires the
shared Crawler database and refuses to run if that role can read
`crawler.channels`. The Observation event interface remains the module seam;
sharing one physical PostgreSQL instance does not permit Feature code to query
Crawler business tables.

Runtime database and ingest secrets are generated once in the
`qy-feature-runtime-secrets` named volume. Compose contains no Feature
password. Recreating containers keeps the credentials; deleting the volume is
a destructive credential rotation and is not part of normal deployment.

The rollout order is:

```text
feature-secret-init
  -> feature-database-provision
  -> feature-schema in bullmq_crawler_migration
  -> one-time feature-state-migrate from the retained legacy database
  -> feature-ingest
  -> feature-relay
  -> crawler-outbox-publisher
  -> lag/gap validation
  -> feature-scheduler-daily
  -> feature-dispatch-publisher
```

The retained `qy-feature-postgres` container is available only through the
`legacy-standalone` or `shared-migration` profile. It is a rollback source and
must not receive new online writes after cutover.

The historical initial Bundle was `v16-initial-2026-07-21-2`: 1,553 Channels
and 6,212 legacy four-domain events. Those counts are audit history, not deployment
defaults, and the data Bundle is intentionally excluded from Git. New Channels
do not run this bulk Bootstrap. A Full
Crawl that passes the 90-day gate emits About, Video, and Agent Observations; online Feature
Ingest creates the Channel's Rolling State and Clocks in the shared database.

Channel identity and header fields are About Current and refresh in the About
execution. The live database physically contains only About, Video, and Agent
Clock columns and Plan masks. Historical Profile event rows may remain as audit
history, but the live contracts reject new Profile Observations.

`feature-scheduler` is a controlled one-shot Daily Planner. The `daily` profile
runs it at `00:30 UTC`, refreshes every five minutes during the safe window, and
retries failures after five minutes. Clocks are authoritative UTC `DATE` values.
The Planner writes unassigned Plans and never creates Outbox rows.

`feature-dispatch-publisher` dynamically releases those Plans inside the
half-open UTC window `[00:30, 21:30)`. It uses current BullMQ queue pressure,
registered workers, global concurrency, competing Full Channel work, Agent
capacity, and ready proxy Channel slots. Overflow remains unassigned and keeps
its original `due_day` priority; no Clock is rolled into another day.

When upgrading a four-Clock database, stop Scheduler and Dispatch Publisher,
take a database backup, and apply `services/feature-engine/sql/schema.sql`. The
transactional Schema migration folds every historical `run_profile=true` mask
into About, recomputes the three-Clock minimum, and physically drops Profile
Clock, Plan, policy, retry, and derived-state columns before online services
restart.

## Clock policy seed

The deployable Clock policy is stored in
`services/feature-engine/src/feature_engine/clock_policy_v16_rule_7.json`.
`v16-rule-7` is the aggregate database snapshot; its latest domain rule origins
are About `v16-rule-5`, Video `v16-rule-2`, and Agent `v16-rule-7`.

`feature-seed-clock-policy` is read-only by default. It validates the database
identity, Feature role, policy contract, local SHA-256, and existing rows, then
prints the exact confirmation required for an activation:

```sh
feature-seed-clock-policy
feature-seed-clock-policy --execute --confirm '<confirmation from the plan>'
```

The activation is transactionally fenced and idempotent. It only installs the
rule policy. It does not synthesize reference distributions, Baseline Bundles,
or Channel Clock rows; those remain separate audited bootstrap operations.
