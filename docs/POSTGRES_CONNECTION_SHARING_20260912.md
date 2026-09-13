# PostgreSQL connections for incremental supervision and notifications

## Problem and change

On September 12, the running remote center held 40 PostgreSQL sessions for 40
remote Worker advisory locks. Twenty paused local incremental Workers each held
another LISTEN session, in addition to the center's listener. These 61 permanent
connections contributed to exhausting the database's 100-connection limit.

`SupervisionGuards` now assigns each independently locked slot to one of four
sessions. It retains the existing advisory lock namespace and key, verifies the
exact backend/key before admission, and serializes full recovery transactions
on each session. Normal channel collection does not run through this serialized
session queue. Releasing one slot unlocks only that key. A broken session
synchronously invalidates all its slots; the other groups retain their locks.
Recovery and reacquisition use the existing supervisor reconciliation loop.

The center's existing `qy_remote_transport` LISTEN session also forwards committed
`local-intake:<worker_id>` hints to `qy.local-intake.changed` over NATS. The 20 local
Workers subscribe using a dedicated read-only principal. Each Worker still reads
its saved database intent, keeps its original instance fence and checks admission
before executing a Plan. Notifications carry no execution authority. The existing
bounded five-second recheck survives missed notifications; SQL state is still the
source of truth. Disconnect/reconnect hints wake all local subscribers.

Target: **40 lock + 21 listener sessions → 4 lock + 1 listener sessions**. This is
not a claim that the whole system uses only five database connections. Ordinary
transactions continue using `pg.Pool` through the configured transaction pooler.
No tables, collection policies, Clock plans, migration data or Rota rules change.

## Validation

- Real PostgreSQL + Redis: 40 separate slot locks on four backend PIDs, 40 active
  BullMQ jobs, ten retained queued jobs after pause, no duplicate processing.
- Real PostgreSQL: competing centers, reentrant acquisition rejection, releasing
  one key without releasing siblings, recovery transaction serialization and
  rollback, backend termination invalidating exactly one group.
- Real supervisor: disconnected ownership cannot authorize writes; paused intent
  persists on restart; blocked slots recover and resume through reconciliation.
- Real WSS/NATS + PostgreSQL: 20 subscribers add only one LISTEN connection;
  committed own-worker changes wake the right subscriber; rolled-back and
  unrelated notifications do not; LISTEN reconnect resumes delivery.
- Actual broker ACL rejects publishing and unrelated subscriptions by the local
  principal. Provisioning produces the restricted account and hot reloads it.
- 27 transport/intake/Clock-fence regressions passed. The separate original Rota
  and BullMQ lifecycle suite passed all 18 tests, including SIGKILL before/after
  commit, recovery of original Plan, API continuation and country handoff.
- A targeted regression also reproduces ownership loss during the initial
  unsettled-work query; the supervisor now refuses to create consumers after
  cleanup has started.
- Both selective production overlay images imported successfully under their
  actual Node runtime, with networking disabled.

Two preexisting tests previously treated one/two manual `tick()` calls as a
completion barrier for asynchronous activation/recovery. They now keep driving
the real repeated reconciliation contract while waiting for the expected result.

## Deployment and rollback

Local Worker notification mode remains compatible with the older direct LISTEN
configuration for rollback. New production configuration selects NATS explicitly;
no credential failure silently falls back to a new direct database connection.

Use `deploy/compose.local-intake-nats.yml` instead of the old direct notification
override. Pin `QY_LOCAL_INCREMENTAL_IMAGE`, set `QY_LOCAL_INTAKE_NATS_URL`, and mount
`nats/secrets/local-intake-password` (mode 0600). The center receives the same file
via `LOCAL_INCREMENTAL_NATS_PASSWORD_FILE`; the broker principal can only subscribe
to the local control hint subject. Remote nodes retain their per-node credentials.

The current production local Workers were explicitly created, not managed by a
Compose service. Exact previous specs and updated create specs are retained under
ignored `runtime/remote-center-production/pg-connections-20260912/`, with private
file permissions. Center image/configuration is also persisted in `compose.env`
and `center.env`. Do not recreate the whole crawler stack to apply this change.

Drain remote incremental intake before replacing the center. Keep the 20 local
Workers paused during their rolling replacement. Migration Workers are outside
this rollout. Restore the original remote allowed count only after the center
and NATS authentication checks pass. Roll back using the retained stopped
containers/specs and environment backups, again draining first.

A group-session failure affects several remote slots rather than one. This is
an explicit connection-saving tradeoff: original ownership checks and recovery
fences remain mandatory, and healthy groups continue operating. It does not
eliminate at-least-once queue delivery or promise a fixed throughput gain.

## Production verification

- Remote intake paused at 07:45 UTC and all in-flight channels finished before
  center replacement at 07:49:14 UTC.
- Twenty local Workers were replaced individually between 07:50:13 and 07:52:33
  UTC, keeping allowed count zero. Restricted NATS authentication was verified
  before replacing them.
- Remote intake restored to 40 at 07:53:05 UTC. At 07:53:28, all 40 were connected,
  ready and executing. PostgreSQL showed **40 locks on four sessions** and exactly
  **one LISTEN session**. All 20 local Workers were connected, paused and inactive.
- NATS admission rejects/timeouts: zero at the verification sample; result stream
  pending zero, four acknowledgements in flight. Incremental queue active 40;
  migration queue active 40. All 40 migration container IDs were unchanged.
- Local Worker restart/OOM counts: zero. Center health check passed. Live env and
  saved image/secret-path configuration match. Collector code and remote collector
  image were not changed by this rollout.

### Post-resume observation and dashboard rate

The fixed 07:53:30–07:58:30 UTC window completed 156 succeeded Plans (31.2/min),
including 46 video Plans, and received 804 video-detail commands. At 07:58:54,
there were still 40 remote active consumers, 40 independent locks on four
sessions, one listener and 20 connected paused local Workers. NATS pending and
unacknowledged were both zero; no admission rejects/timeouts were reported.
This mixed workload is not directly comparable with an earlier video-heavy window.

Collection retry events remain: route-budget exhaustion, proxy-control request
timeouts and an HTTP 503 were observed. At 07:58:09, 13 of 27 jobs with retry
failure events had later completion events, with 14 not yet completed. The
incremental BullMQ failed count was still 18, the same as the paused baseline.
No supervisor session-loss or recovery-fence errors appeared in the checked log.
Do not describe this observation as an error-free collection run.

The dashboard label is "近 5 分钟 X 个/分钟" (a per-minute rate over a rolling
five-minute window). Its rate includes the intentional rollout pause, and its
30-second statistics cache may return older values during refresh. The running
Dashboard endpoint returned 31.0/min with generatedAt 07:58:21 UTC. Reconstructed
succeeded-only rolling rates dipped to 5.8/min at 07:50 and 1.0/min at 07:51,
then recovered to 30.4/min at 07:58. The user's reported 4.8/min screenshot/time
was not captured, so that exact old value cannot be proven from browser state.

A later Dashboard statistics request returned 503. Its log explicitly reported
`canceling statement due to statement timeout`; therefore old browser rates can
also persist when statistics refresh fails. A simultaneous read-only database
sample showed publication onboarding scanning for 244 seconds with DataFileRead
waits, plus concurrent migration statistics scans. This is a remaining SQL/I/O
performance issue, not evidence that the worker fleet fell to 4.8 completions per
five minutes. The current task reduced dedicated connections; it did not optimize
these separate full-dataset statistics/publication queries.
