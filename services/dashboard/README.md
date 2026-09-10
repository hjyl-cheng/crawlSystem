# QY Operations Dashboard

This dashboard is the crawler operations interface. It is different from the
Rota dashboard, which manages proxies.

The dashboard reads Crawler state through PgBouncer, queue state through Redis,
raw-object metadata through MinIO, Rota capacity through the Proxy Control
interface, and publication audit state through its restricted Business database
credential. Runtime addresses are supplied by `deploy/compose.yml`; no production
hostnames are embedded in this source directory.

The main pages cover Query, Migration, Incremental clocks, Agent configuration,
Crawler health, Publication state, and BullMQ queues.

The `/server-nodes` page starts empty. Operators register an execution server
with its SSH address, user, and bootstrap password, then choose **Add and
initialize**. The backend verifies SSH/sudo, appends a dedicated public key and
verifies key-only login, installs Beszel Agent 0.19.0, and waits for a fresh Hub
sample. The page polls durable progress and then enables worker plan editing.
**Worker container deployment remains a separate, not-yet-connected operation**;
saving a plan never starts workers or changes crawler queues.

Passwords are sent only to the JSON initialization endpoint and retained only in
memory for that operation. They never enter registry data, command arguments,
logs, or browser storage. Private keys and authenticated SSH host-key pins are
stored with mode 0600 under `SERVER_NODE_STATE_DIR`. Host-key changes are rejected.
The initial authenticated connection establishes the pin. Only systemd-based
Linux x86_64/arm64 execution nodes are supported by this installer.

The registry and initialization states use existing `crawler.settings` under
`dashboard_server_nodes_v1`; no new PostgreSQL tables are needed. Full-document
compare-and-swap admits at most three concurrent initializations. Each operation
has an ID and a 15-minute deadline; updates from an obsolete operation are rejected.
Remote commands and transfers are bounded below that deadline. If Dashboard is
restarted mid-operation, progress is retained and retry becomes available when
that deadline expires; passwords are not automatically restored. Repeated work
reuses the per-node key, stable Hub system ID, token and dedicated agent service.

Initialization blocks metadata edits/deletion. Afterwards, connection identity
is fixed; users may edit name/notes and worker plans. Known metadata-only legacy
records and new `not_started` records can be removed. Failed authentication can
also be removed when no remote changes were begun. Center nodes and unknown or
initialized runtime states remain protected until worker/task/dispatch checks
are implemented. Metadata deletion never removes remote services, SSH keys or
collected data. Client-supplied provisioning states are rejected.

Enable onboarding with `deploy/compose.server-nodes.yml` in addition to the
existing compose files. Set `QY_BESZEL_EMAIL` and `QY_BESZEL_PASSWORD` in the
protected runtime environment and create matching `{ "email": "...", "password":
"..." }` credentials in `${QY_RUNTIME_ROOT}/server-nodes/beszel.json` (mode 0600,
parent directory 0700). Preserve both `server-nodes` (SSH keys/credentials) and
`beszel` (Hub SQLite data/key) runtime directories across deployments and backups.
The Hub's user is created on its first boot; changing the environment later does
not rotate its account password automatically.

The Hub is internal. nginx exposes only
`/node-monitoring/api/beszel/agent-connect` for outbound Agent WebSockets, using
per-node Beszel tokens and Hub-key verification. The Hub UI and PocketBase APIs
are not exposed. No inbound monitoring port is opened on execution nodes.
Monitoring runs as a dedicated `qy-beszel` user via `qy-beszel-agent.service`,
without Docker socket access. Install archives are pinned and SHA-256 checked
against the official release manifest. The existing dashboard login and JSON
write protections cover initialization and configuration endpoints.

For the isolated persistence/HTTP test, set `SERVER_NODES_TEST_DATABASE_URL` to a
local database named `server_nodes_dashboard_test` and run `npm test`. The test
resets `crawler.settings` in that dedicated test database only.

Run its tests with:

```bash
cd services/dashboard
npm test
```

An isolated end-to-end fixture is provided in `test-fixtures/node-onboarding`.
It uses real OpenSSH, password-based sudo, SSH key authentication and the actual
Beszel binary/Hub. Its small service-manager shim launches the Agent and validates
the unit with `systemd-analyze verify`; it does not test a live systemd daemon or
require a privileged container. Production node initialization is triggered only
by the operator from the page, never by automatic registration at deployment.
