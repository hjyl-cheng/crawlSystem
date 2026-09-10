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

The `/server-nodes` page starts empty. Operators can manually register servers.
The intended flow is registration, SSH/key setup and Beszel onboarding, then
worker configuration. Password fields and initialization controls are visibly
disabled until the onboarding backend is available; passwords are never sent to
the metadata API. SSH aliases are no longer hand-entered, and editing preserves
existing aliases and worker plans. New or changed worker plans are rejected until
readiness can be verified, including requests made directly to the API.

Node cards have edit, initialization, and deletion-review entries. Execution
nodes that have only been registered can be removed, including saved worker
plans and SSH references. This removes metadata only; remote services, SSH keys,
and collected data are untouched. Center nodes are protected. For nodes whose
initialization/deployment has started or whose state is unknown, deletion stays
blocked until dispatch, worker, task, and operation states can be verified.
Unknown state is never shown as zero workers or a successful safety check.

New registrations have a server-owned `provisioning: {state: "not_started"}`
marker. Known legacy V1 metadata records without that marker also qualify,
because V1 had no initialization/deployment execution. Unknown fields or states
block deletion. Metadata edits preserve server-owned markers. Deletion checks
are read-only; DELETE revalidates eligibility and uses a full-registry atomic
compare-and-swap. Future initialization must atomically persist its started
state BEFORE any external work and must not start after a registration is removed.

The registry is persisted in
`crawler.settings` under `dashboard_server_nodes_v1`, with optimistic concurrency
checks. Registration does not connect over SSH, install software, change queues,
or deploy containers. SSH aliases are references only; passwords and private keys
are rejected. No production nodes are embedded or automatically registered.

Monitoring and runtime worker status remain explicitly unavailable until the
monitoring/deployment integrations are implemented. Planned counts are never
presented as running workers. The controlled migration write policy permits only
the exact metadata endpoints; it does not permit deployment or queue controls.

For the isolated persistence/HTTP test, set `SERVER_NODES_TEST_DATABASE_URL` to a
local database named `server_nodes_dashboard_test` and run `npm test`. The test
resets `crawler.settings` in that dedicated test database only.

Run its tests with:

```bash
cd services/dashboard
npm test
```
