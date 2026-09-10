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

The `/server-nodes` page starts empty. Operators can manually register servers
and save per-role worker counts as **plans only**. The registry is persisted in
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
