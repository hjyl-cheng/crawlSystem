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

Run its tests with:

```bash
cd services/dashboard
npm test
```
