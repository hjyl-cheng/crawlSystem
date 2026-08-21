# Newcrawler Runtime

This ignored runtime hosts the isolated `qy-newcrawler` deployment.

- Immutable image tag: pinned by `QY_IMAGE_TAG` in the ignored runtime environment
- Application source revision: recorded in each image's
  `org.opencontainers.image.revision` OCI label
- Dashboard: `https://newcrawdashboard.137-175-93-199.nip.io`
- BullMQ: `https://newcrawqueues.137-175-93-199.nip.io/queues`
- Rota: `https://newcrawrota.137-175-93-199.nip.io`
- MinIO: `https://newcrawminio.137-175-93-199.nip.io`
- Internal public HTTP port: `38082`
- Rota proxy port: `39286` on loopback only

The first phase starts only the Dashboard control-plane dependency closure:

```bash
./scripts/compose.sh newcrawler up -d --no-build nginx
```

It intentionally does not start crawler workers, controllers, schedulers,
Agent workers, or publication workers. Runtime credentials remain under this
directory and are ignored by Git.

The initial empty PostgreSQL, Redis, MinIO, and Rota volumes validate fresh
bootstrap behavior. To reuse the existing QY state without copying it, run
`ops/adopt-qy-shared-runtime.sh newcrawler`. Shared mode starts only the new
control plane and connects it to the existing QY Docker networks; all bundled
state and writer services are excluded by default. The adoption step also
aligns the ignored runtime credentials used by the MinIO and Rota consoles with
the shared QY services.

After the legacy QYBullMQ consumers and daily Feature Clock processes have
gracefully stopped, set `QY_DEPLOYMENT_MODE=shared-qy-workers` to run Controller,
all queue Workers, the daily Scheduler, and Feature Dispatch from the new
project while continuing to share QY PostgreSQL, Redis, MinIO, and Rota. The
default scale is 20 Full workers and 20 Incremental workers, matching the last
stable QY production topology.
