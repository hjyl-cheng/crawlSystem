# Newcrawler Runtime

This ignored runtime hosts the isolated `qy-newcrawler` deployment.

- Application source revision: `3890ea35a22d3610faaa0e562d27da87579a8568`
- Immutable image tag: `pachongsys-3890ea3`
- Dashboard: `https://newcrawdashboard.137-175-93-199.nip.io`
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
bootstrap behavior. Existing production PostgreSQL and MinIO data must be
integrated later through an explicit, separately reviewed migration or
read-only connection plan.
