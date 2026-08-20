# Deployment

## 1. Prepare The Host

Install Docker Engine, Docker Compose v2, OpenSSL, Git, and Git LFS. A clone
must materialize LFS models before image builds:

```bash
git lfs install
git clone <repository-url> pachongsys
cd pachongsys
git lfs pull
./scripts/verify.sh
```

## 2. Create An Isolated Runtime

Every environment has its own ignored runtime directory:

```bash
./scripts/bootstrap.sh local
./scripts/bootstrap.sh smoke
./scripts/bootstrap.sh production
```

The command creates `runtime/<environment>/env/runtime.env`, database and
transport secrets, and directories for YouTube identities and Rota source
credentials. Edit only the selected environment's `runtime.env`.

Real production credentials may be imported into `runtime/production`, but
must never be copied into a Dockerfile, Compose environment committed to Git,
or an image layer. Rota live inventory remains database state and must be
backed up separately.

## 3. Verify Configuration

```bash
./scripts/compose.sh smoke config --quiet
./scripts/verify.sh
```

`scripts/compose.sh` is the deployment interface. It resolves the correct
runtime directory and exports `QY_RUNTIME_ROOT`; direct Compose commands are
discouraged because they can accidentally mix environments.

## 4. Build Immutable Images

```bash
./scripts/build-images.sh smoke pachongsys-$(git rev-parse --short=7 HEAD)-smoke
```

The build writes version, Git revision, build time, and source repository into
OCI labels. `latest` and `local` are rejected by the release build interface.
The same QYBullMQ image digest serves Query, Migration, Incremental, Agent,
Finalize, Controller, Data API, Feature Relay, and Publication roles.
The build interface selects one representative role per image so Compose does
not export the large QYBullMQ image repeatedly.

## 5. Start Infrastructure And Applications

For a fresh isolated environment:

```bash
QY_IMAGE_TAG=pachongsys-$(git rev-parse --short=7 HEAD)-smoke \
  ./scripts/compose.sh smoke up -d
```

The bundled topology starts empty PostgreSQL databases, PgBouncer, Redis,
MinIO, Rota, applications, and Nginx. Configure usable proxy sources in the
isolated Rota before dispatching YouTube jobs.

A production cutover must connect to the approved persistent volumes and
networks, preserve the old immutable tag for rollback, and first replace one
idle canary worker. Never run `down -v` against production data.

## 6. Runtime Verification

```bash
./scripts/compose.sh smoke ps
./scripts/compose.sh smoke logs --tail=200
docker image inspect <image> --format '{{json .Config.Labels}}'
```

Verify Query, Migration, Incremental, local Agent, Finalize, and Publication
through their real interfaces before increasing worker counts. A container
that restarts unexpectedly blocks promotion.

## 7. Fresh Database Limitations

The repository contains schemas, not production rows. Channel/video/comment
data, query terms, Redis jobs, MinIO objects, Rota proxy inventory, and
publication ownership must be restored through separately controlled backups.
Source plus runtime credentials alone does not recreate production data.

## 8. Shared QY Control Plane

An environment can display and operate the existing QY state without copying
PostgreSQL, Redis, MinIO, Business PostgreSQL, or Rota data. Configure the
ignored runtime from the running QY containers, then validate the reduced
service set:

```bash
./ops/adopt-qy-shared-runtime.sh newcrawler
./scripts/compose.sh newcrawler config --services
./scripts/compose.sh newcrawler up -d --no-build nginx
```

In `shared-qy` mode only Auth, QYBullMQ API, Dashboard, Rota Dashboard, and
Nginx run in the new project. They join the existing QY Docker networks. The
bundled empty databases, MinIO, Redis, Rota Core, Workers, Feature services,
and publication writers are excluded by default, preventing duplicate queue
consumption, proxy reconciliation, and business projection.

Credentials remain in the ignored runtime environment. The adoption command
reads them from existing containers without printing their values. It does not
start, stop, or recreate a container and does not modify any database row.

## 9. Shared QY Worker Takeover

Use `shared-qy-workers` only after the existing QYBullMQ consumers have
gracefully stopped. PostgreSQL, Redis, MinIO, Business PostgreSQL, and Rota
remain shared, while the new project starts Controller and every QYBullMQ queue
Worker from one immutable `pachongsys` image:

```bash
QY_DEPLOYMENT_MODE=shared-qy-workers \
  ./scripts/compose.sh newcrawler up -d --no-build \
    local-agent-config controller \
    worker-channel worker-incremental worker-discover worker-query-quality \
    worker-data-api worker-agent worker-finalize
```

The default persistent scale is 20 Full workers and 20 Incremental workers,
matching the last stable QY production topology.
Override `QY_CHANNEL_WORKER_REPLICAS` and `QY_INCREMENTAL_WORKER_REPLICAS` in
the ignored runtime environment when Rota capacity changes. Never run the old
and new consumers together against the shared Redis queues.
