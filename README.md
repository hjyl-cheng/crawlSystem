# QY Crawler Source of Truth

`pachongsys` is the authoritative source repository for the QY YouTube crawler
system. Application changes, tests, image builds, and deployment definitions
must originate here. Running containers are build artifacts, never editable
source locations.

The initial baseline was reconciled against the QY runtime on 2026-08-19 and
unified role-specific QYBullMQ images into one implementation. Windmill,
WebAI2API, Crawl4AI, and unrelated host applications are outside this
repository.

## Repository Layout

```text
services/qybullmq/         all queue and publication roles, one image
services/local-agent/      local profile processor and model bundle
services/feature-engine/   Feature clocks and ingestion
services/feature-dispatch/ dynamic incremental dispatch
services/rota/             proxy lifecycle, slots, and dashboard
services/dashboard/        QY operations dashboard
services/auth/             dashboard authentication gateway
database/                  empty-database bootstrap and schema references
deploy/                    environment-neutral Compose topology
runtime/                   host-only credentials and identities (Git ignored)
backups/                   encrypted runtime exports (Git ignored)
ops/                       controlled one-time operations
scripts/                   build, verification, and runtime entry points
docs/                      architecture, provenance, and release procedure
```

## Source And Runtime Separation

Images contain source, dependencies, and model artifacts. They never contain
database passwords, transport tokens, YouTube cookies, proxy-source accounts,
or live proxy inventory. Those values remain under `runtime/<environment>/`
and are mounted only when containers start.

```text
Git commit -> tests -> immutable images -> runtime mounts -> containers
```

Do not copy edited files into a running container. Build a new immutable image
tag and recreate only the intended roles.

## First Local Or Smoke Run

Prerequisites: Docker Engine, Docker Compose v2, OpenSSL, Git, and Git LFS.

```bash
./scripts/bootstrap.sh smoke
./scripts/compose.sh smoke config --quiet
./scripts/build-images.sh smoke pachongsys-$(git rev-parse --short=7 HEAD)-smoke
QY_IMAGE_TAG=<the-same-tag> ./scripts/compose.sh smoke up -d
```

Start with one replica of each role. Scale network workers only when Rota has
ready capacity:

```bash
QY_IMAGE_TAG=<immutable-tag> ./scripts/compose.sh production up -d \
  --scale worker-channel=20 \
  --scale worker-incremental=5 \
  --scale worker-agent=1
```

Read [Source Of Truth](docs/SOURCE_OF_TRUTH.md),
[Release Procedure](docs/RELEASE.md), [Deployment](docs/DEPLOYMENT.md), and
[Production Baseline](docs/PRODUCTION_BASELINE.md) before a production change.
