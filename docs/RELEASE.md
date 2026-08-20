# Release Procedure

1. Work only in `pachongsys` and review `git status` before testing.
2. Run `./scripts/test.sh` and `./scripts/verify.sh`.
3. Commit the exact source. The build refuses dirty or uncommitted source.
4. Build all affected images with
   `./scripts/build-images.sh <environment> pachongsys-<short-commit> [service ...]`;
   an optional tag suffix such as `-canary` is allowed. Omitting services builds
   every self-owned image. For example, a Dashboard-only change uses
   `./scripts/build-images.sh production pachongsys-<short-commit> dashboard`.
5. Inspect OCI labels and confirm the revision equals the intended commit.
6. Start a fresh isolated smoke project and require zero unexpected restarts.
7. Exercise Query, Migration, Incremental, local Agent, Finalize, and
   Publication end to end.
8. Replace one idle production worker as a canary. Do not mix source files into
   an existing container.
9. Promote role by role while monitoring queue latency, failures, Rota capacity,
   database pools, and publication lag.
10. Keep the previous immutable image digest. Rollback recreates affected roles
    with that digest; it never restores files inside a container.

Production deployment and rollback are deliberate operations. Build scripts do
not touch online containers, queues, databases, or volumes.

## Image Ownership

| Changed source | Build service | Runtime image variable |
| --- | --- | --- |
| `services/qybullmq`, `services/local-agent` | `qybullmq-api` | `QYBULLMQ_IMAGE_TAG` |
| `services/rota/core` | `rota-core` | `QY_ROTA_CORE_IMAGE_TAG` |
| `services/rota/dashboard` | `rota-dashboard` | `QY_ROTA_DASHBOARD_IMAGE_TAG` |
| `services/feature-engine` | `feature-ingest` | `QY_FEATURE_ENGINE_IMAGE_TAG` |
| `services/feature-dispatch` | `feature-dispatch` | `QY_FEATURE_DISPATCH_IMAGE_TAG` |
| `services/dashboard` | `dashboard` | `QY_DASHBOARD_IMAGE_TAG` |
| `services/auth` | `auth` | `QY_AUTH_IMAGE_TAG` |

The global `QY_IMAGE_TAG` remains the default for a fresh unified deployment.
An individual image variable overrides only that component. A shared contract
change must rebuild every image that consumes that contract; in particular,
the Rota identity policy catalog is also embedded in the QYBullMQ image.
