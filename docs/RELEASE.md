# Release Procedure

1. Work only in `pachongsys` and review `git status` before testing.
2. Run `make test` and `./scripts/verify.sh`.
3. Commit the exact source and use an immutable tag containing date and commit.
4. Build with `./scripts/build-images.sh <environment> <tag>`.
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
