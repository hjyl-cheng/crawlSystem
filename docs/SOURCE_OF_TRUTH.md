# Source Of Truth

## Rule

`pachongsys` is the only editable source location for the QY crawler system.
Container filesystems, old workspace trees, temporary deployment directories,
and exported source bundles are evidence or artifacts, not development roots.

## Ownership

- QYBullMQ owns queue orchestration, crawling, local Agent invocation,
  finalization, and publication. Every role uses one source tree and image.
- Local Agent owns profile inference and model artifacts under
  `services/local-agent`; QYBullMQ embeds it at image build time.
- Rota owns proxy inventory, lifecycle, health, cooldown, and slot replacement.
- Feature Engine and Feature Dispatch own clock planning and incremental release.
- Dashboard and Auth own the operator interface and access gateway.

## Artifact Flow

```text
pachongsys commit
  -> tests
  -> immutable image tag + OCI revision label
  -> isolated smoke environment
  -> one-role canary
  -> controlled production replacement
```

`allpachongSystemSrc` is a sanitized release export generated from a tagged
commit. Fixes must never be made only in that export.

## Runtime Material

Host credentials and identities live under ignored `runtime/<environment>`
directories. Production databases, Redis, MinIO, and Rota inventory are
persistent state outside Git. Encrypted exports may be staged in `backups/`,
but encryption keys remain outside the repository.

## Scope

Windmill and other unrelated host applications are not part of this source
repository. A documented external dependency may be referenced without copying
that application's source or runtime data into `pachongsys`.
