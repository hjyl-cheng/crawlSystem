# Runtime Material

This directory is the host-only runtime layer for the source repository. Its
contents are mounted when containers start and are never copied into images or
committed to Git.

Create an environment with:

```bash
./scripts/bootstrap.sh local
./scripts/bootstrap.sh smoke
./scripts/bootstrap.sh production
```

Each environment contains:

- `env/runtime.env`: generated environment configuration
- `secrets/`: database URLs, transport tokens, and authentication material
- `cookies/`: YouTube cookies, Visitor Data, and browser identity material
- `proxy/`: Rota proxy-source credentials and import material

Directories and files containing real values must stay mode `0700`/`0600`
where possible. Secret files mounted read-only into non-root containers may be
mode `0444`, while their host parent directory remains mode `0700`.

Do not place encryption keys for runtime backups inside this repository.
