# Database Files

`bootstrap/` contains the complete Crawler and Business structure snapshots that
initialize fresh Compose volumes. Their DDL was refreshed from production on
2026-09-10 UTC: Crawler has 80 tables (including Feature Clock and Publication),
and Business has 64 tables. No production rows, credentials, object ownership,
or grants are exported.

Each file ends with a `fresh-bootstrap-seeds` section containing the existing
fresh-install configuration: database identity, stopped query scheduler, Content
Enrich defaults, content taxonomy, and an empty Creator Search baseline. These
seeds are not copied from production. Deployment roles and grants are configured
by `init/` and the owning services.

Both updated bootstrap files were restored into isolated PostgreSQL databases.
Their schema-only re-exports matched the corresponding production exports after
removing the random psql restrict token. The seed statements also restored
successfully.

The Business snapshot preserves the current production `raw_contents_v4_shape`
constraint: raw imports do not yet allow `unlisted`, and disabled comments do not
yet require a zero count in that table. Public Content snapshots support
`unlisted` and enforce the v4 Projection zero-count contract. Any future change
to the raw import constraint needs an explicit production migration followed by
a snapshot refresh; do not silently edit the snapshot ahead of production.

To refresh these files, export each production database with its matching
`pg_dump --schema-only --no-owner --no-privileges --lock-wait-timeout=5s`, replace
the existing dump, and retain the `fresh-bootstrap-seeds` section. Validate a
fresh restore and compare its schema-only export before committing.

`reference-snapshots/` contains Feature and Rota schema snapshots for auditing
and comparison only. They are never replayed by the canonical deployment. The
Feature schema already exists inside `bootstrap/crawler.sql`; Rota applies its
own idempotent Go migrations at startup.

Runtime migrations remain next to their owning implementation:

- `services/qybullmq/src/schema.sql` owns Crawler runtime DDL.
- `services/feature-engine/sql/schema.sql` owns Feature Clock migrations.
- `services/rota/core/internal/database/` owns Rota migrations.
