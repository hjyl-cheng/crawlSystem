# Database Files

`bootstrap/` contains the Crawler and Business schema-only snapshots that
initialize fresh Compose volumes. They contain no table rows.

`reference-snapshots/` contains Feature and Rota schema snapshots for auditing
and comparison only. They are never replayed by the canonical deployment. The
Feature schema already exists inside `bootstrap/crawler.sql`; Rota applies its
own idempotent Go migrations at startup.

Runtime migrations remain next to their owning implementation:

- `services/qybullmq/src/schema.sql` owns Crawler runtime DDL.
- `services/feature-engine/sql/schema.sql` owns Feature Clock migrations.
- `services/rota/core/internal/database/` owns Rota migrations.
