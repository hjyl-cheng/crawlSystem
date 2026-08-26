import { readFile } from "node:fs/promises";

const START_MARKER = "-- migration-channel-inventory-schema:start";
const END_MARKER = "-- migration-channel-inventory-schema:end";

export function migrationChannelInventorySchemaBlock(schemaValue) {
  const schema = String(schemaValue ?? "");
  const start = schema.indexOf(START_MARKER);
  const end = schema.indexOf(END_MARKER);
  if (start < 0 || end <= start
      || schema.indexOf(START_MARKER, start + START_MARKER.length) >= 0
      || schema.indexOf(END_MARKER, end + END_MARKER.length) >= 0) {
    throw new Error("Migration channel inventory schema markers are missing or duplicated");
  }
  return schema.slice(start + START_MARKER.length, end).trim();
}

export async function loadMigrationChannelInventorySchemaState(query) {
  if (typeof query !== "function") {
    throw new TypeError("Target query is required for Migration inventory schema check");
  }
  const result = await query(
    `SELECT
       to_regclass('crawler.migration_channel_inventory_syncs') IS NOT NULL
         AS sync_table_ready,
       to_regclass('crawler.migration_channel_inventory') IS NOT NULL
         AS inventory_table_ready,
       (
         SELECT count(*)=10
         FROM information_schema.columns
         WHERE table_schema='crawler'
           AND table_name='migration_channel_inventory_syncs'
           AND column_name=ANY($1::text[])
       ) AS sync_columns_ready,
       (
         SELECT count(*)=13
         FROM information_schema.columns
         WHERE table_schema='crawler'
           AND table_name='migration_channel_inventory'
           AND column_name=ANY($2::text[])
       ) AS inventory_columns_ready,
       index_state.indisvalid IS TRUE AS page_index_valid,
       index_state.indisready IS TRUE AS page_index_ready,
       pg_get_indexdef(index_state.indexrelid) AS page_index_definition
     FROM (SELECT 1) singleton
     LEFT JOIN pg_index index_state
       ON index_state.indexrelid=
          to_regclass('crawler.idx_crawler_migration_inventory_page')`,
    [
      [
        "source_id",
        "source_database",
        "source_database_oid",
        "status",
        "sync_token",
        "eligible_count",
        "started_at",
        "completed_at",
        "last_error",
        "updated_at",
      ],
      [
        "source_id",
        "source_candidate_id",
        "channel_id",
        "channel_url",
        "handle",
        "title",
        "avatar_url",
        "search_subscriber_count",
        "priority",
        "source_candidate_status",
        "source_updated_at",
        "sync_token",
        "synced_at",
      ],
    ],
  );
  if (result.rows.length !== 1) {
    throw new Error("Migration channel inventory schema check returned an ambiguous result");
  }
  return result.rows[0];
}

export function assertMigrationChannelInventorySchemaState(state) {
  const indexDefinition = String(state?.page_index_definition || "")
    .replace(/\s+/g, " ")
    .toLowerCase();
  const ready = state?.sync_table_ready === true
    && state?.inventory_table_ready === true
    && state?.sync_columns_ready === true
    && state?.inventory_columns_ready === true
    && state?.page_index_valid === true
    && state?.page_index_ready === true
    && /\(source_id, priority desc, source_candidate_id\)/.test(indexDefinition);
  if (!ready) {
    const error = new Error(
      "Migration channel inventory schema is not ready; publish the controlled schema before API startup",
    );
    error.code = "migration_inventory_schema_not_ready";
    throw error;
  }
  return state;
}

export async function assertMigrationChannelInventorySchema({ query } = {}) {
  return assertMigrationChannelInventorySchemaState(
    await loadMigrationChannelInventorySchemaState(query),
  );
}

export async function loadMigrationChannelInventorySchemaSql() {
  const schema = await readFile(new URL("./schema.sql", import.meta.url), "utf8");
  return migrationChannelInventorySchemaBlock(schema);
}
