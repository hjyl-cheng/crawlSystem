import { readFile } from "node:fs/promises";

const START_MARKER = "-- migration-channel-inventory-schema:start";
const END_MARKER = "-- migration-channel-inventory-schema:end";
const SYNC_COLUMNS = Object.freeze([
  { name: "source_id", data_type: "text", not_null: true, default_expression: null },
  { name: "source_database", data_type: "text", not_null: true, default_expression: null },
  { name: "source_database_oid", data_type: "oid", not_null: true, default_expression: null },
  { name: "status", data_type: "text", not_null: true, default_expression: "'syncing'::text" },
  { name: "sync_token", data_type: "uuid", not_null: true, default_expression: null },
  { name: "eligible_count", data_type: "bigint", not_null: true, default_expression: "0" },
  { name: "started_at", data_type: "timestamp with time zone", not_null: true, default_expression: "now()" },
  { name: "completed_at", data_type: "timestamp with time zone", not_null: false, default_expression: null },
  { name: "last_error", data_type: "text", not_null: false, default_expression: null },
  { name: "updated_at", data_type: "timestamp with time zone", not_null: true, default_expression: "now()" },
]);
const INVENTORY_COLUMNS = Object.freeze([
  { name: "source_id", data_type: "text", not_null: true, default_expression: null },
  { name: "source_candidate_id", data_type: "bigint", not_null: true, default_expression: null },
  { name: "channel_id", data_type: "text", not_null: true, default_expression: null },
  { name: "channel_url", data_type: "text", not_null: true, default_expression: null },
  { name: "handle", data_type: "text", not_null: false, default_expression: null },
  { name: "title", data_type: "text", not_null: false, default_expression: null },
  { name: "avatar_url", data_type: "text", not_null: false, default_expression: null },
  { name: "search_subscriber_count", data_type: "bigint", not_null: false, default_expression: null },
  { name: "priority", data_type: "integer", not_null: true, default_expression: "100" },
  { name: "source_candidate_status", data_type: "text", not_null: true, default_expression: null },
  { name: "source_updated_at", data_type: "timestamp with time zone", not_null: false, default_expression: null },
  { name: "sync_token", data_type: "uuid", not_null: true, default_expression: null },
  { name: "synced_at", data_type: "timestamp with time zone", not_null: true, default_expression: "now()" },
]);

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
    `WITH expected_sync_column AS (
       SELECT expected.value->>'name' AS name,
              expected.value->>'data_type' AS data_type,
              (expected.value->>'not_null')::boolean AS not_null,
              expected.value->>'default_expression' AS default_expression,
              expected.position
       FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY
         AS expected(value,position)
     ), expected_inventory_column AS (
       SELECT expected.value->>'name' AS name,
              expected.value->>'data_type' AS data_type,
              (expected.value->>'not_null')::boolean AS not_null,
              expected.value->>'default_expression' AS default_expression,
              expected.position
       FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY
         AS expected(value,position)
     ), actual_column AS (
       SELECT table_class.relname AS table_name,
              attribute.attnum::bigint AS position,
              attribute.attname AS name,
              format_type(attribute.atttypid,attribute.atttypmod) AS data_type,
              attribute.attnotnull AS not_null,
              pg_get_expr(default_state.adbin,default_state.adrelid,true)
                AS default_expression,
              attribute.attidentity AS identity_kind,
              attribute.attgenerated AS generated_kind
       FROM pg_class table_class
       JOIN pg_namespace table_namespace
         ON table_namespace.oid=table_class.relnamespace
       JOIN pg_attribute attribute ON attribute.attrelid=table_class.oid
       LEFT JOIN pg_attrdef default_state
         ON default_state.adrelid=attribute.attrelid
        AND default_state.adnum=attribute.attnum
       WHERE table_namespace.nspname='crawler'
         AND table_class.relname IN (
           'migration_channel_inventory_syncs','migration_channel_inventory'
         )
         AND table_class.relkind='r'
         AND attribute.attnum>0
         AND NOT attribute.attisdropped
     ), constraint_state AS (
       SELECT table_class.relname AS table_name,
              constraint_record.contype AS constraint_type,
              constraint_record.convalidated AS validated,
              constraint_record.condeferrable AS is_deferrable,
              constraint_record.condeferred AS is_initially_deferred,
              constraint_record.connoinherit AS no_inherit,
              constraint_record.confdeltype AS delete_action,
              constraint_record.confupdtype AS update_action,
              constraint_record.confmatchtype AS match_type,
              ARRAY(
                SELECT key_attribute.attname
                FROM unnest(constraint_record.conkey) WITH ORDINALITY
                  AS key_column(attnum,position)
                JOIN pg_attribute key_attribute
                  ON key_attribute.attrelid=constraint_record.conrelid
                 AND key_attribute.attnum=key_column.attnum
                ORDER BY key_column.position
              )::text[] AS key_columns,
              referenced_namespace.nspname AS referenced_schema,
              referenced_table.relname AS referenced_table,
              ARRAY(
                SELECT referenced_attribute.attname
                FROM unnest(constraint_record.confkey) WITH ORDINALITY
                  AS referenced_column(attnum,position)
                JOIN pg_attribute referenced_attribute
                  ON referenced_attribute.attrelid=constraint_record.confrelid
                 AND referenced_attribute.attnum=referenced_column.attnum
                ORDER BY referenced_column.position
              )::text[] AS referenced_columns,
              regexp_replace(
                replace(
                  lower(COALESCE(pg_get_expr(
                    constraint_record.conbin,constraint_record.conrelid,false
                  ),'')),
                  '::text',''
                ),
                '[[:space:]()]','','g'
              ) AS check_expression
       FROM pg_constraint constraint_record
       JOIN pg_class table_class ON table_class.oid=constraint_record.conrelid
       JOIN pg_namespace table_namespace
         ON table_namespace.oid=table_class.relnamespace
       LEFT JOIN pg_class referenced_table
         ON referenced_table.oid=constraint_record.confrelid
       LEFT JOIN pg_namespace referenced_namespace
         ON referenced_namespace.oid=referenced_table.relnamespace
       WHERE table_namespace.nspname='crawler'
         AND table_class.relname IN (
           'migration_channel_inventory_syncs','migration_channel_inventory'
         )
     ), index_state AS (
       SELECT index_record.indisvalid,index_record.indisready,index_record.indislive,
              index_record.indisunique,index_record.indisprimary,
              index_record.indisexclusion,index_record.indimmediate,
              index_record.indnkeyatts,index_record.indnatts,
              index_namespace.nspname AS index_schema,
              table_namespace.nspname AS table_schema,
              table_class.relname AS table_name,
              access_method.amname AS access_method,
              index_record.indpred IS NULL AS predicate_absent,
              index_record.indexprs IS NULL AS expressions_absent,
              ARRAY(
                SELECT key_attribute.attname
                FROM generate_series(0,index_record.indnkeyatts-1) key_position
                JOIN pg_attribute key_attribute
                  ON key_attribute.attrelid=index_record.indrelid
                 AND key_attribute.attnum=index_record.indkey[key_position]
                ORDER BY key_position
              )::text[] AS key_columns,
              ARRAY(
                SELECT (index_record.indoption[key_position] & 1)=1
                FROM generate_series(0,index_record.indnkeyatts-1) key_position
                ORDER BY key_position
              )::boolean[] AS descending_keys,
              ARRAY(
                SELECT (index_record.indoption[key_position] & 2)=2
                FROM generate_series(0,index_record.indnkeyatts-1) key_position
                ORDER BY key_position
              )::boolean[] AS nulls_first_keys,
              ARRAY(
                SELECT operator_class.opcname
                FROM generate_series(0,index_record.indnkeyatts-1) key_position
                JOIN pg_opclass operator_class
                  ON operator_class.oid=index_record.indclass[key_position]
                JOIN pg_namespace operator_namespace
                  ON operator_namespace.oid=operator_class.opcnamespace
                 AND operator_namespace.nspname='pg_catalog'
                ORDER BY key_position
              )::text[] AS operator_classes
       FROM pg_class index_class
       JOIN pg_namespace index_namespace
         ON index_namespace.oid=index_class.relnamespace
       JOIN pg_index index_record ON index_record.indexrelid=index_class.oid
       JOIN pg_class table_class ON table_class.oid=index_record.indrelid
       JOIN pg_namespace table_namespace
         ON table_namespace.oid=table_class.relnamespace
       JOIN pg_am access_method ON access_method.oid=index_class.relam
       WHERE index_class.oid=
         to_regclass('crawler.idx_crawler_migration_inventory_page')
     )
     SELECT
       COALESCE((
         SELECT table_class.relkind='r'
         FROM pg_class table_class
         WHERE table_class.oid=
           to_regclass('crawler.migration_channel_inventory_syncs')
       ),false) AS sync_table_ready,
       COALESCE((
         SELECT table_class.relkind='r'
         FROM pg_class table_class
         WHERE table_class.oid=
           to_regclass('crawler.migration_channel_inventory')
       ),false) AS inventory_table_ready,
       (
         (SELECT count(*) FROM actual_column
          WHERE table_name='migration_channel_inventory_syncs')
           =(SELECT count(*) FROM expected_sync_column)
         AND NOT EXISTS (
           SELECT 1
           FROM expected_sync_column expected
           LEFT JOIN actual_column actual
             ON actual.table_name='migration_channel_inventory_syncs'
            AND actual.position=expected.position
           WHERE actual.name IS DISTINCT FROM expected.name
              OR actual.data_type IS DISTINCT FROM expected.data_type
              OR actual.not_null IS DISTINCT FROM expected.not_null
              OR actual.default_expression IS DISTINCT FROM expected.default_expression
              OR actual.identity_kind IS DISTINCT FROM ''
              OR actual.generated_kind IS DISTINCT FROM ''
         )
       ) AS sync_columns_match,
       (
         (SELECT count(*) FROM actual_column
          WHERE table_name='migration_channel_inventory')
           =(SELECT count(*) FROM expected_inventory_column)
         AND NOT EXISTS (
           SELECT 1
           FROM expected_inventory_column expected
           LEFT JOIN actual_column actual
             ON actual.table_name='migration_channel_inventory'
            AND actual.position=expected.position
           WHERE actual.name IS DISTINCT FROM expected.name
              OR actual.data_type IS DISTINCT FROM expected.data_type
              OR actual.not_null IS DISTINCT FROM expected.not_null
              OR actual.default_expression IS DISTINCT FROM expected.default_expression
              OR actual.identity_kind IS DISTINCT FROM ''
              OR actual.generated_kind IS DISTINCT FROM ''
         )
       ) AS inventory_columns_match,
       (
         (SELECT count(*) FROM constraint_state
          WHERE table_name='migration_channel_inventory_syncs')=3
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory_syncs'
             AND constraint_type='p' AND validated
             AND NOT is_deferrable AND NOT is_initially_deferred
             AND key_columns=ARRAY['source_id']::text[]
         )
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory_syncs'
             AND constraint_type='c' AND validated AND NOT no_inherit
             AND check_expression=
               'status=anyarray[''syncing'',''ready'',''failed'']'
         )
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory_syncs'
             AND constraint_type='c' AND validated AND NOT no_inherit
             AND check_expression='eligible_count>=0'
         )
       ) AS sync_constraints_match,
       (
         (SELECT count(*) FROM constraint_state
          WHERE table_name='migration_channel_inventory')=4
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory'
             AND constraint_type='p' AND validated
             AND NOT is_deferrable AND NOT is_initially_deferred
             AND key_columns=ARRAY['source_id','channel_id']::text[]
         )
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory'
             AND constraint_type='u' AND validated
             AND NOT is_deferrable AND NOT is_initially_deferred
             AND key_columns=ARRAY['source_id','source_candidate_id']::text[]
         )
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory'
             AND constraint_type='f' AND validated
             AND NOT is_deferrable AND NOT is_initially_deferred
             AND key_columns=ARRAY['source_id']::text[]
             AND referenced_schema='crawler'
             AND referenced_table='migration_channel_inventory_syncs'
             AND referenced_columns=ARRAY['source_id']::text[]
             AND delete_action='c' AND update_action='a' AND match_type='s'
         )
         AND EXISTS (
           SELECT 1 FROM constraint_state
           WHERE table_name='migration_channel_inventory'
             AND constraint_type='c' AND validated AND NOT no_inherit
             AND check_expression=
               'source_candidate_status=anyarray[''discovered'',''queued'',''validating'',''failed'']'
         )
       ) AS inventory_constraints_match,
       COALESCE((
         SELECT index_record.indisvalid
           AND index_record.indisready
           AND index_record.indislive
           AND NOT index_record.indisunique
           AND NOT index_record.indisprimary
           AND NOT index_record.indisexclusion
           AND index_record.indimmediate
           AND index_record.indnkeyatts=3
           AND index_record.indnatts=3
           AND index_record.index_schema='crawler'
           AND index_record.table_schema='crawler'
           AND index_record.table_name='migration_channel_inventory'
           AND index_record.access_method='btree'
           AND index_record.predicate_absent
           AND index_record.expressions_absent
           AND index_record.key_columns=
             ARRAY['source_id','priority','source_candidate_id']::text[]
           AND index_record.descending_keys=ARRAY[false,true,false]::boolean[]
           AND index_record.nulls_first_keys=ARRAY[false,true,false]::boolean[]
           AND index_record.operator_classes=
             ARRAY['text_ops','int4_ops','int8_ops']::text[]
         FROM index_state index_record
       ),false) AS page_index_match`,
    [JSON.stringify(SYNC_COLUMNS), JSON.stringify(INVENTORY_COLUMNS)],
  );
  if (result.rows.length !== 1) {
    throw new Error("Migration channel inventory schema check returned an ambiguous result");
  }
  return result.rows[0];
}

export function assertMigrationChannelInventorySchemaState(state) {
  const requiredChecks = [
    "sync_table_ready",
    "inventory_table_ready",
    "sync_columns_match",
    "inventory_columns_match",
    "sync_constraints_match",
    "inventory_constraints_match",
    "page_index_match",
  ];
  const failures = requiredChecks.filter((key) => state?.[key] !== true);
  if (failures.length > 0) {
    const error = new Error(
      `Migration channel inventory schema is not ready (${failures.join(", ")}); `
        + "publish the controlled schema before API startup",
    );
    error.code = "migration_inventory_schema_not_ready";
    error.schemaFailures = failures;
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
  const schema = await readFile(new URL("./migrationInventorySchema.sql", import.meta.url), "utf8");
  return migrationChannelInventorySchemaBlock(schema);
}
