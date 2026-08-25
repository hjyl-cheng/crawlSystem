import { environmentValue } from "./runtimeEnvironment.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function explicitCount(environment, name) {
  const raw = requiredText(environment[name], name);
  if (!/^(0|[1-9][0-9]*)$/.test(raw)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  const value = Number(raw);
  if (!Number.isSafeInteger(value)) {
    throw new TypeError(`${name} must be an explicit non-negative integer`);
  }
  return value;
}

export function businessStorageBaselineConfig(environment = process.env) {
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedDatabase: requiredText(
      environment.EXPECTED_BUSINESS_DATABASE,
      "EXPECTED_BUSINESS_DATABASE",
    ),
    expectedBusinessChannelCount: explicitCount(
      environment,
      "EXPECTED_BUSINESS_CHANNEL_COUNT",
    ),
  };
}

const IDENTITY_SQL = `
  /* business-storage-baseline:identity */
  SELECT current_database() AS database_name,
         current_user AS database_user,
         identity.database_kind AS identity_kind,
         identity.database_name AS identity_database,
         (SELECT count(*)::int FROM public.channels) AS channel_count,
         pg_database_size(current_database())::bigint AS database_bytes,
         pg_current_wal_lsn()::text AS wal_lsn,
         stats.stats_reset
  FROM publication.database_identity identity
  JOIN pg_stat_database stats ON stats.datname=current_database()
  WHERE identity.singleton=true
`;

const RELATIONS_SQL = `
  /* business-storage-baseline:relations */
  SELECT namespace.nspname AS schema_name,
         relation.relname AS relation_name,
         pg_relation_size(relation.oid)::bigint AS heap_bytes,
         CASE WHEN relation.reltoastrelid=0 THEN 0
              ELSE pg_total_relation_size(relation.reltoastrelid) END::bigint
           AS toast_bytes,
         pg_indexes_size(relation.oid)::bigint AS index_bytes,
         pg_total_relation_size(relation.oid)::bigint AS total_bytes,
         COALESCE(stats.n_live_tup,0)::bigint AS estimated_live_rows,
         COALESCE(stats.n_dead_tup,0)::bigint AS estimated_dead_rows,
         stats.last_vacuum,stats.last_autovacuum,
         stats.last_analyze,stats.last_autoanalyze
  FROM pg_class relation
  JOIN pg_namespace namespace ON namespace.oid=relation.relnamespace
  LEFT JOIN pg_stat_user_tables stats ON stats.relid=relation.oid
  WHERE namespace.nspname IN ('public','publication','result')
    AND relation.relkind IN ('r','p')
  ORDER BY namespace.nspname,relation.relname
`;

const INDEXES_SQL = `
  /* business-storage-baseline:indexes */
  SELECT table_namespace.nspname AS schema_name,
         table_relation.relname AS relation_name,
         index_relation.relname AS index_name,
         pg_relation_size(index_relation.oid)::bigint AS index_bytes,
         COALESCE(stats.idx_scan,0)::bigint AS idx_scan,
         COALESCE(stats.idx_tup_read,0)::bigint AS idx_tup_read,
         COALESCE(stats.idx_tup_fetch,0)::bigint AS idx_tup_fetch,
         definition.indisprimary AS is_primary,
         definition.indisunique AS is_unique,
         pg_get_indexdef(index_relation.oid) AS index_definition
  FROM pg_index definition
  JOIN pg_class index_relation ON index_relation.oid=definition.indexrelid
  JOIN pg_class table_relation ON table_relation.oid=definition.indrelid
  JOIN pg_namespace table_namespace ON table_namespace.oid=table_relation.relnamespace
  LEFT JOIN pg_stat_user_indexes stats ON stats.indexrelid=index_relation.oid
  WHERE table_namespace.nspname IN ('public','publication','result')
  ORDER BY table_namespace.nspname,table_relation.relname,index_relation.relname
`;

const SEARCH_SQL = `
  /* business-storage-baseline:search */
  SELECT storage.write_mode,storage.read_mode,
         active.watermark AS active_watermark,
         (SELECT count(*)::bigint FROM public.creator_search_live) AS live_rows,
         (SELECT count(*)::bigint FROM public.creator_search_current search
          WHERE search.watermark=active.watermark) AS active_legacy_rows,
         (SELECT count(*)::bigint FROM public.creator_search_current) AS legacy_rows,
         (SELECT count(*)::bigint FROM publication.creator_search_changes) AS change_rows,
         (SELECT count(*)::bigint FROM public.creator_search_releases) AS release_rows,
         (SELECT COALESCE(sum(changed_channel_count),0)::bigint
          FROM public.creator_search_releases) AS cumulative_changed_channels
  FROM publication.creator_search_storage_state storage
  LEFT JOIN public.creator_search_active active ON active.singleton=true
  WHERE storage.singleton=true
`;

const ENTITIES_SQL = `
  /* business-storage-baseline:entities */
  SELECT 'channels' AS entity_name,count(*)::bigint AS row_count FROM public.channels
  UNION ALL
  SELECT 'content_items',count(*)::bigint FROM public.content_items
  UNION ALL
  SELECT 'channel_snapshots',count(*)::bigint FROM public.channel_snapshots
  UNION ALL
  SELECT 'content_snapshots',count(*)::bigint FROM public.content_snapshots
  UNION ALL
  SELECT 'inbox_receipts',count(*)::bigint FROM publication.inbox
  UNION ALL
  SELECT 'revisions',count(*)::bigint FROM publication.revision
  UNION ALL
  SELECT 'result_entity_current',count(*)::bigint FROM result.entity_current
  UNION ALL
  SELECT 'result_content_current',count(*)::bigint FROM result.content_current
  UNION ALL
  SELECT 'result_agent_current',count(*)::bigint FROM result.agent_current
  UNION ALL
  SELECT 'projection_outbox',count(*)::bigint FROM publication.projection_outbox
  ORDER BY entity_name
`;

const SNAPSHOTS_SQL = `
  /* business-storage-baseline:snapshots */
  WITH content_versions AS (
    SELECT video_id,count(*)::bigint AS version_count
    FROM public.content_snapshots
    GROUP BY video_id
  ), version_summary AS (
    SELECT count(*)::bigint AS content_count,
           COALESCE(sum(version_count),0)::bigint AS snapshot_rows,
           percentile_disc(0.50) WITHIN GROUP (ORDER BY version_count)::bigint AS versions_p50,
           percentile_disc(0.95) WITHIN GROUP (ORDER BY version_count)::bigint AS versions_p95,
           percentile_disc(0.99) WITHIN GROUP (ORDER BY version_count)::bigint AS versions_p99,
           max(version_count)::bigint AS versions_max
    FROM content_versions
  ), content_raw AS (
    SELECT COALESCE(sum(octet_length(raw_item::text)),0)::bigint
             AS raw_item_logical_bytes,
           COALESCE(sum(pg_column_size(raw_item)),0)::bigint
             AS raw_item_stored_bytes
    FROM public.content_snapshots
  ), channel_raw AS (
    SELECT COALESCE(sum(octet_length(raw_channel::text)),0)::bigint
             AS raw_channel_logical_bytes,
           COALESCE(sum(pg_column_size(raw_channel)),0)::bigint
             AS raw_channel_stored_bytes
    FROM public.channel_snapshots
  )
  SELECT version_summary.*,content_raw.*,channel_raw.*
  FROM version_summary CROSS JOIN content_raw CROSS JOIN channel_raw
`;

const PROJECTION_SQL = `
  /* business-storage-baseline:projection */
  SELECT status,count(*)::bigint AS row_count,
         COALESCE(max(attempts),0)::bigint AS maximum_attempts
  FROM publication.projection_outbox
  GROUP BY status
  ORDER BY status
`;

const PROJECTION_BATCHES_SQL = `
  /* business-storage-baseline:projection-batches */
  WITH batches AS (
    SELECT status,projection_count,
           CASE WHEN published_at IS NULL THEN NULL ELSE
             round(extract(epoch FROM (published_at-created_at))*1000)::bigint
           END AS duration_ms
    FROM publication.projection_batch
  )
  SELECT status,count(*)::bigint AS batch_count,
         COALESCE(sum(projection_count),0)::bigint AS projected_channel_count,
         percentile_disc(0.50) WITHIN GROUP (ORDER BY duration_ms)
           FILTER (WHERE duration_ms IS NOT NULL) AS duration_ms_p50,
         percentile_disc(0.95) WITHIN GROUP (ORDER BY duration_ms)
           FILTER (WHERE duration_ms IS NOT NULL) AS duration_ms_p95,
         percentile_disc(0.99) WITHIN GROUP (ORDER BY duration_ms)
           FILTER (WHERE duration_ms IS NOT NULL) AS duration_ms_p99,
         max(duration_ms) AS duration_ms_max
  FROM batches
  GROUP BY status
  ORDER BY status
`;

function number(value, field, { nullable = false } = {}) {
  if (nullable && value == null) return null;
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < 0) {
    throw new Error(`Business storage baseline returned invalid ${field}`);
  }
  return output;
}

function timestamp(value) {
  if (value == null) return null;
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return String(value);
  return parsed.toISOString();
}

function normalizeIdentity(row, config) {
  if (!row) throw new Error("Business database identity is missing");
  if (row.database_name !== config.expectedDatabase) {
    throw new Error(
      `refusing unexpected Business database ${row.database_name || "unknown"}; expected ${config.expectedDatabase}`,
    );
  }
  if (row.identity_kind !== "business" || row.identity_database !== row.database_name) {
    throw new Error("Business database identity marker is missing or mismatched");
  }
  const channelCount = number(row.channel_count, "Channel count");
  if (channelCount !== config.expectedBusinessChannelCount) {
    throw new Error(`Business Channel count changed: ${channelCount}`);
  }
  return {
    ...row,
    channel_count: channelCount,
    database_bytes: number(row.database_bytes, "database size"),
    stats_reset: timestamp(row.stats_reset),
  };
}

function normalizeRelation(row) {
  return {
    ...row,
    heap_bytes: number(row.heap_bytes, "relation heap size"),
    toast_bytes: number(row.toast_bytes, "relation TOAST size"),
    index_bytes: number(row.index_bytes, "relation index size"),
    total_bytes: number(row.total_bytes, "relation total size"),
    estimated_live_rows: number(row.estimated_live_rows, "estimated live rows"),
    estimated_dead_rows: number(row.estimated_dead_rows, "estimated dead rows"),
    last_vacuum: timestamp(row.last_vacuum),
    last_autovacuum: timestamp(row.last_autovacuum),
    last_analyze: timestamp(row.last_analyze),
    last_autoanalyze: timestamp(row.last_autoanalyze),
  };
}

function normalizeIndex(row) {
  return {
    ...row,
    index_bytes: number(row.index_bytes, "index size"),
    idx_scan: number(row.idx_scan, "index scan count"),
    idx_tup_read: number(row.idx_tup_read, "index tuple read count"),
    idx_tup_fetch: number(row.idx_tup_fetch, "index tuple fetch count"),
  };
}

function normalizeSearch(row) {
  if (!row) throw new Error("Creator Search storage state is missing");
  const fields = [
    "live_rows",
    "active_legacy_rows",
    "legacy_rows",
    "change_rows",
    "release_rows",
    "cumulative_changed_channels",
  ];
  return Object.fromEntries(Object.entries(row).map(([key, value]) => (
    [key, fields.includes(key) ? number(value, `Search ${key}`) : value]
  )));
}

function normalizeSnapshots(row) {
  if (!row) throw new Error("Snapshot capacity state is missing");
  const nullable = new Set(["versions_p50", "versions_p95", "versions_p99", "versions_max"]);
  return Object.fromEntries(Object.entries(row).map(([key, value]) => (
    [key, number(value, `Snapshot ${key}`, { nullable: nullable.has(key) })]
  )));
}

export class BusinessStorageBaselineReporter {
  constructor({ pool, config }) {
    if (!pool?.connect) throw new TypeError("a PostgreSQL Pool is required");
    this.pool = pool;
    this.config = config;
  }

  async capture() {
    const client = await this.pool.connect();
    let began = false;
    try {
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      began = true;
      await client.query("SET LOCAL lock_timeout='10s'");
      await client.query("SET LOCAL statement_timeout='600s'");
      const database = normalizeIdentity(
        (await client.query(IDENTITY_SQL)).rows[0],
        this.config,
      );
      const relations = (await client.query(RELATIONS_SQL)).rows.map(normalizeRelation);
      const indexes = (await client.query(INDEXES_SQL)).rows.map(normalizeIndex);
      const search = normalizeSearch((await client.query(SEARCH_SQL)).rows[0]);
      const entities = Object.fromEntries((await client.query(ENTITIES_SQL)).rows.map((row) => (
        [row.entity_name, number(row.row_count, `entity ${row.entity_name} row count`)]
      )));
      const snapshots = normalizeSnapshots((await client.query(SNAPSHOTS_SQL)).rows[0]);
      const projection = (await client.query(PROJECTION_SQL)).rows.map((row) => ({
        status: row.status,
        row_count: number(row.row_count, "Projection row count"),
        maximum_attempts: number(row.maximum_attempts, "Projection maximum attempts"),
      }));
      const projectionBatches = (await client.query(PROJECTION_BATCHES_SQL)).rows.map((row) => ({
        status: row.status,
        batch_count: number(row.batch_count, "Projection batch count"),
        projected_channel_count: number(
          row.projected_channel_count,
          "projected Channel count",
        ),
        duration_ms_p50: number(row.duration_ms_p50, "Projection p50 duration", {
          nullable: true,
        }),
        duration_ms_p95: number(row.duration_ms_p95, "Projection p95 duration", {
          nullable: true,
        }),
        duration_ms_p99: number(row.duration_ms_p99, "Projection p99 duration", {
          nullable: true,
        }),
        duration_ms_max: number(row.duration_ms_max, "Projection maximum duration", {
          nullable: true,
        }),
      }));
      await client.query("ROLLBACK");
      began = false;
      return {
        report_format: "business-storage-baseline-v1",
        captured_at: new Date().toISOString(),
        transaction_snapshot: "repeatable_read_read_only",
        database,
        relations,
        indexes,
        search,
        entities,
        snapshots,
        projection,
        projection_batches: projectionBatches,
      };
    } catch (error) {
      if (began) await client.query("ROLLBACK").catch(() => {});
      throw error;
    } finally {
      client.release();
    }
  }
}
