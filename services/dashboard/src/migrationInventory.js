import {
  MIGRATION_WORK_STATUSES,
  MIGRATION_WORK_STATUSES_SQL,
  migrationCandidateStatusSql,
  migrationDisplayStatusSql,
  migrationDoneSql,
  migrationIncompleteSql,
} from "./migrationCompletion.js";

const MIGRATION_CHANNEL_STATUSES = new Set([
  "all",
  ...MIGRATION_WORK_STATUSES,
]);

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${name} is required`);
  return normalized;
}

function boundedInteger(value, fallback, name, { min, max }) {
  const normalized = String(value ?? "").trim();
  const parsed = normalized ? Number(normalized) : fallback;
  if (!Number.isSafeInteger(parsed) || parsed < min || parsed > max) {
    throw new TypeError(`${name} must be an integer between ${min} and ${max}`);
  }
  return parsed;
}

function normalizeFilters(filters = {}) {
  const channelStatus = String(filters.channelStatus || "all").trim();
  if (!MIGRATION_CHANNEL_STATUSES.has(channelStatus)) {
    throw new TypeError(`unsupported Migration channel status ${channelStatus}`);
  }
  return {
    search: String(filters.search || "").trim(),
    channelStatus,
    agentStatus: String(filters.agentStatus || "").trim(),
    finalStatus: String(filters.finalStatus || "").trim(),
    limit: boundedInteger(filters.limit, 50, "limit", { min: 1, max: 50 }),
    offset: boundedInteger(filters.offset, 0, "offset", { min: 0, max: 1_000_000 }),
  };
}

function inventoryStateCte() {
  const candidateStatusSql = migrationCandidateStatusSql();
  const displayStatusSql = migrationDisplayStatusSql();
  const migrationIncomplete = migrationIncompleteSql();
  const migrationDone = migrationDoneSql();
  return `inventory_state AS (
    SELECT inventory.source_candidate_id,inventory.channel_id,
           inventory.channel_url,inventory.handle,inventory.title,inventory.avatar_url,
           inventory.search_subscriber_count,inventory.priority,
           inventory.source_candidate_status,inventory.source_updated_at,
           intent.migration_intent_id,intent.target_candidate_id,
           system_retry.system_retry_id AS active_system_retry_id,
           candidate.status AS target_candidate_status,
           channel.status AS target_channel_status,
           channel.registry_promotion_candidate_id,
           channel.reject_reason AS target_reject_reason,
           COALESCE(channel.agent_status,'pending') AS agent_status,
           channel.latest_run_id,
           COALESCE(finalized.status,'pending') AS final_status,
           COALESCE(finalized.quality_json,'{}'::jsonb) AS quality_json,
           run.status AS run_status,run.detail_status AS run_detail_status,
           ${candidateStatusSql} AS candidate_status,
           ${displayStatusSql} AS status,
           ${migrationIncomplete} AS migration_incomplete,
           ${migrationDone} AS migration_done,
           GREATEST(
             COALESCE(inventory.source_updated_at,inventory.synced_at),
             COALESCE(intent.updated_at,inventory.source_updated_at,inventory.synced_at),
             COALESCE(candidate.updated_at,intent.updated_at,inventory.source_updated_at,inventory.synced_at),
             COALESCE(channel.updated_at,intent.updated_at,inventory.source_updated_at,inventory.synced_at)
           ) AS updated_at
    FROM crawler.migration_channel_inventory inventory
    LEFT JOIN crawler.migration_channel_intents intent
      ON intent.source_id=inventory.source_id
     AND intent.channel_id=inventory.channel_id
    LEFT JOIN crawler.channel_candidates candidate
      ON candidate.candidate_id=intent.target_candidate_id
    LEFT JOIN crawler.migration_system_retry_items system_retry
      ON system_retry.candidate_id=intent.target_candidate_id
     AND system_retry.status IN ('retrying','pending','dispatched')
    LEFT JOIN crawler.channels channel ON channel.channel_id=intent.channel_id
    LEFT JOIN crawler.channel_runs run ON run.run_id=channel.latest_run_id
    LEFT JOIN crawler.finalized_profiles finalized ON finalized.channel_id=intent.channel_id
    WHERE inventory.source_id=$1
  )`;
}

function searchClause(args, search) {
  if (!search) return "TRUE";
  args.push(`%${search}%`);
  return `(state.channel_id ILIKE $${args.length}
    OR state.channel_url ILIKE $${args.length}
    OR state.handle ILIKE $${args.length}
    OR state.title ILIKE $${args.length})`;
}

function filterClause(args, filters) {
  const where = [];
  if (filters.channelStatus === "all") {
    where.push(`state.candidate_status IN ${MIGRATION_WORK_STATUSES_SQL}`);
  } else {
    args.push(filters.channelStatus);
    where.push(`state.candidate_status=$${args.length}`);
  }
  if (filters.agentStatus) {
    args.push(filters.agentStatus);
    where.push(`state.agent_status=$${args.length}`);
  }
  if (filters.finalStatus) {
    args.push(filters.finalStatus);
    where.push(`state.final_status=$${args.length}`);
  }
  return where.join(" AND ");
}

function inventorySummaryQuery(sourceId, filters) {
  const args = [sourceId];
  const searchSql = searchClause(args, filters.search);
  const filterSql = filterClause(args, filters);
  return {
    params: args,
    sql: `WITH ${inventoryStateCte()}, searched_state AS (
      SELECT state.*
      FROM inventory_state state
      WHERE ${searchSql}
    )
    SELECT
      count(*) FILTER (
        WHERE state.candidate_status IN ${MIGRATION_WORK_STATUSES_SQL}
      )::bigint AS total,
      count(*) FILTER (WHERE state.candidate_status='discovered')::bigint AS discovered,
      count(*) FILTER (WHERE state.candidate_status='queued')::bigint AS queued,
      count(*) FILTER (WHERE state.candidate_status='validating')::bigint AS validating,
      count(*) FILTER (WHERE state.candidate_status='finishing')::bigint AS finishing,
      count(*) FILTER (WHERE state.candidate_status='failed')::bigint AS failed,
      count(*) FILTER (WHERE state.migration_done)::bigint AS migration_done,
      count(*) FILTER (
        WHERE state.final_status IN ('ready_auto','ready_partial')
      )::bigint AS final_done,
      count(*) FILTER (WHERE ${filterSql})::bigint AS filtered_count
    FROM searched_state state`,
  };
}

function inventoryPageQuery(sourceId, filters) {
  const args = [sourceId];
  const searchSql = searchClause(args, filters.search);
  const filterSql = filterClause(args, filters);
  args.push(filters.limit, filters.offset);
  const limitParameter = args.length - 1;
  const offsetParameter = args.length;
  return {
    params: args,
    sql: `WITH ${inventoryStateCte()}, filtered_page AS (
      SELECT state.*
      FROM inventory_state state
      WHERE ${searchSql} AND ${filterSql}
      ORDER BY state.priority DESC,state.source_candidate_id ASC
      LIMIT $${limitParameter}::int OFFSET $${offsetParameter}::int
    )
    SELECT page.source_candidate_id::text AS candidate_id,
           page.source_candidate_id::text AS source_candidate_id,
           page.target_candidate_id,page.migration_intent_id,page.active_system_retry_id,
           page.channel_id,page.channel_url,COALESCE(page.handle,'') AS handle,
           COALESCE(page.title,'') AS title,page.avatar_url,
           page.search_subscriber_count,
           page.search_subscriber_count AS subscriber_count,
           page.source_candidate_status,page.candidate_status,page.status,
           page.target_reject_reason AS reject_reason,page.agent_status,
           page.latest_run_id,page.final_status,page.quality_json,
           page.run_status,page.run_detail_status,
           page.target_channel_status IS NULL AS is_candidate_only,
           page.migration_incomplete,
           page.migration_intent_id IS NOT NULL AS migration_started,
           page.migration_done,
           COALESCE(content_stats.content_count,0)::bigint AS content_count,
           COALESCE(content_stats.video_count,0)::bigint AS video_count,
           COALESCE(content_stats.short_count,0)::bigint AS short_count,
           COALESCE(content_stats.live_count,0)::bigint AS live_count,
           page.updated_at
    FROM filtered_page page
    LEFT JOIN LATERAL (
      SELECT count(*)::bigint AS content_count,
             count(*) FILTER (WHERE content.content_type='video')::bigint AS video_count,
             count(*) FILTER (WHERE content.content_type='short')::bigint AS short_count,
             count(*) FILTER (WHERE content.content_type='live')::bigint AS live_count
      FROM crawler.contents content
      WHERE content.channel_id=page.channel_id
        AND content.run_id=page.latest_run_id
    ) content_stats ON true
    ORDER BY page.priority DESC,page.source_candidate_id ASC`,
  };
}

function numericStats(row = {}) {
  return {
    total: Number(row.total || 0),
    discovered: Number(row.discovered || 0),
    queued: Number(row.queued || 0),
    validating: Number(row.validating || 0),
    finishing: Number(row.finishing || 0),
    failed: Number(row.failed || 0),
    migration_done: Number(row.migration_done || 0),
    final_done: Number(row.final_done || 0),
  };
}

export async function loadMigrationChannelInventory({
  read,
  sourceId,
  expectedSourceDatabase,
  expectedSourceDatabaseOid,
  filters = {},
} = {}) {
  if (typeof read !== "function") throw new TypeError("Target read is required");
  const normalizedSourceId = requiredText(sourceId, "Migration source_id");
  const normalizedSourceDatabase = requiredText(
    expectedSourceDatabase,
    "Expected Migration Source database",
  );
  const normalizedSourceDatabaseOid = requiredText(
    expectedSourceDatabaseOid,
    "Expected Migration Source database OID",
  );
  const normalizedFilters = normalizeFilters(filters);
  const syncResult = await read(
    `SELECT source_database,source_database_oid::text,status,
            eligible_count::text,completed_at,last_error
     FROM crawler.migration_channel_inventory_syncs
     WHERE source_id=$1`,
    [normalizedSourceId],
  );
  const sync = syncResult.rows[0];
  const sourceIdentityMatches = sync?.source_database === normalizedSourceDatabase
    && String(sync?.source_database_oid || "") === normalizedSourceDatabaseOid;
  if (sync?.status !== "ready" || !sourceIdentityMatches) {
    const state = sync?.status === "ready" && !sourceIdentityMatches
      ? "source_identity_mismatch"
      : String(sync?.status || "missing");
    const error = new Error(`Migration channel inventory is ${state}`);
    error.code = "migration_inventory_not_ready";
    error.inventoryStatus = state;
    throw error;
  }

  const summaryQuery = inventorySummaryQuery(normalizedSourceId, normalizedFilters);
  const pageQuery = inventoryPageQuery(normalizedSourceId, normalizedFilters);
  const [summaryResult, pageResult] = await Promise.all([
    read(summaryQuery.sql, summaryQuery.params),
    read(pageQuery.sql, pageQuery.params),
  ]);
  const summary = summaryResult.rows[0] || {};
  return {
    channels: pageResult.rows,
    total: Number(summary.filtered_count || 0),
    stats: numericStats(summary),
    sync: {
      status: sync.status,
      eligible_count: Number(sync.eligible_count || 0),
      completed_at: sync.completed_at ?? null,
    },
    filters: normalizedFilters,
  };
}
