import {
  migrationIsWorkStatus,
  migrationLifecycleState,
} from "./migrationCompletion.js";

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new Error(`${name} is required`);
  return normalized;
}

export function assertCrawlerDashboardIdentity(row, {
  expectedDatabase,
  forbiddenDatabase = "bullmq_crawler_migration",
} = {}) {
  const expected = requiredText(expectedDatabase, "EXPECTED_CRAWLER_DATABASE");
  const forbidden = requiredText(forbiddenDatabase, "FORBIDDEN_CRAWLER_DATABASE");
  const database = String(row?.database_name ?? "").trim();
  if (database === forbidden) throw new Error(`refusing forbidden Crawler database ${database}`);
  if (database !== expected) {
    throw new Error(`refusing unexpected Crawler database ${database || "unknown"}; expected ${expected}`);
  }
  if (row?.transaction_read_only !== "off") {
    throw new Error("Dashboard Crawler database must be writable");
  }
  if (row?.identity_kind !== "crawler" || row?.identity_database !== database) {
    throw new Error(`refusing uninitialized or mismatched Crawler database ${database}`);
  }
  return { database, user: requiredText(row?.database_user, "Crawler database user") };
}

export function assertMigrationSourceIdentity(row, {
  expectedDatabase,
  expectedDatabaseOid,
  expectedUser,
  targetDatabase,
} = {}) {
  const expected = requiredText(expectedDatabase, "EXPECTED_MIGRATION_DATABASE");
  const expectedOid = requiredText(expectedDatabaseOid, "EXPECTED_MIGRATION_DATABASE_OID");
  const expectedRole = requiredText(expectedUser, "EXPECTED_MIGRATION_DATABASE_USER");
  const target = requiredText(targetDatabase, "EXPECTED_CRAWLER_DATABASE");
  if (expected === target) throw new Error("Migration Source and Target databases must differ");
  const database = String(row?.database_name ?? "").trim();
  const databaseOid = String(row?.database_oid ?? "").trim();
  const user = String(row?.database_user ?? "").trim();
  if (database !== expected) throw new Error(`unexpected Migration Source database ${database}`);
  if (databaseOid !== expectedOid) throw new Error(`unexpected Migration Source database OID ${databaseOid}`);
  if (user !== expectedRole) throw new Error(`unexpected Migration Source database user ${user}`);
  if (row?.transaction_read_only !== "on") throw new Error("Migration Source transaction is not read-only");
  if (row?.default_transaction_read_only !== "on") {
    throw new Error("Migration Source role must default to read-only");
  }
  if (row?.candidates_ready !== true || row?.channels_ready !== true) {
    throw new Error("Migration Source schema is not ready");
  }
  if (row?.candidate_write !== false || row?.channel_write !== false) {
    throw new Error("Migration Source role must not have write privileges on source tables");
  }
  return { database, databaseOid, user };
}

const MIGRATION_SOURCE_PENDING_SQL = "('discovered','queued','validating','failed')";

function boundedInteger(value, name, { min, max }) {
  const parsed = Number(value);
  if (!Number.isInteger(parsed) || parsed < min || parsed > max) {
    throw new TypeError(`${name} must be an integer between ${min} and ${max}`);
  }
  return parsed;
}

function migrationSourceSearchWhere(search, args) {
  const where = [];
  const normalizedSearch = String(search || "").trim();
  if (normalizedSearch) {
    args.push(`%${normalizedSearch}%`);
    where.push(`(
      candidate.channel_id ILIKE $${args.length}
      OR candidate.channel_url ILIKE $${args.length}
      OR candidate.handle ILIKE $${args.length}
      OR candidate.title ILIKE $${args.length}
    )`);
  }
  return where.length > 0 ? where.join(" AND ") : "TRUE";
}

function rankedMigrationSourceSql() {
  return `SELECT candidate.candidate_id,candidate.channel_id,candidate.channel_url,
                 candidate.handle,candidate.title,candidate.priority,
                 candidate.status AS source_candidate_status,
                 row_number() OVER (
                   PARTITION BY candidate.channel_id
                   ORDER BY candidate.priority DESC,candidate.candidate_id DESC
                 ) AS channel_rank
          FROM crawler.channel_candidates candidate
          WHERE candidate.source_json->>'source'='legacy_results_db'`;
}

export async function loadMigrationSourcePage({
  read,
  search = "",
  limit = 500,
  offset = 0,
} = {}) {
  if (typeof read !== "function") throw new TypeError("read is required");
  const normalizedLimit = boundedInteger(limit, "limit", { min: 1, max: 500 });
  const normalizedOffset = boundedInteger(offset, "offset", { min: 0, max: 1_000_000 });
  const args = [];
  const searchSql = migrationSourceSearchWhere(search, args);
  args.push(normalizedLimit, normalizedOffset);
  const limitParameter = args.length - 1;
  const offsetParameter = args.length;
  const result = await read(`WITH ranked_source AS (
      ${rankedMigrationSourceSql()}
    ), source_page AS (
      SELECT candidate_id,priority
      FROM ranked_source candidate
      WHERE candidate.channel_rank=1
        AND candidate.source_candidate_status IN ${MIGRATION_SOURCE_PENDING_SQL}
        AND ${searchSql}
      ORDER BY priority DESC,candidate_id
      LIMIT $${limitParameter}::int OFFSET $${offsetParameter}::int
    )
    SELECT
      candidate.candidate_id,candidate.dispatch_batch_id,candidate.channel_id,
      candidate.channel_url,candidate.handle,candidate.title,candidate.avatar_url,
      candidate.search_subscriber_count,candidate.status AS source_candidate_status,
      candidate.snapshot_json,candidate.source_json,
      candidate.source_json #>> '{legacy_import,country}' AS legacy_country,
      candidate.source_json #>> '{legacy_import,target_reason}' AS legacy_target_reason,
      candidate.source_json #>> '{legacy_import,br_evidence_score}' AS legacy_evidence_score,
      candidate.source_json #>> '{legacy_import,br_evidence_reasons}' AS legacy_evidence_reasons,
      candidate.source_json #>> '{legacy_import,discovered_at}' AS legacy_discovered_at,
      candidate.source_json->>'source_rowid' AS source_rowid,
      candidate.created_at,candidate.updated_at
    FROM source_page page
    JOIN crawler.channel_candidates candidate ON candidate.candidate_id=page.candidate_id
    ORDER BY page.priority DESC,page.candidate_id`, args);
  return result.rows;
}

export async function loadMigrationSourceCount({ read, search = "" } = {}) {
  if (typeof read !== "function") throw new TypeError("read is required");
  const args = [];
  const searchSql = migrationSourceSearchWhere(search, args);
  const result = await read(`WITH ranked_source AS (
      ${rankedMigrationSourceSql()}
    )
    SELECT count(*)::int AS total
    FROM ranked_source candidate
    WHERE candidate.channel_rank=1
      AND candidate.source_candidate_status IN ${MIGRATION_SOURCE_PENDING_SQL}
      AND ${searchSql}
  `, args);
  return Number(result.rows[0]?.total || 0);
}

export function mergeMigrationCandidate(source, target) {
  const targetState = target || {};
  const targetCandidateStatus = String(targetState.target_candidate_status || "").trim();
  const finalStatus = String(targetState.final_status || "pending");
  const lifecycle = migrationLifecycleState({
    candidateId: targetState.target_candidate_id,
    candidateStatus: targetCandidateStatus,
    channelStatus: targetState.target_channel_status,
    promotionCandidateId: targetState.registry_promotion_candidate_id,
    finalizedStatus: finalStatus,
  });
  return {
    ...source,
    candidate_id: source.candidate_id,
    source_candidate_id: source.candidate_id,
    target_candidate_id: targetState.target_candidate_id ?? null,
    migration_intent_id: targetState.migration_intent_id ?? null,
    channel_url: source.channel_url,
    handle: source.handle || "",
    title: source.title || "",
    avatar_url: source.snapshot_json?.channel_header?.avatar_url || source.avatar_url || null,
    subscriber_count: source.search_subscriber_count ?? null,
    candidate_status: lifecycle.candidateStatus,
    status: lifecycle.status,
    reject_reason: targetState.target_reject_reason ?? null,
    agent_status: targetState.agent_status || "pending",
    latest_run_id: targetState.latest_run_id ?? null,
    final_status: finalStatus,
    quality_json: targetState.quality_json || {},
    run_status: targetState.run_status ?? null,
    run_detail_status: targetState.run_detail_status ?? null,
    is_candidate_only: !targetState.target_channel_status,
    migration_incomplete: lifecycle.migrationIncomplete,
    migration_started: Boolean(targetState.migration_intent_id),
    migration_done: lifecycle.migrationDone,
    content_count: targetState.content_count ?? 0,
    video_count: targetState.video_count ?? 0,
    short_count: targetState.short_count ?? 0,
    live_count: targetState.live_count ?? 0,
    updated_at: targetState.updated_at || source.updated_at,
  };
}

export function mergeMigrationCandidates(sourceRows, targetRows) {
  const bySourceCandidate = new Map();
  const byChannel = new Map();
  for (const target of targetRows) {
    bySourceCandidate.set(String(target.source_candidate_id), target);
    byChannel.set(String(target.channel_id), target);
  }
  return sourceRows.map((source) => mergeMigrationCandidate(
    source,
    bySourceCandidate.get(String(source.candidate_id))
      || byChannel.get(String(source.channel_id))
      || null,
  ));
}

export function migrationCandidateMatches(row, {
  channelStatus = "all",
  agentStatus = "",
  finalStatus = "",
} = {}) {
  return migrationIsWorkStatus(row.candidate_status)
    && (channelStatus === "all" || row.candidate_status === channelStatus)
    && (!agentStatus || row.agent_status === agentStatus)
    && (!finalStatus || row.final_status === finalStatus);
}

export function migrationFiltersIncludeUnstarted(filters = {}) {
  return migrationCandidateMatches({
    candidate_status: "discovered",
    agent_status: "pending",
    final_status: "pending",
  }, filters);
}

export function migrationSourceCandidateFromIntent(row) {
  const snapshot = row?.source_snapshot && typeof row.source_snapshot === "object"
    ? row.source_snapshot
    : {};
  const sourceJson = snapshot.source_json && typeof snapshot.source_json === "object"
    ? snapshot.source_json
    : {};
  const legacyImport = sourceJson.legacy_import && typeof sourceJson.legacy_import === "object"
    ? sourceJson.legacy_import
    : {};
  return {
    candidate_id: row.source_candidate_id ?? snapshot.source_candidate_id,
    dispatch_batch_id: snapshot.source_dispatch_batch_id ?? null,
    channel_id: row.channel_id ?? snapshot.channel_id,
    channel_url: snapshot.channel_url,
    handle: snapshot.handle ?? "",
    title: snapshot.title ?? "",
    avatar_url: snapshot.avatar_url ?? null,
    search_subscriber_count: snapshot.search_subscriber_count ?? null,
    source_candidate_status: snapshot.source_candidate_status ?? null,
    snapshot_json: snapshot.snapshot_json ?? {},
    source_json: sourceJson,
    legacy_country: legacyImport.country ?? null,
    legacy_target_reason: legacyImport.target_reason ?? null,
    legacy_evidence_score: legacyImport.br_evidence_score ?? null,
    legacy_evidence_reasons: legacyImport.br_evidence_reasons ?? null,
    legacy_discovered_at: legacyImport.discovered_at ?? null,
    source_rowid: sourceJson.source_rowid ?? null,
    created_at: snapshot.source_created_at ?? null,
    updated_at: snapshot.source_updated_at ?? null,
  };
}

export function migrationReadModelStatsFromSummary(sourceTotal, summary = {}) {
  const total = Number(sourceTotal || 0);
  const started = Number(summary.started || 0);
  const discovered = Number(summary.discovered || 0);
  const queued = Number(summary.queued || 0);
  const validating = Number(summary.validating || 0);
  const finishing = Number(summary.finishing || 0);
  const failed = Number(summary.failed || 0);
  return {
    total: Math.max(0, total - started) + discovered + queued + validating + finishing + failed,
    discovered: Math.max(0, total - started) + discovered,
    queued,
    validating,
    finishing,
    failed,
    migration_done: Number(summary.migration_done || 0),
    final_done: Number(summary.final_done || 0),
  };
}
