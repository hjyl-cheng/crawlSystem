import { createHash } from "node:crypto";
import { environmentValue } from "./runtimeEnvironment.js";
import { verifyMigrationSourceDatabase } from "./databaseIdentity.js";

let defaultPool = null;

export const MIGRATION_SOURCE_PENDING_STATUSES = Object.freeze([
  "discovered",
  "queued",
  "validating",
  "failed",
]);
const MIGRATION_SOURCE_PENDING_SQL = "('discovered','queued','validating','failed')";
const migrationSourcePendingStatuses = new Set(MIGRATION_SOURCE_PENDING_STATUSES);

export function migrationSourceCandidateIsPending(status) {
  return migrationSourcePendingStatuses.has(String(status ?? ""));
}

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${name} is required`);
  return normalized;
}

function positiveInteger(value, fallback, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) return fallback;
  const parsed = Number(normalized);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return parsed;
}

export function migrationSourceRuntimeConfig(environment = process.env) {
  const expectedDatabase = requiredText(
    environment.EXPECTED_MIGRATION_DATABASE,
    "EXPECTED_MIGRATION_DATABASE",
  );
  const expectedDatabaseOid = requiredText(
    environment.EXPECTED_MIGRATION_DATABASE_OID,
    "EXPECTED_MIGRATION_DATABASE_OID",
  );
  if (!/^\d+$/.test(expectedDatabaseOid) || expectedDatabaseOid === "0") {
    throw new TypeError("EXPECTED_MIGRATION_DATABASE_OID must be a positive PostgreSQL OID");
  }
  const expectedUser = requiredText(
    environment.EXPECTED_MIGRATION_DATABASE_USER,
    "EXPECTED_MIGRATION_DATABASE_USER",
  );
  const targetDatabase = requiredText(
    environment.EXPECTED_CRAWLER_DATABASE,
    "EXPECTED_CRAWLER_DATABASE",
  );
  if (expectedDatabase === targetDatabase) {
    throw new Error("Migration Source and Target databases must differ");
  }
  return {
    databaseUrl: environmentValue("MIGRATION_DATABASE_URL", { environment }),
    sourceId: requiredText(environment.MIGRATION_SOURCE_ID, "MIGRATION_SOURCE_ID"),
    expectedDatabase,
    expectedDatabaseOid,
    expectedUser,
    targetDatabase,
    statementTimeoutMs: positiveInteger(
      environment.MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS,
      10000,
      "MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS",
    ),
  };
}

async function migrationSourcePool(config) {
  if (!defaultPool) {
    const pg = await import("pg");
    const Pool = pg.default?.Pool || pg.Pool;
    defaultPool = new Pool({
      connectionString: config.databaseUrl,
      application_name: "newcrawler-migration-source-readonly",
      options: "-c timezone=UTC",
      max: Math.max(1, Number(process.env.MIGRATION_POSTGRES_POOL_MAX || 4)),
    });
  }
  return defaultPool;
}

export async function closeMigrationSourcePool() {
  const pool = defaultPool;
  defaultPool = null;
  if (pool) await pool.end();
}

export async function withMigrationSourceReadTransaction(action, {
  pool = null,
  environment = process.env,
  statementTimeoutMs = null,
} = {}) {
  if (typeof action !== "function") throw new TypeError("Migration Source action is required");
  const config = migrationSourceRuntimeConfig(environment);
  const selectedStatementTimeoutMs = statementTimeoutMs == null
    ? config.statementTimeoutMs
    : positiveInteger(statementTimeoutMs, null, "Migration Source statement timeout");
  const selectedPool = pool || await migrationSourcePool(config);
  const client = await selectedPool.connect();
  try {
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await client.query(`SET LOCAL statement_timeout=${selectedStatementTimeoutMs}`);
    const identity = await verifyMigrationSourceDatabase(client.query.bind(client), environment);
    const result = await action(client, identity, config);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

function canonicalize(value) {
  if (value instanceof Date) return value.toISOString();
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalize);
  return Object.fromEntries(
    Object.keys(value)
      .filter((key) => key !== "snapshot_sha256")
      .sort()
      .map((key) => [key, canonicalize(value[key])]),
  );
}

export function sourceSnapshotHash(snapshot) {
  return createHash("sha256")
    .update(JSON.stringify(canonicalize(snapshot)))
    .digest("hex");
}

function migrationSourceSnapshot(row, identity, config) {
  const snapshot = {
    source_id: config.sourceId,
    source_database: identity.database,
    source_database_oid: identity.databaseOid,
    source_candidate_id: String(row.candidate_id),
    source_candidate_status: requiredText(
      row.source_candidate_status,
      "Migration Source candidate status",
    ),
    source_dispatch_batch_id: row.dispatch_batch_id == null
      ? null
      : String(row.dispatch_batch_id),
    channel_id: requiredText(row.channel_id, "Migration Source channel_id"),
    channel_url: requiredText(row.channel_url, "Migration Source channel_url"),
    handle: row.handle ?? null,
    title: row.title ?? null,
    description: row.description ?? null,
    avatar_url: row.avatar_url ?? null,
    search_subscriber_count: row.search_subscriber_count ?? null,
    search_subscriber_count_text: row.search_subscriber_count_text ?? null,
    is_verified: row.is_verified ?? null,
    priority: Number(row.priority ?? 100),
    snapshot_json: row.snapshot_json ?? {},
    source_json: row.source_json ?? {},
    source_created_at: row.created_at ?? null,
    source_updated_at: row.updated_at ?? null,
  };
  return { ...snapshot, snapshot_sha256: sourceSnapshotHash(snapshot) };
}

export async function loadMigrationSourceChannel({
  channelId,
  candidateId = null,
  pool = null,
  environment = process.env,
} = {}) {
  const normalizedChannelId = requiredText(channelId, "channel_id");
  const normalizedCandidateId = candidateId == null || candidateId === ""
    ? null
    : requiredText(candidateId, "candidate_id");
  return withMigrationSourceReadTransaction(async (client, identity, config) => {
    const result = await client.query(
      `WITH source_candidates AS (
         SELECT candidate_id,dispatch_batch_id,channel_id,channel_url,handle,title,
                description,avatar_url,search_subscriber_count,
                search_subscriber_count_text,is_verified,priority,
                status AS source_candidate_status,snapshot_json,source_json,
                created_at,updated_at,
                row_number() OVER (
                  PARTITION BY channel_id
                  ORDER BY priority DESC,candidate_id DESC
                ) AS channel_rank
         FROM crawler.channel_candidates
         WHERE channel_id=$1
           AND source_json->>'source'='legacy_results_db'
       )
       SELECT candidate_id,dispatch_batch_id,channel_id,channel_url,handle,title,
              description,avatar_url,search_subscriber_count,
              search_subscriber_count_text,is_verified,priority,
              source_candidate_status,snapshot_json,source_json,created_at,updated_at
       FROM source_candidates
       WHERE channel_rank=1
         AND source_candidate_status IN ${MIGRATION_SOURCE_PENDING_SQL}
         AND ($2::bigint IS NULL OR candidate_id=$2::bigint)
       LIMIT 1`,
      [normalizedChannelId, normalizedCandidateId],
    );
    if (result.rows.length !== 1) {
      const error = new Error(`Migration Source candidate not found: ${normalizedChannelId}`);
      error.code = "migration_source_candidate_not_found";
      throw error;
    }
    return migrationSourceSnapshot(result.rows[0], identity, config);
  }, { pool, environment });
}

export async function loadMigrationSourceBatch({
  limit,
  excludeSourceCandidateIds = [],
  excludeChannelIds = [],
  pool = null,
  environment = process.env,
} = {}) {
  const normalizedLimit = positiveInteger(limit, null, "limit");
  if (normalizedLimit > 2000) throw new TypeError("Migration Source batch limit cannot exceed 2000");
  const candidateIds = [...new Set(
    excludeSourceCandidateIds.map((value) => requiredText(value, "source_candidate_id")),
  )];
  const channelIds = [...new Set(
    excludeChannelIds.map((value) => requiredText(value, "channel_id")),
  )];
  return withMigrationSourceReadTransaction(async (client, identity, config) => {
    const result = await client.query(
      `WITH source_candidates AS (
         SELECT candidate_id,channel_id,priority,
                status AS source_candidate_status,
                row_number() OVER (
                  PARTITION BY channel_id
                  ORDER BY priority DESC,candidate_id DESC
                ) AS channel_rank
         FROM crawler.channel_candidates
         WHERE source_json->>'source'='legacy_results_db'
       ), source_page AS (
         SELECT candidate_id,priority
         FROM source_candidates
         WHERE channel_rank=1
           AND source_candidate_status IN ${MIGRATION_SOURCE_PENDING_SQL}
           AND NOT (candidate_id=ANY($1::bigint[]))
           AND NOT (channel_id=ANY($2::text[]))
         ORDER BY priority DESC,candidate_id
         LIMIT $3::int
       )
       SELECT candidate.candidate_id,candidate.dispatch_batch_id,
              candidate.channel_id,candidate.channel_url,candidate.handle,candidate.title,
              candidate.description,candidate.avatar_url,candidate.search_subscriber_count,
              candidate.search_subscriber_count_text,candidate.is_verified,candidate.priority,
              candidate.status AS source_candidate_status,candidate.snapshot_json,
              candidate.source_json,candidate.created_at,candidate.updated_at
       FROM source_page page
       JOIN crawler.channel_candidates candidate ON candidate.candidate_id=page.candidate_id
       ORDER BY page.priority DESC,page.candidate_id`,
      [candidateIds, channelIds, normalizedLimit],
    );
    return result.rows.map((row) => migrationSourceSnapshot(row, identity, config));
  }, { pool, environment });
}
