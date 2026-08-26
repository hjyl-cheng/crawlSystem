import { randomUUID } from "node:crypto";
import {
  migrationSourceRuntimeConfig,
  withMigrationSourceReadTransaction,
} from "./migrationSource.js";

const DEFAULT_BATCH_SIZE = 5000;
const MAX_BATCH_SIZE = 20000;
const DEFAULT_SOURCE_STATEMENT_TIMEOUT_MS = 120000;
const SOURCE_CURSOR = "migration_channel_inventory_source";

function positiveBatchSize(value) {
  const parsed = Number(value ?? DEFAULT_BATCH_SIZE);
  if (!Number.isSafeInteger(parsed) || parsed <= 0 || parsed > MAX_BATCH_SIZE) {
    throw new TypeError(`Migration inventory batch size must be between 1 and ${MAX_BATCH_SIZE}`);
  }
  return parsed;
}

function errorText(error) {
  return String(error?.message || error || "migration inventory sync failed").slice(0, 2000);
}

export function migrationInventorySyncConfigured(environment = process.env) {
  const databaseUrl = String(environment.MIGRATION_DATABASE_URL || "").trim();
  const databaseUrlFile = String(environment.MIGRATION_DATABASE_URL_FILE || "").trim();
  return Boolean(databaseUrl || databaseUrlFile);
}

export function migrationInventoryForceSyncEnabled(environment = process.env) {
  const value = String(environment.MIGRATION_INVENTORY_FORCE_SYNC ?? "")
    .trim()
    .toLowerCase();
  if (!value || value === "false") return false;
  if (value === "true") return true;
  throw new TypeError("MIGRATION_INVENTORY_FORCE_SYNC must be true or false");
}

function sourceStatementTimeoutMs(value) {
  const normalized = String(value ?? "").trim();
  const parsed = normalized ? Number(normalized) : DEFAULT_SOURCE_STATEMENT_TIMEOUT_MS;
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError("MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS must be a positive integer");
  }
  return parsed;
}

function inventoryRecord(row) {
  return {
    source_candidate_id: String(row.candidate_id),
    channel_id: String(row.channel_id),
    channel_url: String(row.channel_url),
    handle: row.handle ?? null,
    title: row.title ?? null,
    avatar_url: row.avatar_url ?? null,
    search_subscriber_count: row.search_subscriber_count ?? null,
    priority: Number(row.priority ?? 100),
    source_candidate_status: String(row.source_candidate_status),
    source_updated_at: row.updated_at ?? null,
  };
}

async function writeInventoryBatch(client, { sourceId, syncToken, rows }) {
  if (rows.length === 0) return;
  await client.query(
    `INSERT INTO crawler.migration_channel_inventory (
       source_id,source_candidate_id,channel_id,channel_url,handle,title,avatar_url,
       search_subscriber_count,priority,source_candidate_status,source_updated_at,
       sync_token,synced_at
     )
     SELECT $1,item.source_candidate_id,item.channel_id,item.channel_url,
            item.handle,item.title,item.avatar_url,item.search_subscriber_count,
            item.priority,item.source_candidate_status,item.source_updated_at,
            $2::uuid,now()
     FROM jsonb_to_recordset($3::jsonb) AS item(
       source_candidate_id bigint,
       channel_id text,
       channel_url text,
       handle text,
       title text,
       avatar_url text,
       search_subscriber_count bigint,
       priority integer,
       source_candidate_status text,
       source_updated_at timestamptz
     )
     ON CONFLICT (source_id,channel_id) DO UPDATE
     SET source_candidate_id=EXCLUDED.source_candidate_id,
         channel_url=EXCLUDED.channel_url,
         handle=EXCLUDED.handle,
         title=EXCLUDED.title,
         avatar_url=EXCLUDED.avatar_url,
         search_subscriber_count=EXCLUDED.search_subscriber_count,
         priority=EXCLUDED.priority,
         source_candidate_status=EXCLUDED.source_candidate_status,
         source_updated_at=EXCLUDED.source_updated_at,
         sync_token=EXCLUDED.sync_token,
         synced_at=now()`,
    [sourceId, syncToken, JSON.stringify(rows.map(inventoryRecord))],
  );
}

async function loadSourceInventory(sourceClient, {
  batchSize,
  onBatch,
}) {
  await sourceClient.query(`DECLARE ${SOURCE_CURSOR} NO SCROLL CURSOR FOR
    WITH ranked_source AS (
      SELECT candidate.candidate_id,candidate.channel_id,candidate.channel_url,
             candidate.handle,candidate.title,candidate.avatar_url,
             candidate.search_subscriber_count,candidate.priority,
             candidate.status AS source_candidate_status,candidate.updated_at,
             row_number() OVER (
               PARTITION BY candidate.channel_id
               ORDER BY candidate.priority DESC,candidate.candidate_id DESC
             ) AS channel_rank
      FROM crawler.channel_candidates candidate
      WHERE candidate.source_json->>'source'='legacy_results_db'
    )
    SELECT candidate_id,channel_id,channel_url,handle,title,avatar_url,
           search_subscriber_count,priority,source_candidate_status,updated_at
    FROM ranked_source
    WHERE channel_rank=1
      AND source_candidate_status IN ('discovered','queued','validating','failed')
    ORDER BY candidate_id`);

  let total = 0;
  while (true) {
    const result = await sourceClient.query(`FETCH FORWARD ${batchSize} FROM ${SOURCE_CURSOR}`);
    if (result.rows.length === 0) break;
    await onBatch(result.rows, total);
    total += result.rows.length;
  }
  await sourceClient.query(`CLOSE ${SOURCE_CURSOR}`);
  return total;
}

export async function syncMigrationChannelInventory({
  targetPool,
  sourcePool = null,
  environment = process.env,
  batchSize = environment.MIGRATION_INVENTORY_SYNC_BATCH_SIZE,
  statementTimeoutMs = environment.MIGRATION_INVENTORY_SOURCE_STATEMENT_TIMEOUT_MS,
  force = false,
  onProgress = () => {},
} = {}) {
  if (!targetPool || typeof targetPool.connect !== "function") {
    throw new TypeError("Target pool is required for Migration inventory sync");
  }
  if (typeof onProgress !== "function") throw new TypeError("onProgress must be a function");

  const normalizedBatchSize = positiveBatchSize(batchSize);
  const normalizedStatementTimeoutMs = sourceStatementTimeoutMs(statementTimeoutMs);
  const config = migrationSourceRuntimeConfig(environment);
  const targetClient = await targetPool.connect();
  const lockKey = `migration-inventory-sync:${config.sourceId}`;
  let syncToken = null;
  let inTransaction = false;
  let current = null;

  try {
    await targetClient.query("BEGIN");
    inTransaction = true;
    await targetClient.query(
      "SELECT pg_advisory_xact_lock(hashtextextended($1,0))",
      [lockKey],
    );

    const existing = await targetClient.query(
      `SELECT source_database,source_database_oid::text,status,eligible_count,
              completed_at,
              (SELECT count(*)::bigint
               FROM crawler.migration_channel_inventory inventory
               WHERE inventory.source_id=sync.source_id) AS inventory_count
       FROM crawler.migration_channel_inventory_syncs sync
       WHERE sync.source_id=$1`,
      [config.sourceId],
    );
    current = existing.rows[0] || null;
    if (!force
      && current?.status === "ready"
      && current.source_database === config.expectedDatabase
      && String(current.source_database_oid) === String(config.expectedDatabaseOid)
      && Number(current.inventory_count) === Number(current.eligible_count)) {
      await targetClient.query("COMMIT");
      inTransaction = false;
      return {
        status: "ready",
        skipped: true,
        source_id: config.sourceId,
        eligible_count: Number(current.eligible_count || 0),
        completed_at: current.completed_at,
      };
    }

    syncToken = randomUUID();
    await targetClient.query(
      `INSERT INTO crawler.migration_channel_inventory_syncs (
         source_id,source_database,source_database_oid,status,sync_token,
         eligible_count,started_at,completed_at,last_error,updated_at
       ) VALUES ($1,$2,$3::oid,'syncing',$4::uuid,0,now(),NULL,NULL,now())
       ON CONFLICT (source_id) DO UPDATE
       SET source_database=EXCLUDED.source_database,
           source_database_oid=EXCLUDED.source_database_oid,
           status='syncing',sync_token=EXCLUDED.sync_token,
           eligible_count=0,started_at=now(),completed_at=NULL,
           last_error=NULL,updated_at=now()`,
      [config.sourceId, config.expectedDatabase, config.expectedDatabaseOid, syncToken],
    );

    const eligibleCount = await withMigrationSourceReadTransaction(
      async (sourceClient, identity) => {
        if (identity.database !== config.expectedDatabase
          || String(identity.databaseOid) !== String(config.expectedDatabaseOid)) {
          throw new Error("Migration inventory Source identity changed during sync");
        }
        return loadSourceInventory(sourceClient, {
          batchSize: normalizedBatchSize,
          onBatch: async (rows, previousCount) => {
            await writeInventoryBatch(targetClient, {
              sourceId: config.sourceId,
              syncToken,
              rows,
            });
            const completed = previousCount + rows.length;
            await onProgress({ source_id: config.sourceId, eligible_count: completed });
          },
        });
      },
      {
        pool: sourcePool,
        environment,
        statementTimeoutMs: normalizedStatementTimeoutMs,
      },
    );

    await targetClient.query(
      `DELETE FROM crawler.migration_channel_inventory
       WHERE source_id=$1 AND sync_token<>$2::uuid`,
      [config.sourceId, syncToken],
    );
    await targetClient.query(
      `UPDATE crawler.migration_channel_inventory_syncs
       SET status='ready',eligible_count=$2,completed_at=now(),
           last_error=NULL,updated_at=now()
       WHERE source_id=$1 AND sync_token=$3::uuid`,
      [config.sourceId, eligibleCount, syncToken],
    );
    await targetClient.query("COMMIT");
    inTransaction = false;
    await targetClient.query("ANALYZE crawler.migration_channel_inventory").catch(() => {});

    return {
      status: "ready",
      skipped: false,
      source_id: config.sourceId,
      eligible_count: eligibleCount,
    };
  } catch (error) {
    if (inTransaction) {
      await targetClient.query("ROLLBACK").catch(() => {});
      inTransaction = false;
    }
    if (syncToken) {
      const currentIdentityIsReady = current?.status === "ready"
        && current.source_database === config.expectedDatabase
        && String(current.source_database_oid) === String(config.expectedDatabaseOid);
      if (currentIdentityIsReady) {
        await targetClient.query(
          `UPDATE crawler.migration_channel_inventory_syncs
           SET last_error=$2,updated_at=now()
           WHERE source_id=$1`,
          [config.sourceId, errorText(error)],
        ).catch(() => {});
      } else {
        await targetClient.query(
          `INSERT INTO crawler.migration_channel_inventory_syncs (
             source_id,source_database,source_database_oid,status,sync_token,
             eligible_count,started_at,completed_at,last_error,updated_at
           ) VALUES ($1,$2,$3::oid,'failed',$4::uuid,0,now(),NULL,$5,now())
           ON CONFLICT (source_id) DO UPDATE
           SET source_database=EXCLUDED.source_database,
               source_database_oid=EXCLUDED.source_database_oid,
               status='failed',sync_token=EXCLUDED.sync_token,
               completed_at=NULL,last_error=EXCLUDED.last_error,updated_at=now()`,
          [
            config.sourceId,
            config.expectedDatabase,
            config.expectedDatabaseOid,
            syncToken,
            errorText(error),
          ],
        ).catch(() => {});
      }
    }
    throw error;
  } finally {
    targetClient.release();
  }
}
