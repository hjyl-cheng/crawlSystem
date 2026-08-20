import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import pg from "pg";
import { assertChannelExecutionIdentity } from "./channelExecutionContext.js";
import { crawlerRuntimeSchema } from "./publicationCurrentSchema.js";
import { PUBLICATION_WRITER_VERSION } from "./publicationWriterVersion.js";
import { environmentValue } from "./runtimeEnvironment.js";

const { Pool } = pg;

const __dirname = dirname(fileURLToPath(import.meta.url));

export function databaseUrl(environment = process.env) {
  return environmentValue("DATABASE_URL", { environment, required: false }) || [
    "postgres://",
    encodeURIComponent(environment.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(environment.POSTGRES_PASSWORD || "bullmq"),
    "@",
    environment.POSTGRES_HOST || "127.0.0.1",
    ":",
    environment.POSTGRES_PORT || "5432",
    "/",
    environment.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

export const pool = new Pool({
  connectionString: databaseUrl(),
  application_name: PUBLICATION_WRITER_VERSION,
  options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  max: Number(process.env.POSTGRES_POOL_MAX || 10),
  min: Math.max(0, Number(process.env.POSTGRES_POOL_MIN || 1)),
  idleTimeoutMillis: Math.max(1000, Number(process.env.POSTGRES_IDLE_TIMEOUT_MS || 300000)),
  connectionTimeoutMillis: Math.max(1000, Number(process.env.POSTGRES_CONNECTION_TIMEOUT_MS || 10000)),
  keepAlive: true,
  keepAliveInitialDelayMillis: Math.max(0, Number(process.env.POSTGRES_KEEP_ALIVE_DELAY_MS || 10000)),
});

pool.on("error", (error) => {
  console.error(JSON.stringify({
    event: "postgres_pool_idle_client_error",
    error: error?.message || String(error),
  }));
});

let schemaReady = null;
let schemaSkipLogged = false;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function withStartupRetry(fn) {
  const attempts = Number(process.env.POSTGRES_STARTUP_ATTEMPTS || 30);
  let lastError = null;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      return await fn();
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) break;
      await sleep(Math.min(1000 * attempt, 5000));
    }
  }
  throw lastError;
}

async function runSchemaMigration(schema) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(781137199)");
    await client.query(schema);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function ensureSchema() {
  if (String(process.env.SKIP_SCHEMA_MIGRATION || "").toLowerCase() === "true") {
    if (!schemaSkipLogged) {
      schemaSkipLogged = true;
      console.log("schema migration skipped by SKIP_SCHEMA_MIGRATION=true");
    }
    return;
  }
  if (!schemaReady) {
    schemaReady = (async () => {
      const schema = await readFile(join(__dirname, "schema.sql"), "utf8");
      await withStartupRetry(() => runSchemaMigration(crawlerRuntimeSchema(schema)));
    })().catch((error) => {
      schemaReady = null;
      throw error;
    });
  }
  return schemaReady;
}

export async function query(text, params = []) {
  await ensureSchema();
  return pool.query(text, params);
}

export async function withTransaction(action) {
  await ensureSchema();
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    assertChannelExecutionIdentity();
    const result = await action(client);
    assertChannelExecutionIdentity();
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

export async function warmDb() {
  const startedAt = Date.now();
  await query("SELECT 1");
  return {
    mode: "persistent_pool",
    warm_ms: Date.now() - startedAt,
    total_connections: pool.totalCount,
    idle_connections: pool.idleCount,
    waiting_requests: pool.waitingCount,
  };
}

export async function closeDb() {
  await pool.end();
}

export async function logTaskEvent({ queueName, jobId, jobName, entityKey, status, payload = {}, errorMessage = null }) {
  await query(
    `INSERT INTO crawler.task_events (
       queue_name, job_id, job_name, entity_key, status, payload_json, error_message
     )
     VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
    [
      queueName,
      jobId == null ? null : String(jobId),
      jobName == null ? null : String(jobName),
      entityKey == null ? null : String(entityKey),
      status,
      JSON.stringify(payload ?? {}),
      errorMessage,
    ],
  );
}
