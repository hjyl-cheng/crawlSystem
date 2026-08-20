import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Client } = pg;
const START_MARKER = "-- qy-rota-worker-v2-schema:start";
const END_MARKER = "-- qy-rota-worker-v2-schema:end";

function nonNegativeInteger(value, field) {
  if (value === null || value === undefined || String(value).trim() === "") {
    throw new Error(`${field} must be an explicit non-negative integer`);
  }
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 0) {
    throw new Error(`${field} must be an explicit non-negative integer`);
  }
  return parsed;
}

export function rotaWorkerV2SchemaBlock(schema) {
  const start = schema.indexOf(START_MARKER);
  const end = schema.indexOf(END_MARKER);
  if (start < 0 || end < 0 || end <= start) {
    throw new Error("Rota Worker V2 schema markers are missing or invalid");
  }
  return schema.slice(start + START_MARKER.length, end).trim();
}

export function guardedRotaWorkerSchemaConfig(environment = process.env) {
  if (String(environment.ROTA_WORKER_V2_SCHEMA_APPLY ?? "").trim().toLowerCase() !== "true") {
    throw new Error("ROTA_WORKER_V2_SCHEMA_APPLY=true is required");
  }
  const database = String(environment.POSTGRES_DB ?? "").trim();
  const confirmedDatabase = String(
    environment.CONFIRM_ROTA_WORKER_V2_DATABASE ?? "",
  ).trim();
  if (!database || confirmedDatabase !== database) {
    throw new Error("CONFIRM_ROTA_WORKER_V2_DATABASE must equal POSTGRES_DB");
  }
  const host = String(environment.POSTGRES_HOST ?? "").trim();
  if (!host) throw new Error("POSTGRES_HOST is required");
  return Object.freeze({
    connection: Object.freeze({
      host,
      port: Number(environment.POSTGRES_PORT || 5432),
      user: String(environment.POSTGRES_USER || "bullmq"),
      password: String(environment.POSTGRES_PASSWORD || ""),
      database,
    }),
    expectedChannelCount: nonNegativeInteger(
      environment.EXPECTED_CRAWLER_CHANNEL_COUNT,
      "EXPECTED_CRAWLER_CHANNEL_COUNT",
    ),
  });
}

async function main() {
  if (!process.argv.includes("--apply")) {
    throw new Error("refusing to apply: pass --apply explicitly");
  }
  const guarded = guardedRotaWorkerSchemaConfig();
  const schemaPath = fileURLToPath(new URL("../src/schema.sql", import.meta.url));
  const ddl = rotaWorkerV2SchemaBlock(await readFile(schemaPath, "utf8"));
  const client = new Client(guarded.connection);
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock(781137218)");
    const preflight = await client.query(
      `SELECT current_database() AS database_name,
              (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
    );
    const actual = preflight.rows[0];
    if (actual.database_name !== guarded.connection.database) {
      throw new Error(`database confirmation mismatch: ${actual.database_name}`);
    }
    if (Number(actual.channel_count) !== guarded.expectedChannelCount) {
      throw new Error(
        `Channel count mismatch: expected ${guarded.expectedChannelCount}, got ${actual.channel_count}`,
      );
    }
    await client.query(ddl);
    const verified = await client.query(
      `SELECT
         to_regclass('crawler.business_run_bindings') IS NOT NULL AS business_run_bindings,
         to_regclass('crawler.query_quality_chunks') IS NOT NULL AS query_quality_chunks,
         to_regclass('crawler.query_quality_chunk_members') IS NOT NULL AS query_quality_chunk_members,
         to_regclass('crawler.proxy_job_dispatch_outbox') IS NOT NULL AS proxy_job_dispatch_outbox,
         EXISTS (
           SELECT 1 FROM information_schema.columns
           WHERE table_schema='crawler' AND table_name='channel_runs'
             AND column_name='identity_policy_hash'
         ) AS channel_run_policy,
         EXISTS (
           SELECT 1 FROM pg_trigger
           WHERE tgname='trg_guard_managed_query_page_state' AND NOT tgisinternal
         ) AS discover_state_guard,
         EXISTS (
           SELECT 1 FROM pg_trigger
           WHERE tgname='trg_guard_query_quality_chunk_members' AND NOT tgisinternal
         ) AS query_quality_member_guard`,
    );
    if (!Object.values(verified.rows[0] ?? {}).every(Boolean)) {
      throw new Error("Rota Worker V2 schema verification failed");
    }
    await client.query("COMMIT");
    began = false;
    process.stdout.write(`${JSON.stringify({
      ok: true,
      database: actual.database_name,
      channel_count: Number(actual.channel_count),
      migration: "qy-rota-worker-v2-schema",
    })}\n`);
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
