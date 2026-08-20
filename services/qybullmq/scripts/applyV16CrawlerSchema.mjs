import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { V16_SCHEMA_VERIFICATION_SQL } from "../src/v16SchemaVerification.js";

const { Client } = pg;
const here = dirname(fileURLToPath(import.meta.url));

function databaseUrl() {
  return process.env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(process.env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(process.env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    process.env.POSTGRES_HOST || "127.0.0.1",
    ":",
    process.env.POSTGRES_PORT || "5432",
    "/",
    process.env.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

async function migrationSql() {
  const schema = await readFile(join(here, "../src/schema.sql"), "utf8");
  const startMarker = "-- v16-rule-clock-schema:start";
  const endMarker = "-- v16-rule-clock-schema:end";
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  assert.ok(start >= 0 && end > start, "V16 schema markers are missing");
  return schema.slice(start + startMarker.length, end);
}

if (!process.argv.includes("--apply")) {
  throw new Error("refusing to apply: pass --apply explicitly");
}
const expectedChannelCount = Number.parseInt(
  String(process.env.EXPECTED_CRAWLER_CHANNEL_COUNT ?? ""),
  10,
);
if (!Number.isSafeInteger(expectedChannelCount) || expectedChannelCount < 0) {
  throw new Error("EXPECTED_CRAWLER_CHANNEL_COUNT must be an explicit non-negative integer");
}
const confirmedDatabase = String(process.env.CONFIRM_V16_CRAWLER_SCHEMA_APPLY ?? "").trim();
if (!confirmedDatabase) {
  throw new Error("CONFIRM_V16_CRAWLER_SCHEMA_APPLY must equal the target database name");
}

const client = new Client({ connectionString: databaseUrl() });
let began = false;
try {
  await client.connect();
  await client.query("BEGIN");
  began = true;
  await client.query("SET LOCAL lock_timeout = '10s'");
  await client.query("SET LOCAL statement_timeout = '120s'");
  await client.query("SELECT pg_advisory_xact_lock(781137216)");

  const preflight = await client.query(
    `SELECT current_database() AS database_name,
            (SELECT count(*)::int FROM crawler.channels) AS channel_count`,
  );
  const actual = preflight.rows[0];
  if (actual.database_name !== confirmedDatabase) {
    throw new Error(
      `database confirmation mismatch: expected ${confirmedDatabase}, got ${actual.database_name}`,
    );
  }
  if (Number(actual.channel_count) !== expectedChannelCount) {
    throw new Error(
      `Channel count mismatch: expected ${expectedChannelCount}, got ${actual.channel_count}`,
    );
  }

  await client.query(await migrationSql());
  const verified = await client.query(V16_SCHEMA_VERIFICATION_SQL);
  if (!Object.values(verified.rows[0]).every(Boolean)) {
    throw new Error("V16 schema verification failed");
  }
  await client.query("COMMIT");
  began = false;
  console.log(JSON.stringify({
    ok: true,
    database: actual.database_name,
    channel_count: Number(actual.channel_count),
    migration: "v16-rule-clock-schema",
  }));
} catch (error) {
  if (began) await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  await client.end().catch(() => {});
}
