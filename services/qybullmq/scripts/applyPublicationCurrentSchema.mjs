import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { publicationCurrentSchemaBlock } from "../src/publicationCurrentSchema.js";

export { publicationCurrentSchemaBlock };

const { Client } = pg;

export function publicationCurrentApplyGuard(env = process.env, argv = process.argv) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(env.CONFIRM_PUBLICATION_CURRENT_SCHEMA_APPLY ?? "").trim();
  if (!confirmedDatabase) {
    throw new Error("CONFIRM_PUBLICATION_CURRENT_SCHEMA_APPLY must equal the target database name");
  }
  const expectedChannelCountValue = String(env.EXPECTED_CRAWLER_CHANNEL_COUNT ?? "").trim();
  const expectedChannelCount = Number(expectedChannelCountValue);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(expectedChannelCountValue)
      && Number.isSafeInteger(expectedChannelCount),
    "EXPECTED_CRAWLER_CHANNEL_COUNT must be an explicit non-negative integer",
  );
  return { confirmedDatabase, expectedChannelCount };
}

function databaseUrl(env = process.env) {
  return env.DATABASE_URL || [
    "postgres://",
    encodeURIComponent(env.POSTGRES_USER || "bullmq"),
    ":",
    encodeURIComponent(env.POSTGRES_PASSWORD || "bullmq"),
    "@",
    env.POSTGRES_HOST || "127.0.0.1",
    ":",
    env.POSTGRES_PORT || "5432",
    "/",
    env.POSTGRES_DB || "bullmq_crawler",
  ].join("");
}

async function main() {
  const guard = publicationCurrentApplyGuard();
  const schemaPath = fileURLToPath(new URL("../src/schema.sql", import.meta.url));
  const ddl = publicationCurrentSchemaBlock(await readFile(schemaPath, "utf8"));
  const client = new Client({ connectionString: databaseUrl() });
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
    assert.equal(actual.database_name, guard.confirmedDatabase, "unexpected Crawler database");
    assert.equal(Number(actual.channel_count), guard.expectedChannelCount, "unexpected Crawler Channel count");
    await client.query(ddl);
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: actual.database_name,
      channel_count: Number(actual.channel_count),
      migration: "publication-current-schema",
    }));
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
