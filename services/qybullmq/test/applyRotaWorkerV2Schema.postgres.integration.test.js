import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { rotaWorkerV2SchemaBlock } from "../scripts/applyRotaWorkerV2Schema.mjs";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

test("Rota Worker V2 schema block applies transactionally and is idempotent", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  t.after(async () => {
    await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await client.end();
  });

  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  // Production databases created before the managed-job schema do not have this index.
  await client.query("DROP TABLE crawler.query_quality_chunk_members");
  await client.query("DROP INDEX crawler.ux_crawler_query_quality_tasks_batch_task");
  const block = rotaWorkerV2SchemaBlock(schema);
  await client.query("BEGIN");
  try {
    await client.query(block);
    await client.query(block);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  }

  const verified = await client.query(
    `SELECT
       to_regclass('crawler.business_run_bindings') IS NOT NULL AS binding,
       to_regclass('crawler.proxy_job_dispatch_outbox') IS NOT NULL AS outbox,
       EXISTS (
         SELECT 1 FROM pg_trigger
         WHERE tgname='trg_guard_query_quality_chunk_members' AND NOT tgisinternal
       ) AS member_guard,
       to_regclass('crawler.ux_crawler_query_quality_tasks_batch_task') IS NOT NULL
         AS legacy_parent_key_repaired`,
  );
  assert.deepEqual(verified.rows[0], {
    binding: true,
    outbox: true,
    member_guard: true,
    legacy_parent_key_repaired: true,
  });
});
