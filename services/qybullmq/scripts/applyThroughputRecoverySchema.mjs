import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";
assert(
  process.argv.includes("--apply"),
  "Pass --apply to apply the additive throughput recovery schema",
);
const expected = process.env.EXPECTED_CRAWLER_DATABASE;
assert(
  expected &&
    process.env.CONFIRM_THROUGHPUT_RECOVERY_SCHEMA_APPLY === expected,
  "Confirm the expected crawler database",
);
const client = new pg.Client({
  connectionString: databaseUrl(process.env),
  application_name: "throughput-recovery-schema",
});
try {
  await client.connect();
  await client.query("BEGIN");
  await client.query("SET LOCAL lock_timeout='10s'");
  await client.query("SET LOCAL statement_timeout='60s'");
  const identity = await verifyCrawlerWriterDatabase(
    client.query.bind(client),
    process.env,
  );
  assert.equal(identity.database, expected);
  await client.query("SELECT pg_advisory_xact_lock(781137247)");
  const before = await client.query(
    "SELECT count(*)::int AS n FROM crawler.migration_channel_inventory",
  );
  await client.query(
    await readFile(
      new URL("../src/throughputRecoverySchema.sql", import.meta.url),
      "utf8",
    ),
  );
  const after = await client.query(
    "SELECT count(*)::int AS n FROM crawler.migration_channel_inventory",
  );
  assert.equal(before.rows[0].n, after.rows[0].n);
  await client.query("COMMIT");
  console.log(
    JSON.stringify({
      ok: true,
      database: identity.database,
      inventory_count: after.rows[0].n,
    }),
  );
} catch (error) {
  await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  await client.end();
}
