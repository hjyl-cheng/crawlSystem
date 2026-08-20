import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Client } = pg;

export function businessPublicationActivationSchemaApplyGuard(
  env = process.env,
  argv = process.argv,
) {
  if (!argv.includes("--apply")) throw new Error("refusing to apply: pass --apply explicitly");
  const confirmedDatabase = String(env.CONFIRM_BUSINESS_PUBLICATION_ACTIVATION_SCHEMA_APPLY ?? "").trim();
  if (!confirmedDatabase) {
    throw new Error("CONFIRM_BUSINESS_PUBLICATION_ACTIVATION_SCHEMA_APPLY must equal the target database name");
  }
  const expectedChannelCountValue = String(env.EXPECTED_BUSINESS_CHANNEL_COUNT ?? "").trim();
  const expectedChannelCount = Number(expectedChannelCountValue);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(expectedChannelCountValue)
      && Number.isSafeInteger(expectedChannelCount),
    "EXPECTED_BUSINESS_CHANNEL_COUNT must be an explicit non-negative integer",
  );
  const databaseUrl = String(env.BUSINESS_DATABASE_URL ?? "").trim();
  if (!databaseUrl) throw new Error("BUSINESS_DATABASE_URL is required");
  return { confirmedDatabase, expectedChannelCount, databaseUrl };
}

async function main() {
  const guard = businessPublicationActivationSchemaApplyGuard();
  const schema = await readFile(
    new URL("../src/businessPublicationActivationSchema.sql", import.meta.url),
    "utf8",
  );
  const client = new Client({ connectionString: guard.databaseUrl });
  let began = false;
  try {
    await client.connect();
    await client.query("BEGIN");
    began = true;
    await client.query("SET LOCAL lock_timeout = '10s'");
    await client.query("SET LOCAL statement_timeout = '120s'");
    await client.query("SELECT pg_advisory_xact_lock(781137231)");
    const preflight = await client.query(
      `SELECT current_database() AS database_name,
              to_regclass('public.channels') IS NOT NULL AS business_schema_ready,
              to_regclass('publication.revision') IS NOT NULL AS ingress_schema_ready,
              (SELECT count(*)::int FROM public.channels) AS channel_count`,
    );
    const actual = preflight.rows[0] ?? {};
    assert.equal(actual.database_name, guard.confirmedDatabase, "unexpected Business database");
    assert.equal(actual.business_schema_ready, true, "Business public.channels is missing");
    assert.equal(actual.ingress_schema_ready, true, "Business Publication Ingress schema is missing");
    assert.equal(Number(actual.channel_count), guard.expectedChannelCount, "unexpected Business Channel count");
    await client.query(schema);
    await client.query("COMMIT");
    began = false;
    console.log(JSON.stringify({
      ok: true,
      database: actual.database_name,
      channel_count: Number(actual.channel_count),
      migration: "business-publication-activation-schema",
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
