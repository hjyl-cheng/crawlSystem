import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";

const { Client } = pg;
const START_MARKER = "-- qy-fingerprint-schema:start";
const END_MARKER = "-- qy-fingerprint-schema:end";

export function guardedConnectionConfig(env = process.env) {
  const database = String(env.POSTGRES_DB || "");
  const host = String(env.POSTGRES_HOST || "");
  const approved = String(env.QY_FINGERPRINT_SCHEMA_APPLY || "").toLowerCase() === "true";
  if (!approved) throw new Error("QY_FINGERPRINT_SCHEMA_APPLY=true is required");
  if (database !== "bullmq_crawler_migration") {
    throw new Error(`refusing fingerprint schema apply for database ${database || "<empty>"}`);
  }
  if (!host.includes("migration-postgres")) {
    throw new Error(`refusing fingerprint schema apply for host ${host || "<empty>"}`);
  }
  return {
    host,
    port: Number(env.POSTGRES_PORT || 5432),
    user: String(env.POSTGRES_USER || "bullmq"),
    password: String(env.POSTGRES_PASSWORD || ""),
    database,
  };
}

export function fingerprintSchemaBlock(schema) {
  const start = schema.indexOf(START_MARKER);
  const end = schema.indexOf(END_MARKER);
  if (start < 0 || end < 0 || end <= start) throw new Error("fingerprint schema markers are missing or invalid");
  return schema.slice(start + START_MARKER.length, end).trim();
}

async function main() {
  const schemaPath = fileURLToPath(new URL("../src/schema.sql", import.meta.url));
  const ddl = fingerprintSchemaBlock(await readFile(schemaPath, "utf8"));
  const client = new Client(guardedConnectionConfig());
  await client.connect();
  try {
    await client.query("BEGIN");
    await client.query("SELECT pg_advisory_xact_lock(781137209)");
    await client.query(ddl);
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end();
  }
  console.log(JSON.stringify({
    event: "qy_fingerprint_schema_applied",
    database: process.env.POSTGRES_DB,
    host: process.env.POSTGRES_HOST,
  }));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error?.stack || String(error));
    process.exitCode = 1;
  });
}
