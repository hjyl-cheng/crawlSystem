import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";

assert.ok(process.argv.includes("--apply"), "pass --apply explicitly");
const expected = process.env.CONFIRM_VIDEO_API_SCHEMA_DATABASE;
assert.ok(expected, "CONFIRM_VIDEO_API_SCHEMA_DATABASE is required");
const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
const start = schema.indexOf("-- qy-video-api-detail-requests:start");
const end = schema.indexOf("-- qy-video-api-detail-requests:end");
assert.ok(start >= 0 && end > start, "Video API schema block is missing");
const client = new pg.Client({ connectionString: databaseUrl() });
try {
  await client.connect();
  await client.query("BEGIN");
  await client.query("SET LOCAL lock_timeout='10s'");
  await client.query("SET LOCAL statement_timeout='60s'");
  const identity = (await client.query("SELECT current_database() AS name")).rows[0];
  assert.equal(identity.name, expected, "unexpected Crawler database");
  await client.query(schema.slice(start, end));
  assert.equal((await client.query("SELECT to_regclass('crawler.youtube_api_detail_requests') IS NOT NULL AS ready")).rows[0].ready, true);
  await client.query("COMMIT");
  console.log(JSON.stringify({ ok: true, database: expected, migration: "video-api-detail-requests-v1" }));
} catch (error) {
  await client.query("ROLLBACK").catch(() => {});
  throw error;
} finally {
  await client.end();
}
