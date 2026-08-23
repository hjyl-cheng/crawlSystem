import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Content Enrich schema persists dispatch fencing and defaults cutover ownership to Clock", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");

  assert.match(schema, /content_enrich_tasks ADD COLUMN IF NOT EXISTS dispatch_generation BIGINT NOT NULL DEFAULT 0/);
  assert.match(
    schema,
    /content_enrich_tasks_status_check\s+CHECK \(status IN \([^;]*'dead_letter'[^;]*\)\);/,
  );
  assert.match(schema, /setting_key,value_json[\s\S]*'content_enrich_dispatch'[\s\S]*'\{"mode":"clock"\}'::jsonb/);
  assert.match(schema, /'content_enrich_dispatch_cursor'[\s\S]*'\{"channel_id":""\}'::jsonb/);
  assert.match(schema, /'content_enrich_dispatch_mutex'[\s\S]*'\{"owner":null,"expires_at":null\}'::jsonb/);
  assert.match(schema, /idx_crawler_content_enrich_tasks_dispatch[\s\S]*job_type,status,next_retry_at,priority,created_at,channel_id/);
  assert.match(schema, /idx_crawler_content_enrich_tasks_lease_owner[\s\S]*lease_owner/);
});

test("fresh Crawler bootstrap includes the complete Content Enrich dispatch contract", async () => {
  const bootstrap = await readFile(
    new URL("../../../database/bootstrap/crawler.sql", import.meta.url),
    "utf8",
  );

  assert.match(bootstrap, /dispatch_generation bigint DEFAULT 0 NOT NULL/);
  assert.match(
    bootstrap,
    /content_enrich_tasks_status_check CHECK \(\(status = ANY \(ARRAY\[[^;\n]*'dead_letter'[^;\n]*\]\)\)\)/,
  );
  assert.match(bootstrap, /CREATE INDEX idx_crawler_content_enrich_tasks_dispatch/);
  assert.match(bootstrap, /CREATE INDEX idx_crawler_content_enrich_tasks_lease_owner/);
  assert.match(bootstrap, /'content_enrich_dispatch'[\s\S]*'\{"mode":"clock"\}'::jsonb/);
  assert.match(bootstrap, /'content_enrich_dispatch_mutex'[\s\S]*'\{"owner":null,"expires_at":null\}'::jsonb/);
});
