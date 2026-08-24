import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Video disposition schema constrains schedules and indexes due history lookups", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");

  assert.match(schema, /ADD COLUMN IF NOT EXISTS disposition TEXT/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ/);
  assert.match(schema, /legacy_video_disposition_backfill/);
  assert.match(
    schema,
    /WHEN candidate\.content_key IS NOT NULL THEN 'stored'/,
  );
  assert.match(
    schema,
    /ELSE 'deferred'/,
  );
  assert.match(
    schema,
    /WHERE candidate\.disposition IS NULL/,
  );
  assert.match(schema, /content_candidates_disposition_kind_check/);
  assert.match(schema, /content_candidates_disposition_schedule_check/);
  assert.match(
    schema,
    /disposition='stored' AND next_attempt_at IS NULL/,
  );
  assert.match(
    schema,
    /disposition IN \('deferred','terminal_excluded'\)[\s\S]*AND next_attempt_at IS NOT NULL/,
  );
  assert.match(schema, /idx_crawler_content_candidates_channel_disposition_due/);
  assert.match(schema, /idx_crawler_content_candidates_disposition_history/);
  assert.match(schema, /\(channel_id, source_content_id, candidate_id DESC\)/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS first_seen_ledger_status TEXT NOT NULL DEFAULT 'not_applicable'/);
  assert.match(schema, /ADD COLUMN IF NOT EXISTS first_seen_ledger_observation_id UUID/);
  assert.match(schema, /content_candidates_first_seen_ledger_shape_check/);
  assert.match(schema, /first_seen_ledger_status='pending' AND first_seen_ledger_observation_id IS NULL/);
  assert.match(schema, /first_seen_ledger_status='consumed' AND first_seen_ledger_observation_id IS NOT NULL/);
  assert.match(schema, /content_candidates_first_seen_ledger_observation_id_fkey[\s\S]*DEFERRABLE INITIALLY DEFERRED NOT VALID/);
  assert.doesNotMatch(
    schema,
    /CREATE INDEX IF NOT EXISTS idx_crawler_content_candidates_first_seen_ledger_pending/,
  );
});

test("fresh Crawler bootstrap includes the Video disposition contract", async () => {
  const bootstrap = await readFile(
    new URL("../../../database/bootstrap/crawler.sql", import.meta.url),
    "utf8",
  );

  assert.match(bootstrap, /\n    disposition text,/);
  assert.match(bootstrap, /\n    next_attempt_at timestamp with time zone,/);
  assert.match(bootstrap, /CONSTRAINT content_candidates_disposition_kind_check/);
  assert.match(bootstrap, /CONSTRAINT content_candidates_disposition_schedule_check/);
  assert.match(bootstrap, /CREATE INDEX idx_crawler_content_candidates_disposition_due/);
  assert.match(bootstrap, /CREATE INDEX idx_crawler_content_candidates_channel_disposition_due/);
  assert.match(bootstrap, /CREATE INDEX idx_crawler_content_candidates_disposition_history/);
  assert.match(bootstrap, /first_seen_ledger_status text DEFAULT 'not_applicable'::text NOT NULL/);
  assert.match(bootstrap, /first_seen_ledger_observation_id uuid/);
  assert.match(bootstrap, /CONSTRAINT content_candidates_first_seen_ledger_shape_check/);
  assert.match(bootstrap, /content_candidates_first_seen_ledger_observation_id_fkey[\s\S]*DEFERRABLE INITIALLY DEFERRED/);
  assert.match(
    bootstrap,
    /CREATE INDEX idx_crawler_content_candidates_first_seen_ledger_pending[\s\S]*\(channel_id, candidate_id\)/,
  );
});
