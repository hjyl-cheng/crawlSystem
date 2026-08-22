import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  videoDispositionSchemaApplyGuard,
  videoDispositionSchemaBlock,
} from "../scripts/applyVideoDispositionSchema.mjs";

test("Video disposition controlled deployment extracts only its schema block", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = videoDispositionSchemaBlock(schema);

  assert.match(block, /ADD COLUMN IF NOT EXISTS disposition TEXT/);
  assert.match(block, /legacy_video_disposition_backfill/);
  assert.match(block, /content_candidates_disposition_schedule_check/);
  assert.match(block, /idx_crawler_content_candidates_disposition_history/);
  assert.doesNotMatch(block, /CREATE TABLE IF NOT EXISTS crawler\.channels/);
  assert.doesNotMatch(block, /CREATE TABLE IF NOT EXISTS publication\./);
});

test("Video disposition schema apply pins database identity and Candidate counts", () => {
  const base = {
    DATABASE_URL: "postgres://crawler-admin:secret@crawler/crawler_test",
    CONFIRM_VIDEO_DISPOSITION_SCHEMA_APPLY: "crawler_test",
    EXPECTED_CRAWLER_CANDIDATE_COUNT: "1058693",
    EXPECTED_UNDISPOSED_CANDIDATE_COUNT: "1058693",
  };

  assert.throws(
    () => videoDispositionSchemaApplyGuard(base, ["node", "script"]),
    /pass --apply explicitly/,
  );
  assert.throws(() => videoDispositionSchemaApplyGuard({
    ...base,
    CONFIRM_VIDEO_DISPOSITION_SCHEMA_APPLY: "",
  }, ["node", "script", "--apply"]), /CONFIRM_VIDEO_DISPOSITION_SCHEMA_APPLY/);
  for (const name of [
    "EXPECTED_CRAWLER_CANDIDATE_COUNT",
    "EXPECTED_UNDISPOSED_CANDIDATE_COUNT",
  ]) {
    assert.throws(() => videoDispositionSchemaApplyGuard({
      ...base,
      [name]: "1junk",
    }, ["node", "script", "--apply"]), new RegExp(name));
  }
  assert.deepEqual(
    videoDispositionSchemaApplyGuard(base, ["node", "script", "--apply"]),
    {
      databaseUrl: base.DATABASE_URL,
      confirmedDatabase: "crawler_test",
      expectedCandidateCount: 1058693,
      expectedUndisposedCandidateCount: 1058693,
    },
  );
});

test("Video disposition controlled apply verifies transactional postconditions", async () => {
  const [source, packageJson] = await Promise.all([
    readFile(new URL("../scripts/applyVideoDispositionSchema.mjs", import.meta.url), "utf8"),
    readFile(new URL("../package.json", import.meta.url), "utf8"),
  ]);

  assert.match(source, /BEGIN/);
  assert.match(source, /LOCK TABLE crawler\.content_candidates IN SHARE ROW EXCLUSIVE MODE/);
  assert.match(source, /lock_timeout/);
  assert.match(source, /statement_timeout/);
  assert.match(source, /pg_advisory_xact_lock/);
  assert.match(source, /undisposed_count/);
  assert.match(source, /invalid_schedule_count/);
  assert.match(source, /constraints_validated/);
  assert.match(source, /indexes_ready/);
  assert.match(source, /ROLLBACK/);
  assert.match(packageJson, /schema:video-disposition/);
});
