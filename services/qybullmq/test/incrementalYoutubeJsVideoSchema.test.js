import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { incrementalYoutubeJsVideoCheckpointSchemaApplyGuard } from "../scripts/applyIncrementalYoutubeJsVideoCheckpointSchema.mjs";
import { incrementalYoutubeJsVideoCheckpointSchemaBlock } from "../src/incrementalYoutubeJsVideoSchema.js";

test("checkpoint schema block contains generation-scoped Batch and CAS Item constraints", async () => {
  const [schema, bootstrap] = await Promise.all([
    readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
    readFile(new URL("../../../database/bootstrap/crawler.sql", import.meta.url), "utf8"),
  ]);
  const block = incrementalYoutubeJsVideoCheckpointSchemaBlock(schema);
  // pg_dump emits tables, constraints and indexes separately, without the
  // runtime migration markers. Check the exported checkpoint contract directly.
  assert.match(bootstrap, /CREATE TABLE crawler\.incremental_youtubejs_video_batches/);
  assert.match(bootstrap, /incremental_youtubejs_video_batches_pkey PRIMARY KEY \(run_id, cycle_key\)/);
  assert.match(bootstrap, /CREATE TABLE crawler\.incremental_youtubejs_video_items/);
  assert.match(bootstrap, /incremental_youtubejs_video_items_pkey PRIMARY KEY \(run_id, cycle_key, phase, video_id\)/);
  assert.match(bootstrap, /UNIQUE \(run_id, cycle_key, video_id\)/);
  assert.match(bootstrap, /claim_token uuid/);
  assert.match(bootstrap, /first_seen_checkpoints_json jsonb/);
  assert.match(bootstrap, /final_observation_id uuid/);
  assert.match(bootstrap, /final_result_json ->> 'observation_id'::text\) IS DISTINCT FROM \(final_observation_id\)::text/);
  assert.match(bootstrap, /status = 'captured'::text[^\n]*detail_json IS NOT NULL/);
  assert.match(bootstrap, /status = 'settled_error'::text[^\n]*error_json IS NOT NULL/);
  assert.match(block, /incremental_youtubejs_video_batches/);
  assert.match(block, /PRIMARY KEY \(run_id, cycle_key\)/);
  assert.match(block, /incremental_youtubejs_video_items/);
  assert.match(block, /UNIQUE \(run_id, cycle_key, video_id\)/);
  assert.match(block, /claim_token UUID/);
  assert.match(block, /first_seen_checkpoints_json JSONB/);
  assert.match(block, /final_observation_id UUID/);
  assert.match(
    block,
    /final_result_json->>'observation_id'\)\s+IS NOT DISTINCT FROM final_observation_id::text/,
  );
  assert.match(block, /status='captured'[\s\S]*detail_json IS NOT NULL/);
  assert.match(block, /status='settled_error'[\s\S]*error_json IS NOT NULL/);
});

test("checkpoint schema parser rejects missing and duplicate markers", () => {
  assert.throws(
    () => incrementalYoutubeJsVideoCheckpointSchemaBlock("SELECT 1"),
    /missing or duplicated/,
  );
  assert.throws(
    () => incrementalYoutubeJsVideoCheckpointSchemaBlock(`
      -- incremental-youtubejs-video-checkpoint-schema:start
      SELECT 1;
      -- incremental-youtubejs-video-checkpoint-schema:start
      SELECT 2;
      -- incremental-youtubejs-video-checkpoint-schema:end
    `),
    /missing or duplicated/,
  );
});

test("checkpoint schema apply requires explicit database and Run-count confirmation", () => {
  assert.throws(
    () => incrementalYoutubeJsVideoCheckpointSchemaApplyGuard({}, ["node", "script"]),
    /pass --apply/,
  );
  assert.throws(
    () => incrementalYoutubeJsVideoCheckpointSchemaApplyGuard({}, ["node", "script", "--apply"]),
    /CONFIRM_INCREMENTAL_YOUTUBEJS_CHECKPOINT_SCHEMA_APPLY/,
  );
  const guard = incrementalYoutubeJsVideoCheckpointSchemaApplyGuard({
    CONFIRM_INCREMENTAL_YOUTUBEJS_CHECKPOINT_SCHEMA_APPLY: "crawler_test",
    EXPECTED_INCREMENTAL_RUN_MIN_COUNT: "12",
    DATABASE_URL: "postgres://localhost/crawler_test",
  }, ["node", "script", "--apply"]);
  assert.deepEqual(guard, {
    confirmedDatabase: "crawler_test",
    expectedMinimumRunCount: 12,
    databaseUrl: "postgres://localhost/crawler_test",
  });

  const composeGuard = incrementalYoutubeJsVideoCheckpointSchemaApplyGuard({
    CONFIRM_INCREMENTAL_YOUTUBEJS_CHECKPOINT_SCHEMA_APPLY: "crawler_test",
    EXPECTED_INCREMENTAL_RUN_MIN_COUNT: "12",
    POSTGRES_HOST: "crawler-pgbouncer",
    POSTGRES_PORT: "6432",
    POSTGRES_USER: "crawler writer",
    POSTGRES_PASSWORD: "secret/value",
    POSTGRES_DB: "crawler_test",
  }, ["node", "script", "--apply"]);
  assert.equal(
    composeGuard.databaseUrl,
    "postgres://crawler%20writer:secret%2Fvalue@crawler-pgbouncer:6432/crawler_test",
  );
});

test("Incremental Worker defaults to the YouTubeJS checkpoint executor", async () => {
  const [source, compose, environment, capabilities] = await Promise.all([
    readFile(new URL("../src/worker.js", import.meta.url), "utf8"),
    readFile(new URL("../../../deploy/compose.yml", import.meta.url), "utf8"),
    readFile(new URL("../../../.env.example", import.meta.url), "utf8"),
    readFile(new URL("../src/channelExtractorCapabilities.js", import.meta.url), "utf8"),
  ]);
  assert.match(capabilities, /INCREMENTAL_VIDEO_EXECUTOR \|\| "youtubejs_checkpoint_v1"/);
  assert.match(capabilities, /\["legacy", "youtubejs_checkpoint_v1"\]/);
  assert.match(source, /video: incrementalVideoExecutor/);

  const start = compose.indexOf("  worker-incremental:");
  const end = compose.indexOf("\n  worker-content-enrich:", start);
  assert.ok(start >= 0 && end > start);
  const incrementalWorker = compose.slice(start, end);
  assert.match(
    incrementalWorker,
    /INCREMENTAL_VIDEO_EXECUTOR: \$\{INCREMENTAL_VIDEO_EXECUTOR:-youtubejs_checkpoint_v1\}/,
  );
  assert.match(
    incrementalWorker,
    /YOUTUBEJS_EXTRACTOR_MODE: \$\{YOUTUBEJS_EXTRACTOR_MODE:-full\}/,
  );
  assert.match(environment, /^INCREMENTAL_VIDEO_EXECUTOR=youtubejs_checkpoint_v1$/m);
  assert.match(environment, /^YOUTUBEJS_EXTRACTOR_MODE=full$/m);
});
