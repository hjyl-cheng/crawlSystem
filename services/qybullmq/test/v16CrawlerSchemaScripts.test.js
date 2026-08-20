import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(path) {
  return readFile(new URL(path, import.meta.url), "utf8");
}

function v16Fragment(schema) {
  const startMarker = "-- v16-rule-clock-schema:start";
  const endMarker = "-- v16-rule-clock-schema:end";
  const start = schema.indexOf(startMarker);
  const end = schema.indexOf(endMarker);
  assert.ok(start >= 0 && end > start, "V16 schema markers are missing");
  return schema.slice(start + startMarker.length, end);
}

test("the controlled V16 migration removes the Candidate identity index", async () => {
  const schema = await source("../src/schema.sql");
  const fragment = v16Fragment(schema);
  assert.match(
    fragment,
    /DROP INDEX IF EXISTS crawler\.idx_crawler_content_candidates_channel_source;/,
  );
  assert.match(fragment, /ADD COLUMN IF NOT EXISTS dormant_recheck_day DATE;/);
  assert.match(fragment, /status IN \('active', 'dormant', 'paused', 'archived', 'rejected', 'removed'\)/);
});

test("V16 migration scripts verify the Candidate identity index is absent", async () => {
  const [scripts, verification] = await Promise.all([
    Promise.all([
      source("../scripts/applyV16CrawlerSchema.mjs"),
      source("../scripts/validateV16CrawlerMigration.mjs"),
    ]),
    source("../src/v16SchemaVerification.js"),
  ]);

  for (const script of scripts) {
    assert.match(script, /V16_SCHEMA_VERIFICATION_SQL/);
    assert.doesNotMatch(script, /known_identity_lookup_ready/);
  }
  assert.match(
    verification,
    /to_regclass\('crawler\.idx_crawler_content_candidates_channel_source'\) IS NULL\s+AS candidate_identity_index_removed/,
  );
  assert.match(
    verification,
    /to_regclass\('crawler\.ux_crawler_contents_channel_source'\) IS NOT NULL\s+AS video_identity_index_ready/,
  );
  assert.match(verification, /AS video_identity_deduplicated/);
  assert.match(verification, /AS unlisted_access_status_ready/);
  assert.match(verification, /AS dormant_lifecycle_ready/);
  assert.match(verification, /AS channel_publication_fields_ready/);
});

test("the live Crawler schema exposes only About, Video, and Agent observations", async () => {
  const schema = await source("../src/schema.sql");
  const fragment = v16Fragment(schema);
  assert.match(
    fragment,
    /CHECK \(observation_kind IN \('about', 'video', 'agent'\)\)/,
  );
  assert.match(fragment, /about_identity_last_observed_at/);
  assert.match(fragment, /about_identity_current_hash/);
  assert.match(fragment, /is_verified_status/);
  assert.match(fragment, /external_links_status/);
  assert.match(fragment, /publication_item_hash/);
  assert.match(fragment, /contents_publication_item_hash_check/);
  assert.match(fragment, /input_content_ids/);
  assert.match(fragment, /input_content_hash/);
  assert.match(fragment, /taxonomy_version/);
  assert.match(fragment, /agent_version_hash/);
  assert.match(fragment, /agent_profiles_agent_version_hash_check/);

  const activeSources = await Promise.all([
    source("../src/incrementalChannelRunner.js"),
    source("../src/incrementalPlan.js"),
    source("../src/initialFullObservations.js"),
    source("../src/baselineBundle.js"),
  ]);
  for (const activeSource of activeSources) {
    assert.doesNotMatch(activeSource, /observationKind:\s*["']profile["']/);
  }

  const worker = await source("../src/worker.js");
  assert.match(worker, /return \["about", "video"\]/);
  assert.doesNotMatch(worker, /return \["profile", "about", "video"\]/);
});
