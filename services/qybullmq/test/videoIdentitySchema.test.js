import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("V16 schema uses Contents as the only persistent Video identity", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  assert.doesNotMatch(schema, /channel_video_catalog/i);
  assert.match(
    schema,
    /CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_contents_channel_source\s+ON crawler\.contents \(channel_id, source_content_id\)/,
  );
  assert.doesNotMatch(schema, /UNIQUE \(channel_id, content_type, source_content_id\)/);
  assert.match(
    schema,
    /access_status IN \('public', 'unlisted', 'members_only', 'private', 'unavailable', 'login_required', 'unknown'\)/,
  );
  assert.match(schema, /DROP INDEX IF EXISTS crawler\.idx_crawler_content_candidates_channel_source/);
  assert.doesNotMatch(schema, /CREATE (?:UNIQUE )?INDEX[^;]*idx_crawler_content_candidates_channel_source/i);
});

test("V16 migration merges legacy type duplicates before enforcing Video identity", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const startMarker = "-- v16-rule-clock-schema:start";
  const endMarker = "-- v16-rule-clock-schema:end";
  const fragment = schema.slice(
    schema.indexOf(startMarker) + startMarker.length,
    schema.indexOf(endMarker),
  );

  assert.match(fragment, /CREATE TEMP TABLE content_identity_merge_map/);
  assert.match(fragment, /UPDATE crawler\.content_candidates candidate/);
  assert.match(fragment, /UPDATE crawler\.content_enrich_tasks survivor_task/);
  assert.match(fragment, /DELETE FROM crawler\.contents duplicate/);
  assert.match(
    fragment,
    /DROP CONSTRAINT IF EXISTS contents_channel_id_content_type_source_content_id_key/,
  );
  assert.match(
    fragment,
    /CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_contents_channel_source/,
  );
  assert.ok(
    fragment.indexOf("DELETE FROM crawler.contents duplicate")
      < fragment.indexOf("CREATE UNIQUE INDEX IF NOT EXISTS ux_crawler_contents_channel_source"),
    "duplicate Contents must be removed before the unique identity index is created",
  );
});

test("Video identity migration fences active Content Enrich leases before moving Tasks", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const startMarker = "-- video-identity-schema:start";
  const endMarker = "-- video-identity-schema:end";
  const fragment = schema.slice(
    schema.indexOf(startMarker) + startMarker.length,
    schema.indexOf(endMarker),
  );

  assert.match(
    fragment,
    /status IN \('queued','leased','running'\)[\s\S]*THEN 'queued'/,
  );
  assert.match(
    fragment,
    /dispatch_generation=GREATEST\(survivor_task\.dispatch_generation,duplicate_task\.dispatch_generation\)[\s\S]*status IN \('leased','running'\)[\s\S]*THEN 1/,
  );
  assert.match(
    fragment,
    /status=CASE WHEN task\.status IN \('leased','running'\) THEN 'queued' ELSE task\.status END,[\s\S]*dispatch_generation=task\.dispatch_generation[\s\S]*THEN 1/,
  );
});
