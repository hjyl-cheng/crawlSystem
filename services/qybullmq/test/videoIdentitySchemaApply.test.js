import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  videoIdentityApplyGuard,
  videoIdentitySchemaBlock,
} from "../scripts/applyVideoIdentitySchema.mjs";

test("Video identity controlled deployment extracts only its nested schema block", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = videoIdentitySchemaBlock(schema);

  assert.match(block, /CREATE TEMP TABLE content_identity_merge_map/);
  assert.match(block, /DELETE FROM crawler\.contents duplicate/);
  assert.match(block, /ux_crawler_contents_channel_source/);
  assert.match(block, /unlisted/);
  assert.doesNotMatch(block, /CREATE TABLE IF NOT EXISTS crawler\.crawl_observations/);
  assert.doesNotMatch(block, /CREATE TABLE IF NOT EXISTS publication\./);
});

test("Video identity schema apply requires exact database and row-count confirmation", () => {
  const base = {
    CONFIRM_VIDEO_IDENTITY_SCHEMA_APPLY: "bullmq_crawler_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "1561",
    EXPECTED_CRAWLER_CONTENT_COUNT: "109337",
    EXPECTED_VIDEO_IDENTITY_DUPLICATE_GROUP_COUNT: "5720",
  };

  assert.throws(() => videoIdentityApplyGuard(base, ["node", "script"]), /--apply/);
  assert.throws(() => videoIdentityApplyGuard({
    ...base,
    CONFIRM_VIDEO_IDENTITY_SCHEMA_APPLY: "",
  }, ["node", "script", "--apply"]), /CONFIRM_VIDEO_IDENTITY_SCHEMA_APPLY/);
  for (const name of [
    "EXPECTED_CRAWLER_CHANNEL_COUNT",
    "EXPECTED_CRAWLER_CONTENT_COUNT",
    "EXPECTED_VIDEO_IDENTITY_DUPLICATE_GROUP_COUNT",
  ]) {
    assert.throws(() => videoIdentityApplyGuard({
      ...base,
      [name]: "1junk",
    }, ["node", "script", "--apply"]), new RegExp(name));
  }
  assert.deepEqual(
    videoIdentityApplyGuard(base, ["node", "script", "--apply"]),
    {
      confirmedDatabase: "bullmq_crawler_test",
      expectedChannelCount: 1561,
      expectedContentCount: 109337,
      expectedDuplicateGroupCount: 5720,
    },
  );
});

test("Video identity controlled apply verifies the post-migration invariants", async () => {
  const source = await readFile(
    new URL("../scripts/applyVideoIdentitySchema.mjs", import.meta.url),
    "utf8",
  );
  assert.match(source, /BEGIN/);
  assert.match(source, /PUBLICATION_WRITER_VERSION/);
  assert.match(source, /set_config\('publication\.writer_version'/);
  assert.match(source, /pg_advisory_xact_lock/);
  assert.match(source, /duplicate_group_count/);
  assert.match(
    source,
    /expectedContentCountAfter = Number\(before\.content_count\)[\s\S]*Number\(before\.duplicate_excess_row_count\)/,
  );
  assert.match(source, /video_identity_index_ready/);
  assert.match(source, /video_identity_deduplicated/);
  assert.match(source, /unlisted_access_status_ready/);
  assert.match(source, /ROLLBACK/);
});
