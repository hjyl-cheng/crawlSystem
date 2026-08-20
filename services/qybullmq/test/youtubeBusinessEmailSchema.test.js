import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { youtubeBusinessEmailCrawlerSchemaApplyGuard } from "../scripts/applyYoutubeBusinessEmailCrawlerSchema.mjs";
import { youtubeBusinessEmailCrawlerSchemaBlock } from "../src/youtubeBusinessEmailSchema.js";

test("crawler schema stores a nullable availability with a matching observation time", async () => {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const block = youtubeBusinessEmailCrawlerSchemaBlock(schema);
  assert.match(block, /youtube_business_email_available BOOLEAN/);
  assert.match(block, /youtube_business_email_observed_at TIMESTAMPTZ/);
  assert.match(block, /channels_youtube_business_email_shape/);
});

test("crawler schema apply guard pins database identity and Channel count", () => {
  assert.deepEqual(youtubeBusinessEmailCrawlerSchemaApplyGuard({
    DATABASE_URL: "postgres://crawler/db",
    CONFIRM_YOUTUBE_BUSINESS_EMAIL_CRAWLER_SCHEMA_APPLY: "crawler_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "19",
  }, ["node", "script", "--apply"]), {
    confirmedDatabase: "crawler_test",
    expectedChannelCount: 19,
    databaseUrl: "postgres://crawler/db",
  });
  assert.throws(
    () => youtubeBusinessEmailCrawlerSchemaApplyGuard({}, ["node", "script"]),
    /pass --apply explicitly/,
  );
});
