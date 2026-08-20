import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Video Item Hash backfill requires explicit database and row-count confirmation", async () => {
  const script = await readFile(new URL(
    "../scripts/backfillVideoPublicationItemHashes.mjs",
    import.meta.url,
  ), "utf8");

  assert.match(script, /process\.argv\.includes\("--apply"\)/);
  assert.match(script, /CONFIRM_VIDEO_ITEM_HASH_BACKFILL/);
  assert.match(script, /EXPECTED_CRAWLER_CONTENT_COUNT/);
  assert.match(script, /pg_advisory_lock/);
  assert.match(script, /PUBLICATION_WRITER_VERSION/);
  assert.match(script, /set_config\('publication\.writer_version'/);
  assert.doesNotMatch(script, /bullmq_crawler_migration/);
});
