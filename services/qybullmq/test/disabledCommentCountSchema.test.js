import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

async function source(path) {
  return readFile(new URL(path, import.meta.url), "utf8");
}

test("Crawler schemas require disabled comments to carry an authoritative zero", async () => {
  const [runtime, bootstrap] = await Promise.all([
    source("../src/schema.sql"),
    source("../../../database/bootstrap/crawler.sql"),
  ]);

  for (const schema of [runtime, bootstrap]) {
    assert.match(schema, /contents_comment_state_shape/);
    assert.match(
      schema,
      /comments_disabled IS TRUE[\s\S]*comment_count\s*=\s*0[\s\S]*comment_count_status\s*=\s*'disabled'/,
    );
    assert.match(
      schema,
      /comments_disabled IS DISTINCT FROM TRUE[\s\S]*comment_count_status\s*<>\s*'disabled'/i,
    );
  }
  assert.match(runtime, /WHEN merged\.comments_disabled=true THEN 0/);
});

test("Business schemas keep legacy snapshots readable and require v4 projections to store zero", async () => {
  const [runtime, bootstrap] = await Promise.all([
    source("../src/businessPublicationProjectionSchema.sql"),
    source("../../../database/bootstrap/business.sql"),
  ]);

  assert.match(runtime, /business-publication-projection-v4/);
  assert.match(runtime, /content_snapshots_access_shape/);
  assert.match(
    runtime,
    /business-publication-projection-v4'[\s\S]*comment_count=0[\s\S]*comment_count_status='exact'/,
  );
  assert.match(
    runtime,
    /comment_count IS NULL[\s\S]*comment_count_status='unavailable'/,
  );
  assert.match(
    bootstrap,
    /content_snapshots_access_shape[\s\S]*comment_count = 0[\s\S]*comment_count_status = 'exact'/,
  );
  // The production raw table still uses the legacy constraint. It ties the
  // disabled flag to its status; authoritative zero is enforced by v4 Projection.
  assert.match(
    bootstrap,
    /raw_contents_v4_shape[^\n]*\(comments_disabled IS TRUE\) = \(comment_count_status = 'disabled'::text\)/,
  );
});
