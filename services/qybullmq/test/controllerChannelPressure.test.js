import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

test("Controller expires stale Channel terminal samples from pressure metrics", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");

  assert.match(
    source,
    /created_at >= now\(\) - \(\$3::int \* interval '1 second'\)/,
  );
  assert.match(
    source,
    /\[queuesByRole\.channelCrawl, channelPressureSampleSize, channelPressureWindowSeconds\]/,
  );
});
