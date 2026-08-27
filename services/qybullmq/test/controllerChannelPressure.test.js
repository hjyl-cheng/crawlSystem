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

test("Controller persists a Channel generation and exact Outbox without deleting terminal Jobs", async () => {
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  const start = source.indexOf("async function reconcileChannelCandidateQueue");
  const end = source.indexOf("async function updateSchedulerRuntime", start);
  const reconcile = source.slice(start, end);

  assert.match(reconcile, /withTransaction\(\(client\) => allocateChannelSnapshotDispatchOutbox/);
  assert.match(reconcile, /expectedGeneration: generation/);
  assert.match(reconcile, /dispatch_generation: nextGeneration/);
  assert.match(reconcile, /`g\$\{nextGeneration\}`/);
  assert.doesNotMatch(reconcile, /\.remove\(/);
  assert.doesNotMatch(reconcile, /queues\[queuesByRole\.channelCrawl\]\.add/);
});
