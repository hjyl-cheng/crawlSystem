import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { loadLatestContentEnrichOperational } from "./contentEnrichOperational.js";

test("Dashboard exposes the latest persisted Content Enrich operational snapshot", async () => {
  const snapshot = {
    observed_at: "2026-08-23T11:59:30.000Z",
    task_backlog: 46_103,
    alerts: { active: [{ code: "content_enrich_backlog_high" }] },
  };
  const queryDb = async () => ({
    rows: [{
      operational: snapshot,
      created_at: "2026-08-23T11:59:31.000Z",
    }],
  });

  const result = await loadLatestContentEnrichOperational(queryDb, {
    now: () => new Date("2026-08-23T12:00:00.000Z"),
    staleAfterMs: 120_000,
  });

  assert.deepEqual(result, {
    status: "alerting",
    sampled_at: "2026-08-23T11:59:31.000Z",
    snapshot,
  });
  assert.deepEqual(
    await loadLatestContentEnrichOperational(async () => ({ rows: [] })),
    { status: "unavailable", sampled_at: null, snapshot: null },
  );
});

test("Dashboard health and JSON API consume the persisted Content Enrich snapshot", async () => {
  const source = await readFile(new URL("./server.js", import.meta.url), "utf8");

  assert.match(source, /loadLatestContentEnrichOperational/);
  assert.match(source, /app\.get\("\/api\/content-enrich\/operational"/);
  assert.match(source, /content_enrich:\s*await latestContentEnrichOperational\(\)/);
});
