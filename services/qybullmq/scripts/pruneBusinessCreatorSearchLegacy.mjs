#!/usr/bin/env node
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { environmentValue } from "../src/runtimeEnvironment.js";
import { planLegacySearchPrune, pruneLegacySearchBatch } from "../src/businessCreatorSearchLegacyPruner.js";

// Default is a read-only, fixed inventory. Apply requires the exact file digest.
const apply = process.argv.includes("--apply");
const fileIndex = process.argv.indexOf("--plan-file");
assert.ok(fileIndex > 0 && process.argv[fileIndex + 1], "--plan-file is required");
assert.ok(process.argv.slice(2).every((v, i, args) =>
  v === "--apply" || v === "--plan-file" || args[i - 1] === "--plan-file"), "Unknown argument");
const planFile = process.argv[fileIndex + 1];
const database = process.env.EXPECTED_BUSINESS_DATABASE;
assert.ok(database, "EXPECTED_BUSINESS_DATABASE is required");
const client = new pg.Client({
  connectionString: environmentValue("BUSINESS_ADMIN_DATABASE_URL"),
  application_name: "business-creator-search-legacy-pruner",
  options: "-c jit=off -c statement_timeout=120000",
});
const digest = (bytes) => createHash("sha256").update(bytes).digest("hex");
const emit = (event) => console.log(JSON.stringify({ at: new Date().toISOString(), ...event }));
let stopping = false;
process.once("SIGTERM", () => { stopping = true; });
process.once("SIGINT", () => { stopping = true; });
await client.connect();
try {
  if (!apply) {
    const plan = await planLegacySearchPrune(client, database);
    const bytes = `${JSON.stringify(plan, null, 2)}\n`;
    await writeFile(planFile, bytes, { flag: "wx" });
    emit({ event: "prune_plan", database, delete_rows: plan.delete_rows,
      versions: plan.candidates.length, retained: plan.retained, sha256: digest(bytes) });
  } else {
    const bytes = await readFile(planFile);
    assert.equal(process.env.CONFIRM_SEARCH_PRUNE_PLAN_SHA256, digest(bytes), "Exact plan SHA256 required");
    const plan = JSON.parse(bytes);
    const actor = process.env.PUBLICATION_OPERATOR;
    const reason = process.env.PUBLICATION_ACTION_REASON;
    assert.ok(actor?.trim() && reason?.trim(), "Operator and action reason are required");
    let deleted = 0;
    let batches = 0;
    let busyCount = 0;
    const started = Date.now();
    emit({ event: "prune_started", database, planned_rows: plan.delete_rows, versions: plan.candidates.length });
    for (const candidate of plan.candidates) {
      let remaining = candidate.row_count;
      while (remaining > 0 && !stopping) {
        const before = Date.now();
        const result = await pruneLegacySearchBatch(client, {
          plan, database, watermark: candidate.watermark, expectedRemaining: remaining,
          batchSize: 5000, actor, reason,
        });
        if (result.outcome === "busy") {
          busyCount += 1;
          assert.ok(busyCount <= 120, "Publisher has priority; stop after 120 busy observations");
          await new Promise((resolve) => setTimeout(resolve, 1000));
          continue;
        }
        busyCount = 0;
        remaining = result.remaining;
        deleted += result.deleted;
        batches += 1;
        // Every committed batch has a matching database audit; stdout also
        // permits precise reconciliation if the process stops between commits.
        emit({ event: "prune_batch", watermark: candidate.watermark, ...result,
          elapsed_ms: Date.now() - before, batches, total_deleted: deleted });
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      if (stopping) break;
    }
    if (stopping) {
      emit({ event: "prune_stopped", deleted, batches });
      process.exitCode = 2;
    } else {
      assert.equal(deleted, plan.delete_rows, "Deleted total must match fixed inventory");
      emit({ event: "prune_complete", deleted, batches, elapsed_ms: Date.now() - started });
    }
  }
} finally {
  await client.end();
}
