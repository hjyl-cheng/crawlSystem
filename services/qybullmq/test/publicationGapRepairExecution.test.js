import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  completeAboutOnlyPublicationGapRepair,
  deferAboutObservationUntilRepairFinalize,
  isAboutOnlyPublicationGapRepair,
  publicationGapRepairJobIntent,
} from "../src/publicationGapRepairExecution.js";

function aboutOnlyJob(overrides = {}) {
  return {
    run_id: "run:promotion",
    candidate_id: 42,
    publication_gap_domains: ["channel"],
    publication_gap_root_run_id: "run:promotion",
    require_complete_about_metrics: true,
    publication_gap_scope: "about_only",
    ...overrides,
  };
}

test("only an explicitly scoped and internally consistent Publication Gap is About-only", () => {
  assert.equal(isAboutOnlyPublicationGapRepair(aboutOnlyJob()), true);
  assert.equal(isAboutOnlyPublicationGapRepair(aboutOnlyJob({
    publication_gap_scope: "full",
  })), false);
  assert.equal(isAboutOnlyPublicationGapRepair(aboutOnlyJob({
    repair_parent_run_id: "run:parent",
  })), false);
  assert.equal(isAboutOnlyPublicationGapRepair(aboutOnlyJob({
    publication_gap_root_run_id: "run:other",
  })), false);
});

test("a Publication Gap Child keeps one Promotion identity and rejects partial evidence", () => {
  assert.deepEqual(publicationGapRepairJobIntent({
    repair_parent_run_id: "run:promotion",
    publication_gap_domains: ["video", "channel", "video"],
    publication_gap_root_run_id: "run:promotion",
    require_complete_about_metrics: true,
  }), {
    strategy: "child_run",
    rootRunId: "run:promotion",
    parentRunId: "run:promotion",
    domains: ["channel", "video"],
    requiresAbout: true,
    scope: null,
  });
  assert.throws(() => publicationGapRepairJobIntent({
    repair_parent_run_id: "run:first-child",
    publication_gap_domains: ["channel"],
    publication_gap_root_run_id: "run:promotion",
    require_complete_about_metrics: true,
  }), /parent_run_id must equal/);
  assert.throws(() => publicationGapRepairJobIntent({
    repair_parent_run_id: "run:promotion",
    publication_gap_root_run_id: "run:promotion",
  }), /publication_gap_domains is required/);
});

test("About-only Gap Repair stages its new About Observation for the same Finalize", () => {
  assert.equal(deferAboutObservationUntilRepairFinalize(aboutOnlyJob(), {}), true);
  assert.equal(deferAboutObservationUntilRepairFinalize({}, {
    publication_repair: { batch_id: "full-repair" },
  }), true);
  assert.equal(deferAboutObservationUntilRepairFinalize({
    run_id: "run:ordinary",
  }, {}), false);
});

test("About-only completion accepts a narrowly proven legacy partial and never rewrites videos", async () => {
  const calls = [];
  const finalizeCalls = [];
  const aboutObservationCommand = {
    idempotencyKey: "about:run:promotion:channel-attempt:3",
    channelId: "UCpromotion",
    runId: "run:promotion",
    observedAt: "2026-08-16T10:00:00.000Z",
    about: { outcome: "complete" },
    current: {},
  };
  const result = await completeAboutOnlyPublicationGapRepair(async (sql, params) => {
    calls.push({ sql: String(sql), params });
    return { rowCount: 1, rows: [{ expected_content_count: 30 }] };
  }, {
    jobData: aboutOnlyJob(),
    runId: "run:promotion",
    channelId: "UCpromotion",
    aboutOutcome: "complete",
    aboutObservationCommand,
    enqueueFinalize: async (command) => finalizeCalls.push(command),
  });

  assert.equal(calls.length, 1);
  assert.doesNotMatch(calls[0].sql, /(?:INSERT|UPDATE)\s+(?:INTO\s+)?crawler\.(?:content_candidates|contents)/i);
  assert.match(calls[0].sql, /publication_finalized_status='ready_partial'/);
  assert.match(calls[0].sql, /publication_initial_package/);
  assert.match(calls[0].sql, /initial_observations,outcomes,about/);
  assert.match(calls[0].sql, /unavailable_candidate_count/);
  assert.match(calls[0].sql, /classified_content_count/);
  assert.match(calls[0].sql, /pending_initial_about_observation/);
  assert.deepEqual(calls[0].params, [
    "run:promotion",
    "UCpromotion",
    42,
    "complete",
    JSON.stringify(aboutObservationCommand),
  ]);
  assert.deepEqual(finalizeCalls, [{
    channelId: "UCpromotion",
    runId: "run:promotion",
    reason: "publication-gap-about-only",
  }]);
  assert.deepEqual(result, {
    scope: "about_only",
    candidate_count: 30,
    about_outcome: "complete",
  });
});

test("the About-only branch runs before Uploads are fetched or persisted", async () => {
  const source = await readFile(new URL("../src/pipelineV2.js", import.meta.url), "utf8");
  const branch = source.indexOf("completeAboutOnlyPublicationGapRepair(");
  const uploadsFetch = source.indexOf("let uploads;", branch);
  const candidateInsert = source.indexOf("INSERT INTO crawler.content_candidates", branch);
  assert.notEqual(branch, -1);
  assert.ok(uploadsFetch > branch);
  assert.ok(candidateInsert > uploadsFetch);
});
