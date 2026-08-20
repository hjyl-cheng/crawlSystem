import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyTerminalChannelError,
  markChannelRemoved,
  terminalChannelEvidenceFromInitialData,
} from "../src/channelLifecycle.js";

test("classifies explicit YouTube Community Guidelines removal", () => {
  assert.deepEqual(
    classifyTerminalChannelError(
      new Error("This channel was removed because it violated our Community Guidelines."),
    ),
    {
      failure_kind: "channel_removed",
      removed_reason: "community_guidelines",
      removed_source: "youtube_alert",
      evidence: "This channel was removed because it violated our Community Guidelines.",
    },
  );
});

test("classifies an explicit nonexistent Channel separately from a ban", () => {
  assert.deepEqual(
    classifyTerminalChannelError(new Error("This channel does not exist.")),
    {
      failure_kind: "channel_removed",
      removed_reason: "channel_not_found",
      removed_source: "youtube_alert",
      evidence: "This channel does not exist.",
    },
  );
});

test("reads terminal evidence from the structured YouTube ERROR alert", () => {
  const removed = terminalChannelEvidenceFromInitialData({
    alerts: [{
      alertRenderer: {
        type: "ERROR",
        text: {
          simpleText: "This channel was removed because it violated our Community Guidelines.",
        },
      },
    }],
  });
  const missing = terminalChannelEvidenceFromInitialData({
    alerts: [{
      alertRenderer: {
        type: "ERROR",
        text: { runs: [{ text: "This channel does not exist." }] },
      },
    }],
  });

  assert.equal(removed?.removed_reason, "community_guidelines");
  assert.equal(missing?.removed_reason, "channel_not_found");
  assert.equal(terminalChannelEvidenceFromInitialData({
    alerts: [{ alertRenderer: { type: "INFO", text: { simpleText: "This channel does not exist." } } }],
  }), null);
});

test("does not permanently remove a Channel for transient or ambiguous failures", () => {
  for (const message of [
    "request timed out",
    "proxy tunnel failed",
    "channel unavailable",
    "This page isn't available. Try again later.",
    "parser contract failed",
  ]) {
    assert.equal(classifyTerminalChannelError(new Error(message)), null, message);
  }
});

test("persists the terminal Channel lifecycle and cancels later work", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql: " ".concat(sql).trim().replace(/\s+/g, " "), params });
      if (sql.includes("publication-reconciler:find-owner")) return { rowCount: 0, rows: [] };
      return { rowCount: 1, rows: [{ channel_id: "UCremoved" }] };
    },
  };

  const result = await markChannelRemoved(client, {
    channelId: "UCremoved",
    candidateId: 42,
    runId: "run-42",
    terminal: classifyTerminalChannelError(
      new Error("This channel was removed because it violated our Community Guidelines."),
    ),
    observedAt: "2026-07-23T03:24:09.000Z",
  });

  assert.equal(result.removed, true);
  assert.equal(calls.some(({ sql }) => (
    sql.includes("UPDATE crawler.channels") && sql.includes("status='removed'")
  )), true);
  assert.equal(calls.some(({ sql }) => (
    sql.includes("UPDATE crawler.channel_candidates")
      && sql.includes("promotion.is_registry_promotion")
      && sql.includes("ELSE 'rejected'")
  )), true);
  assert.equal(calls.some(({ sql }) => (
    sql.includes("UPDATE crawler.agent_refresh_requests") && sql.includes("status='cancelled'")
  )), true);
  assert.equal(calls.some(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("status='skipped'")
  )), true);
  const runUpdate = calls.findIndex(({ sql }) => (
    sql.includes("UPDATE crawler.channel_runs") && sql.includes("status='skipped'")
  ));
  const publication = calls.findIndex(({ sql }) => sql.includes("publication-reconciler:find-owner"));
  assert.ok(runUpdate >= 0 && publication > runUpdate);
  assert.equal(result.publication.status, "not_owned");
});
