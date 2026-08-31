import assert from "node:assert/strict";
import test from "node:test";

import {
  finalizeDispatchRevision,
  finalizeSourceRevision,
} from "../src/finalizePolicy.js";
import {
  finalizeDispatchStateFromSource,
  lockFinalizeCommitSource,
} from "../src/finalizeSourceFence.js";

function sourceFixture() {
  return {
    channel: {
      channel_id: "UC-finalize-fence",
      latest_run_id: "run-finalize-fence",
      status: "active",
      agent_status: "done",
      updated_at: new Date("2026-08-31T10:00:00.000Z"),
    },
    run: {
      run_id: "run-finalize-fence",
      channel_id: "UC-finalize-fence",
      detail_status: "done",
      expected_content_count: 1,
      result_json: {
        pipeline_cycle_id: "batch-finalize-fence",
        final_repair: { rounds: 2 },
      },
    },
    candidates: [{
      candidate_id: 91,
      run_id: "run-finalize-fence",
      channel_id: "UC-finalize-fence",
      source_content_id: "video-finalize-fence",
      position: 1,
      detail_status: "done",
      api_status: "not_needed",
      missing_fields: [],
      updated_at: new Date("2026-08-31T10:01:00.000Z"),
    }],
    contents: [{
      content_key: "UC-finalize-fence:video:video-finalize-fence",
      run_id: "run-finalize-fence",
      channel_id: "UC-finalize-fence",
      source_content_id: "video-finalize-fence",
      content_type: "video",
      position: 1,
      description: "current description",
      last_seen_at: new Date("2026-08-31T10:02:00.000Z"),
      last_enriched_at: null,
    }],
    agent: {
      channel_id: "UC-finalize-fence",
      agent_mode: "basic",
      status: "success",
      metrics_json: { audience_profile_agent: { creator_language: { value: "Portuguese" } } },
      updated_at: new Date("2026-08-31T10:03:00.000Z"),
    },
  };
}

function result(rows) {
  return { rowCount: rows.length, rows };
}

function fakeClient(source, events) {
  return {
    async query(sql) {
      const statement = String(sql);
      if (statement.includes("publication-channel-mutation-lock:transaction-guard")) {
        events.push("publication-savepoint");
        return result([]);
      }
      if (statement === "RELEASE SAVEPOINT publication_channel_mutation_lock_guard") {
        events.push("publication-savepoint-release");
        return result([]);
      }
      if (statement.includes("publication-channel-mutation-lock:channel")) {
        events.push("publication-advisory");
        return result([{}]);
      }
      if (statement.includes("finalize-source-fence:run")) {
        events.push("run");
        return result([source.run]);
      }
      if (statement.includes("finalize-source-fence:channel")) {
        events.push("channel");
        return result([source.channel]);
      }
      if (statement.includes("finalize-source-fence:candidates")) {
        events.push("candidates");
        return result(source.candidates);
      }
      if (statement.includes("finalize-source-fence:contents")) {
        events.push("contents");
        return result(source.contents);
      }
      if (statement.includes("finalize-source-fence:agent")) {
        events.push("agent");
        return result(source.agent ? [source.agent] : []);
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };
}

test("Finalize commit obtains the Publication lock before its guard and complete source locks", async () => {
  const source = sourceFixture();
  const events = [];
  const accepted = await lockFinalizeCommitSource(fakeClient(source, events), {
    channelId: source.channel.channel_id,
    runId: source.run.run_id,
    expectedSourceRevision: finalizeSourceRevision(source),
    expectedDispatchRevision: finalizeDispatchRevision(finalizeDispatchStateFromSource(source)),
    transactionGuard: async () => {
      events.push("guard");
      return true;
    },
  });

  assert.equal(accepted.accepted, true);
  assert.deepEqual(events, [
    "publication-savepoint",
    "publication-savepoint-release",
    "publication-advisory",
    "guard",
    "run",
    "channel",
    "contents",
    "candidates",
    "agent",
  ]);
});

test("the queue projection and complete source produce the same canonical dispatch revision", () => {
  const source = sourceFixture();
  const queueRow = {
    content_updated_at: source.contents[0].last_seen_at,
    candidate_count: 1,
    run_final_repair: { rounds: 2 },
    channel_id: source.channel.channel_id,
    channel_updated_at: new Date("2026-08-31T11:00:00.000Z"),
    expected_content_count: 1,
    agent_status: "done",
    latest_run_id: source.run.run_id,
    candidate_updated_at: source.candidates[0].updated_at,
    pipeline_cycle_id: "batch-finalize-fence",
    content_count: 1,
    detail_status: "done",
    channel_status: "active",
    agent_updated_at: source.agent.updated_at,
  };

  assert.equal(
    finalizeDispatchRevision(queueRow),
    finalizeDispatchRevision(finalizeDispatchStateFromSource(source)),
  );
});

test("Finalize commit rejects either a stale build source or stale dispatch source", async () => {
  const source = sourceFixture();
  const dispatchRevision = finalizeDispatchRevision(finalizeDispatchStateFromSource(source));
  const staleBuild = await lockFinalizeCommitSource(fakeClient(source, []), {
    channelId: source.channel.channel_id,
    runId: source.run.run_id,
    expectedSourceRevision: "stale-build",
    expectedDispatchRevision: dispatchRevision,
  });
  assert.deepEqual(staleBuild, {
    accepted: false,
    reason: "source_revision_stale",
    source: staleBuild.source,
  });

  const staleDispatch = await lockFinalizeCommitSource(fakeClient(source, []), {
    channelId: source.channel.channel_id,
    runId: source.run.run_id,
    expectedSourceRevision: finalizeSourceRevision(source),
    expectedDispatchRevision: "stale-dispatch",
  });
  assert.deepEqual(staleDispatch, {
    accepted: false,
    reason: "dispatch_revision_stale",
    source: staleDispatch.source,
  });
});
