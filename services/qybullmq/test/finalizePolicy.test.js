import assert from "node:assert/strict";
import test from "node:test";
import {
  FINALIZABLE_CHANNEL_STATUSES,
  finalizeStatusCanAdvance,
  resolveFinalizeStatus,
  finalizedRunState,
  finalizedProfileIsCurrent,
  finalizeDispatchRevision,
  finalizePublicationContext,
  finalizeSourceRevision,
  isSuccessfulPublicationFinalize,
  isFinalizableChannelStatus,
  representedFinalizeRunIds,
  isCurrentChannelRun,
} from "../src/finalizePolicy.js";

test("Publication Finalize success status is defined by one shared policy", () => {
  assert.equal(isSuccessfulPublicationFinalize("ready_auto"), true);
  assert.equal(isSuccessfulPublicationFinalize("ready_partial"), true);
  assert.equal(isSuccessfulPublicationFinalize("pending_agent"), false);
});

test("Publication Finalize Channel eligibility is defined by one shared policy", () => {
  assert.deepEqual(FINALIZABLE_CHANNEL_STATUSES, ["active", "dormant"]);
  assert.equal(isFinalizableChannelStatus("active"), true);
  assert.equal(isFinalizableChannelStatus("dormant"), true);
  assert.equal(isFinalizableChannelStatus("paused"), false);
});

test("Publication Repair identity and clock come from persisted repair state", () => {
  const context = finalizePublicationContext({
    job: {
      id: "changing-finalize-job",
      data: { repair_batch_id: "batch-7" },
      timestamp: Date.parse("2026-07-27T17:00:00.000Z"),
    },
    run: {
      run_id: "run-7",
      started_at: "2026-07-27T16:00:00.000Z",
      result_json: {
        publication_repair: {
          batch_id: "batch-7",
          prepared_at: "2026-07-27T15:00:00.000Z",
        },
      },
    },
    defaultAsOf: "2026-07-27T18:00:00.000Z",
  });

  assert.deepEqual(context, {
    revisionType: "repair",
    repairId: "batch-7",
    asOf: "2026-07-27T15:00:00.000Z",
  });
});

test("automatic final Repair has a stable round identity", () => {
  const context = finalizePublicationContext({
    job: { id: "finalize-job" },
    run: {
      run_id: "run-new",
      started_at: "2026-07-27T16:00:00.000Z",
      result_json: {
        final_repair: { parent_run_id: "run-parent", rounds: 2, mode: "channel" },
      },
    },
    defaultAsOf: "2026-07-27T18:00:00.000Z",
  });

  assert.deepEqual(context, {
    revisionType: "repair",
    repairId: "final-repair:run-parent:2",
    asOf: "2026-07-27T16:00:00.000Z",
  });
});

test("About-only Publication Gap Finalize is a repair even without final_repair metadata", () => {
  const context = finalizePublicationContext({
    job: {
      id: "finalize-about-only",
      data: { reason: "publication-gap-about-only" },
    },
    run: {
      run_id: "run-about-only",
      started_at: "2026-08-17T08:33:00.000Z",
      result_json: {
        publication_gap_repair_execution: {
          scope: "about_only",
          status: "staged",
          about_outcome: "complete",
        },
      },
    },
    runId: "run-about-only",
    defaultAsOf: "2026-08-17T08:40:00.000Z",
  });

  assert.deepEqual(context, {
    revisionType: "repair",
    repairId: "publication-gap-about-only:run-about-only",
    asOf: "2026-08-17T08:33:00.000Z",
  });
});

test("ordinary Finalize keeps the source completion clock", () => {
  assert.deepEqual(finalizePublicationContext({
    job: { id: "ordinary-finalize" },
    run: { run_id: "run-ordinary", result_json: {} },
    defaultAsOf: "2026-07-27T18:00:00.000Z",
  }), {
    revisionType: "incremental",
    repairId: null,
    asOf: "2026-07-27T18:00:00.000Z",
  });
});

function fixture() {
  return {
    channel: {
      channel_id: "UC1",
      latest_run_id: "run-1",
      status: "active",
      agent_status: "done",
      updated_at: "2026-07-11T00:00:00Z",
    },
    run: {
      run_id: "run-1",
      status: "waiting_agent",
      detail_status: "done",
      expected_content_count: 1,
      updated_at: "2026-07-11T00:00:01Z",
    },
    candidates: [{
      candidate_id: 1,
      content_type: "video",
      content_key: "UC1:video:v1",
      detail_status: "done",
      api_status: "not_needed",
      missing_fields: [],
      updated_at: "2026-07-11T00:00:02Z",
    }],
    contents: [{
      content_key: "UC1:video:v1",
      content_type: "video",
      source_content_id: "v1",
      last_enriched_at: "2026-07-11T00:00:02Z",
    }],
    agent: {
      status: "success",
      prompt_hash: "prompt-1",
      attempts: 1,
      updated_at: "2026-07-11T00:00:03Z",
    },
  };
}

test("finalize revision changes only when profile source data changes", () => {
  const first = fixture();
  const sameSource = structuredClone(first);
  sameSource.run.status = "done";
  sameSource.run.updated_at = "2026-07-11T00:01:00Z";
  assert.equal(finalizeSourceRevision(first), finalizeSourceRevision(sameSource));

  const changed = structuredClone(first);
  changed.candidates[0].missing_fields = ["view_count"];
  changed.candidates[0].updated_at = "2026-07-11T00:01:00Z";
  assert.notEqual(finalizeSourceRevision(first), finalizeSourceRevision(changed));

  const enriched = structuredClone(first);
  enriched.contents[0].last_enriched_at = "2026-07-11T00:01:00Z";
  assert.notEqual(finalizeSourceRevision(first), finalizeSourceRevision(enriched));
});

test("finalize accepts only the channel latest run", () => {
  assert.equal(isCurrentChannelRun({ latest_run_id: "run-1" }, "run-1"), true);
  assert.equal(isCurrentChannelRun({ latest_run_id: "run-2" }, "run-1"), false);
  assert.equal(isCurrentChannelRun({ latest_run_id: null }, "run-1"), false);
});

test("existing finalized profile is reusable only for the same run and revision", () => {
  const sourceRevision = finalizeSourceRevision(fixture());
  const existing = { run_id: "run-1", quality_json: { source_revision: sourceRevision } };
  assert.equal(finalizedProfileIsCurrent(existing, "run-1", sourceRevision), true);
  assert.equal(finalizedProfileIsCurrent(existing, "run-2", sourceRevision), false);
  assert.equal(finalizedProfileIsCurrent(existing, "run-1", "different"), false);
});

test("a later complete source observation invalidates a ready_partial Finalize", () => {
  const sourceRevision = finalizeSourceRevision(fixture());
  const existing = {
    run_id: "run-1",
    quality_json: {
      source_revision: sourceRevision,
      initial_observations: { outcomes: { about: "complete", video: "partial", agent: "complete" } },
    },
  };
  assert.equal(finalizedProfileIsCurrent(existing, "run-1", sourceRevision, {
    about: "complete",
    video: "partial",
    agent: "complete",
  }), true);
  assert.equal(finalizedProfileIsCurrent(existing, "run-1", sourceRevision, {
    about: "complete",
    video: "complete",
    agent: "complete",
  }), false);
});

test("dispatch revision is stable for an unchanged queue source snapshot", () => {
  const state = { channel_id: "UC1", run_id: "run-1", candidate_count: 30 };
  assert.equal(finalizeDispatchRevision(state), finalizeDispatchRevision(structuredClone(state)));
  assert.notEqual(finalizeDispatchRevision(state), finalizeDispatchRevision({ ...state, candidate_count: 31 }));
});

test("Finalize reconciliation recognizes every in-flight run exactly once", () => {
  assert.deepEqual(representedFinalizeRunIds([
    { data: { run_id: "run-2" } },
    { data: { run_id: "run-1" } },
    { data: { run_id: "run-2" } },
    { data: { run_id: "" } },
    { data: {} },
  ]), ["run-1", "run-2"]);
});

test("finalize status is monotonic for the same channel run", () => {
  assert.equal(finalizeStatusCanAdvance("pending_agent", "ready_partial"), true);
  assert.equal(finalizeStatusCanAdvance("ready_partial", "ready_auto"), true);
  assert.equal(finalizeStatusCanAdvance("ready_partial", "pending_detail"), false);
  assert.equal(finalizeStatusCanAdvance("ready_auto", "ready_partial"), false);
  assert.equal(finalizeStatusCanAdvance("ready_auto", "pending_agent"), false);
  assert.equal(finalizeStatusCanAdvance("ready_auto", "pending_detail", false), true);
});

test("dormant Finalize is publishable as partial without an Agent", () => {
  assert.equal(resolveFinalizeStatus({
    channelStatus: "dormant",
    hasAgent: false,
    missingAgentFieldCount: 9,
  }), "ready_partial");
  assert.equal(resolveFinalizeStatus({
    channelStatus: "dormant",
    openDetailCount: 1,
  }), "pending_detail");
  assert.equal(resolveFinalizeStatus({
    channelStatus: "active",
    hasAgent: false,
  }), "pending_agent");
});

test("an incomplete source observation cannot be finalized as ready_auto", () => {
  assert.equal(resolveFinalizeStatus({
    channelStatus: "active",
    hasAgent: true,
    incompleteSourceObservationCount: 1,
  }), "ready_partial");
});

test("finalized profile status determines the matching channel run state", () => {
  assert.deepEqual(finalizedRunState("ready_auto"), { status: "done", terminal: true });
  assert.deepEqual(finalizedRunState("ready_partial"), { status: "done", terminal: true });
  assert.deepEqual(finalizedRunState("pending_agent"), { status: "waiting_agent", terminal: false });
  assert.deepEqual(finalizedRunState("pending_api"), { status: "waiting_detail", terminal: false });
});
