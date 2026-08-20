import assert from "node:assert/strict";
import test from "node:test";
import {
  activeFinalRepairExclusions,
  loadPublicationGapRepairCandidates,
  mergeFinalRepairCandidates,
  publicationGapRepairTarget,
} from "../src/finalizeRecoveryPolicy.js";

test("an active multi-round Publication Gap excludes its immutable Promotion root", () => {
  assert.deepEqual(activeFinalRepairExclusions([{
    data: {
      run_id: "run:second-child",
      repair_parent_run_id: "run:promotion",
      publication_gap_root_run_id: "run:promotion",
    },
  }]), {
    runIds: ["run:second-child", "run:promotion"],
    publicationGapRootRunIds: ["run:promotion"],
  });
});

test("a legacy About-only partial resumes its immutable Promotion Run", () => {
  assert.deepEqual(publicationGapRepairTarget({
    run_id: "run:partial",
    candidate_id: "42",
    publication_finalized_status: "ready_partial",
    repair_domains: ["channel"],
  }), {
    strategy: "resume_run",
    run_id: "run:partial",
    candidate_id: 42,
    publication_gap_domains: ["channel"],
    publication_gap_root_run_id: "run:partial",
    require_complete_about_metrics: true,
    publication_gap_scope: "about_only",
  });
});

test("a frozen ready_auto gap uses a child Repair Run", () => {
  assert.deepEqual(publicationGapRepairTarget({
    run_id: "run:frozen",
    publication_finalized_status: "ready_auto",
    repair_domains: ["channel"],
  }), {
    strategy: "child_run",
    repair_parent_run_id: "run:frozen",
    publication_gap_domains: ["channel"],
    publication_gap_root_run_id: "run:frozen",
    require_complete_about_metrics: true,
  });
});

test("a later Publication Gap round stays rooted at the immutable Promotion Run", () => {
  const target = publicationGapRepairTarget({
    run_id: "run:first-child",
    publication_gap_root_run_id: "run:promotion",
    publication_finalized_status: "ready_auto",
    repair_domains: ["channel", "video"],
  });

  assert.equal(target.strategy, "child_run");
  assert.equal(target.repair_parent_run_id, "run:promotion");
  assert.equal(target.publication_gap_root_run_id, "run:promotion");
});

test("a Publication Gap cannot replace the Registry Promotion root", () => {
  assert.throws(() => publicationGapRepairTarget({
    run_id: "run:first-child",
    registry_promotion_run_id: "run:promotion",
    publication_gap_root_run_id: "run:forged",
    publication_finalized_status: "ready_auto",
    repair_domains: ["channel"],
  }), /conflicts with the immutable Promotion Run/);
});

test("a mixed Channel and Video gap cannot accidentally take the About-only shortcut", () => {
  assert.deepEqual(publicationGapRepairTarget({
    run_id: "run:mixed",
    candidate_id: "43",
    publication_finalized_status: "ready_partial",
    repair_domains: ["channel", "video"],
  }), {
    strategy: "resume_run",
    run_id: "run:mixed",
    candidate_id: 43,
    publication_gap_domains: ["channel", "video"],
    publication_gap_root_run_id: "run:mixed",
    require_complete_about_metrics: true,
  });
});

test("Publication Gap selection includes only proven source gaps without open work or an owner", async () => {
  const rows = await loadPublicationGapRepairCandidates(async (sql, params) => {
    const statement = String(sql);
    assert.match(statement, /publication_gap_repair,status/);
    assert.match(statement, /initial_observations,outcomes,about/);
    assert.match(statement, /initial_observations,outcomes,video/);
    assert.match(statement, /initial_observations,outcomes,agent/);
    assert.match(statement, /ARRAY\['channel','video'\]::text\[\]/);
    assert.match(statement, /unavailable_candidate_count/);
    assert.match(statement, /classified_content_count/);
    assert.match(statement, /missing_channel_fields/);
    assert.match(statement, /publication\.channel_stream_state/);
    assert.match(statement, /owner\.seed_status='pending'/);
    assert.match(statement, /ownership_reference->>'initial_full_run_id'/);
    assert.match(statement, /registry_promotion_run_id/);
    assert.match(statement, /publication_gap_root_run_id/);
    assert.match(statement, /content\.detail_status IN \('queued','running','failed'\)/);
    assert.match(statement, /publication_gap_repair,root_run_id/);
    assert.match(statement, /=ANY\(\$6::text\[\]\)/);
    assert.deepEqual(params, [
      "cycle-8",
      7,
      ["run:active"],
      3,
      [],
      ["run:promotion-active"],
    ]);
    return {
      rows: [{
        run_id: "run:partial",
        channel_id: "UCpartial",
        candidate_id: "42",
        publication_finalized_status: "ready_partial",
        repair_domains: ["channel"],
      }],
    };
  }, {
    pipelineCycleId: "cycle-8",
    limit: 7,
    excludedRunIds: ["run:active"],
    excludedPublicationGapRootRunIds: ["run:promotion-active"],
    maxRounds: 3,
  });

  assert.equal(rows.length, 1);
  assert.deepEqual(rows[0].repair_domains, ["channel"]);
});

test("a Publication Gap overrides a generic repair row for the same Run", () => {
  const rows = mergeFinalRepairCandidates({
    standardRows: [{ run_id: "run:partial", failed_candidates: 1 }],
    publicationGapRows: [{
      run_id: "run:partial",
      publication_finalized_status: "ready_partial",
      repair_domains: ["channel"],
    }],
    limit: 5,
  });
  assert.equal(rows.length, 1);
  assert.equal(rows[0].publication_gap, true);
  assert.equal(rows[0].failed_candidates, 0);
});

test("bounded recovery gives explicit Publication Gaps capacity before generic repairs", () => {
  const rows = mergeFinalRepairCandidates({
    standardRows: [
      { run_id: "run:standard-1" },
      { run_id: "run:standard-2" },
    ],
    publicationGapRows: [{
      run_id: "run:gap",
      publication_finalized_status: "ready_auto",
      repair_domains: ["channel"],
    }],
    limit: 2,
  });
  assert.deepEqual(rows.map((row) => row.run_id), ["run:gap", "run:standard-1"]);
});
