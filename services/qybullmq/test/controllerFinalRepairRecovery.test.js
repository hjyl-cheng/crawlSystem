import assert from "node:assert/strict";
import test from "node:test";
import { finalRepairCandidateSql } from "../src/finalRepairCandidatePolicy.js";

test("automatic final repair retries unknown access without admitting terminal content", async () => {
  const aliased = finalRepairCandidateSql("cc");
  const unaliased = finalRepairCandidateSql();
  assert.match(aliased, /cc\.detail_status='unavailable'/);
  assert.match(aliased, /access,access_status/);
  assert.match(unaliased, /detail_status='unavailable'/);
  assert.match(aliased, /NOT IN \('members_only','private','unlisted','unavailable'\)/);
});

test("the Controller merges Publication Gap repair candidates into bounded final recovery", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  assert.match(source, /loadPublicationGapRepairCandidates/);
  assert.match(source, /mergeFinalRepairCandidates/);
  assert.match(source, /publicationGapRepairTarget/);
  assert.match(source, /publicationOpen/);
});

test("the Controller recovers an unexecuted Detail Repair without consuming another round", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  assert.match(source, /recoverablePreparedFinalDetailRepairSql/);
  assert.match(source, /recoverable_prepared_detail_candidates/);
  assert.match(source, /FinalRepairExecutionRecovery/);
  assert.match(source, /prepareDetailDispatch/);
  assert.match(source, /isBusinessComplete/);
  assert.match(source, /recoveringRecordedDetailRound/);
});

test("the Controller gives an exhausted Business Run a bounded checkpoint child", async () => {
  const { readFile } = await import("node:fs/promises");
  const source = await readFile(new URL("../src/controller.js", import.meta.url), "utf8");
  assert.match(source, /FINAL_CHECKPOINT_REPAIR_MAX_ROUNDS/);
  assert.match(source, /finalRepairRoundDecision/);
  assert.match(source, /finalRepairRoundEligibilitySql/);
  assert.match(source, /businessRunBudgetExhausted/);
  assert.match(source, /channel-checkpoint-repair/);
  assert.match(source, /forceChildRun:\s*businessRunBudgetExhausted/);
  assert.match(source, /checkpoint_target_run_id:\s*row\.run_id/);
  assert.match(source, /checkpoint_repair_round:\s*checkpointRepairRound/);
});
