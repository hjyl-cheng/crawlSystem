import assert from "node:assert/strict";
import test from "node:test";
import {
  FINALIZABLE_CHANNEL_STATUSES,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "../src/finalizePolicy.js";
import {
  loadFinalizeRecoveryCandidates,
  loadPipelineFinalizeBlockers,
} from "../src/finalizeRecoveryPolicy.js";

test("Finalize recovery is database-driven and can scan after a scheduler stops", async () => {
  const calls = [];
  const rows = await loadFinalizeRecoveryCandidates(async (sql, params) => {
    calls.push({ sql: String(sql), params });
    return {
      rows: [{
        channel_id: "UCdormant",
        run_id: "run:promotion",
        source_updated_at: "2026-07-28T12:00:00.000Z",
      }],
    };
  }, { pipelineCycleId: null, limit: 17 });

  assert.equal(rows[0].run_id, "run:promotion");
  assert.deepEqual(calls[0].params, [
    null,
    17,
    FINALIZABLE_CHANNEL_STATUSES,
    SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
  ]);
  assert.match(calls[0].sql, /channel\.status='dormant'/);
  assert.match(calls[0].sql, /run\.publication_finalized_at IS NULL/);
  assert.match(calls[0].sql, /run\.publication_finalized_status IS NULL/);
  assert.match(calls[0].sql, /finalized\.status=ANY\(\$4::text\[\]\)/);
  assert.match(calls[0].sql, /\$1::text IS NULL/);
});

test("pipeline completion counts an unfinalized dormant Promotion Run", async () => {
  const blockers = await loadPipelineFinalizeBlockers(async (sql, params) => {
    const statement = String(sql);
    assert.match(statement, /promotion_run\.publication_finalized_at IS NULL/);
    assert.match(statement, /channel\.registry_promotion_run_id IS NOT NULL/);
    assert.match(statement, /publication\.stream AS automatic_stream/);
    assert.match(statement, /promotion_candidate\.accepted_at>=automatic_stream\.capture_enabled_at/);
    assert.match(statement, /dead_letter_recovery/);
    assert.match(statement, /bool_and\(delivery\.mode='online'\)/);
    assert.match(statement, /owner\.seed_status='pending'/);
    assert.deepEqual(params, ["cycle-7", SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES]);
    return { rows: [{ agent_open: "0", final_open: "1" }] };
  }, "cycle-7");

  assert.deepEqual(blockers, { agentOpen: 0, finalOpen: 1, publicationOpen: 0 });
});
