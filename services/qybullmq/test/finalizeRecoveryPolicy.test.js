import assert from "node:assert/strict";
import test from "node:test";
import {
  FINALIZABLE_CHANNEL_STATUSES,
  SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES,
} from "../src/finalizePolicy.js";
import {
  hasOpenPipelineCrawlerWork,
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
  assert.match(calls[0].sql, /content\.channel_id=channel\.channel_id AND content\.run_id=run\.run_id/);
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

test("a Batch-scoped system failure does not leave its materialized Channel Run open", async () => {
  const batchId = "legacy-results-canary-1788072676068-25df5b5e";
  const open = await hasOpenPipelineCrawlerWork(async (sql, params) => {
    const statement = String(sql);
    assert.deepEqual(params, [batchId, true]);
    assert.match(statement, /FROM crawler\.channel_runs run/);
    assert.match(statement, /FROM crawler\.migration_system_retry_items retry/);
    assert.match(statement, /retry\.candidate_id=run\.candidate_id/);
    assert.match(statement, /retry\.failed_dispatch_batch_id=\$1/);
    return {
      rows: [{
        open_query_pages: false,
        open_channel_runs: false,
        open_channel_candidates: false,
      }],
    };
  }, batchId);

  assert.equal(open, false);
});

test("a Batch-scoped system failure does not block Agent or Publication settlement", async () => {
  const batchId = "legacy-results-canary-1788072676068-25df5b5e";
  const blockers = await loadPipelineFinalizeBlockers(async (sql, params) => {
    const statement = String(sql);
    assert.deepEqual(params, [batchId, SUCCESSFUL_PUBLICATION_FINALIZE_STATUSES]);
    assert.match(statement, /FROM crawler\.migration_system_retry_items retry/);
    assert.match(statement, /retry\.candidate_id=current_run\.candidate_id/);
    assert.match(statement, /retry\.failed_dispatch_batch_id=\$1/);
    return { rows: [{ agent_open: 0, final_open: 0, publication_open: 0 }] };
  }, batchId);

  assert.deepEqual(blockers, { agentOpen: 0, finalOpen: 0, publicationOpen: 0 });
});
