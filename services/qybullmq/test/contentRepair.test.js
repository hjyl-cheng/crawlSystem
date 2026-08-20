import test from "node:test";
import assert from "node:assert/strict";

import {
  enqueueContentRepairTargets,
  hasPendingContentRepairs,
  loadContentRepairTargets,
  prepareContentRepairTargets,
} from "../src/contentRepair.js";
import { queuesByRole } from "../src/queues.js";

test("content repair targets use the configured publication window", async () => {
  const statements = [];
  const parameters = [];
  const dbQuery = async (sql, params = []) => {
    statements.push(sql);
    parameters.push(params);
    return { rows: [] };
  };

  const targets = await loadContentRepairTargets(dbQuery, { pipelineCycleId: "pipeline:test" });

  assert.deepEqual(targets, {
    detailRows: [],
    detailRuns: [],
    channelRuns: [],
    staleRuns: [],
  });
  assert.match(statements[0], /content_max_age_days/);
  assert.match(statements[0], /description_status NOT IN/);
  assert.match(statements[0], /c\.access_status<>'unlisted'/);
  assert.match(statements[0], /candidate_content\.access_status/);
  assert.match(statements[0], /candidate_content\.published_at>=now\(\)/);
  assert.match(statements[0], /pipeline_cycle_id/);
  assert.match(statements[0], /NOT \(current_run\.result_json \? 'parser_contract_error'\)/);
  assert.match(statements[0], /NOT \(cc\.result_json \? 'parser_contract_error'\)/);
  assert.match(statements[0], /cc\.detail_status<>'api_pending'/);
  assert.match(statements[0], /crawler\.youtube_api_tasks/);
  assert.match(statements[0], /api_candidate\.run_id=cc\.run_id/);
  assert.match(statements[0], /task\.status IN \('pending','queued','running','failed'\)/);
  assert.match(statements[1], /NOT \(cr\.result_json \? 'parser_contract_error'\)/);
  assert.match(statements[2], /NOT \(cr\.result_json \? 'parser_contract_error'\)/);
  assert.doesNotMatch(statements[0], /c\.is_recent=true/);
  assert.equal(parameters[0][3], "pipeline:test");
});

test("pending repair detection uses the same publication window", async () => {
  let statement = "";
  const pending = await hasPendingContentRepairs(async (sql, params) => {
    statement = sql;
    assert.equal(params[1], "pipeline:test");
    return { rows: [{ pending: false }] };
  }, undefined, "pipeline:test");

  assert.equal(pending, false);
  assert.match(statement, /content_max_age_days/);
  assert.match(statement, /description_status NOT IN/);
  assert.match(statement, /candidate_content\.published_at>=now\(\)/);
  assert.match(statement, /pipeline_cycle_id/);
  assert.match(statement, /NOT \(current_run\.result_json \? 'parser_contract_error'\)/);
  assert.match(statement, /NOT \(cc\.result_json \? 'parser_contract_error'\)/);
  assert.doesNotMatch(statement, /c\.is_recent=true/);
});

test("positive comment counts without a first page remain content repair targets", async () => {
  const targetStatements = [];
  await loadContentRepairTargets(async (sql) => {
    targetStatements.push(sql);
    return { rows: [] };
  }, { pipelineCycleId: "pipeline:test" });

  let pendingStatement = "";
  await hasPendingContentRepairs(async (sql) => {
    pendingStatement = sql;
    return { rows: [{ pending: false }] };
  }, undefined, "pipeline:test");

  for (const statement of [targetStatements[0], pendingStatement]) {
    assert.match(statement, /comments_first_page IS NULL/);
    assert.match(statement, /comments_disabled IS DISTINCT FROM true/);
    assert.match(statement, /comment_count > 0/);
  }
});

test("repair state resets retain parser contract failures", async () => {
  const statements = [];
  const dbQuery = async (sql) => {
    statements.push(sql);
    return { rows: [] };
  };
  await prepareContentRepairTargets(dbQuery, {
    detailRows: [{ candidate_id: 11 }],
    detailRuns: [{ run_id: "run:detail" }],
    channelRuns: [{ run_id: "run:channel" }],
    staleRuns: [{ run_id: "run:stale" }],
  }, { batchId: "repair:test" });

  const candidateReset = statements.find((sql) => /UPDATE crawler\.content_candidates/.test(sql));
  const runResets = statements.filter((sql) => /UPDATE crawler\.channel_runs/.test(sql));
  assert.match(candidateReset, /NOT \((?:candidate\.)?result_json \? 'parser_contract_error'\)/);
  assert.equal(runResets.length, 3);
  for (const statement of runResets) {
    assert.match(statement, /NOT \(result_json \? 'parser_contract_error'\)/);
    assert.match(statement, /publication_repair/);
  }
  assert.equal(
    statements.some((sql) => /UPDATE crawler\.youtube_api_tasks/.test(sql)),
    false,
    "scrape repair must not cancel a pending API fallback",
  );
});

test("a newly prepared repair replaces a terminal job with the same id", async () => {
  let removed = false;
  const added = [];
  const existing = {
    getState: async () => "completed",
    remove: async () => {
      removed = true;
    },
  };
  const queue = {
    getJob: async () => (removed ? null : existing),
    add: async (name, data, options) => {
      added.push({ name, data, options });
    },
  };
  const queues = {
    [queuesByRole.channelCrawl]: queue,
    [queuesByRole.finalize]: { getJob: async () => null, add: async () => {} },
  };

  const result = await enqueueContentRepairTargets(async () => ({ rows: [] }), queues, {
    detailRuns: [{ run_id: "run:detail", channel_id: "UCdetail" }],
    channelRuns: [],
    staleRuns: [],
  }, {
    batchId: "repair:second-pass",
    pipelineCycleId: "pipeline:test",
  });

  assert.equal(removed, true);
  assert.equal(added.length, 1);
  assert.equal(added[0].name, "channel-detail-repair");
  assert.equal(added[0].data.api_fallback_mode, "emergency");
  assert.equal(result.detail, 1);
});
