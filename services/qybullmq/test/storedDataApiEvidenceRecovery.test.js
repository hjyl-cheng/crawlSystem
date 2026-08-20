import assert from "node:assert/strict";
import test from "node:test";

import {
  STORED_DATA_API_EVIDENCE_TARGET_SQL,
  recoverStoredDataApiEvidence,
} from "../src/storedDataApiEvidenceRecovery.js";
import { STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID } from
  "../src/youtubeDataApiEvidence.js";

const runId = "run:parent";

function target(overrides = {}) {
  return {
    candidate_id: "501",
    run_id: runId,
    channel_id: "UC1",
    source_content_id: "video-1",
    detail_status: "unavailable",
    api_status: "unavailable",
    missing_fields: ["access_status"],
    error_message: "content access unknown after detail and api",
    candidate_result_json: {
      access: { access_status: "unknown" },
      api_detail: { privacy_status: "public" },
    },
    task_id: "91",
    task_status: "unavailable",
    task_candidate_ids: [501],
    task_missing_fields: ["access_status"],
    task_result_json: {
      title: "Recovered video",
      privacy_status: "public",
      source: "youtube_data_api_videos_list",
      api_verification: { videos_list: { returned: true } },
    },
    ...overrides,
  };
}

function database(rows = [target()]) {
  const calls = [];
  const query = async (sql, params) => {
    calls.push({ sql, params });
    if (sql === STORED_DATA_API_EVIDENCE_TARGET_SQL) {
      return { rows, rowCount: rows.length };
    }
    throw new Error(`unexpected SQL outside transaction: ${sql}`);
  };
  const transactionCalls = [];
  const withTransaction = async (action) => action({
    async query(sql, params) {
      transactionCalls.push({ sql, params });
      if (sql === STORED_DATA_API_EVIDENCE_TARGET_SQL) {
        return { rows, rowCount: rows.length };
      }
      if (/UPDATE crawler\.youtube_api_tasks/.test(sql)) {
        return { rows: [{ task_id: 91 }], rowCount: 1 };
      }
      if (/UPDATE crawler\.content_candidates/.test(sql)) {
        return { rows: [{ candidate_id: 501 }], rowCount: 1 };
      }
      return { rows: [{}], rowCount: 1 };
    },
  });
  return { calls, query, transactionCalls, withTransaction };
}

test("stored evidence selector is restricted to one Run and authoritative returned public evidence", () => {
  assert.match(STORED_DATA_API_EVIDENCE_TARGET_SQL, /candidate\.run_id=\$1/);
  assert.match(STORED_DATA_API_EVIDENCE_TARGET_SQL, /videos_list,returned/);
  assert.match(STORED_DATA_API_EVIDENCE_TARGET_SQL, /privacy_status/);
  assert.match(STORED_DATA_API_EVIDENCE_TARGET_SQL, /access_status/);
});

test("stored evidence recovery defaults to a read-only exact preview", async () => {
  const db = database();
  const result = await recoverStoredDataApiEvidence({
    query: db.query,
    withTransaction: db.withTransaction,
    queue: { async add() { throw new Error("dry-run must not enqueue"); } },
    runId,
    apply: false,
  });

  assert.equal(result.applied, false);
  assert.equal(result.candidate_count, 1);
  assert.equal(result.task_count, 1);
  assert.equal(db.transactionCalls.length, 0);
});

test("apply requires the exact expected candidate count before any write", async () => {
  const db = database();
  await assert.rejects(
    recoverStoredDataApiEvidence({
      query: db.query,
      withTransaction: db.withTransaction,
      queue: { async add() {} },
      runId,
      expectedCount: 2,
      apply: true,
    }),
    /target count changed: expected 2, got 1/,
  );
  assert.equal(db.transactionCalls.length, 0);
});

test("apply marks the exact source evidence and enqueues a zero-request replay", async () => {
  const db = database();
  const jobs = [];
  const result = await recoverStoredDataApiEvidence({
    query: db.query,
    withTransaction: db.withTransaction,
    queue: {
      async add(name, data, options) {
        jobs.push({ name, data, options });
        return { id: options.jobId, async getState() { return "waiting"; } };
      },
    },
    runId,
    expectedCount: 1,
    apply: true,
  });

  assert.equal(result.applied, true);
  assert.equal(jobs.length, 1);
  assert.equal(jobs[0].name, "youtube-data-api-batch");
  assert.equal(
    jobs[0].data.stored_evidence_replay.operation_id,
    STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID,
  );
  assert.deepEqual(jobs[0].data.stored_evidence_replay.task_ids, [91]);
  const taskUpdate = db.transactionCalls.find(({ sql }) => (
    /UPDATE crawler\.youtube_api_tasks/.test(sql)
  ));
  const marker = JSON.parse(taskUpdate.params[2]);
  assert.equal(marker.operation_id, STORED_DATA_API_EVIDENCE_REPLAY_OPERATION_ID);
  assert.equal(marker.external_request_count, 0);
});

test("recovery rejects task evidence that could require another API request", async () => {
  const db = database([target({ task_missing_fields: ["comments_first_page"] })]);
  await assert.rejects(
    recoverStoredDataApiEvidence({
      query: db.query,
      withTransaction: db.withTransaction,
      queue: { async add() {} },
      runId,
      apply: false,
    }),
    /would require another API request/,
  );
});
