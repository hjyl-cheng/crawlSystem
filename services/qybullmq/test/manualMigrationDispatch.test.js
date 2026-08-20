import assert from "node:assert/strict";
import test from "node:test";
import {
  DEFAULT_MANUAL_MIGRATION_BATCH_ID,
  dispatchManualMigrationBatch,
  dispatchManualMigrationChannel,
  normalizeManualMigrationBatchSelection,
  prepareManualMigrationBatch,
  prepareManualMigration,
  schedulerConflict,
} from "../src/manualMigrationDispatch.js";

function candidate(overrides = {}) {
  return {
    candidate_id: 42,
    dispatch_batch_id: "legacy-results-full-v1",
    pipeline_cycle_id: "legacy-results-full-v1",
    channel_id: "UC1234567890123456789012",
    channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
    priority: 100,
    status: "discovered",
    snapshot_attempts: 0,
    source_json: { source: "legacy_results_db" },
    ...overrides,
  };
}

function clientFixture(candidateRow = candidate(), scheduler = { status: "stopped" }) {
  const queries = [];
  return {
    queries,
    client: {
      async query(sql, params = []) {
        queries.push({ sql, params });
        if (sql.includes("FROM crawler.channel_candidates") && sql.includes("FOR UPDATE")) {
          return { rows: [candidateRow] };
        }
        if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
          return { rows: [{ value_json: scheduler }] };
        }
        if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("RETURNING candidate_id")) {
          return {
            rows: [{
              ...candidateRow,
              dispatch_batch_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
              pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID,
              status: "queued",
              snapshot_attempts: candidateRow.status === "failed" ? 0 : candidateRow.snapshot_attempts,
            }],
          };
        }
        return { rows: [], rowCount: 1 };
      },
    },
  };
}

test("manual migration does not replace another active crawler pipeline", () => {
  assert.deepEqual(
    schedulerConflict({ status: "finishing", pipeline_cycle_id: "legacy-results-pilot-30-v1" }),
    {
      code: "pipeline_busy",
      message: "crawler pipeline legacy-results-pilot-30-v1 is finishing",
    },
  );
  assert.equal(
    schedulerConflict({ status: "finishing", pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID }),
    null,
  );
  assert.equal(schedulerConflict({ status: "stopped", pipeline_cycle_id: "old-batch" }), null);
  assert.equal(schedulerConflict({ status: "paused", pipeline_cycle_id: DEFAULT_MANUAL_MIGRATION_BATCH_ID }).code, "pipeline_paused");
});

test("manual batch migration accepts the dashboard selections including 500", () => {
  assert.deepEqual(normalizeManualMigrationBatchSelection("500"), { selection: "500", limit: 500 });
  assert.deepEqual(normalizeManualMigrationBatchSelection("all"), { selection: "all", limit: null });
  assert.throws(
    () => normalizeManualMigrationBatchSelection("499"),
    (error) => error.code === "invalid_batch_selection",
  );
});

test("a manual migration batch atomically rehomes eligible legacy Candidates for the Controller", async () => {
  const batchCandidates = [
    candidate(),
    candidate({
      candidate_id: 43,
      channel_id: "UC2234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC2234567890123456789012",
      status: "failed",
      snapshot_attempts: 4,
    }),
  ];
  const queries = [];
  const client = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rows: [{ value_json: { status: "stopped" } }] };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("ORDER BY priority")) {
        return { rows: batchCandidates };
      }
      if (sql.includes("UPDATE crawler.channel_candidates") && sql.includes("RETURNING candidate_id")) {
        return { rows: batchCandidates.map(({ candidate_id }) => ({ candidate_id })), rowCount: 2 };
      }
      return { rows: [], rowCount: 1 };
    },
  };

  const result = await prepareManualMigrationBatch(client, {
    selection: "500",
    batchId: "manual-batch-500-test",
  });

  assert.equal(result.targetCount, 2);
  assert.equal(result.batchId, "manual-batch-500-test");
  assert.equal(result.firstCandidateId, 42);
  assert.equal(result.lastCandidateId, 43);
  assert.equal(
    queries.some(({ sql }) => sql.includes("status='discovered'") && sql.includes("mode','dashboard_batch'")),
    true,
  );
  assert.equal(
    queries.some(({ sql }) => sql.includes("'status','finishing'") && sql.includes("manual_migration_batch_dispatch")),
    true,
  );
});

test("an empty manual migration batch does not activate the crawler pipeline", async () => {
  const queries = [];
  const client = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")) {
        return { rows: [{ value_json: { status: "stopped" } }] };
      }
      if (sql.includes("FROM crawler.channel_candidates") && sql.includes("ORDER BY priority")) {
        return { rows: [] };
      }
      return { rows: [], rowCount: 1 };
    },
  };

  const result = await dispatchManualMigrationBatch({
    selection: "500",
    batchId: "manual-batch-empty-test",
    transaction: (action) => action(client),
  });

  assert.equal(result.created, false);
  assert.equal(result.target_count, 0);
  assert.equal(result.batch_id, null);
  assert.equal(queries.some(({ sql }) => sql.includes("INSERT INTO crawler.query_dispatch_batches")), false);
  assert.equal(queries.some(({ sql }) => sql.includes("manual_migration_batch_dispatch")), false);
});

test("an in-progress Candidate makes a repeated click idempotent", async () => {
  const fixture = clientFixture(candidate({ status: "queued" }));
  const result = await prepareManualMigration(fixture.client, {
    channelId: "UC1234567890123456789012",
    candidateId: 42,
  });

  assert.equal(result.shouldEnqueue, false);
  assert.equal(result.alreadyInProgress, true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("FROM crawler.settings")), false);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("INSERT INTO crawler.query_dispatch_batches")), false);
});

test("a manual migration rehomes one Candidate and queues the existing full-crawl contract", async () => {
  const fixture = clientFixture();
  const added = [];
  const queue = {
    name: "youtube-channel-crawl",
    async getJob() { return null; },
    async add(name, data, options) {
      added.push({ name, data, options });
      return { id: options.jobId };
    },
  };
  const result = await dispatchManualMigrationChannel({
    channelId: "UC1234567890123456789012",
    candidateId: 42,
    queue,
    transaction: (action) => action(fixture.client),
    dbQuery: async () => ({ rowCount: 1 }),
  });

  assert.equal(result.created, true);
  assert.equal(result.batch_id, DEFAULT_MANUAL_MIGRATION_BATCH_ID);
  assert.equal(added.length, 1);
  assert.equal(added[0].name, "channel-snapshot");
  assert.equal(added[0].data.crawl_mode, "full");
  assert.equal(added[0].data.reject_if_no_recent_content, true);
  assert.equal(added[0].data.dispatch_batch_id, DEFAULT_MANUAL_MIGRATION_BATCH_ID);
  assert.equal(added[0].options.jobId, `channel-snapshot__${DEFAULT_MANUAL_MIGRATION_BATCH_ID}__UC1234567890123456789012`);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("INSERT INTO crawler.query_dispatch_batches")), true);
  assert.equal(fixture.queries.some(({ sql }) => sql.includes("'status','finishing'")), true);
});

test("a Redis delivery failure marks the prepared Candidate failed for explicit retry", async () => {
  const fixture = clientFixture();
  const compensation = [];
  const queue = {
    name: "youtube-channel-crawl",
    async getJob() { return null; },
    async add() { throw new Error("redis unavailable"); },
  };

  await assert.rejects(
    dispatchManualMigrationChannel({
      channelId: "UC1234567890123456789012",
      candidateId: 42,
      queue,
      transaction: (action) => action(fixture.client),
      dbQuery: async (sql, params) => {
        compensation.push({ sql, params });
        return { rowCount: 1 };
      },
    }),
    /redis unavailable/,
  );
  assert.equal(compensation.length, 1);
  assert.match(compensation[0].sql, /SET status='failed'/);
  assert.equal(compensation[0].params[0], 42);
});
