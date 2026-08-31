import assert from "node:assert/strict";
import test from "node:test";

import {
  fullAgentMigrationRunScope,
  lockGenericFullAgentAgainstMigrationSystemRetry,
  lockGenericFullAgentBatchAgainstMigrationSystemRetry,
} from "../src/fullAgentMigrationRetryGuard.js";

test("full Agent run scopes use the channel and Run loaded by the Worker", () => {
  assert.deepEqual(fullAgentMigrationRunScope({
    channel_id: " UCguard ",
    latest_run_id: " run:guard ",
    candidate_id: "482",
    candidate_dispatch_batch_id: " batch:guard ",
    candidate_dispatch_generation: "2",
  }), {
    channelId: "UCguard",
    runId: "run:guard",
    candidateId: 482,
    dispatchBatchId: "batch:guard",
    dispatchGeneration: 2,
  });
  assert.deepEqual(fullAgentMigrationRunScope({
    channel_id: "UCordinary",
    latest_run_id: "run:ordinary",
    candidate_id: null,
    candidate_dispatch_batch_id: null,
    candidate_dispatch_generation: null,
  }), {
    channelId: "UCordinary",
    runId: "run:ordinary",
    candidateId: null,
    dispatchBatchId: null,
    dispatchGeneration: null,
  });
  assert.throws(
    () => fullAgentMigrationRunScope({ channel_id: "UCguard" }),
    /channelId and runId are required/,
  );
  assert.throws(
    () => fullAgentMigrationRunScope({
      channel_id: "UCguard",
      latest_run_id: "run:guard",
      candidate_id: 482,
    }),
    /Candidate identity must be complete/,
  );
});

test("the generic full Agent guard freezes Candidate generation and current Channel Run", async () => {
  const calls = [];
  const allowed = await lockGenericFullAgentBatchAgainstMigrationSystemRetry({
    async query(sql, params) {
      const statement = { sql: String(sql), params };
      calls.push(statement);
      if (statement.sql.includes("full-agent-migration-guard:candidates")) {
        return {
          rows: [
            { candidate_id: "10", dispatch_batch_id: "batch:a", snapshot_dispatch_generation: "3" },
            { candidate_id: "11", dispatch_batch_id: "batch:z", snapshot_dispatch_generation: "4" },
          ],
        };
      }
      if (statement.sql.includes("full-agent-migration-guard:retries")) {
        return { rows: [{ system_retry_id: "19", status: "resolved" }] };
      }
      if (statement.sql.includes("full-agent-migration-guard:runs")) {
        return {
          rows: [
            { run_id: "run:a", channel_id: "UCa", candidate_id: "10" },
            { run_id: "run:z", channel_id: "UCz", candidate_id: "11" },
          ],
        };
      }
      return {
        rows: [
          { channel_id: "UCa", latest_run_id: "run:a", status: "active" },
          { channel_id: "UCz", latest_run_id: "run:z", status: "active" },
        ],
      };
    },
  }, [
    {
      channelId: "UCz",
      runId: "run:z",
      candidateId: 11,
      dispatchBatchId: "batch:z",
      dispatchGeneration: 4,
    },
    {
      channelId: "UCa",
      runId: "run:a",
      candidateId: 10,
      dispatchBatchId: "batch:a",
      dispatchGeneration: 3,
    },
    {
      channel_id: "UCa",
      latest_run_id: "run:a",
      candidate_id: 10,
      candidate_dispatch_batch_id: "batch:a",
      candidate_dispatch_generation: 3,
    },
  ]);

  assert.equal(allowed, true);
  assert.equal(calls.length, 4);
  assert.match(calls[0].sql, /ORDER BY candidate\.candidate_id\s+FOR UPDATE OF candidate/);
  assert.match(calls[1].sql, /ORDER BY expected\.candidate_id,retry\.system_retry_id\s+FOR UPDATE OF retry/);
  assert.match(calls[2].sql, /ORDER BY run\.run_id\s+FOR UPDATE OF run/);
  assert.match(calls[3].sql, /ORDER BY channel\.channel_id\s+FOR UPDATE OF channel/);
  assert.deepEqual(calls[0].params, [[10, 11], ["batch:a", "batch:z"], [3, 4]]);
  assert.deepEqual(calls[2].params, [
    ["UCa", "UCz"],
    ["run:a", "run:z"],
    [10, 11],
    ["batch:a", "batch:z"],
  ]);
  assert.deepEqual(calls[3].params, [["UCa", "UCz"], ["run:a", "run:z"]]);
});

test("an active Migration retry rejects a generic full Agent transaction", async () => {
  let calls = 0;
  const allowed = await lockGenericFullAgentAgainstMigrationSystemRetry({
    async query(sql) {
      calls += 1;
      if (String(sql).includes("full-agent-migration-guard:candidates")) {
        return {
          rows: [{
            candidate_id: "482",
            dispatch_batch_id: "batch:guard",
            snapshot_dispatch_generation: "2",
          }],
        };
      }
      return { rows: [{ system_retry_id: "19", status: "dispatched" }] };
    },
  }, {
    channelId: "UC0NoarYHkSxek05QDqhtoYw",
    runId: "run:g2",
    candidateId: 482,
    dispatchBatchId: "batch:guard",
    dispatchGeneration: 2,
  });

  assert.equal(allowed, false);
  assert.equal(calls, 2);
});

test("a non-Migration Run and an empty batch remain outside the guard", async () => {
  let calls = 0;
  const client = {
    async query(sql) {
      calls += 1;
      if (String(sql).includes("full-agent-migration-guard:runs")) {
        return {
          rows: [{ run_id: "run:ordinary", channel_id: "UCordinary", candidate_id: null }],
        };
      }
      assert.match(String(sql), /full-agent-migration-guard:channels/);
      return {
        rows: [{
          channel_id: "UCordinary",
          latest_run_id: "run:ordinary",
          status: "active",
        }],
      };
    },
  };

  assert.equal(await lockGenericFullAgentAgainstMigrationSystemRetry(client, {
    channelId: "UCordinary",
    runId: "run:ordinary",
    candidateId: null,
    dispatchBatchId: null,
    dispatchGeneration: null,
  }), true);
  assert.equal(await lockGenericFullAgentBatchAgainstMigrationSystemRetry(client, []), true);
  assert.equal(calls, 2);
});

test("the generic full Agent guard requires an active PostgreSQL transaction client", async () => {
  await assert.rejects(
    lockGenericFullAgentAgainstMigrationSystemRetry(null, {
      channelId: "UCguard",
      runId: "run:guard",
    }),
    /active PostgreSQL client is required/,
  );
});
