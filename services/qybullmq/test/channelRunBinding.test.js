import assert from "node:assert/strict";
import test from "node:test";
import {
  bindPreparedChannelRun,
  prepareChannelRun,
  prepareChannelRunAndBindJob,
} from "../src/channelRunBinding.js";

test("re-preparing a Channel Run merges later result fields", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      calls.push({ sql, params });
      return { rows: [], rowCount: 1 };
    },
  };
  const pendingAbout = {
    current: { isVerified: false, isVerifiedStatus: "not_verified" },
  };

  await prepareChannelRun(client, {
    runId: "run:accepted-candidate",
    channelId: "UCaccepted",
    candidateId: 1255,
    crawlMode: "full",
    contentLimit: 30,
    resultJson: { pending_initial_about_observation: pendingAbout },
  });

  assert.match(
    calls[0].sql,
    /THEN crawler\.channel_runs\.result_json \|\| EXCLUDED\.result_json/,
  );
  assert.deepEqual(JSON.parse(calls[0].params[5]), {
    pending_initial_about_observation: pendingAbout,
  });
});

test("a reused Run ID cannot change its Channel, Candidate, or crawl mode", async () => {
  const client = {
    async query(sql) {
      if (String(sql).startsWith("INSERT INTO crawler.channel_runs")) {
        return { rows: [], rowCount: 0 };
      }
      throw new Error("latest_run_id must not change after an identity conflict");
    },
  };

  await assert.rejects(
    prepareChannelRun(client, {
      runId: "run:collision",
      channelId: "UCwrong",
      candidateId: 99,
      crawlMode: "incremental",
      contentLimit: 30,
      resultJson: {},
    }),
    /Channel Run identity conflict: run:collision/,
  );
});

test("a run id is written to the BullMQ job only after the run is prepared", async () => {
  const calls = [];
  const job = {
    data: { channel_id: "UCtest", candidate_id: 7 },
    async updateData(data) {
      calls.push("bind");
      this.data = data;
    },
  };

  await prepareChannelRunAndBindJob({
    job,
    runId: "run:test",
    prepare: async () => { calls.push("prepare"); },
  });

  assert.deepEqual(calls, ["prepare", "bind"]);
  assert.equal(job.data.run_id, "run:test");
  assert.equal(job.data.candidate_id, 7);
});

test("a finalized Promotion Run is not reopened or restored as latest by a retry", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      const statement = String(sql);
      calls.push({ sql: statement, params });
      if (statement.startsWith("INSERT INTO crawler.channel_runs")) {
        return {
          rowCount: 1,
          rows: [{
            run_id: "run:promotion",
            publication_finalized_at: "2026-07-28T01:00:00.000Z",
          }],
        };
      }
      if (statement.includes("latest_run_id")) {
        throw new Error("a finalized retry must not replace the later latest Run");
      }
      return { rowCount: 1, rows: [{ channel_id: "UCpromotion" }] };
    },
  };

  await prepareChannelRun(client, {
    runId: "run:promotion",
    channelId: "UCpromotion",
    candidateId: 42,
    crawlMode: "full",
    contentLimit: 30,
    resultJson: { recovered_retry: true },
  });

  const prepared = calls.find((call) => call.sql.startsWith("INSERT INTO crawler.channel_runs"));
  assert.match(prepared.sql, /publication_finalized_at/);
  assert.match(prepared.sql, /CASE/);
  assert.equal(calls.some((call) => call.sql.includes("SET latest_run_id")), false);
});

test("a ready_partial Promotion Run cannot be bypassed by a later Run", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      const statement = String(sql);
      calls.push({ sql: statement, params });
      if (statement.startsWith("INSERT INTO crawler.channel_runs")) {
        return {
          rowCount: 1,
          rows: [{ run_id: "run:later", publication_finalized_at: null }],
        };
      }
      if (statement.startsWith("UPDATE crawler.channels")) {
        return { rowCount: 0, rows: [] };
      }
      if (statement.startsWith("SELECT channel.status,channel.latest_run_id")) {
        return {
          rowCount: 1,
          rows: [{
            status: "active",
            latest_run_id: "run:promotion",
            registry_promotion_run_id: "run:promotion",
          }],
        };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  await assert.rejects(
    prepareChannelRun(client, {
      runId: "run:later",
      channelId: "UCpartialpromotion",
      candidateId: 42,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: {},
    }),
    /Channel Registry promotion Run must Finalize before a later Run/,
  );
  const binding = calls.find((call) => call.sql.startsWith("UPDATE crawler.channels"));
  assert.match(binding.sql, /crawler\.registry_promotion_is_complete/);
  assert.match(binding.sql, /crawler\.registry_publication_gap_repair_is_allowed/);
});

test("an explicit Publication Gap Child may advance from its matching pending Promotion", async () => {
  const calls = [];
  const client = {
    async query(sql, params) {
      const statement = String(sql);
      calls.push({ sql: statement, params });
      if (statement.startsWith("INSERT INTO crawler.channel_runs")) {
        return {
          rowCount: 1,
          rows: [{ run_id: "run:gap-child", publication_finalized_at: null }],
        };
      }
      if (statement.startsWith("UPDATE crawler.channels")) {
        assert.match(statement, /registry_publication_gap_repair_is_allowed/);
        assert.deepEqual(params, ["run:gap-child", "UCgap"]);
        return { rowCount: 1, rows: [{ latest_run_id: "run:gap-child" }] };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  await prepareChannelRun(client, {
    runId: "run:gap-child",
    channelId: "UCgap",
    candidateId: 42,
    crawlMode: "full",
    contentLimit: 30,
    resultJson: {
      final_repair: { rounds: 1, parent_run_id: "run:promotion", mode: "channel" },
      publication_gap_repair: {
        status: "required",
        domains: ["channel"],
        root_run_id: "run:promotion",
      },
    },
  });

  assert.equal(calls.length, 2);
});

test("an invalid historical latest Run remains blocked while its Promotion is ready_partial", async () => {
  const client = {
    async query(sql) {
      const statement = String(sql);
      if (statement.startsWith("INSERT INTO crawler.channel_runs")) {
        return {
          rowCount: 1,
          rows: [{ run_id: "run:later", publication_finalized_at: null }],
        };
      }
      if (statement.startsWith("UPDATE crawler.channels")) {
        return { rowCount: 0, rows: [] };
      }
      if (statement.startsWith("SELECT channel.status,channel.latest_run_id")) {
        return {
          rowCount: 1,
          rows: [{
            status: "active",
            latest_run_id: "run:later",
            registry_promotion_run_id: "run:promotion",
            promotion_finalized_status: "ready_partial",
            promotion_finalized_at: "2026-07-28T01:00:00.000Z",
          }],
        };
      }
      throw new Error(`unexpected query: ${statement}`);
    },
  };

  await assert.rejects(
    prepareChannelRun(client, {
      runId: "run:later",
      channelId: "UChistoricalpartial",
      candidateId: 42,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: {},
    }),
    /Channel Registry promotion Run must Finalize before a later Run/,
  );
});

test("a failed run preparation does not leave a stale run id on the job", async () => {
  let bound = false;
  const job = {
    data: { channel_id: "UCtest" },
    async updateData() { bound = true; },
  };

  await assert.rejects(prepareChannelRunAndBindJob({
    job,
    runId: "run:missing",
    prepare: async () => { throw new Error("prepare failed"); },
  }), /prepare failed/);

  assert.equal(bound, false);
  assert.equal(job.data.run_id, undefined);
});

test("an atomically prepared Registry run can be bound without preparing it twice", async () => {
  const job = {
    data: { channel_id: "UCwinner" },
    async updateData(data) { this.data = data; },
  };

  const result = await bindPreparedChannelRun({ job, runId: "run:winner" });

  assert.equal(result, "run:winner");
  assert.equal(job.data.run_id, "run:winner");
});
