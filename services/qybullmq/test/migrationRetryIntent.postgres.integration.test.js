import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";

import {
  markMigrationRetryIntentRunning,
  MigrationRetryIntentConflictError,
  MigrationRetryIntentJobReconciler,
  MigrationRetryIntentStore,
  PostgresMigrationRetryIntentRepository,
} from "../src/migrationRetryIntent.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";
import {
  parseMigrationRetryIntentCommand,
  runMigrationRetryIntentCommand,
} from "../scripts/createMigrationRetryIntent.mjs";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("Recovery Intent, Candidate generation, and Outbox commit as one PostgreSQL unit", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID();
  const batchId = `retry-test:${suffix}`;
  const oldRunId = `run:old:${suffix}`;

  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'failed','{}'::jsonb)`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,source_json
       ) VALUES ($1,$1,$2,$3,'failed',4,'{"source":"legacy_results_db"}'::jsonb)
       RETURNING candidate_id`,
      [batchId, `UC${suffix.replaceAll("-", "")}`, `https://www.youtube.com/channel/UC${suffix}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.business_run_bindings (
         business_run_key,business_run_id,intent_hash,intent_json,
         identity_policy_id,identity_policy_version,identity_policy_hash,
         run_kind,channel_id,candidate_id,status,terminal_reason
       ) SELECT 'full-candidate:' || candidate_id::text,$2,'sha256:old','{}'::jsonb,
                'channel-sticky-v1',1,'sha256:policy','full',channel_id,candidate_id,
                'terminal','proxy_control_business_run_budget_exhausted'
         FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId, oldRunId],
    );

    const repository = new PostgresMigrationRetryIntentRepository({
      withTransaction: (action) => action(client),
    });
    const ids = [
      "33333333-3333-4333-8333-333333333333",
      "44444444-4444-4444-8444-444444444444",
    ];
    const store = new MigrationRetryIntentStore({
      repository,
      randomUUID: () => ids.shift(),
      now: () => new Date("2026-08-26T13:00:00.000Z"),
    });
    const input = {
      requestKey: `request:${suffix}`,
      candidateId,
      previousBusinessRunId: oldRunId,
      reason: "isolated integration recovery",
    };
    const cliEnvironment = {
      EXPECTED_CRAWLER_DATABASE: "rota_test",
      FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
    };
    const cliCommand = parseMigrationRetryIntentCommand([
      "--candidate-id", String(candidateId),
      "--previous-business-run-id", oldRunId,
      "--request-key", input.requestKey,
      "--reason", input.reason,
      "--min-subscriber-count", "1000",
    ], cliEnvironment);
    const cliPlan = await runMigrationRetryIntentCommand(cliCommand, {
      query: client.query.bind(client),
      environment: cliEnvironment,
    });
    assert.equal(cliPlan.mode, "plan");
    assert.equal(cliPlan.committed_writes, false);
    assert.equal(cliPlan.target.eligible, true);

    const created = await store.prepare(input);
    const replayed = await store.prepare(input);

    assert.equal(created.created, true);
    assert.equal(replayed.created, false);
    await assert.rejects(
      store.prepare({ ...input, minSubscriberCount: 2000 }),
      MigrationRetryIntentConflictError,
    );
    const state = await client.query(
      `SELECT intent.status,intent.dispatch_status,intent.dispatch_generation::text,
              candidate.status AS candidate_status,
              candidate.snapshot_dispatch_generation::text,
              old_binding.status AS old_binding_status,
              count(outbox.dispatch_id)::int AS outbox_count
       FROM crawler.migration_retry_intents intent
       JOIN crawler.channel_candidates candidate ON candidate.candidate_id=intent.candidate_id
       JOIN crawler.business_run_bindings old_binding
         ON old_binding.business_run_id=intent.previous_business_run_id
       LEFT JOIN crawler.proxy_job_dispatch_outbox outbox
         ON outbox.aggregate_kind='migration_retry'
        AND outbox.aggregate_id=intent.retry_intent_id
        AND outbox.intent_hash=intent.intent_hash
       WHERE intent.request_key=$1
       GROUP BY intent.status,intent.dispatch_status,intent.dispatch_generation,
                candidate.status,candidate.snapshot_dispatch_generation,old_binding.status`,
      [input.requestKey],
    );
    assert.deepEqual(state.rows[0], {
      status: "requested",
      dispatch_status: "pending",
      dispatch_generation: "5",
      candidate_status: "queued",
      snapshot_dispatch_generation: "5",
      old_binding_status: "terminal",
      outbox_count: 1,
    });

    await client.query("SAVEPOINT migration_retry_missing_outbox");
    await client.query(
      `DELETE FROM crawler.proxy_job_dispatch_outbox
       WHERE aggregate_kind='migration_retry' AND aggregate_id=$1`,
      [created.intent.retry_intent_id],
    );
    await assert.rejects(
      store.prepare(input),
      MigrationRetryIntentConflictError,
    );
    await client.query("ROLLBACK TO SAVEPOINT migration_retry_missing_outbox");

    const queued = [];
    const dispatcher = new ManagedJobOutboxDispatcher({
      repository: new PostgresManagedJobDispatchRepository({
        withTransaction: (action) => action(client),
      }),
      queues: {
        "youtube-channel-crawl": {
          async add(name, payload, options) {
            queued.push({ name, payload, options });
            return { id: options.jobId, name, data: payload };
          },
        },
      },
    });
    assert.equal((await dispatcher.dispatchAvailable({ limit: 1 })).sent, 1);
    assert.equal(queued[0].name, "channel-snapshot-recovery");
    const job = {
      id: created.intent.new_job_id,
      data: queued[0].payload,
    };
    assert.equal(await markMigrationRetryIntentRunning(client.query.bind(client), job), true);
    const reconciled = await new MigrationRetryIntentJobReconciler({
      repository,
      queue: {
        async getJob(jobId) {
          if (jobId !== job.id) return null;
          return { ...job, async getState() { return "completed"; } };
        },
      },
    }).reconcileAvailable({ limit: 10 });
    assert.deepEqual(reconciled, {
      scanned: 1,
      finished: 1,
      failed: 0,
      pending: 0,
      missing: 0,
    });
    assert.deepEqual((await client.query(
      `SELECT status,dispatch_status,dispatched_at IS NOT NULL AS dispatched,
              finished_at IS NOT NULL AS finished
       FROM crawler.migration_retry_intents WHERE retry_intent_id=$1`,
      [created.intent.retry_intent_id],
    )).rows[0], {
      status: "finished",
      dispatch_status: "terminal",
      dispatched: true,
      finished: true,
    });

    await client.query(
      "UPDATE crawler.channel_candidates SET status='failed' WHERE candidate_id=$1",
      [candidateId],
    );
    const secondIds = [
      "55555555-5555-4555-8555-555555555555",
      "66666666-6666-4666-8666-666666666666",
    ];
    const second = await new MigrationRetryIntentStore({
      repository,
      randomUUID: () => secondIds.shift(),
    }).prepare({
      ...input,
      requestKey: `second-request:${suffix}`,
    });
    const dead = await new ManagedJobOutboxDispatcher({
      repository: new PostgresManagedJobDispatchRepository({
        withTransaction: (action) => action(client),
      }),
      queues: {
        "youtube-channel-crawl": {
          async add() { throw new Error("isolated Redis rejection"); },
        },
      },
      maxAttempts: 1,
    }).dispatchAvailable({ limit: 1 });
    assert.equal(dead.dead, 1);
    assert.deepEqual((await client.query(
      `SELECT intent.status,intent.dispatch_status,candidate.status AS candidate_status
       FROM crawler.migration_retry_intents intent
       JOIN crawler.channel_candidates candidate ON candidate.candidate_id=intent.candidate_id
       WHERE intent.retry_intent_id=$1`,
      [second.intent.retry_intent_id],
    )).rows[0], {
      status: "failed",
      dispatch_status: "terminal",
      candidate_status: "failed",
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
