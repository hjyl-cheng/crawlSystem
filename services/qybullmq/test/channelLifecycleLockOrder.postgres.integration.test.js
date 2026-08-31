import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  claimContentDetailExecution,
  contentDetailExecutionFence,
  lockContentDetailExecution,
} from "../src/contentDetailExecutionFence.js";
import { markChannelRemoved } from "../src/channelLifecycle.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";

const { Client } = pg;
const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL ?? "").trim();

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

async function transaction(client, action) {
  await client.query("BEGIN");
  try {
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

async function waitFor(check, label, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${label}`);
}

test("Channel removal and Content Detail share Candidate to Run to Channel lock order", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const detail = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const removal = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  const observer = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    await Promise.all([
      detail.query("ROLLBACK").catch(() => {}),
      removal.query("ROLLBACK").catch(() => {}),
    ]);
    if (schemaInitialized) {
      await setup.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await Promise.all([
      setup.end().catch(() => {}),
      detail.end().catch(() => {}),
      removal.end().catch(() => {}),
      observer.end().catch(() => {}),
    ]);
  });

  await Promise.all([setup.connect(), detail.connect(), removal.connect(), observer.connect()]);
  await setup.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await setup.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await setup.query(crawlerRuntimeSchema(schema));
  await setup.query("CREATE SCHEMA publication");
  await setup.query(
    `CREATE TABLE publication.stream (
       publication_stream_id uuid PRIMARY KEY
     )`,
  );
  await setup.query(
    `CREATE TABLE publication.channel_stream_state (
       publication_stream_id uuid NOT NULL REFERENCES publication.stream(publication_stream_id),
       channel_id text NOT NULL,
       status text NOT NULL,
       owned_at timestamptz NOT NULL DEFAULT now(),
       PRIMARY KEY (publication_stream_id,channel_id)
     )`,
  );
  schemaInitialized = true;

  const batchId = "channel-removal-lock-order";
  const channelId = "UCchannelRemovalLockOrder";
  const runId = "run-channel-removal-lock-order";
  const snapshotJobId = "channel-snapshot-removal-lock-order";
  await setup.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,total_channel_count
     ) VALUES ($1,$1,'running',1)`,
    [batchId],
  );
  const candidateId = Number((await setup.query(
    `INSERT INTO crawler.channel_candidates (
       dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
       snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$1,$2,$3,'accepted',1,$4,1,'{}'::jsonb,'{}'::jsonb,now(),now())
     RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, snapshotJobId],
  )).rows[0].candidate_id);
  await transaction(setup, async (client) => {
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,latest_run_id,registry_promotion_candidate_id,
         registry_promotion_run_id
       ) VALUES ($1,$2,'Channel removal lock order','active',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,result_json
       ) VALUES ($1,$2,$3,'waiting_detail','full','queued',1,$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({ pipeline_cycle_id: batchId })],
    );
  });
  const candidateAttemptFence = {
    candidateId,
    dispatchGeneration: 1,
    jobId: snapshotJobId,
    bullmqAttempt: 1,
  };
  const executionFence = contentDetailExecutionFence({
    id: snapshotJobId,
    attemptsStarted: 1,
    data: {
      run_id: runId,
      channel_id: channelId,
      pipeline_cycle_id: batchId,
    },
  }, { executionMode: "channel_inline", candidateAttemptFence });
  assert.ok(await transaction(setup, (client) => claimContentDetailExecution(
    client,
    executionFence,
  )));

  await detail.query("BEGIN");
  await detail.query("SET LOCAL deadlock_timeout='50ms'");
  await detail.query("SET LOCAL statement_timeout='5s'");
  await detail.query(
    "SELECT candidate_id FROM crawler.channel_candidates WHERE candidate_id=$1 FOR UPDATE",
    [candidateId],
  );
  await removal.query("SET application_name='channel-removal-lock-order-test'");
  await removal.query("BEGIN");
  await removal.query("SET LOCAL deadlock_timeout='50ms'");
  await removal.query("SET LOCAL statement_timeout='5s'");
  const removalPromise = markChannelRemoved(removal, {
    channelId,
    candidateId,
    candidateAttemptFence,
    runId,
    terminal: {
      failure_kind: "channel_removed",
      removed_reason: "channel_not_found",
      removed_source: "lock_order_test",
      evidence: "This channel does not exist.",
    },
  }).then(async (result) => {
    await removal.query("COMMIT");
    return result;
  }).catch(async (error) => {
    await removal.query("ROLLBACK").catch(() => {});
    throw error;
  });
  await waitFor(async () => (await observer.query(
    `SELECT wait_event_type
     FROM pg_stat_activity
     WHERE application_name='channel-removal-lock-order-test'`,
  )).rows[0]?.wait_event_type === "Lock", "Channel removal lock wait");

  const locked = await lockContentDetailExecution(detail, executionFence);
  assert.ok(locked);
  await detail.query("COMMIT");
  const removed = await removalPromise;
  assert.equal(removed.removed, true);
  assert.deepEqual((await observer.query(
    `SELECT channel.status AS channel_status,run.status AS run_status
     FROM crawler.channels channel
     JOIN crawler.channel_runs run ON run.run_id=$2
     WHERE channel.channel_id=$1`,
    [channelId, runId],
  )).rows[0], {
    channel_status: "removed",
    run_status: "skipped",
  });
});
