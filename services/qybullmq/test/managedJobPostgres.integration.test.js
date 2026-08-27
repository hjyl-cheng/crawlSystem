import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import {
  ManagedJobIntentStore,
  PostgresManagedJobIntentRepository,
} from "../src/managedJobIntentStore.js";
import {
  ManagedJobOutboxDispatcher,
  PostgresManagedJobDispatchRepository,
} from "../src/managedJobDispatchOutbox.js";

const databaseUrl = String(process.env.MANAGED_JOB_TEST_DATABASE_URL || "").trim();
const shouldRun = Boolean(databaseUrl);
const { Client } = pg;

const policies = [
  {
    id: "qy-br-discover-anonymous-v1",
    version: 1,
    hash: "sha256:discover-br",
    role: "discover",
    youtube_language: "pt-BR",
    youtube_country: "BR",
  },
  {
    id: "qy-br-query-quality-anonymous-v1",
    version: 1,
    hash: "sha256:quality-br",
    role: "query_quality",
    youtube_language: "pt-BR",
    youtube_country: "BR",
  },
];

function assertDedicatedTestDatabase(value) {
  const url = new URL(value);
  const databaseName = decodeURIComponent(url.pathname.replace(/^\//, ""));
  assert.match(databaseName, /test/i, "integration test database name must contain 'test'");
  assert.ok(
    ["127.0.0.1", "localhost", "::1"].includes(url.hostname),
    "integration test database must be local",
  );
}

async function resetDatabase(client) {
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
}

function transactionUsing(client) {
  return async (action) => {
    await client.query("BEGIN");
    try {
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  };
}

test("PostgreSQL persists managed Intents, replays Outbox, and freezes Chunk members", {
  skip: shouldRun ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 60_000,
}, async (t) => {
  assertDedicatedTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl });
  await client.connect();
  t.after(async () => {
    await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await client.end();
  });
  await resetDatabase(client);

  const withTransaction = transactionUsing(client);
  const intentStore = new ManagedJobIntentStore({
    repository: new PostgresManagedJobIntentRepository({ withTransaction }),
    policies,
    qualityChunkSize: 2,
  });
  const queryRow = await client.query(
    `INSERT INTO crawler.query_terms (
       query_text,language,country,quality_score,quality_status
     ) VALUES ('roblox brasil','pt-BR','BR',80,'scored')
     RETURNING query_id`,
  );
  const queryId = Number(queryRow.rows[0].query_id);

  const pageInput = {
    pageId: "query:test:page:1",
    queryId,
    queryText: "roblox brasil",
    pageNo: 1,
    discoveryRunId: "query:test",
    pipelineCycleId: "pipeline:test",
    dispatchBatchId: "pipeline:test",
    language: "pt-BR",
    country: "BR",
  };
  const page = await intentStore.prepareDiscoverPage(pageInput);
  assert.equal(page.created, true);
  assert.equal((await client.query(
    "SELECT count(*)::int AS count FROM crawler.proxy_job_dispatch_outbox",
  )).rows[0].count, 1);

  const queueCalls = [];
  const persistedJobs = new Map();
  const queues = {
    "youtube-discover-page": {
      add: async (name, payload, options) => {
        queueCalls.push(options.jobId);
        const job = { id: options.jobId, name, data: payload };
        if (!persistedJobs.has(options.jobId)) persistedJobs.set(options.jobId, job);
        return job;
      },
      getJob: async (jobId) => persistedJobs.get(jobId) ?? null,
    },
  };
  const postgresDispatchRepository = new PostgresManagedJobDispatchRepository({
    withTransaction,
    sendingTimeoutMs: 1_000,
  });
  let failMarkSent = true;
  const crashAfterQueueAdd = {
    claimNext: (input) => postgresDispatchRepository.claimNext(input),
    markFailed: (input) => postgresDispatchRepository.markFailed(input),
    markSent: async (input) => {
      if (failMarkSent) {
        failMarkSent = false;
        throw new Error("simulated crash after queue.add");
      }
      return postgresDispatchRepository.markSent(input);
    },
  };
  await assert.rejects(
    new ManagedJobOutboxDispatcher({ repository: crashAfterQueueAdd, queues })
      .dispatchAvailable({ limit: 1 }),
    /simulated crash/,
  );
  await client.query(
    "UPDATE crawler.proxy_job_dispatch_outbox SET updated_at=now()-interval '2 seconds'",
  );
  const replay = await new ManagedJobOutboxDispatcher({
    repository: postgresDispatchRepository,
    queues,
  }).dispatchAvailable({ limit: 1 });
  assert.equal(replay.sent, 1);
  assert.equal(queueCalls.length, 2);
  assert.equal(queueCalls[0], queueCalls[1]);
  assert.equal((await client.query(
    "SELECT dispatch_status FROM crawler.query_pages WHERE page_id=$1",
    [pageInput.pageId],
  )).rows[0].dispatch_status, "enqueued");

  await client.query(
    `INSERT INTO crawler.query_quality_batches (
       quality_batch_id,status,total_count,options_json
     ) VALUES ('batch-1','queued',2,'{"language":"pt-BR","country":"BR"}'::jsonb)`,
  );
  const secondQuery = await client.query(
    `INSERT INTO crawler.query_terms (
       query_text,language,country,quality_status
     ) VALUES ('minecraft brasil','pt-BR','BR','unscored')
     RETURNING query_id`,
  );
  const taskRows = await client.query(
    `INSERT INTO crawler.query_quality_tasks (quality_batch_id,query_id,status)
     VALUES ('batch-1',$1,'queued'),('batch-1',$2,'queued')
     RETURNING quality_task_id`,
    [queryId, secondQuery.rows[0].query_id],
  );
  const quality = await intentStore.prepareQueryQualityBatch("batch-1");
  assert.equal(quality.chunks.length, 1);
  const chunk = quality.chunks[0];
  assert.equal(chunk.status, "pending");
  assert.deepEqual(chunk.quality_task_ids, taskRows.rows.map((row) => Number(row.quality_task_id)));

  await client.query(
    `INSERT INTO crawler.query_quality_chunks (
       quality_chunk_id,quality_batch_id,chunk_intent_hash,intent_schema_version,
       effective_language,effective_country,identity_policy_id,identity_policy_version,
       identity_policy_hash,scoring_options,scoring_options_hash,status
     ) VALUES (
       'other-chunk','batch-1','sha256:other',1,'pt-BR','BR',
       'qy-br-query-quality-anonymous-v1',1,'sha256:quality-br','{}','sha256:options','pending'
     )`,
  );
  await assert.rejects(
    client.query(
      `INSERT INTO crawler.query_quality_chunk_members (
         quality_batch_id,quality_chunk_id,quality_task_id,member_ordinal
       ) VALUES ('batch-1','other-chunk',$1,1)`,
      [taskRows.rows[0].quality_task_id],
    ),
    (error) => error?.code === "23505",
  );

  await client.query(
    "UPDATE crawler.query_quality_chunks SET status='queued' WHERE quality_chunk_id=$1",
    [chunk.quality_chunk_id],
  );
  await assert.rejects(
    client.query(
      `UPDATE crawler.query_quality_chunk_members
       SET member_ordinal=member_ordinal+10
       WHERE quality_chunk_id=$1`,
      [chunk.quality_chunk_id],
    ),
    (error) => error?.code === "23514",
  );

  await client.query(
    `UPDATE crawler.query_pages
     SET managed_fetch_status='done',dispatch_status='terminal'
     WHERE page_id=$1`,
    [pageInput.pageId],
  );
  await assert.rejects(
    client.query(
      "UPDATE crawler.query_pages SET managed_fetch_status='running' WHERE page_id=$1",
      [pageInput.pageId],
    ),
    (error) => error?.code === "23514",
  );
});
