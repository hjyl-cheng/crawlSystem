import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import {
  claimDataApiBatchExecution,
  dataApiBatchExecutionFence,
  lockDataApiBatchExecution,
  settleOrphanedDataApiBatchExecution,
} from "../src/dataApiBatchExecutionFence.js";
import { dataApiBatchJobIntent } from "../src/dataApiBatchJobRecovery.js";
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

function recoveryJob({
  batchId,
  taskId,
  videoId,
  systemRetryId,
  recoveryRunId,
  attemptsStarted,
}) {
  return {
    id: `youtube-data-api__${batchId}`,
    name: "youtube-data-api-batch",
    queueName: "youtube-data-api-batch",
    attemptsStarted,
    data: {
      batch_id: batchId,
      task_ids: [taskId],
      video_ids: [videoId],
      pipeline_cycle_id: batchId,
      migration_system_retry_ids: [systemRetryId],
      recovery_run_ids: [recoveryRunId],
    },
  };
}

async function seedRecoveryScenario(client) {
  const batchId = "data-api-execution-fence-batch";
  const channelCandidateId = 482;
  const channelId = "UC0NoarYHkSxek05QDqhtoYw";
  const recoveryRunId = "data-api-execution-fence-run";
  const outsideRunId = "data-api-execution-fence-outside-run";
  const videoId = "data-api-execution-fence-video";

  await client.query(
    `INSERT INTO crawler.query_dispatch_batches (
       dispatch_batch_id,pipeline_cycle_id,status,discovered_candidate_count,total_channel_count
     ) VALUES ($1,$1,'completed',1,1)`,
    [batchId],
  );
  await client.query(
    `INSERT INTO crawler.channel_candidates (
       candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
       snapshot_dispatch_generation,snapshot_json,source_json,accepted_at,validation_finished_at
     ) VALUES ($1,$2,$2,$3,$4,'accepted',2,'{}'::jsonb,
               '{"source":"legacy_results_db"}'::jsonb,now(),now())`,
    [channelCandidateId, batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  const migrationIntentId = Number((await client.query(
    `INSERT INTO crawler.migration_channel_intents (
       source_id,source_database,source_database_oid,source_candidate_id,channel_id,
       source_snapshot,snapshot_sha256,target_candidate_id,first_dispatch_batch_id,
       dispatch_attempts,last_dispatch_at
     ) VALUES (
       'legacy-results-v1',current_database(),
       (SELECT oid FROM pg_database WHERE datname=current_database()),$1,$2,
       '{}'::jsonb,repeat('a',64),$1,$3,2,now()
     ) RETURNING migration_intent_id`,
    [channelCandidateId, channelId, batchId],
  )).rows[0].migration_intent_id);
  await transaction(client, async (tx) => {
    await tx.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,subscriber_count,status,ready_for_agent,
         agent_status,latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Recovery Channel',2000,'active',true,'pending',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, recoveryRunId, channelCandidateId],
    );
    await tx.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         expected_content_count,started_at,result_json
       ) VALUES
         ($1,$3,$4,'waiting_detail','full','api_pending',1,now(),$5::jsonb),
         ($2,$3,NULL,'waiting_detail','full','api_pending',1,now(),$6::jsonb)`,
      [
        recoveryRunId,
        outsideRunId,
        channelId,
        channelCandidateId,
        JSON.stringify({ dispatch_batch_id: batchId, pipeline_cycle_id: batchId }),
        JSON.stringify({ dispatch_batch_id: "ordinary-batch", pipeline_cycle_id: "ordinary-batch" }),
      ],
    );
  });
  const contentCandidates = await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json
     ) VALUES
       ($1,$3,$4,1,'video','resolved','uploads_playlist','api_pending','queued',
        ARRAY['access_status']::text[],'{}'::jsonb),
       ($2,$3,$4,1,'video','resolved','uploads_playlist','api_pending','pending',
        ARRAY['access_status']::text[],'{}'::jsonb)
     RETURNING candidate_id,run_id`,
    [recoveryRunId, outsideRunId, channelId, videoId],
  );
  const recoveryContentCandidateId = Number(
    contentCandidates.rows.find(({ run_id: runId }) => runId === recoveryRunId).candidate_id,
  );
  const outsideContentCandidateId = Number(
    contentCandidates.rows.find(({ run_id: runId }) => runId === outsideRunId).candidate_id,
  );
  const taskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids
     ) VALUES ($1,'queued',ARRAY['access_status']::text[],$2::bigint[])
     RETURNING task_id`,
    [videoId, [recoveryContentCandidateId, outsideContentCandidateId]],
  )).rows[0].task_id);
  const systemRetryId = Number((await client.query(
    `INSERT INTO crawler.migration_system_retry_items (
       migration_intent_id,candidate_id,failed_dispatch_batch_id,
       failed_dispatch_generation,failed_job_id,failed_job_attempt,
       failure_code,failure_category,failure_evidence,status,
       retry_dispatch_generation,recovery_run_id,dispatched_at
     ) VALUES (
       $1,$2,$3,1,'failed-channel-job',1,'LEASE_CONFLICT','lease','{}'::jsonb,
       'dispatched',2,$4,now()
     ) RETURNING system_retry_id`,
    [migrationIntentId, channelCandidateId, batchId, recoveryRunId],
  )).rows[0].system_retry_id);
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,result_json
     ) VALUES ($1,'queued',ARRAY[$2]::bigint[],ARRAY[$3]::text[],$4::jsonb)`,
    [
      batchId,
      taskId,
      videoId,
      JSON.stringify({
        dispatch_intent: {
          pipeline_cycle_id: batchId,
          migration_system_retry_ids: [systemRetryId],
          recovery_run_ids: [recoveryRunId],
        },
      }),
    ],
  );
  return {
    batchId,
    recoveryRunId,
    outsideRunId,
    videoId,
    taskId,
    systemRetryId,
    recoveryContentCandidateId,
    outsideContentCandidateId,
  };
}

test("claiming a legacy Data API Batch persists the immutable Job identity for orphan recovery", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    if (schemaInitialized) {
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await client.end().catch(() => {});
  });

  await client.connect();
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  const batchId = "legacy-data-api-batch-without-dispatch-intent";
  const videoId = "legacy-data-api-video";
  const taskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids
     ) VALUES ($1,'queued',ARRAY['description']::text[],'{}'::bigint[])
     RETURNING task_id`,
    [videoId],
  )).rows[0].task_id);
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,result_json
     ) VALUES ($1,'queued',ARRAY[$2]::bigint[],ARRAY[$3]::text[],'{}'::jsonb)`,
    [batchId, taskId, videoId],
  );
  const legacyJob = {
    id: `youtube-data-api__${batchId}`,
    name: "youtube-data-api-batch",
    attemptsStarted: 1,
    data: {
      batch_id: batchId,
      task_ids: [taskId],
      video_ids: [videoId],
      pipeline_cycle_id: "legacy-data-api-cycle",
    },
  };
  const fence = dataApiBatchExecutionFence(legacyJob);

  const claimed = await transaction(client, (tx) => claimDataApiBatchExecution(tx, fence));
  assert.deepEqual(claimed, {
    recovery: false,
    authorized_candidate_ids_by_task: null,
    recovery_run_ids: [],
    migration_system_retry_ids: [],
  });
  const batch = (await client.query(
    `SELECT batch_id,status,task_ids,video_ids,result_json,
            active_job_id,active_job_attempt
     FROM crawler.youtube_api_batches
     WHERE batch_id=$1`,
    [batchId],
  )).rows[0];
  assert.deepEqual(batch.result_json.dispatch_intent, {
    pipeline_cycle_id: "legacy-data-api-cycle",
    migration_system_retry_ids: [],
    recovery_run_ids: [],
  });
  const recovered = dataApiBatchJobIntent(batch);
  assert.deepEqual(recovered.job.data, legacyJob.data);
  assert.equal(recovered.fence.jobAttempt, 1);
});

test("a newer BullMQ start fences a stalled Data API execution and preserves recovery scope", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    if (schemaInitialized) {
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await client.end().catch(() => {});
  });

  await client.connect();
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  const scenario = await seedRecoveryScenario(client);
  const first = dataApiBatchExecutionFence(recoveryJob({ ...scenario, attemptsStarted: 1 }));
  const takeover = dataApiBatchExecutionFence(recoveryJob({ ...scenario, attemptsStarted: 2 }));

  assert.equal(
    (await transaction(client, (tx) => claimDataApiBatchExecution(tx, first)))?.recovery,
    true,
  );
  assert.equal(
    (await transaction(client, (tx) => claimDataApiBatchExecution(tx, takeover)))?.recovery,
    true,
  );
  assert.equal(await transaction(client, (tx) => lockDataApiBatchExecution(tx, first)), null);

  const activeScope = await transaction(client, (tx) => lockDataApiBatchExecution(tx, takeover));
  assert.deepEqual(activeScope, {
    recovery: true,
    authorized_candidate_ids_by_task: {
      [scenario.taskId]: [scenario.recoveryContentCandidateId],
    },
    recovery_run_ids: [scenario.recoveryRunId],
    migration_system_retry_ids: [scenario.systemRetryId],
  });
  assert.ok(
    !activeScope.authorized_candidate_ids_by_task[scenario.taskId]
      .includes(scenario.outsideContentCandidateId),
  );
  await client.query(
    `UPDATE crawler.youtube_api_tasks
     SET candidate_ids=$2::bigint[]
     WHERE task_id=$1`,
    [scenario.taskId, [scenario.outsideContentCandidateId]],
  );
  const reconciledScope = await transaction(
    client,
    (tx) => lockDataApiBatchExecution(tx, takeover),
  );
  assert.deepEqual(
    reconciledScope.authorized_candidate_ids_by_task[scenario.taskId],
    [scenario.recoveryContentCandidateId],
    "mutable shared-task membership must not revoke the durable recovery execution Fence",
  );

  const execution = (await client.query(
    `SELECT active_job_id,active_job_attempt
     FROM crawler.youtube_api_batches WHERE batch_id=$1`,
    [scenario.batchId],
  )).rows[0];
  assert.deepEqual(execution, {
    active_job_id: `youtube-data-api__${scenario.batchId}`,
    active_job_attempt: "2",
  });

  await client.query(
    `UPDATE crawler.migration_system_retry_items
     SET status='resolved',resolution='recovery_finalized',resolved_at=now(),updated_at=now()
     WHERE system_retry_id=$1`,
    [scenario.systemRetryId],
  );
  assert.equal(await transaction(client, (tx) => lockDataApiBatchExecution(tx, takeover)), null);
});

test("an exact terminal Job Fence resets only unfinished recovery work", {
  skip: databaseUrl ? false : "MANAGED_JOB_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 3000 });
  let schemaInitialized = false;
  t.after(async () => {
    if (schemaInitialized) {
      await client.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
      await client.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    }
    await client.end().catch(() => {});
  });

  await client.connect();
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
  schemaInitialized = true;
  const scenario = await seedRecoveryScenario(client);
  const videoId = `${scenario.videoId}-finished`;
  const finishedCandidateId = Number((await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json,disposition,finished_at
     ) SELECT $1,channel_id,$2,2,'video','resolved','youtube_data_api',
              'done','done','{}'::text[],'{}'::jsonb,'stored',now()
       FROM crawler.content_candidates
       WHERE candidate_id=$3
       RETURNING candidate_id`,
    [scenario.recoveryRunId, videoId, scenario.recoveryContentCandidateId],
  )).rows[0].candidate_id);
  const finishedTaskId = Number((await client.query(
    `INSERT INTO crawler.youtube_api_tasks (
       source_content_id,status,missing_fields,candidate_ids,attempts,finished_at
     ) VALUES ($1,'done','{}'::text[],ARRAY[$2]::bigint[],1,now())
     RETURNING task_id`,
    [videoId, finishedCandidateId],
  )).rows[0].task_id);
  await client.query(
    `UPDATE crawler.youtube_api_batches
     SET task_ids=$2::bigint[],video_ids=$3::text[]
     WHERE batch_id=$1`,
    [
      scenario.batchId,
      [scenario.taskId, finishedTaskId],
      [scenario.videoId, videoId],
    ],
  );
  const job = {
    id: `youtube-data-api__${scenario.batchId}`,
    name: "youtube-data-api-batch",
    attemptsStarted: 1,
    data: {
      batch_id: scenario.batchId,
      task_ids: [scenario.taskId, finishedTaskId],
      video_ids: [scenario.videoId, videoId],
      pipeline_cycle_id: scenario.batchId,
      migration_system_retry_ids: [scenario.systemRetryId],
      recovery_run_ids: [scenario.recoveryRunId],
    },
  };
  const exactFence = dataApiBatchExecutionFence(job);
  assert.equal(
    (await transaction(client, (tx) => claimDataApiBatchExecution(tx, exactFence)))?.recovery,
    true,
  );
  await client.query(
    `UPDATE crawler.youtube_api_tasks SET status='running' WHERE task_id=$1`,
    [scenario.taskId],
  );
  await client.query(
    `UPDATE crawler.content_candidates SET api_status='running' WHERE candidate_id=$1`,
    [scenario.recoveryContentCandidateId],
  );
  const wrongAttemptFence = dataApiBatchExecutionFence({ ...job, attemptsStarted: 2 });
  const evidence = {
    failure_type: "retryable_system_failure",
    failure_code: "DATA_API_BATCH_EXECUTION_ORPHANED",
    failure_category: "control_plane",
    observation_kind: "failed",
    observed_at: "2026-08-30T12:00:00.000Z",
  };

  assert.deepEqual(
    await transaction(client, (tx) => settleOrphanedDataApiBatchExecution(tx, {
      fence: wrongAttemptFence,
      evidence,
    })),
    { settled: false, reason: "stale_execution_fence" },
  );
  const settled = await transaction(client, (tx) => settleOrphanedDataApiBatchExecution(tx, {
    fence: exactFence,
    evidence,
  }));
  assert.equal(settled.settled, true);
  assert.deepEqual(settled.reset_task_ids, [scenario.taskId]);
  assert.deepEqual(settled.reset_candidate_ids, [scenario.recoveryContentCandidateId]);
  assert.deepEqual(settled.affected_run_ids, [scenario.recoveryRunId]);

  const state = (await client.query(
    `SELECT batch.status AS batch_status,batch.active_job_id,batch.active_job_attempt,
            batch.result_json->'execution_orphan_recovery' AS recovery_evidence,
            unfinished.status AS unfinished_task_status,
            finished.status AS finished_task_status,
            recovery.api_status AS recovery_api_status,
            outside.api_status AS outside_api_status
     FROM crawler.youtube_api_batches batch
     JOIN crawler.youtube_api_tasks unfinished ON unfinished.task_id=$2
     JOIN crawler.youtube_api_tasks finished ON finished.task_id=$3
     JOIN crawler.content_candidates recovery ON recovery.candidate_id=$4
     JOIN crawler.content_candidates outside ON outside.candidate_id=$5
     WHERE batch.batch_id=$1`,
    [
      scenario.batchId,
      scenario.taskId,
      finishedTaskId,
      scenario.recoveryContentCandidateId,
      scenario.outsideContentCandidateId,
    ],
  )).rows[0];
  assert.equal(state.batch_status, "failed");
  assert.equal(state.active_job_id, exactFence.jobId);
  assert.equal(Number(state.active_job_attempt), 1);
  assert.equal(state.recovery_evidence.failure_type, "retryable_system_failure");
  assert.equal(state.unfinished_task_status, "pending");
  assert.equal(state.finished_task_status, "done");
  assert.equal(state.recovery_api_status, "pending");
  assert.equal(state.outside_api_status, "pending");
  assert.equal(await transaction(client, (tx) => lockDataApiBatchExecution(tx, exactFence)), null);
  assert.deepEqual(
    await transaction(client, (tx) => settleOrphanedDataApiBatchExecution(tx, {
      fence: exactFence,
      evidence,
    })),
    { settled: false, reason: "stale_execution_fence" },
  );

  const replayBatchId = "stored-replay-orphan-batch";
  const replayOperationId = "stored-data-api-public-access-replay-v1";
  await client.query(
    `UPDATE crawler.youtube_api_tasks
     SET status='queued',candidate_ids=ARRAY[$2]::bigint[],next_retry_at=NULL
     WHERE task_id=$1`,
    [scenario.taskId, scenario.recoveryContentCandidateId],
  );
  await client.query(
    `UPDATE crawler.content_candidates
     SET detail_status='api_pending',api_status='queued',disposition=NULL,finished_at=NULL
     WHERE candidate_id=$1`,
    [scenario.recoveryContentCandidateId],
  );
  await client.query(
    `INSERT INTO crawler.youtube_api_batches (
       batch_id,status,task_ids,video_ids,result_json
     ) VALUES ($1,'queued',ARRAY[$2]::bigint[],ARRAY[$3]::text[],$4::jsonb)`,
    [
      replayBatchId,
      scenario.taskId,
      scenario.videoId,
      JSON.stringify({
        stored_evidence_recovery: {
          operation_id: replayOperationId,
          run_id: scenario.recoveryRunId,
          batch_id: replayBatchId,
          candidate_ids: [scenario.recoveryContentCandidateId],
          task_ids: [scenario.taskId],
        },
      }),
    ],
  );
  const replayJob = {
    id: `youtube-data-api__${replayBatchId}`,
    name: "youtube-data-api-batch",
    attemptsStarted: 1,
    data: {
      batch_id: replayBatchId,
      task_ids: [scenario.taskId],
      video_ids: [scenario.videoId],
      stored_evidence_replay: {
        operation_id: replayOperationId,
        run_id: scenario.recoveryRunId,
        expected_candidate_count: 1,
        task_ids: [scenario.taskId],
      },
    },
  };
  const replayFence = dataApiBatchExecutionFence(replayJob);
  assert.equal(
    (await transaction(client, (tx) => claimDataApiBatchExecution(tx, replayFence)))?.recovery,
    false,
  );
  await client.query(
    `UPDATE crawler.youtube_api_tasks SET status='running' WHERE task_id=$1`,
    [scenario.taskId],
  );
  await client.query(
    `UPDATE crawler.content_candidates SET api_status='running' WHERE candidate_id=$1`,
    [scenario.recoveryContentCandidateId],
  );
  const replaySettlement = await transaction(
    client,
    (tx) => settleOrphanedDataApiBatchExecution(tx, {
      fence: replayFence,
      evidence,
    }),
  );
  assert.equal(replaySettlement.settled, true);
  assert.ok(replaySettlement.successor_batch_id);
  const replayState = (await client.query(
    `SELECT source.status AS source_status,successor.status AS successor_status,
            successor.task_ids,successor.video_ids,
            successor.result_json->'stored_evidence_recovery' AS marker,
            task.status AS task_status,candidate.api_status AS candidate_api_status
     FROM crawler.youtube_api_batches source
     JOIN crawler.youtube_api_batches successor ON successor.batch_id=$2
     JOIN crawler.youtube_api_tasks task ON task.task_id=$3
     JOIN crawler.content_candidates candidate ON candidate.candidate_id=$4
     WHERE source.batch_id=$1`,
    [
      replayBatchId,
      replaySettlement.successor_batch_id,
      scenario.taskId,
      scenario.recoveryContentCandidateId,
    ],
  )).rows[0];
  assert.equal(replayState.source_status, "failed");
  assert.equal(replayState.successor_status, "queued");
  assert.deepEqual(replayState.task_ids.map(Number), [scenario.taskId]);
  assert.deepEqual(replayState.video_ids, [scenario.videoId]);
  assert.equal(replayState.marker.batch_id, replaySettlement.successor_batch_id);
  assert.deepEqual(replayState.marker.candidate_ids, [scenario.recoveryContentCandidateId]);
  assert.equal(replayState.task_status, "queued");
  assert.equal(replayState.candidate_api_status, "queued");
});
