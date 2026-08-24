import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import {
  ContentEnrichDispatcher,
  PostgresContentEnrichDispatchRepository,
} from "../src/contentEnrichDispatch.js";
import {
  ContentEnrichExecutor,
  PostgresContentEnrichExecutionRepository,
} from "../src/contentEnrichExecution.js";
import { PostgresContentEnrichObservabilityRepository } from "../src/contentEnrichObservability.js";
import {
  applyIncrementalVideoDetail,
  queueRefreshTask,
} from "../src/incrementalVideo.js";
import { reconcilePublication } from "../src/publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";

const { Pool } = pg;
const integrationUrl = process.env.CONTENT_ENRICH_POSTGRES_TEST_URL;

async function assertPostgresObservability(pool) {
  const jobId = `content-enrich-observability:${randomUUID()}`;
  const repository = new PostgresContentEnrichObservabilityRepository({
    queryFn: (sql, params) => pool.query(sql, params),
    withTransaction: transactionRunner(pool),
    queryTimeoutMs: 5_000,
  });
  try {
    const before = await repository.loadSnapshot({ windowMs: 60_000 });
    await pool.query(
      `INSERT INTO crawler.task_events (
         queue_name,job_id,job_name,entity_key,status,payload_json
       ) VALUES
         ('youtube-content-enrich',$1,'content-enrich-claim','UC-observability','claimed',$2::jsonb),
         ('youtube-content-enrich',$1 || ':retry','content-enrich-outcome','UC-observability','checkpointed',$3::jsonb),
         ('youtube-content-enrich',$1 || ':legacy','content-enrich','UC-observability','completed',$4::jsonb)`,
      [
        jobId,
        JSON.stringify({
          claimed: 5,
          done: 0,
          retryable: 0,
          terminal: 0,
          dead_letter: 0,
        }),
        JSON.stringify({
          claimed: 0,
          done: 2,
          retryable: 2,
          terminal: 1,
          dead_letter: 1,
        }),
        JSON.stringify({ tasks: [{ task_id: "legacy-event-without-result" }] }),
      ],
    );
    const after = await repository.loadSnapshot({ windowMs: 60_000 });
    assert.deepEqual(
      Object.fromEntries(Object.keys(after.outcome_counts).map((key) => [
        key,
        after.outcome_counts[key] - before.outcome_counts[key],
      ])),
      { claimed: 5, success: 2, retry: 2, terminal: 1, dead_letter: 1 },
    );
    assert.equal((await pool.query("SHOW statement_timeout")).rows[0].statement_timeout, "0");

    const locker = await pool.connect();
    try {
      await locker.query("BEGIN");
      await locker.query("LOCK TABLE crawler.content_enrich_tasks IN ACCESS EXCLUSIVE MODE");
      const boundedRepository = new PostgresContentEnrichObservabilityRepository({
        queryFn: (sql, params) => pool.query(sql, params),
        withTransaction: transactionRunner(pool),
        queryTimeoutMs: 50,
      });
      const startedAt = Date.now();
      await assert.rejects(
        boundedRepository.loadSnapshot({ windowMs: 60_000 }),
        (error) => error?.code === "57014",
      );
      assert.ok(Date.now() - startedAt < 1_000, "observability aggregate ignored statement_timeout");
    } finally {
      await locker.query("ROLLBACK").catch(() => {});
      locker.release();
    }
    assert.equal((await pool.query("SHOW statement_timeout")).rows[0].statement_timeout, "0");
  } finally {
    await pool.query(
      "DELETE FROM crawler.task_events WHERE job_id IN ($1,$1 || ':retry',$1 || ':legacy')",
      [jobId],
    ).catch(() => {});
  }
}

function transactionRunner(pool) {
  return async (action) => {
    const client = await pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        "SELECT set_config('publication.writer_version',$1,true)",
        [PUBLICATION_WRITER_VERSION],
      );
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  };
}

function queueFixture({ failedAdds = 0 } = {}) {
  const jobs = new Map();
  let remainingFailures = failedAdds;
  return {
    jobs,
    async getJobCounts() {
      const counts = {};
      for (const job of jobs.values()) counts[job.state] = (counts[job.state] ?? 0) + 1;
      return counts;
    },
    async getJob(jobId) {
      return jobs.get(jobId) ?? null;
    },
    async add(name, data, { jobId }) {
      if (remainingFailures > 0) {
        remainingFailures -= 1;
        throw new Error("simulated BullMQ outage");
      }
      const existing = jobs.get(jobId);
      if (existing) return existing;
      const job = {
        id: jobId,
        name,
        queueName: "youtube-content-enrich",
        data,
        state: "waiting",
        async getState() { return this.state; },
      };
      jobs.set(jobId, job);
      return job;
    },
  };
}

function publicDetail(videoId) {
  return {
    id: videoId,
    title: `Enriched ${videoId}`,
    published_at: "2026-08-01T12:00:00.000Z",
    published_at_precision: "second",
    published_at_source: "youtubejs_player",
    view_count: 123,
    view_count_text: "123",
    view_count_source: "youtubejs_player",
    duration_seconds: 90,
    duration_source: "youtubejs_player",
    access_status: "public",
    access_status_source: "youtubejs_player",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
  };
}

test("Content Enrich PostgreSQL lifecycle is fenced, recoverable, and idempotent", {
  skip: !integrationUrl,
  timeout: 60_000,
}, async (context) => {
  const pool = new Pool({ connectionString: integrationUrl, max: 6 });
  const withTransaction = transactionRunner(pool);
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const channelA = "UCcontentenrichA";
  const channelB = "UCcontentenrichB";
  const channelC = "UCcontentenrichC";
  const contentKey = (channelId) => `${channelId}:video:video-${channelId.at(-1).toLowerCase()}`;
  const taskId = (channelId) => `player-refresh:${channelId}`;
  const now = new Date(Date.now() + 60_000);

  try {
    const database = (await pool.query("SELECT current_database() AS name")).rows[0].name;
    assert.match(database, /test/i, "integration test refuses to reset a non-test database");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.query(schema);
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       SELECT channel_id,'https://www.youtube.com/channel/' || channel_id,channel_id,'active'
       FROM unnest($1::text[]) AS input(channel_id)`,
      [[channelA, channelB, channelC]],
    );
    for (const channelId of [channelA, channelB, channelC]) {
      await pool.query(
        `INSERT INTO crawler.contents (
           content_key,channel_id,content_type,content_type_source,source_content_id,
           title,url,published_at,published_at_status,published_at_precision,
           access_status,first_seen_at,last_seen_at
         ) VALUES ($1,$2,'video','youtube_uploads_default:video',$3,$3,$4,
                   '2026-08-01T00:00:00Z','exact','second','unknown',now(),now())`,
        [contentKey(channelId), channelId, `video-${channelId.at(-1).toLowerCase()}`,
          `https://www.youtube.com/watch?v=video-${channelId.at(-1).toLowerCase()}`],
      );
      await pool.query(
        `INSERT INTO crawler.content_enrich_tasks (
           task_id,content_key,channel_id,job_type,status,priority,next_retry_at
         ) VALUES ($1,$2,$3,'player-refresh','queued',10,now())`,
        [taskId(channelId), contentKey(channelId), channelId],
      );
    }
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"mode":"queue"}'::jsonb
       WHERE setting_key='content_enrich_dispatch'`,
    );
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"channel_id":""}'::jsonb
       WHERE setting_key='content_enrich_dispatch_cursor'`,
    );

    const dispatchRepository = new PostgresContentEnrichDispatchRepository({
      queryFn: pool.query.bind(pool),
      withTransaction,
    });
    let releaseDispatchLock;
    let markDispatchLockAcquired;
    const dispatchLockAcquired = new Promise((resolve) => { markDispatchLockAcquired = resolve; });
    const holdDispatchLock = new Promise((resolve) => { releaseDispatchLock = resolve; });
    const firstDispatch = dispatchRepository.withDispatchLock(async () => {
      markDispatchLockAcquired();
      await holdDispatchLock;
      return "first";
    });
    await dispatchLockAcquired;
    let contendedActionRan = false;
    const contendedDispatch = await dispatchRepository.withDispatchLock(async () => {
      contendedActionRan = true;
      return "second";
    });
    assert.deepEqual(contendedDispatch, { acquired: false, result: null });
    assert.equal(contendedActionRan, false);
    releaseDispatchLock();
    assert.deepEqual(await firstDispatch, { acquired: true, result: "first" });
    await pool.query(
      `UPDATE crawler.settings
       SET value_json=jsonb_build_object(
         'owner','content-enrich-controller:crashed',
         'expires_at',clock_timestamp()-interval '1 second'
       )
       WHERE setting_key='content_enrich_dispatch_mutex'`,
    );
    assert.deepEqual(
      await dispatchRepository.withDispatchLock(async () => "recovered"),
      { acquired: true, result: "recovered" },
    );

    const staleOwner = "content-enrich-controller:stale";
    const staleRepository = new PostgresContentEnrichDispatchRepository({
      queryFn: pool.query.bind(pool),
      withTransaction,
      dispatchLockOwner: () => staleOwner,
    });
    try {
      await assert.rejects(
        staleRepository.withDispatchLock(async (repository, lock) => {
          await pool.query(
            `UPDATE crawler.settings
             SET value_json=jsonb_build_object(
               'owner','content-enrich-controller:replacement',
               'expires_at',clock_timestamp()+interval '1 minute'
             )
             WHERE setting_key='content_enrich_dispatch_mutex'`,
          );
          return repository.leaseFairBatches({
            maxJobs: 1,
            batchSize: 1,
            leaseDurationMs: 60_000,
            dispatchOwner: lock.owner,
          });
        }),
        /dispatch mutex ownership was lost/,
      );
    } finally {
      await pool.query(
        `UPDATE crawler.settings
         SET value_json='{"owner":null,"expires_at":null}'::jsonb
         WHERE setting_key='content_enrich_dispatch_mutex'`,
      );
    }

    const mutexClockOwner = "content-enrich-controller:clock-fence";
    await pool.query(
      `UPDATE crawler.settings
       SET value_json=jsonb_build_object(
         'owner',$1::text,
         'expires_at',clock_timestamp()+interval '200 milliseconds'
       )
       WHERE setting_key='content_enrich_dispatch_mutex'`,
      [mutexClockOwner],
    );
    const mutexClockLocker = await pool.connect();
    await mutexClockLocker.query("BEGIN");
    await mutexClockLocker.query(
      `SELECT setting_key
       FROM crawler.settings
       WHERE setting_key='content_enrich_dispatch_mutex'
       FOR UPDATE`,
    );
    try {
      const staleMutexRefresh = dispatchRepository.refreshLease({
        jobId: "content_enrich__clock_fence__missing",
        taskIds: ["missing-clock-fence-task"],
        leaseDurationMs: 60_000,
        dispatchOwner: mutexClockOwner,
      });
      await new Promise((resolve) => setTimeout(resolve, 350));
      await mutexClockLocker.query("COMMIT");
      await assert.rejects(staleMutexRefresh, /dispatch mutex ownership was lost/);
    } finally {
      await mutexClockLocker.query("ROLLBACK").catch(() => {});
      mutexClockLocker.release();
      await pool.query(
        `UPDATE crawler.settings
         SET value_json='{"owner":null,"expires_at":null}'::jsonb
         WHERE setting_key='content_enrich_dispatch_mutex'`,
      );
    }

    const locker = await pool.connect();
    await locker.query("BEGIN");
    await locker.query(
      "SELECT task_id FROM crawler.content_enrich_tasks WHERE task_id=$1 FOR UPDATE",
      [taskId(channelA)],
    );
    try {
      const lockedBatches = await dispatchRepository.withDispatchLock((repository, lock) => (
        repository.leaseFairBatches({
          maxJobs: 2,
          batchSize: 1,
          now,
          leaseExpiresAt: new Date(now.getTime() + 5 * 60_000),
          leaseDurationMs: 5 * 60_000,
          dispatchOwner: lock.owner,
        })
      ));
      assert.equal(lockedBatches.acquired, true);
      const batches = lockedBatches.result;
      assert.deepEqual(batches.map((batch) => batch.channel_id), [channelB]);
    } finally {
      await locker.query("ROLLBACK");
      locker.release();
    }

    const clockFenceOwner = "content-enrich-clock-fence";
    const clockFence = (await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='running',lease_owner=$2,
           lease_expires_at=clock_timestamp()+interval '200 milliseconds'
       WHERE channel_id=$1
       RETURNING task_id,dispatch_generation`,
      [channelB, clockFenceOwner],
    )).rows[0];
    const leaseClockLocker = await pool.connect();
    await leaseClockLocker.query("BEGIN");
    await leaseClockLocker.query(
      "SELECT task_id FROM crawler.content_enrich_tasks WHERE task_id=$1 FOR UPDATE",
      [clockFence.task_id],
    );
    const clientTimeBeforeWait = (await pool.query(
      "SELECT clock_timestamp() AS observed_at",
    )).rows[0].observed_at;
    let expiredDetailApplications = 0;
    const clockExecutionRepository = new PostgresContentEnrichExecutionRepository({
      withTransaction,
      applyDetail: async () => { expiredDetailApplications += 1; },
      refreshHashes: async () => {},
      reconcilePublication: async () => {},
    });
    const staleRenewal = clockExecutionRepository.renewBatch({
      jobId: clockFenceOwner,
      channelId: channelB,
      tasks: [{
        task_id: clockFence.task_id,
        dispatch_generation: Number(clockFence.dispatch_generation),
      }],
      now: clientTimeBeforeWait,
      leaseExpiresAt: new Date(clientTimeBeforeWait.getTime() + 60_000),
      leaseDurationMs: 60_000,
    });
    await new Promise((resolve) => setTimeout(resolve, 350));
    await leaseClockLocker.query("COMMIT");
    leaseClockLocker.release();
    assert.equal(
      await staleRenewal,
      0,
      "a heartbeat queued behind a row lock must not resurrect an expired lease",
    );

    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='running',lease_owner=$2,
           lease_expires_at=clock_timestamp()+interval '200 milliseconds'
       WHERE task_id=$1`,
      [clockFence.task_id, clockFenceOwner],
    );
    const settleClockLocker = await pool.connect();
    await settleClockLocker.query("BEGIN");
    await settleClockLocker.query(
      "SELECT task_id FROM crawler.content_enrich_tasks WHERE task_id=$1 FOR UPDATE",
      [clockFence.task_id],
    );
    const staleSettlement = clockExecutionRepository.settleBatch({
      jobId: clockFenceOwner,
      channelId: channelB,
      outcomes: [{
        task_id: clockFence.task_id,
        dispatch_generation: Number(clockFence.dispatch_generation),
        kind: "done",
        detail: publicDetail("video-b"),
        access_status: "public",
        observed_at: clientTimeBeforeWait.toISOString(),
        error_message: null,
      }],
      unattemptedTasks: [],
      observedAt: clientTimeBeforeWait,
      settledAt: clientTimeBeforeWait,
    });
    await new Promise((resolve) => setTimeout(resolve, 350));
    await settleClockLocker.query("COMMIT");
    settleClockLocker.release();
    assert.deepEqual(await staleSettlement, {
      done: 0,
      terminal: 0,
      retryable: 0,
      dead_letter: 0,
      skipped: 1,
    });
    assert.equal(expiredDetailApplications, 0);

    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='done',lease_owner=NULL,lease_expires_at=NULL
       WHERE channel_id=ANY($1::text[])`,
      [[channelA, channelB, channelC]],
    );
    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='queued',next_retry_at=clock_timestamp()-interval '2 days'
       WHERE channel_id=$1`,
      [channelA],
    );
    const skewedControllerNow = new Date(Date.now() - 24 * 60 * 60_000);
    let immediateClaims = [];
    const immediateQueue = {
      async getJobCounts() { return {}; },
      async getJob() { return null; },
      async add(name, data, { jobId }) {
        immediateClaims = await clockExecutionRepository.claimBatch({
          jobId,
          channelId: data.channel_id,
          tasks: data.tasks,
          leaseDurationMs: 60_000,
        });
        return { id: jobId, name, data, state: "active" };
      },
    };
    const immediateDispatcher = new ContentEnrichDispatcher({
      repository: dispatchRepository,
      queue: immediateQueue,
      enabled: true,
      highWater: 1,
      refill: 1,
      batchSize: 1,
      leaseDurationMs: 60_000,
      now: () => skewedControllerNow,
    });
    const immediateDelivery = await immediateDispatcher.dispatchAvailable();
    assert.equal(immediateDelivery.enqueued, 1);
    assert.equal(immediateClaims.length, 1);
    assert.equal(immediateClaims[0].channel_id, channelA);

    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='done',lease_owner=NULL,lease_expires_at=NULL
       WHERE channel_id=$1`,
      [channelA],
    );
    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='queued',next_retry_at=clock_timestamp()-interval '1 second'
       WHERE channel_id=$1`,
      [channelC],
    );
    const queue = queueFixture({ failedAdds: 1 });
    const dispatcher = new ContentEnrichDispatcher({
      repository: dispatchRepository,
      queue,
      enabled: true,
      highWater: 2,
      refill: 1,
      batchSize: 1,
      leaseDurationMs: 5 * 60_000,
      now: () => skewedControllerNow,
    });

    const failedDelivery = await dispatcher.dispatchAvailable();
    assert.equal(failedDelivery.failed, 1);
    const leasedAfterFailure = (await pool.query(
      `SELECT status,dispatch_generation,lease_owner
       FROM crawler.content_enrich_tasks WHERE channel_id=$1`,
      [channelC],
    )).rows[0];
    assert.equal(leasedAfterFailure.status, "leased");
    assert.equal(leasedAfterFailure.dispatch_generation, "1");
    assert.ok(leasedAfterFailure.lease_owner);

    const recoveredDelivery = await dispatcher.dispatchAvailable();
    assert.equal(recoveredDelivery.recovered, 1);
    assert.equal(queue.jobs.has(leasedAfterFailure.lease_owner), true);
    const firstJob = queue.jobs.get(leasedAfterFailure.lease_owner);
    const publicationCalls = [];
    const executionRepository = new PostgresContentEnrichExecutionRepository({
      withTransaction,
      applyDetail: applyIncrementalVideoDetail,
      refreshHashes: refreshVideoPublicationItemHashes,
      reconcilePublication: async (_client, input) => publicationCalls.push(input),
    });
    const retryExecutor = new ContentEnrichExecutor({
      repository: executionRepository,
      fetchDetail: async () => { throw new Error("temporary upstream timeout"); },
      now: () => now,
      leaseDurationMs: 60_000,
      retryBaseMs: 1_000,
      retryMaxMs: 8_000,
    });

    const retryResult = await retryExecutor.execute(firstJob);
    assert.equal(retryResult.retryable, 1);
    assert.deepEqual((await pool.query(
      `SELECT status,payload_json
       FROM crawler.task_events
       WHERE queue_name='youtube-content-enrich'
         AND job_id=$1
         AND status IN ('claimed','checkpointed')
       ORDER BY event_id`,
      [firstJob.id],
    )).rows, [
      {
        status: "claimed",
        payload_json: {
          requested: 1,
          claimed: 1,
          attempted: 0,
          done: 0,
          terminal: 0,
          retryable: 0,
          dead_letter: 0,
          skipped: 0,
        },
      },
      {
        status: "checkpointed",
        payload_json: {
        requested: 1,
        claimed: 0,
        attempted: 1,
        done: 0,
        terminal: 0,
        retryable: 1,
        dead_letter: 0,
        skipped: 0,
        },
      },
    ]);
    const failedTask = (await pool.query(
      `SELECT status,attempts,next_retry_at,dispatch_generation
       FROM crawler.content_enrich_tasks WHERE channel_id=$1`,
      [channelC],
    )).rows[0];
    assert.equal(failedTask.status, "failed");
    assert.equal(failedTask.attempts, 1);
    assert.equal(failedTask.dispatch_generation, "1");
    assert.equal(failedTask.next_retry_at.toISOString(), new Date(now.getTime() + 1_000).toISOString());
    await withTransaction((client) => queueRefreshTask(client, {
      contentKey: contentKey(channelC),
      channelId: channelC,
      runId: "incremental:content-enrich:reentry",
      observationId: randomUUID(),
      jobType: "player-refresh",
      error: new Error("same open Task observed again"),
    }));
    const reenteredTask = (await pool.query(
      `SELECT status,attempts,next_retry_at
       FROM crawler.content_enrich_tasks
       WHERE channel_id=$1`,
      [channelC],
    )).rows[0];
    assert.equal(reenteredTask.status, "failed");
    assert.equal(reenteredTask.attempts, 1);
    assert.equal(reenteredTask.next_retry_at.toISOString(), failedTask.next_retry_at.toISOString());

    firstJob.state = "completed";
    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET next_retry_at=clock_timestamp()-interval '1 second'
       WHERE channel_id=$1`,
      [channelC],
    );
    const streamId = randomUUID();
    const observationId = randomUUID();
    const videoRunId = `incremental:content-enrich:${channelC}`;
    const factsHash = `sha256:${"c".repeat(64)}`;
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,trigger_reason,
         policy_version,crawler_version,started_at,finished_at
       ) VALUES ($1,$2,'done','incremental','done','clock_due',
                 'v16-rule-6','integration-test',$3,$3)`,
      [videoRunId, channelC, now],
    );
    await pool.query(
      `INSERT INTO crawler.crawl_observations (
         observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
         trigger_reason,outcome,outcome_reason_code,result_summary_json,facts_hash,
         crawler_version,extractor_versions
       ) VALUES (
         $1,$2,$3,$4,'video',1,'clock_due','complete','video_cycle_complete',
         '{"discovery":{"items":1,"stop_reason":"list_end","parse_gap_count":0}}'::jsonb,
         $5,'integration-test','{}'::jsonb
       )`,
      [observationId, now, channelC, videoRunId, factsHash],
    );
    await pool.query(
      `INSERT INTO crawler.channel_domain_cursors (
         channel_id,observation_kind,latest_sequence,latest_observation_id,
         latest_observed_at,latest_complete_observation_id,latest_complete_observed_at,
         source_cursor,current_facts_hash
       ) VALUES ($1,'video',1,$2,$3,$2,$3,'{"terminal_reason":"list_end"}'::jsonb,$4)`,
      [channelC, observationId, now, factsHash],
    );
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"content-enrich-test"}'::jsonb,$3,now(),
         'integration-test','Content Enrich test','integration-test','capture enabled'
       )`,
      [streamId, `content-enrich-${channelC}`, PUBLICATION_WRITER_VERSION],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','Content Enrich owner')`,
      [streamId, channelC],
    );
    await pool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'integration-test','hold for baseline')`,
      [streamId, channelC],
    );
    const nextDelivery = await dispatcher.dispatchAvailable();
    assert.equal(nextDelivery.enqueued, 1);
    const secondJob = [...queue.jobs.values()].find((job) => job.state === "waiting");
    assert.notEqual(secondJob.id, firstJob.id);
    const publishingRepository = new PostgresContentEnrichExecutionRepository({
      withTransaction,
      applyDetail: applyIncrementalVideoDetail,
      refreshHashes: refreshVideoPublicationItemHashes,
      reconcilePublication,
    });
    const successExecutor = new ContentEnrichExecutor({
      repository: publishingRepository,
      fetchDetail: async (videoId) => publicDetail(videoId),
      now: () => now,
      leaseDurationMs: 60_000,
    });

    const successResult = await successExecutor.execute(secondJob);
    assert.equal(successResult.done, 1);
    const enriched = (await pool.query(
      `SELECT task.status,task.attempts,task.dispatch_generation,
              content.title,content.last_enriched_at,content.publication_item_hash
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.channel_id=$1`,
      [channelC],
    )).rows[0];
    assert.equal(enriched.status, "done");
    assert.equal(enriched.attempts, 1);
    assert.equal(enriched.dispatch_generation, "2");
    assert.equal(enriched.title, "Enriched video-c");
    assert.equal(enriched.last_enriched_at.toISOString(), now.toISOString());
    assert.match(enriched.publication_item_hash, /^sha256:[0-9a-f]{64}$/);
    const published = await pool.query(
      `SELECT current.readiness_status,current.data_sequence,
              revision.revision_type,revision.operation,outbox.status AS outbox_status
       FROM publication.domain_current current
       JOIN publication.revision revision
         ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE current.publication_stream_id=$1
         AND current.channel_id=$2
         AND current.domain='video'`,
      [streamId, channelC],
    );
    assert.deepEqual(published.rows, [{
      readiness_status: "ready",
      data_sequence: "1",
      revision_type: "bootstrap",
      operation: "replace_window",
      outbox_status: "held",
    }]);

    await withTransaction((client) => queueRefreshTask(client, {
      contentKey: contentKey(channelC),
      channelId: channelC,
      runId: "incremental:content-enrich:new-round",
      observationId: randomUUID(),
      jobType: "player-refresh",
      error: new Error("new detail round after completion"),
    }));
    const newRoundTask = (await pool.query(
      `SELECT status,attempts
       FROM crawler.content_enrich_tasks
       WHERE channel_id=$1`,
      [channelC],
    )).rows[0];
    assert.deepEqual(newRoundTask, { status: "queued", attempts: 0 });

    for (const queuedJob of queue.jobs.values()) queuedJob.state = "completed";
    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='queued',attempts=0,
           next_retry_at=clock_timestamp()-interval '1 second',
           lease_owner=NULL,lease_expires_at=NULL
       WHERE channel_id=$1`,
      [channelA],
    );
    const terminalDelivery = await dispatcher.dispatchAvailable();
    assert.equal(terminalDelivery.enqueued, 1);
    const terminalJob = [...queue.jobs.values()].find((job) => job.state === "waiting");
    const privateError = Object.assign(new Error("This is a private video"), {
      youtube_failure_evidence: { source: "yt_dlp_detail" },
    });
    const terminalExecutor = new ContentEnrichExecutor({
      repository: executionRepository,
      fetchDetail: async () => { throw privateError; },
      now: () => now,
      leaseDurationMs: 60_000,
    });

    const terminalResult = await terminalExecutor.execute(terminalJob);
    assert.equal(terminalResult.terminal, 1);
    const terminal = (await pool.query(
      `SELECT task.status,task.next_retry_at,task.dispatch_generation,
              content.access_status,content.access_status_source
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.channel_id=$1`,
      [channelA],
    )).rows[0];
    assert.deepEqual(terminal, {
      status: "terminal",
      next_retry_at: new Date(now.getTime() + 7 * 24 * 60 * 60_000),
      dispatch_generation: "2",
      access_status: "private",
      access_status_source: "yt_dlp_detail",
    });
    const duplicate = await terminalExecutor.execute(terminalJob);
    assert.equal(duplicate.claimed, 0);
    assert.equal(duplicate.skipped, 1);

    for (const queuedJob of queue.jobs.values()) queuedJob.state = "completed";
    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='done',next_retry_at=NULL,lease_owner=NULL,lease_expires_at=NULL
       WHERE channel_id<>$1`,
      [channelA],
    );
    const earlyAccessRecheck = await dispatcher.dispatchAvailable();
    assert.equal(earlyAccessRecheck.enqueued, 0);
    assert.equal(
      [...queue.jobs.values()].filter((queuedJob) => queuedJob.state === "waiting").length,
      0,
    );

    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET attempts=5,next_retry_at=clock_timestamp()-interval '1 second'
       WHERE channel_id=$1 AND status='terminal'`,
      [channelA],
    );
    const dueAccessRecheck = await dispatcher.dispatchAvailable();
    assert.equal(dueAccessRecheck.enqueued, 1);
    const accessRecheckJob = [...queue.jobs.values()].find(
      (queuedJob) => queuedJob.state === "waiting",
    );
    assert.ok(accessRecheckJob);
    assert.notEqual(accessRecheckJob.id, terminalJob.id);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts,dispatch_generation,lease_owner
       FROM crawler.content_enrich_tasks WHERE channel_id=$1`,
      [channelA],
    )).rows[0], {
      status: "leased",
      attempts: 0,
      dispatch_generation: "3",
      lease_owner: accessRecheckJob.id,
    });
    const duplicateAccessRecheck = await dispatcher.dispatchAvailable();
    assert.equal(duplicateAccessRecheck.enqueued, 0);
    assert.equal(duplicateAccessRecheck.existing, 1);
    assert.equal(
      [...queue.jobs.values()].filter((queuedJob) => queuedJob.state === "waiting").length,
      1,
    );

    const mixedChannel = "UCcontentenrichMixed";
    const mixedJobId = `content_enrich__${mixedChannel}__mixed`;
    const mixedTasks = [
      { taskId: "player-refresh:mixed-retry", videoId: "mixed-retry", attempts: 2 },
      { taskId: "player-refresh:mixed-success", videoId: "mixed-success", attempts: 0 },
    ];
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,'https://www.youtube.com/channel/' || $1,$1,'active')`,
      [mixedChannel],
    );
    for (const mixed of mixedTasks) {
      const mixedContentKey = `${mixedChannel}:video:${mixed.videoId}`;
      await pool.query(
        `INSERT INTO crawler.contents (
           content_key,channel_id,content_type,content_type_source,source_content_id,
           title,url,published_at,published_at_status,published_at_precision,
           access_status,first_seen_at,last_seen_at
         ) VALUES ($1,$2,'video','youtube_uploads_default:video',$3,$3,
                   'https://www.youtube.com/watch?v=' || $3,
                   '2026-08-01T00:00:00Z','exact','second','unknown',now(),now())`,
        [mixedContentKey, mixedChannel, mixed.videoId],
      );
      await pool.query(
        `INSERT INTO crawler.content_enrich_tasks (
           task_id,content_key,channel_id,job_type,status,priority,attempts,
           next_retry_at,lease_owner,lease_expires_at,dispatch_generation
         ) VALUES ($1,$2,$3,'player-refresh','leased',10,$4,$6,$5,
                   clock_timestamp()+interval '5 minutes',1)`,
        [mixed.taskId, mixedContentKey, mixedChannel, mixed.attempts, mixedJobId, now],
      );
    }
    const publicationFailure = new Error("simulated Publication failure");
    const mixedRepository = new PostgresContentEnrichExecutionRepository({
      withTransaction,
      applyDetail: applyIncrementalVideoDetail,
      refreshHashes: refreshVideoPublicationItemHashes,
      reconcilePublication: async () => { throw publicationFailure; },
    });
    const mixedExecutor = new ContentEnrichExecutor({
      repository: mixedRepository,
      fetchDetail: async (videoId) => {
        if (videoId === "mixed-retry") throw new Error("temporary upstream timeout");
        return publicDetail(videoId);
      },
      now: () => now,
      leaseDurationMs: 60_000,
      retryBaseMs: 60_000,
    });
    const mixedJob = {
      id: mixedJobId,
      queueName: "youtube-content-enrich",
      data: {
        channel_id: mixedChannel,
        tasks: mixedTasks.map((mixed) => ({
          task_id: mixed.taskId,
          dispatch_generation: 1,
        })),
      },
    };

    await assert.rejects(mixedExecutor.execute(mixedJob), (error) => {
      assert.equal(error, publicationFailure);
      assert.deepEqual(error.content_enrich_result, {
        requested: 2,
        claimed: 2,
        attempted: 2,
        done: 0,
        terminal: 0,
        retryable: 1,
        dead_letter: 0,
        skipped: 0,
      });
      return true;
    });
    assert.deepEqual((await pool.query(
      `SELECT content.source_content_id,task.status,task.attempts,task.next_retry_at,
              task.lease_owner,content.last_enriched_at
       FROM crawler.content_enrich_tasks task
       JOIN crawler.contents content USING (content_key)
       WHERE task.channel_id=$1
       ORDER BY content.source_content_id`,
      [mixedChannel],
    )).rows, [
      {
        source_content_id: "mixed-retry",
        status: "failed",
        attempts: 3,
        next_retry_at: new Date(now.getTime() + 4 * 60_000),
        lease_owner: null,
        last_enriched_at: null,
      },
      {
        source_content_id: "mixed-success",
        status: "running",
        attempts: 0,
        next_retry_at: now,
        lease_owner: mixedJobId,
        last_enriched_at: null,
      },
    ]);

    const deadChannel = "UCcontentenrichDead";
    const deadContentKey = `${deadChannel}:video:video-dead`;
    const deadTaskId = `player-refresh:${deadChannel}`;
    const deadJobId = `content_enrich__${deadChannel}__dead`;
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,'https://www.youtube.com/channel/' || $1,$1,'active')`,
      [deadChannel],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,content_type_source,source_content_id,
         title,url,published_at,published_at_status,published_at_precision,
         access_status,first_seen_at,last_seen_at
       ) VALUES ($1,$2,'video','youtube_uploads_default:video','video-dead',
                 'video-dead','https://www.youtube.com/watch?v=video-dead',
                 '2026-08-01T00:00:00Z','exact','second','unknown',now(),now())`,
      [deadContentKey, deadChannel],
    );
    await pool.query(
      `INSERT INTO crawler.content_enrich_tasks (
         task_id,content_key,channel_id,job_type,status,priority,attempts,
         next_retry_at,lease_owner,lease_expires_at,dispatch_generation
       ) VALUES ($1,$2,$3,'player-refresh','leased',10,3,now(),$4,
                 clock_timestamp()+interval '5 minutes',1)`,
      [deadTaskId, deadContentKey, deadChannel, deadJobId],
    );
    const deadJob = {
      id: deadJobId,
      queueName: "youtube-content-enrich",
      data: {
        channel_id: deadChannel,
        tasks: [{ task_id: deadTaskId, dispatch_generation: 1 }],
      },
    };
    const exhaustedExecutor = new ContentEnrichExecutor({
      repository: executionRepository,
      fetchDetail: async () => { throw new Error("temporary upstream timeout"); },
      now: () => now,
      leaseDurationMs: 60_000,
      maxAttempts: 4,
    });

    const exhausted = await exhaustedExecutor.execute(deadJob);
    assert.equal(exhausted.dead_letter, 1);
    assert.deepEqual((await pool.query(
      `SELECT status,attempts,next_retry_at
       FROM crawler.content_enrich_tasks WHERE task_id=$1`,
      [deadTaskId],
    )).rows[0], {
      status: "dead_letter",
      attempts: 4,
      next_retry_at: null,
    });
    await withTransaction((client) => queueRefreshTask(client, {
      contentKey: deadContentKey,
      channelId: deadChannel,
      runId: "incremental:content-enrich:dead-reentry",
      observationId: randomUUID(),
      jobType: "player-refresh",
      error: new Error("same exhausted Task observed again"),
    }));
    assert.deepEqual((await pool.query(
      `SELECT status,attempts
       FROM crawler.content_enrich_tasks WHERE task_id=$1`,
      [deadTaskId],
    )).rows[0], { status: "dead_letter", attempts: 4 });
    assert.equal((await exhaustedExecutor.execute(deadJob)).skipped, 1);

    await pool.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='done',lease_owner=NULL,lease_expires_at=NULL,next_retry_at=NULL`,
    );
    await pool.query(
      `UPDATE crawler.settings SET value_json='{"channel_id":""}'::jsonb
       WHERE setting_key='content_enrich_dispatch_cursor'`,
    );
    const priorityChannels = [
      {
        channelId: "UCpriorityADormant",
        status: "dormant",
        publishedAt: new Date(now.getTime() - 10 * 24 * 60 * 60_000),
      },
      {
        channelId: "UCpriorityBHistory",
        status: "active",
        publishedAt: new Date(now.getTime() - 120 * 24 * 60 * 60_000),
      },
      {
        channelId: "UCpriorityCRecent",
        status: "active",
        publishedAt: new Date(now.getTime() - 10 * 24 * 60 * 60_000),
      },
    ];
    for (const priorityChannel of priorityChannels) {
      const videoId = `video-${priorityChannel.channelId}`;
      const priorityContentKey = `${priorityChannel.channelId}:video:${videoId}`;
      await pool.query(
        `INSERT INTO crawler.channels (
           channel_id,channel_url,title,status,dormant_reason,dormant_since,
           dormant_recheck_day,dormant_last_probe_at,dormant_cycle
         ) VALUES (
           $1,'https://www.youtube.com/channel/' || $1,$1,$2,
           CASE WHEN $2='dormant' THEN 'no_published_content_within_90_days' END,
           CASE WHEN $2='dormant' THEN now() END,
           CASE WHEN $2='dormant' THEN current_date+30 END,
           CASE WHEN $2='dormant' THEN now() END,
           CASE WHEN $2='dormant' THEN 1 ELSE 0 END
         )`,
        [priorityChannel.channelId, priorityChannel.status],
      );
      await pool.query(
        `INSERT INTO crawler.contents (
           content_key,channel_id,content_type,content_type_source,source_content_id,
           title,url,published_at,published_at_status,published_at_precision,
           access_status,first_seen_at,last_seen_at
         ) VALUES ($1,$2,'video','youtube_uploads_default:video',$3,$3,
                   'https://www.youtube.com/watch?v=' || $3,$4,'exact','second',
                   'unknown',now(),now())`,
        [priorityContentKey, priorityChannel.channelId, videoId, priorityChannel.publishedAt],
      );
      await pool.query(
        `INSERT INTO crawler.content_enrich_tasks (
           task_id,content_key,channel_id,job_type,status,priority,next_retry_at
         ) VALUES ($1,$2,$3,'player-refresh','queued',10,now())`,
        [`player-refresh:${priorityChannel.channelId}`, priorityContentKey, priorityChannel.channelId],
      );
    }
    const priorityLease = await dispatchRepository.withDispatchLock((repository, lock) => (
      repository.leaseFairBatches({
        maxJobs: 3,
        batchSize: 1,
        leaseDurationMs: 60_000,
        dispatchOwner: lock.owner,
      })
    ));
    assert.equal(priorityLease.acquired, true);
    assert.deepEqual(priorityLease.result.map((batch) => batch.channel_id), [
      "UCpriorityCRecent",
      "UCpriorityBHistory",
      "UCpriorityADormant",
    ]);
    assert.deepEqual(priorityLease.result.map((batch) => batch.tasks.length), [1, 1, 1]);
    await context.test(
      "observability aggregates only durable Task claim and outcome events",
      () => assertPostgresObservability(pool),
    );
  } finally {
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE").catch(() => {});
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE").catch(() => {});
    await pool.end();
  }
});
