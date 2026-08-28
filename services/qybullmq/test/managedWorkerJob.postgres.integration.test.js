import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";

import {
  markChannelCandidateJobAttemptActive,
  recordChannelCandidateJobFailure,
} from "../src/managedWorkerJob.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("a concurrent terminal write and newer dispatch generation reject a late failed event", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 3 });
  const setup = await pool.connect();
  const terminalWriter = await pool.connect();
  const failedListener = await pool.connect();
  const suffix = randomUUID();
  const batchId = `managed-worker-failure-test:${suffix}`;
  let candidateId = null;
  const jobId = `managed-worker-failure-job:${suffix}`;

  try {
    const identity = await setup.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /test/i, "integration URL must target a test database");
    await setup.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'running','{}'::jsonb)`,
      [batchId],
    );
    const candidate = await setup.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,source_json
       ) VALUES ($1,$1,$2,$3,'queued',5,'{}'::jsonb)
       RETURNING candidate_id`,
      [batchId, `UC${suffix.replaceAll("-", "")}`, `https://www.youtube.com/channel/UC${suffix}`],
    );
    candidateId = Number(candidate.rows[0].candidate_id);
    assert.equal(await markChannelCandidateJobAttemptActive(
      setup.query.bind(setup),
      {
        id: jobId,
        attemptsMade: 0,
        data: { candidate_id: candidateId, dispatch_generation: 5 },
      },
    ), true);

    await terminalWriter.query("BEGIN");
    await terminalWriter.query(
      `UPDATE crawler.channel_candidates
       SET status='failed',error_message='budget exhausted',updated_at=now()
       WHERE candidate_id=$1`,
      [candidateId],
    );
    const lateFailure = recordChannelCandidateJobFailure(
      failedListener.query.bind(failedListener),
      {
        id: jobId,
        attemptsMade: 1,
        data: { candidate_id: candidateId, dispatch_generation: 5 },
      },
      { disposition: "queued", message: "late BullMQ failed event" },
    );
    await new Promise((resolve) => setImmediate(resolve));
    await terminalWriter.query("COMMIT");

    assert.equal(await lateFailure, false);
    assert.deepEqual((await setup.query(
      `SELECT status,error_message FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "failed",
      error_message: "budget exhausted",
    });

    await setup.query(
      `UPDATE crawler.channel_candidates
       SET status='queued',snapshot_dispatch_generation=6,error_message=NULL,updated_at=now()
       WHERE candidate_id=$1`,
      [candidateId],
    );
    assert.equal(await markChannelCandidateJobAttemptActive(
      setup.query.bind(setup),
      {
        id: jobId,
        attemptsMade: 0,
        data: { candidate_id: candidateId, dispatch_generation: 6 },
      },
    ), true);
    assert.equal(await recordChannelCandidateJobFailure(
      setup.query.bind(setup),
      {
        id: jobId,
        attemptsMade: 1,
        data: { candidate_id: candidateId, dispatch_generation: 5 },
      },
      { disposition: "failed", message: "stale generation" },
    ), false);
    assert.deepEqual((await setup.query(
      `SELECT status,snapshot_dispatch_generation::text,error_message
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "queued",
      snapshot_dispatch_generation: "6",
      error_message: null,
    });

    await setup.query(
      `UPDATE crawler.channel_candidates
       SET status='validating',snapshot_dispatch_generation=7,error_message=NULL,updated_at=now()
       WHERE candidate_id=$1`,
      [candidateId],
    );
    assert.equal(await markChannelCandidateJobAttemptActive(
      setup.query.bind(setup),
      {
        id: jobId,
        attemptsMade: 1,
        data: { candidate_id: candidateId, dispatch_generation: 7 },
      },
    ), true);
    assert.equal(await recordChannelCandidateJobFailure(
      setup.query.bind(setup),
      {
        id: jobId,
        queueName: "youtube-channel-crawl",
        attemptsMade: 1,
        data: { candidate_id: candidateId, dispatch_generation: 7 },
      },
      { disposition: "queued", message: "late attempt one failure" },
    ), false);
    assert.deepEqual((await setup.query(
      `SELECT status,snapshot_dispatch_generation::text,error_message
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "validating",
      snapshot_dispatch_generation: "7",
      error_message: null,
    });
  } finally {
    await terminalWriter.query("ROLLBACK").catch(() => {});
    if (candidateId !== null) {
      await setup.query("DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1", [batchId])
        .catch(() => {});
    }
    setup.release();
    terminalWriter.release();
    failedListener.release();
    await pool.end();
  }
});

test("an accepted Candidate can resume its Job without becoming mutable by failed events", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID();
  const batchId = `managed-worker-accepted-retry:${suffix}`;
  const jobId = `managed-worker-accepted-job:${suffix}`;

  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'running','{}'::jsonb)`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,accepted_at,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt,
         source_json
       ) VALUES ($1,$1,$2,$3,'accepted',now(),3,$4,1,'{}'::jsonb)
       RETURNING candidate_id`,
      [
        batchId,
        `UC${suffix.replaceAll("-", "")}`,
        `https://www.youtube.com/channel/UC${suffix}`,
        jobId,
      ],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    const retryJob = {
      id: jobId,
      attemptsMade: 1,
      data: { candidate_id: candidateId, dispatch_generation: 3 },
    };

    assert.equal(
      await markChannelCandidateJobAttemptActive(client.query.bind(client), retryJob),
      true,
    );
    assert.equal(await recordChannelCandidateJobFailure(
      client.query.bind(client),
      { ...retryJob, attemptsMade: 2 },
      { disposition: "queued", message: "retry failed after promotion" },
    ), false);
    assert.deepEqual((await client.query(
      `SELECT status,snapshot_active_job_id,snapshot_active_job_attempt,error_message
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "accepted",
      snapshot_active_job_id: jobId,
      snapshot_active_job_attempt: 2,
      error_message: null,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
