import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  beginChannelCandidateValidation,
  markChannelCandidateAlreadyPromoted,
  persistChannelCandidateParserContractFailure,
  recordAcceptedChannelCandidateSnapshot,
  rejectChannelCandidateAdmission,
} from "../src/channelCandidateAttemptMutations.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("only the current Snapshot attempt can begin Candidate validation", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const dispatchBatchId = `candidate-fence-${suffix}`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [dispatchBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'queued',2,'channel-snapshot:g2',3)
       RETURNING candidate_id`,
      [
        dispatchBatchId,
        `UCcandidatefence${suffix}`,
        `https://www.youtube.com/channel/UCcandidatefence${suffix}`,
      ],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);

    await assert.rejects(
      beginChannelCandidateValidation(client.query.bind(client), {
        candidateId,
        dispatchGeneration: 1,
        jobId: "channel-snapshot:g1",
        bullmqAttempt: 3,
      }),
      /Candidate attempt Fence is stale/,
    );
    assert.equal((await client.query(
      "SELECT snapshot_attempts FROM crawler.channel_candidates WHERE candidate_id=$1",
      [candidateId],
    )).rows[0].snapshot_attempts, 0);

    await beginChannelCandidateValidation(client.query.bind(client), {
      candidateId,
      dispatchGeneration: 2,
      jobId: "channel-snapshot:g2",
      bullmqAttempt: 3,
    });
    const current = await client.query(
      `SELECT status,snapshot_attempts
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    );
    assert.deepEqual(current.rows[0], { status: "validating", snapshot_attempts: 1 });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("an accepted Candidate snapshot only accepts its current attempt", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const dispatchBatchId = `accepted-fence-${suffix}`;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [dispatchBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,accepted_at,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'accepted',now(),6,'channel-snapshot:g6',1)
       RETURNING candidate_id`,
      [
        dispatchBatchId,
        `UCacceptedfence${suffix}`,
        `https://www.youtube.com/channel/UCacceptedfence${suffix}`,
      ],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);
    const sourceJson = { qualification: { qualified: true } };

    await assert.rejects(
      recordAcceptedChannelCandidateSnapshot(
        client.query.bind(client),
        {
          candidateId,
          dispatchGeneration: 5,
          jobId: "channel-snapshot:g5",
          bullmqAttempt: 1,
        },
        { sourceJson },
      ),
      /Candidate attempt Fence is stale/,
    );
    assert.equal((await client.query(
      "SELECT snapshot_json ? 'qualification' AS qualified FROM crawler.channel_candidates WHERE candidate_id=$1",
      [candidateId],
    )).rows[0].qualified, false);

    await recordAcceptedChannelCandidateSnapshot(
      client.query.bind(client),
      {
        candidateId,
        dispatchGeneration: 6,
        jobId: "channel-snapshot:g6",
        bullmqAttempt: 1,
      },
      { sourceJson },
    );
    assert.deepEqual((await client.query(
      `SELECT status,snapshot_json->'qualification' AS qualification
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "accepted",
      qualification: sourceJson.qualification,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("a stale Snapshot attempt cannot reject Candidate admission", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const dispatchBatchId = `admission-fence-${suffix}`;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [dispatchBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'validating',5,'channel-snapshot:g5',2)
       RETURNING candidate_id`,
      [
        dispatchBatchId,
        `UCadmissionfence${suffix}`,
        `https://www.youtube.com/channel/UCadmissionfence${suffix}`,
      ],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);
    const rejection = {
      reason: "subscriber_count_below_minimum",
      sourceJson: { qualification: { qualified: false } },
    };

    await assert.rejects(
      rejectChannelCandidateAdmission(
        client.query.bind(client),
        {
          candidateId,
          dispatchGeneration: 4,
          jobId: "channel-snapshot:g4",
          bullmqAttempt: 2,
        },
        rejection,
      ),
      /Candidate attempt Fence is stale/,
    );
    assert.equal((await client.query(
      "SELECT status FROM crawler.channel_candidates WHERE candidate_id=$1",
      [candidateId],
    )).rows[0].status, "validating");

    await rejectChannelCandidateAdmission(
      client.query.bind(client),
      {
        candidateId,
        dispatchGeneration: 5,
        jobId: "channel-snapshot:g5",
        bullmqAttempt: 2,
      },
      rejection,
    );
    assert.deepEqual((await client.query(
      `SELECT status,reject_reason,snapshot_json->'qualification' AS qualification
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "rejected",
      reject_reason: rejection.reason,
      qualification: rejection.sourceJson.qualification,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("a stale Snapshot attempt cannot mark a Candidate as already promoted", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const dispatchBatchId = `existing-fence-${suffix}`;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [dispatchBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'queued',2,'channel-snapshot:g2',1)
       RETURNING candidate_id`,
      [
        dispatchBatchId,
        `UCexistingfence${suffix}`,
        `https://www.youtube.com/channel/UCexistingfence${suffix}`,
      ],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);

    await assert.rejects(
      markChannelCandidateAlreadyPromoted(client.query.bind(client), {
        candidateId,
        dispatchGeneration: 1,
        jobId: "channel-snapshot:g1",
        bullmqAttempt: 1,
      }),
      /Candidate attempt Fence is stale/,
    );
    assert.equal((await client.query(
      "SELECT status FROM crawler.channel_candidates WHERE candidate_id=$1",
      [candidateId],
    )).rows[0].status, "queued");

    await markChannelCandidateAlreadyPromoted(client.query.bind(client), {
      candidateId,
      dispatchGeneration: 2,
      jobId: "channel-snapshot:g2",
      bullmqAttempt: 1,
    });
    assert.deepEqual((await client.query(
      `SELECT status,reject_reason
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    )).rows[0], {
      status: "existing",
      reject_reason: "channel_already_promoted",
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("a parser failure cannot overwrite a newer Snapshot attempt", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const dispatchBatchId = `parser-fence-${suffix}`;
  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [dispatchBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_active_job_id,snapshot_active_job_attempt
       ) VALUES ($1,$1,$2,$3,'validating',4,'channel-snapshot:g4',2)
       RETURNING candidate_id`,
      [
        dispatchBatchId,
        `UCparserfence${suffix}`,
        `https://www.youtube.com/channel/UCparserfence${suffix}`,
      ],
    );
    const candidateId = Number(inserted.rows[0].candidate_id);
    const failure = {
      message: "Parser contract changed",
      details: { code: "PARSER_CONTRACT_CHANGED" },
    };

    await assert.rejects(
      persistChannelCandidateParserContractFailure(
        client.query.bind(client),
        {
          candidateId,
          dispatchGeneration: 3,
          jobId: "channel-snapshot:g3",
          bullmqAttempt: 2,
        },
        failure,
      ),
      /Candidate attempt Fence is stale/,
    );
    const stale = await client.query(
      `SELECT status,snapshot_json ? 'parser_contract_error' AS has_parser_error
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    );
    assert.deepEqual(stale.rows[0], { status: "validating", has_parser_error: false });

    await persistChannelCandidateParserContractFailure(
      client.query.bind(client),
      {
        candidateId,
        dispatchGeneration: 4,
        jobId: "channel-snapshot:g4",
        bullmqAttempt: 2,
      },
      failure,
    );
    const current = await client.query(
      `SELECT status,snapshot_json->'parser_contract_error' AS parser_error
       FROM crawler.channel_candidates WHERE candidate_id=$1`,
      [candidateId],
    );
    assert.deepEqual(current.rows[0], {
      status: "failed",
      parser_error: failure.details,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
