import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  hasOpenPipelineCrawlerWork,
  loadPipelineFinalizeBlockers,
  loadPublicationGapRepairCandidates,
} from "../src/finalizeRecoveryPolicy.js";
import { prepareChannelRun } from "../src/channelRunBinding.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("only a pending Batch-scoped system retry releases its materialized open Channel Run", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `migration-finalize-blocker-${suffix}`;
  const historicalBatchId = `historical-migration-finalize-blocker-${suffix}`;
  const channelId = `UCmigrationfinalizeblocker${suffix}`;
  const runId = `run:migration-finalize-blocker:${suffix}`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'finishing',now())`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,validation_finished_at,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted',2,now(),now())
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    const intent = await client.query(
      `INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) VALUES (
         $1,current_database(),
         (SELECT oid FROM pg_database WHERE datname=current_database()),$2,
         $3,'{}'::jsonb,repeat('c',64),$2,$4,2,now()
       ) RETURNING migration_intent_id`,
      [`migration-finalize-blocker:${suffix}`, candidateId, channelId, batchId],
    );
    const migrationIntentId = Number(intent.rows[0].migration_intent_id);
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,agent_status,latest_run_id
       ) VALUES ($1,$2,'Migration finalize blocker','active','pending',$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         started_at,result_json
       ) VALUES ($1,$2,$3,'waiting_agent','full','done',now(),$4::jsonb)`,
      [
        runId,
        channelId,
        candidateId,
        JSON.stringify({ dispatch_batch_id: batchId, pipeline_cycle_id: batchId }),
      ],
    );

    const hasOpenWork = () => hasOpenPipelineCrawlerWork(
      client.query.bind(client),
      batchId,
    );
    const loadBlockers = () => loadPipelineFinalizeBlockers(
      client.query.bind(client),
      batchId,
    );
    const expectedBlockers = { agentOpen: 1, finalOpen: 1, publicationOpen: 0 };

    assert.equal(await hasOpenWork(), true);
    assert.deepEqual(await loadBlockers(), expectedBlockers);

    await client.query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,resolved_at,resolution
       ) VALUES ($1,$2,$3,1,$4,0,'LEASE_CONFLICT','lease','{}'::jsonb,
                 'resolved',now(),'historical test evidence')`,
      [migrationIntentId, candidateId, historicalBatchId, `historical-job-${suffix}`],
    );

    assert.equal(await hasOpenWork(), true);
    assert.deepEqual(await loadBlockers(), expectedBlockers);

    await client.query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status
       ) VALUES ($1,$2,$3,2,$4,0,'LEASE_CONFLICT','lease','{}'::jsonb,'pending')`,
      [migrationIntentId, candidateId, batchId, `current-job-${suffix}`],
    );

    assert.equal(await hasOpenWork(), false);
    assert.deepEqual(await loadBlockers(), {
      agentOpen: 0,
      finalOpen: 0,
      publicationOpen: 0,
    });

    for (const status of ["retrying", "dispatched", "resolved", "cancelled"]) {
      await client.query(
        `UPDATE crawler.migration_system_retry_items
         SET status=$2,updated_at=now()
         WHERE candidate_id=$1 AND failed_dispatch_batch_id=$3`,
        [candidateId, status, batchId],
      );
      assert.equal(await hasOpenWork(), true, `${status} retry must leave the Run open`);
      assert.deepEqual(
        await loadBlockers(),
        expectedBlockers,
        `${status} retry must leave finalize blockers open`,
      );
    }
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("Publication completion blocks only for an eligible online automatic stream", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `publication-blocker-${suffix}`;
  const channelId = `UCpublicationblocker${suffix}`;
  const deadLetterSeedChannelId = `UCdeadletterseed${suffix}`;
  const automaticSeedChannelId = `UCautomaticseed${suffix}`;
  const runId = `run:publication-blocker:${suffix}`;
  const deadLetterStreamId = randomUUID();
  const automaticStreamId = randomUUID();
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         validation_finished_at,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted','2026-02-01T00:00:00Z','2026-02-01T00:00:00Z')
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,agent_status,latest_run_id,
         registry_promotion_candidate_id,registry_promotion_run_id,created_at
       ) VALUES
         ($1,$4,'Publication blocker','active','done',$7,$8,$7,'2026-02-01T00:00:00Z'),
         ($2,$5,'Dead letter route seed','active','pending',NULL,NULL,NULL,'2026-01-01T00:00:00Z'),
         ($3,$6,'Automatic route seed','active','pending',NULL,NULL,NULL,'2026-01-01T00:00:00Z')`,
      [
        channelId,
        deadLetterSeedChannelId,
        automaticSeedChannelId,
        `https://www.youtube.com/channel/${channelId}`,
        `https://www.youtube.com/channel/${deadLetterSeedChannelId}`,
        `https://www.youtube.com/channel/${automaticSeedChannelId}`,
        runId,
        candidateId,
      ],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         publication_finalized_status,publication_finalized_at,started_at,finished_at,result_json
       ) VALUES (
         $1,$2,$3,'done','full','done','ready_auto',now(),now(),now(),$4::jsonb
       )`,
      [runId, channelId, candidateId, JSON.stringify({ pipeline_cycle_id: batchId })],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now())`,
      [channelId, runId],
    );

    const blockers = () => loadPipelineFinalizeBlockers(client.query.bind(client), batchId);
    assert.deepEqual(await blockers(), { agentOpen: 0, finalOpen: 0, publicationOpen: 0 });

    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"stream_role":"dead_letter_recovery"}'::jsonb,$3,
         '2026-01-01T00:00:00Z','integration-test','dead letter stream',
         'integration-test','capture enabled'
       )`,
      [deadLetterStreamId, `dead-letter-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','dead letter route seed')`,
      [deadLetterStreamId, deadLetterSeedChannelId],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',now(),'integration-test','dead letter route seed')`,
      [deadLetterStreamId, deadLetterSeedChannelId],
    );
    assert.deepEqual(await blockers(), { agentOpen: 0, finalOpen: 0, publicationOpen: 0 });

    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,
         '2026-01-01T00:00:00Z','integration-test','automatic stream',
         'integration-test','capture enabled'
       )`,
      [automaticStreamId, `automatic-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','automatic route seed')`,
      [automaticStreamId, automaticSeedChannelId],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',now(),'integration-test','automatic route seed')`,
      [automaticStreamId, automaticSeedChannelId],
    );
    assert.deepEqual(await blockers(), { agentOpen: 0, finalOpen: 0, publicationOpen: 1 });

    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,ownership_reference,
         state_changed_by,state_reason
       ) VALUES (
         $1,$2,'bootstrap',$3::jsonb,
         'integration-test','pending automatic Bootstrap'
       )`,
      [
        automaticStreamId,
        channelId,
        JSON.stringify({
          onboarding_mode: "automatic_bootstrap",
          initial_full_run_id: runId,
        }),
      ],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,
         state_changed_by,state_reason
       ) VALUES (
         'business',$1,$2,'online',now(),'integration-test','pending automatic Bootstrap'
       )`,
      [automaticStreamId, channelId],
    );
    assert.deepEqual(await blockers(), { agentOpen: 0, finalOpen: 0, publicationOpen: 1 });

    await client.query(
      `UPDATE publication.channel_stream_state
       SET seed_status='complete',seed_completed_at=now(),
           state_changed_at=now(),state_changed_by='integration-test',
           state_reason='automatic Bootstrap complete',updated_at=now()
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [automaticStreamId, channelId],
    );
    assert.deepEqual(await blockers(), { agentOpen: 0, finalOpen: 0, publicationOpen: 0 });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("Publication Gap recovery admits only the matching pending automatic Bootstrap owner", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `pending-owner-gap-${suffix}`;
  const channelId = `UCpendingownergap${suffix}`;
  const runId = `run:pending-owner-gap:${suffix}`;
  const streamId = randomUUID();
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         validation_finished_at,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted',now(),now())
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,agent_status,latest_run_id,
         registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Pending Owner Gap','active','done',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
         publication_finalized_status,publication_finalized_at,started_at,finished_at,
         result_json,updated_at
       ) VALUES (
         $1,$2,$3,'done','full','done','ready_auto',now()-interval '2 minutes',
         now()-interval '3 minutes',now()-interval '2 minutes',$4::jsonb,
         now()-interval '2 minutes'
       )`,
      [
        runId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          publication_gap_repair: { status: "required", domains: ["channel"] },
        }),
      ],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now()-interval '2 minutes')`,
      [channelId, runId],
    );
    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,now()-interval '1 day',
         'integration-test','pending owner recovery','integration-test','capture enabled'
       )`,
      [streamId, `pending-owner-gap-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,seed_status,ownership_reference,
         state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','pending',$3::jsonb,'integration-test','pending automatic Bootstrap')`,
      [
        streamId,
        channelId,
        JSON.stringify({
          onboarding_mode: "automatic_bootstrap",
          initial_full_run_id: runId,
        }),
      ],
    );

    const load = () => loadPublicationGapRepairCandidates(client.query.bind(client), {
      pipelineCycleId: batchId,
      includeRunIds: [runId],
      limit: 5,
    });
    assert.deepEqual((await load()).map((row) => row.run_id), [runId]);

    await client.query("SAVEPOINT pending_owner_gap_child_prepare");
    const childRunId = `run:pending-owner-gap-child:${suffix}`;
    await prepareChannelRun(client, {
      runId: childRunId,
      channelId,
      candidateId,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: {
        pipeline_cycle_id: batchId,
        final_repair: { rounds: 1, parent_run_id: runId, mode: "channel" },
        publication_gap_repair: {
          status: "required",
          domains: ["channel"],
          root_run_id: runId,
        },
      },
    });
    const advanced = await client.query(
      "SELECT latest_run_id FROM crawler.channels WHERE channel_id=$1",
      [channelId],
    );
    assert.equal(advanced.rows[0].latest_run_id, childRunId);
    await client.query("ROLLBACK TO SAVEPOINT pending_owner_gap_child_prepare");

    await client.query(
      `UPDATE publication.channel_stream_state
       SET ownership_reference=jsonb_set(
             ownership_reference,'{initial_full_run_id}',to_jsonb($3::text),true
           )
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [streamId, channelId, `run:other:${suffix}`],
    );
    assert.deepEqual(await load(), []);

    await client.query(
      `UPDATE publication.channel_stream_state
       SET ownership_reference=$3::jsonb
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [streamId, channelId, JSON.stringify({ cohort_id: "manual-cutover" })],
    );
    assert.deepEqual(await load(), []);
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
