import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { markChannelRemoved } from "../src/channelLifecycle.js";
import { prepareChannelRun } from "../src/channelRunBinding.js";
import { claimChannelRegistryPromotion } from "../src/channelRegistryPromotion.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("the Registry primary key selects one immutable promotion winner and Repair cannot replace it", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCregistrypromotion${suffix}`;
  const winnerBatchId = `registry-winner-${suffix}`;
  const loserBatchId = `registry-loser-${suffix}`;
  const winnerRunId = `run:registry-winner:${suffix}`;
  const repairRunId = `run:registry-repair:${suffix}`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES
         ($1,$1,'validation_closed',now()),
         ($2,$2,'validation_closed',now())`,
      [winnerBatchId, loserBatchId],
    );
    const candidates = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status
       ) VALUES
         ($1,$1,$3,$4,'validating'),
         ($2,$2,$3,$4,'validating')
       RETURNING candidate_id,dispatch_batch_id`,
      [
        winnerBatchId,
        loserBatchId,
        channelId,
        `https://www.youtube.com/channel/${channelId}`,
      ],
    );
    const candidateByBatch = new Map(
      candidates.rows.map((row) => [row.dispatch_batch_id, Number(row.candidate_id)]),
    );
    const winnerCandidateId = candidateByBatch.get(winnerBatchId);
    const loserCandidateId = candidateByBatch.get(loserBatchId);
    const winner = await claimChannelRegistryPromotion(client, {
      candidateId: winnerCandidateId,
      runId: winnerRunId,
      channelId,
      channelUrl: `https://www.youtube.com/channel/${channelId}`,
      title: "Registry Winner",
      subscriberCount: 1234,
      subscriberCountText: "1.23K subscribers",
      readyForAgent: true,
      sourceJson: { source: "integration_winner" },
    });
    assert.equal(winner.status, "promoted");
    await prepareChannelRun(client, {
      runId: winnerRunId,
      channelId,
      candidateId: winnerCandidateId,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: { source: "integration_winner" },
    });
    await client.query("SAVEPOINT immutable_promotion_run");
    await assert.rejects(
      client.query(
        "UPDATE crawler.channel_runs SET crawl_mode='incremental' WHERE run_id=$1",
        [winnerRunId],
      ),
      /Channel Registry promotion Run identity is immutable and must be Full Crawl/,
    );
    await client.query("ROLLBACK TO SAVEPOINT immutable_promotion_run");

    const loser = await claimChannelRegistryPromotion(client, {
      candidateId: loserCandidateId,
      runId: `run:registry-loser:${suffix}`,
      channelId,
      channelUrl: `https://www.youtube.com/channel/${channelId}`,
      title: "Registry Loser",
      subscriberCount: 1234,
      subscriberCountText: "1.23K subscribers",
      readyForAgent: true,
      sourceJson: { source: "integration_loser" },
    });
    assert.deepEqual(loser, {
      status: "existing",
      promoted: false,
      channel_id: channelId,
      promotion_candidate_id: winnerCandidateId,
      promotion_run_id: winnerRunId,
    });

    const state = await client.query(
      `SELECT channel.registry_promotion_candidate_id,
              channel.registry_promotion_run_id,
              winner.status AS winner_status,loser.status AS loser_status
       FROM crawler.channels AS channel
       JOIN crawler.channel_candidates AS winner
         ON winner.candidate_id=$2
       JOIN crawler.channel_candidates AS loser
         ON loser.candidate_id=$3
       WHERE channel.channel_id=$1`,
      [channelId, winnerCandidateId, loserCandidateId],
    );
    assert.deepEqual(state.rows[0], {
      registry_promotion_candidate_id: String(winnerCandidateId),
      registry_promotion_run_id: winnerRunId,
      winner_status: "accepted",
      loser_status: "existing",
    });

    const streamId = randomUUID();
    const seedChannelId = `UCregistrypromotionseed${suffix}`;
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Registry Promotion route seed','active')`,
      [seedChannelId, `https://www.youtube.com/channel/${seedChannelId}`],
    );
    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,now()-interval '1 hour',
         'integration-test','Promotion package gate','integration-test','Capture enabled'
       )`,
      [streamId, `registry-promotion-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'baseline','integration-test','Online route seed')`,
      [streamId, seedChannelId],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',now(),'integration-test','Online route seed')`,
      [streamId, seedChannelId],
    );

    await client.query("SAVEPOINT immutable_promotion_candidate");
    await assert.rejects(
      client.query(
        `UPDATE crawler.channel_candidates
         SET status='rejected',reject_reason='retry_qualification_failed'
         WHERE candidate_id=$1`,
        [winnerCandidateId],
      ),
      /Accepted Channel Registry promotion Candidate is immutable/,
    );
    await client.query("ROLLBACK TO SAVEPOINT immutable_promotion_candidate");

    const mismatchedChannelId = `UCregistrywrong${suffix}`;
    const mismatchedRunId = `run:registry-wrong:${suffix}`;
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,detail_status
       ) VALUES ($1,$2,$3,'running','full',30,'pending')`,
      [mismatchedRunId, channelId, loserCandidateId],
    );
    await client.query("SAVEPOINT mismatched_promotion_identity");
    await assert.rejects(
      client.query(
        `INSERT INTO crawler.channels (
           channel_id,channel_url,title,status,
           registry_promotion_candidate_id,registry_promotion_run_id
         ) VALUES ($1,$2,'Mismatched Registry evidence','active',$3,$4)`,
        [
          mismatchedChannelId,
          `https://www.youtube.com/channel/${mismatchedChannelId}`,
          loserCandidateId,
          mismatchedRunId,
        ],
      ),
      /Channel Registry promotion Run must be its Full Crawl/,
    );
    await client.query("ROLLBACK TO SAVEPOINT mismatched_promotion_identity");

    await client.query("SAVEPOINT promotion_must_finalize");
    await assert.rejects(
      prepareChannelRun(client, {
        runId: repairRunId,
        channelId,
        candidateId: winnerCandidateId,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: { publication_repair: { batch_id: `repair-${suffix}` } },
      }),
      /Channel Registry promotion Run must Finalize before a later Run/,
    );
    await client.query("ROLLBACK TO SAVEPOINT promotion_must_finalize");
    await client.query(
      `UPDATE crawler.channel_runs
       SET status='done',detail_status='done',finished_at=now(),
           publication_finalized_status='ready_partial',publication_finalized_at=now()
       WHERE run_id=$1`,
      [winnerRunId],
    );
    await client.query("SAVEPOINT promotion_must_be_ready_auto");
    await assert.rejects(
      prepareChannelRun(client, {
        runId: repairRunId,
        channelId,
        candidateId: winnerCandidateId,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: { publication_repair: { batch_id: `repair-${suffix}` } },
      }),
      /Channel Registry promotion Run must Finalize before a later Run/,
    );
    await client.query("ROLLBACK TO SAVEPOINT promotion_must_be_ready_auto");
    await client.query(
      `UPDATE crawler.channel_runs
       SET publication_finalized_status='ready_auto',publication_finalized_at=now()
       WHERE run_id=$1`,
      [winnerRunId],
    );
    await client.query("SAVEPOINT promotion_requires_initial_package");
    await assert.rejects(
      prepareChannelRun(client, {
        runId: repairRunId,
        channelId,
        candidateId: winnerCandidateId,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: { publication_repair: { batch_id: `repair-${suffix}` } },
      }),
      /Channel Registry promotion Run must Finalize before a later Run/,
    );
    await client.query("ROLLBACK TO SAVEPOINT promotion_requires_initial_package");
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,ownership_reference,
         state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap',$3::jsonb,'integration-test','Pending Initial Package')`,
      [
        streamId,
        channelId,
        JSON.stringify({
          onboarding_mode: "automatic_bootstrap",
          initial_full_run_id: winnerRunId,
        }),
      ],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,
         source_ownership_reference,state_changed_by,state_reason
       ) VALUES (
         'business',$1,$2,'online',now(),$3::jsonb,'integration-test','Pending Initial Package'
       )`,
      [
        streamId,
        channelId,
        JSON.stringify({
          onboarding_mode: "automatic_bootstrap",
          initial_full_run_id: winnerRunId,
        }),
      ],
    );
    await client.query("SAVEPOINT pending_initial_package_cannot_pass");
    await assert.rejects(
      prepareChannelRun(client, {
        runId: repairRunId,
        channelId,
        candidateId: winnerCandidateId,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: { publication_repair: { batch_id: `repair-${suffix}` } },
      }),
      /Channel Registry promotion Run must Finalize before a later Run/,
    );
    await client.query("ROLLBACK TO SAVEPOINT pending_initial_package_cannot_pass");
    await client.query(
      `UPDATE publication.channel_stream_state
       SET seed_status='complete',seed_completed_at=now(),updated_at=now()
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [streamId, channelId],
    );
    await prepareChannelRun(client, {
      runId: repairRunId,
      channelId,
      candidateId: winnerCandidateId,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: { publication_repair: { batch_id: `repair-${suffix}` } },
    });
    const afterRepair = await client.query(
      `SELECT latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       FROM crawler.channels WHERE channel_id=$1`,
      [channelId],
    );
    assert.deepEqual(afterRepair.rows[0], {
      latest_run_id: repairRunId,
      registry_promotion_candidate_id: String(winnerCandidateId),
      registry_promotion_run_id: winnerRunId,
    });

    await client.query("SAVEPOINT immutable_evidence");
    await assert.rejects(
      client.query(
        `UPDATE crawler.channels
         SET registry_promotion_candidate_id=$2,registry_promotion_run_id=$3
         WHERE channel_id=$1`,
        [channelId, loserCandidateId, repairRunId],
      ),
      /Channel Registry promotion evidence is immutable/,
    );
    await client.query("ROLLBACK TO SAVEPOINT immutable_evidence");

    await client.query(
      "UPDATE crawler.channels SET status='paused' WHERE channel_id=$1",
      [channelId],
    );
    await client.query("SAVEPOINT immutable_paused_candidate");
    await assert.rejects(
      client.query(
        `UPDATE crawler.channel_candidates
         SET accepted_at=accepted_at + interval '1 second'
         WHERE candidate_id=$1`,
        [winnerCandidateId],
      ),
      /Accepted Channel Registry promotion Candidate is immutable/,
    );
    await client.query("ROLLBACK TO SAVEPOINT immutable_paused_candidate");

    await markChannelRemoved(client, {
      channelId,
      candidateId: winnerCandidateId,
      terminal: {
        failure_kind: "channel_removed",
        removed_reason: "channel_not_found",
        removed_source: "integration_test",
        evidence: "This channel does not exist.",
      },
      observedAt: "2026-07-28T12:00:00.000Z",
    });
    const removedPromotion = await client.query(
      `SELECT channel.status AS channel_status,candidate.status AS candidate_status,
              candidate.accepted_at
       FROM crawler.channels AS channel
       JOIN crawler.channel_candidates AS candidate
         ON candidate.candidate_id=channel.registry_promotion_candidate_id
       WHERE channel.channel_id=$1`,
      [channelId],
    );
    assert.equal(removedPromotion.rows[0].channel_status, "removed");
    assert.equal(removedPromotion.rows[0].candidate_status, "accepted");
    assert.ok(removedPromotion.rows[0].accepted_at);

    await client.query("SAVEPOINT immutable_removed_candidate");
    await assert.rejects(
      client.query(
        `UPDATE crawler.channel_candidates
         SET status='rejected',reject_reason='removed_retry'
         WHERE candidate_id=$1`,
        [winnerCandidateId],
      ),
      /Accepted Channel Registry promotion Candidate is immutable/,
    );
    await client.query("ROLLBACK TO SAVEPOINT immutable_removed_candidate");
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
