import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { ensureAutomaticPublicationOnboarding } from "../src/publicationChannelOnboarding.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("new Channel ownership waits for both online Delivery and a complete Initial Package", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const seedChannelId = `UCpublicationseed${suffix}`;
  const newChannelId = `UCpublicationnew${suffix}`;
  const oldChannelId = `UCpublicationold${suffix}`;
  const replayChannelId = `UCpublicationreplay${suffix}`;
  const batchId = `publication-auto-${suffix}`;
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
    const candidates = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         validation_finished_at,accepted_at
       ) VALUES
         ($1,$1,$2,$4,'accepted','1900-03-01T00:01:00Z','1900-03-01T00:01:00Z'),
         ($1,$1,$3,$5,'accepted','1900-01-31T00:01:00Z','1900-01-31T00:01:00Z'),
         ($1,$1,$6,$7,'accepted','1900-03-31T00:01:00Z','1900-03-31T00:01:00Z')
       RETURNING candidate_id,channel_id`,
      [
        batchId,
        newChannelId,
        oldChannelId,
        `https://www.youtube.com/channel/${newChannelId}`,
        `https://www.youtube.com/channel/${oldChannelId}`,
        replayChannelId,
        `https://www.youtube.com/channel/${replayChannelId}`,
      ],
    );
    const candidateByChannel = new Map(
      candidates.rows.map((row) => [row.channel_id, Number(row.candidate_id)]),
    );
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,agent_status,latest_run_id,
         registry_promotion_candidate_id,registry_promotion_run_id,created_at
       ) VALUES
         ($1,$5,'Publication seed','active','done','run:seed',NULL,NULL,'1900-01-01T00:00:00Z'),
         ($2,$6,'Publication new','active','done','run:new',$9,'run:new','1900-03-01T00:00:00Z'),
         ($3,$7,'Publication old','active','done','run:old',$10,'run:old','1900-01-31T00:00:00Z'),
         ($4,$8,'Publication repair replay','active','done','run:replay',NULL,NULL,'1900-03-31T00:00:00Z')`,
      [
        seedChannelId,
        newChannelId,
        oldChannelId,
        replayChannelId,
        `https://www.youtube.com/channel/${seedChannelId}`,
        `https://www.youtube.com/channel/${newChannelId}`,
        `https://www.youtube.com/channel/${oldChannelId}`,
        `https://www.youtube.com/channel/${replayChannelId}`,
        candidateByChannel.get(newChannelId),
        candidateByChannel.get(oldChannelId),
      ],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,detail_status,started_at,finished_at,
         publication_finalized_status,publication_finalized_at
       ) VALUES
         ('run:new',$1,$3,'done','full','done','1900-03-01T00:01:00Z','1900-03-01T00:10:00Z',
          'ready_auto','1900-03-01T00:10:00Z'),
         ('run:old',$2,$4,'done','full','done','1900-01-31T00:01:00Z','1900-01-31T00:10:00Z',
          'ready_auto','1900-01-31T00:10:00Z'),
         ('run:replay',$5,$6,'done','full','done','1900-03-31T00:01:00Z','1900-03-31T00:10:00Z',
          NULL,NULL)`,
      [
        newChannelId,
        oldChannelId,
        candidateByChannel.get(newChannelId),
        candidateByChannel.get(oldChannelId),
        replayChannelId,
        candidateByChannel.get(replayChannelId),
      ],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES
         ($1,'run:new','ready_auto','{}'::jsonb,'{}'::jsonb,'1900-03-01T00:10:00Z'),
         ($2,'run:old','ready_auto','{}'::jsonb,'{}'::jsonb,'1900-01-31T00:10:00Z')`,
      [newChannelId, oldChannelId],
    );
    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,'1900-02-01T00:00:00Z',
         'integration-test','automatic onboarding test','integration-test','capture enabled'
       )`,
      [streamId, `publication-auto-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','online seed owner')`,
      [streamId, seedChannelId],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',now(),'integration-test','online seed route')`,
      [streamId, seedChannelId],
    );

    const manualBootstrap = await ensureAutomaticPublicationOnboarding(client, {
      channelId: seedChannelId,
      runId: "run:seed",
    });
    assert.equal(manualBootstrap.status, "existing");

    const incomplete = await ensureAutomaticPublicationOnboarding(client, {
      channelId: newChannelId,
      runId: "run:new",
    });
    assert.equal(incomplete.status, "initial_package_not_ready");
    assert.equal(incomplete.domains.length, 3);
    assert.equal(incomplete.domains.some((domain) => domain.readiness_status === "not_ready"), true);
    const noPartialBootstrap = await client.query(
      `SELECT
         (SELECT count(*)::int FROM publication.channel_stream_state
           WHERE channel_id=$1) AS owners,
         (SELECT count(*)::int FROM publication.channel_delivery_state
           WHERE channel_id=$1) AS deliveries,
         (SELECT count(*)::int FROM publication.domain_current
           WHERE channel_id=$1) AS currents,
         (SELECT count(*)::int FROM publication.revision
           WHERE channel_id=$1) AS revisions,
         (SELECT count(*)::int FROM publication.outbox AS outbox
           JOIN publication.revision AS revision USING (revision_id)
           WHERE revision.channel_id=$1) AS outbox`,
      [newChannelId],
    );
    assert.deepEqual(noPartialBootstrap.rows[0], {
      owners: 0,
      deliveries: 0,
      currents: 0,
      revisions: 0,
      outbox: 0,
    });

    const old = await ensureAutomaticPublicationOnboarding(client, {
      channelId: oldChannelId,
      runId: "run:old",
    });
    assert.equal(old.status, "automatic_onboarding_not_online");
    const oldOwnership = await client.query(
      "SELECT count(*)::int AS count FROM publication.channel_stream_state WHERE channel_id=$1",
      [oldChannelId],
    );
    assert.equal(oldOwnership.rows[0].count, 0);

    const replay = await ensureAutomaticPublicationOnboarding(client, {
      channelId: replayChannelId,
      runId: "run:replay",
    });
    assert.equal(replay.status, "not_new_channel");
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
