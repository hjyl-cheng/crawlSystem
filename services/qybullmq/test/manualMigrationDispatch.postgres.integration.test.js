import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { prepareManualMigration } from "../src/manualMigrationDispatch.js";
import { sourceSnapshotHash } from "../src/migrationSource.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("Target SQL materializes one immutable idempotent intent without a Source write", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCmanual${suffix}`;
  const batchId = `manual-target:${suffix}`;
  const rawSnapshot = {
    source_id: "integration-source-v1",
    source_database: "migration_source_test",
    source_database_oid: "16384",
    source_candidate_id: "42",
    source_candidate_status: "discovered",
    source_dispatch_batch_id: "legacy-source-batch",
    channel_id: channelId,
    channel_url: `https://www.youtube.com/channel/${channelId}`,
    handle: "@integration",
    title: "Integration Source",
    description: null,
    avatar_url: null,
    search_subscriber_count: "1200",
    search_subscriber_count_text: "1.2K",
    is_verified: false,
    priority: 100,
    snapshot_json: {},
    source_json: { source: "legacy_results_db" },
    source_created_at: "2026-08-01T00:00:00.000Z",
    source_updated_at: "2026-08-02T00:00:00.000Z",
  };
  const sourceSnapshot = {
    ...rawSnapshot,
    snapshot_sha256: sourceSnapshotHash(rawSnapshot),
  };

  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
       VALUES ('query_scheduler',jsonb_build_object('status','stopped'),now())
       ON CONFLICT (setting_key) DO UPDATE
       SET value_json=jsonb_build_object('status','stopped'),updated_at=now()`,
    );

    const first = await prepareManualMigration(client, {
      sourceSnapshot,
      batchId,
      minSubscriberCount: 1000,
    });
    const repeated = await prepareManualMigration(client, {
      sourceSnapshot,
      batchId,
      minSubscriberCount: 1000,
    });

    assert.equal(first.shouldEnqueue, true);
    assert.equal(first.previousStatus, null);
    assert.equal(first.candidate.snapshot_dispatch_generation, 1);
    assert.equal(repeated.shouldEnqueue, false);
    assert.equal(repeated.alreadyInProgress, true);
    assert.equal(repeated.intentId, first.intentId);
    assert.equal(repeated.candidate.candidate_id, first.candidate.candidate_id);

    const state = await client.query(
      `SELECT intent.source_id,intent.source_database,
              intent.source_candidate_id::text,intent.channel_id,
              intent.snapshot_sha256,intent.dispatch_attempts,
              candidate.status,candidate.dispatch_batch_id,
              candidate.snapshot_dispatch_generation::text,
              candidate.source_json#>>'{migration_source,database}' AS source_database_metadata
       FROM crawler.migration_channel_intents intent
       JOIN crawler.channel_candidates candidate
         ON candidate.candidate_id=intent.target_candidate_id
       WHERE intent.migration_intent_id=$1`,
      [first.intentId],
    );
    assert.deepEqual(state.rows[0], {
      source_id: "integration-source-v1",
      source_database: "migration_source_test",
      source_candidate_id: "42",
      channel_id: channelId,
      snapshot_sha256: sourceSnapshot.snapshot_sha256,
      dispatch_attempts: 1,
      status: "queued",
      dispatch_batch_id: batchId,
      snapshot_dispatch_generation: "1",
      source_database_metadata: "migration_source_test",
    });

    await assert.rejects(
      client.query(
        `UPDATE crawler.migration_channel_intents
         SET source_snapshot=jsonb_set(source_snapshot,'{title}','\"changed\"'::jsonb)
         WHERE migration_intent_id=$1`,
        [first.intentId],
      ),
      /immutable/,
    );
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
