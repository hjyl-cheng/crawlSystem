import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { prepareManualMigration } from "../src/manualMigrationDispatch.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

test("manual migration SQL atomically rehomes exactly one legacy Candidate", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const sourceBatchId = `manual-test-source:${suffix}`;
  const targetBatchId = `manual-test-target:${suffix}`;
  const sourcePageId = `${sourceBatchId}:page:1`;
  const channelId = `UCmanual${suffix}`;

  try {
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at,result_json
       ) VALUES ($1,$1,'discovery_closed',now(),'{}'::jsonb)`,
      [sourceBatchId],
    );
    await client.query(
      `INSERT INTO crawler.query_pages (
         page_id,query_text,page_no,status,candidate_count,dispatch_batch_id
       ) VALUES ($1,'manual migration integration source',1,'done',1,$2)`,
      [sourcePageId, sourceBatchId],
    );
    const inserted = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,source_json
       ) VALUES ($1,$1,$2,$3,'discovered',jsonb_build_object('source','legacy_results_db'))
       RETURNING candidate_id`,
      [sourceBatchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = inserted.rows[0].candidate_id;
    await client.query(
      `INSERT INTO crawler.channel_candidate_sources (
         candidate_id,page_id,query_text,discovery_strategy,source_json
       ) VALUES ($1,$2,'manual migration integration source','results_db_import','{}'::jsonb)`,
      [candidateId, sourcePageId],
    );
    await client.query(
      `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
       VALUES ('query_scheduler',jsonb_build_object('status','stopped'),now())
       ON CONFLICT (setting_key) DO UPDATE
       SET value_json=crawler.settings.value_json || jsonb_build_object('status','stopped'),updated_at=now()`,
    );

    const prepared = await prepareManualMigration(client, {
      channelId,
      candidateId,
      batchId: targetBatchId,
      minSubscriberCount: 1000,
    });
    assert.equal(prepared.shouldEnqueue, true);
    assert.equal(prepared.previousStatus, "discovered");

    const state = await client.query(
      `SELECT candidate.status,candidate.dispatch_batch_id,candidate.pipeline_cycle_id,
              candidate.source_json#>>'{manual_migration,source_batch_id}' AS source_batch_id,
              source.page_id,source.query_text,
              scheduler.value_json->>'status' AS scheduler_status,
              scheduler.value_json->>'pipeline_cycle_id' AS scheduler_batch
       FROM crawler.channel_candidates candidate
       JOIN crawler.channel_candidate_sources source USING (candidate_id)
       JOIN crawler.settings scheduler ON scheduler.setting_key='query_scheduler'
       WHERE candidate.candidate_id=$1`,
      [candidateId],
    );
    assert.deepEqual(state.rows[0], {
      status: "queued",
      dispatch_batch_id: targetBatchId,
      pipeline_cycle_id: targetBatchId,
      source_batch_id: sourceBatchId,
      page_id: `${targetBatchId}:page:1`,
      query_text: "results.db manual migration",
      scheduler_status: "finishing",
      scheduler_batch: targetBatchId,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
