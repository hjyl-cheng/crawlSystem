import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { applyMigrationActivityGate } from "../src/migrationActivityGate.js";

const { Pool } = pg;
const integrationUrl = process.env.INCREMENTAL_POSTGRES_TEST_URL;

async function applyGate(pool, runId) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await applyMigrationActivityGate(client, {
      runId,
      detailStatus: "done",
      evaluatedAt: "2026-07-21T12:00:00Z",
    });
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

test("migration activity SQL admits zero-content Channels as dormant", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `migration-gate:${suffix}`;
  const cases = [
    { name: "recent-boundary", publishedAt: "2026-04-23T00:00:00Z", expected: "passed" },
    { name: "old", publishedAt: "2026-04-22T23:59:00Z", expected: "dormant" },
    { name: "upcoming", excluded: "upcoming_live", expected: "dormant" },
    { name: "empty", expected: "dormant" },
    { name: "unknown-date", unknown: true, expected: "inconclusive" },
  ];
  const channelIds = [];

  try {
    await pool.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,result_json
       ) VALUES ($1,$1,'discovery_closed','{}'::jsonb)`,
      [batchId],
    );

    for (const item of cases) {
      const channelId = `UCgate${item.name.replaceAll("-", "")}${suffix}`;
      const runId = `run:gate:${item.name}:${suffix}`;
      channelIds.push(channelId);
      const candidate = await pool.query(
        `INSERT INTO crawler.channel_candidates (
           dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,accepted_at
         ) VALUES ($1,$1,$2,$3,'accepted',now())
         RETURNING candidate_id`,
        [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
      );
      const candidateId = candidate.rows[0].candidate_id;
      await pool.query(
        `INSERT INTO crawler.channels (
           channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status
         ) VALUES ($1,$2,$3,'active',1000,false,'pending')`,
        [channelId, `https://www.youtube.com/channel/${channelId}`, item.name],
      );
      await pool.query(
        `INSERT INTO crawler.channel_runs (
           run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
           detail_status,started_at,result_json
         ) VALUES (
           $1,$2,$3,'waiting_agent','full',30,'done','2026-07-21T23:59:00Z',
           jsonb_build_object(
             'dispatch_batch_id',$4::text,
             'migration_activity_gate',jsonb_build_object(
               'required',true,'decision','pending','max_age_days',90
             )
           )
         )`,
        [runId, channelId, candidateId, batchId],
      );

      if (item.publishedAt) {
        const contentKey = `${channelId}:video:${item.name}`;
        await pool.query(
          `INSERT INTO crawler.contents (
             content_key,channel_id,run_id,content_type,source_content_id,title,
             published_at,published_at_status,published_at_precision,is_recent
           ) VALUES ($1,$2,$3,'video',$4,$4,$5,'exact','second',true)`,
          [contentKey, channelId, runId, item.name, item.publishedAt],
        );
        await pool.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,content_type,type_status,
             detail_status,api_status,content_key
           ) VALUES ($1,$2,$3,1,'video','resolved','done','not_needed',$4)`,
          [runId, channelId, item.name, contentKey],
        );
      } else if (item.excluded) {
        await pool.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,content_type,type_status,
             detail_status,api_status,result_json
           ) VALUES (
             $1,$2,$3,1,'live','resolved','done','not_needed',
             jsonb_build_object('scope',jsonb_build_object('status','excluded','reason',$4::text))
           )`,
          [runId, channelId, item.name, item.excluded],
        );
      } else if (item.unknown) {
        await pool.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,type_status,
             detail_status,api_status,result_json
           ) VALUES ($1,$2,$3,1,'unavailable','unavailable','unavailable','{}'::jsonb)`,
          [runId, channelId, item.name],
        );
      }

      const decision = await applyGate(pool, runId);
      assert.equal(decision.decision, item.expected, item.name);
      const state = await pool.query(
        `SELECT channel.status AS channel_status,channel.ready_for_agent,
                channel.agent_status,candidate.status AS candidate_status,
                run.status AS run_status,
                run.result_json#>>'{migration_activity_gate,reference_day}' AS reference_day,
                (SELECT count(*)::int FROM crawler.crawl_observations observation
                 WHERE observation.channel_id=channel.channel_id) AS observation_count,
                (SELECT count(*)::int FROM crawler.crawler_outbox outbox
                 WHERE outbox.aggregate_key LIKE channel.channel_id || ':%') AS outbox_count
         FROM crawler.channels channel
         JOIN crawler.channel_candidates candidate ON candidate.candidate_id=$2
         JOIN crawler.channel_runs run ON run.run_id=$1
         WHERE channel.channel_id=$3`,
        [runId, candidateId, channelId],
      );
      const row = state.rows[0];
      assert.equal(row.reference_day, "2026-07-21", item.name);
      const dormant = item.expected === "dormant";
      assert.equal(row.observation_count, dormant ? 1 : 0, item.name);
      assert.equal(row.outbox_count, dormant ? 1 : 0, item.name);
      if (dormant) {
        assert.equal(row.channel_status, "dormant", item.name);
        assert.equal(row.ready_for_agent, false, item.name);
        assert.equal(row.agent_status, "skipped", item.name);
        assert.equal(row.candidate_status, "accepted", item.name);
        assert.equal(row.run_status, "done", item.name);
      } else {
        assert.equal(row.channel_status, "active", item.name);
        assert.equal(row.ready_for_agent, true, item.name);
        assert.equal(row.candidate_status, "accepted", item.name);
      }
    }
  } finally {
    await pool.query(
      "DELETE FROM crawler.channels WHERE channel_id=ANY($1::text[])",
      [channelIds],
    ).catch(() => {});
    await pool.query(
      "DELETE FROM crawler.query_dispatch_batches WHERE dispatch_batch_id=$1",
      [batchId],
    ).catch(() => {});
    await pool.end();
  }
});
