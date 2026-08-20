import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { preparedFinalDetailRepairSql } from "../src/finalRepairCandidatePolicy.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("prepared Detail Repair evidence applies only to the next unrecorded round", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpreparedrepair${suffix}`;
  const runId = `run:prepared-repair:${suffix}`;
  const isPrepared = async () => {
    const result = await client.query(
      `SELECT ${preparedFinalDetailRepairSql("candidate", "run")} AS prepared
       FROM crawler.content_candidates AS candidate
       JOIN crawler.channel_runs AS run ON run.run_id=candidate.run_id
       WHERE candidate.run_id=$1`,
      [runId],
    );
    return result.rows[0]?.prepared === true;
  };

  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Prepared Detail Repair','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,result_json
       ) VALUES ($1,$2,'failed','full','failed',$3::jsonb)`,
      [runId, channelId, JSON.stringify({ final_repair: { rounds: 2 } })],
    );
    await client.query(
      "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
      [channelId, runId],
    );
    await client.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,detail_status,result_json
       ) VALUES ($1,$2,$3,1,'queued',$4::jsonb)`,
      [
        runId,
        channelId,
        `video-${suffix}`,
        JSON.stringify({
          final_repair_dispatch: {
            status: "prepared",
            mode: "detail",
            repair_round: 3,
            job_id: `final-repair-${suffix}`,
          },
        }),
      ],
    );

    assert.equal(await isPrepared(), true);
    await client.query(
      `UPDATE crawler.content_candidates
       SET detail_status='failed',missing_fields=ARRAY['content_type']::text[]
       WHERE run_id=$1`,
      [runId],
    );
    assert.equal(await isPrepared(), true);
    await client.query(
      `UPDATE crawler.content_candidates
       SET detail_status='done'
       WHERE run_id=$1`,
      [runId],
    );
    assert.equal(await isPrepared(), true);
    await client.query(
      `UPDATE crawler.channel_runs
       SET result_json=jsonb_set(result_json,'{final_repair,rounds}','3'::jsonb)
       WHERE run_id=$1`,
      [runId],
    );
    assert.equal(await isPrepared(), false);
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
