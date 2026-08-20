import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { prepareChannelRun } from "../src/channelRunBinding.js";

const { Pool } = pg;
const integrationUrl = process.env.CHANNEL_RUN_POSTGRES_TEST_URL;

test("Channel Run re-prepare persists later result fields", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  let client = null;
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCrunmerge${suffix}`;
  const runId = `run:merge:${suffix}`;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Channel Run merge integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await prepareChannelRun(client, {
      runId,
      channelId,
      candidateId: null,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: { job_id: "first-job", shared: "first" },
    });
    await prepareChannelRun(client, {
      runId,
      channelId,
      candidateId: null,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: {
        pending_initial_about_observation: {
          current: { isVerified: false, isVerifiedStatus: "not_verified" },
        },
        shared: "second",
      },
    });

    const result = await client.query(
      "SELECT result_json FROM crawler.channel_runs WHERE run_id=$1",
      [runId],
    );
    assert.deepEqual(result.rows[0].result_json, {
      job_id: "first-job",
      pending_initial_about_observation: {
        current: { isVerified: false, isVerifiedStatus: "not_verified" },
      },
      shared: "second",
    });
  } finally {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
      client.release();
    }
    await pool.end();
  }
});

test("Channel Run identity cannot be changed by reusing a Run ID", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  let client = null;
  const suffix = randomUUID().replaceAll("-", "");
  const firstChannelId = `UCrunfirst${suffix}`;
  const secondChannelId = `UCrunsecond${suffix}`;
  const runId = `run:identity:${suffix}`;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$3,'First Channel','active'),($2,$4,'Second Channel','active')`,
      [
        firstChannelId,
        secondChannelId,
        `https://www.youtube.com/channel/${firstChannelId}`,
        `https://www.youtube.com/channel/${secondChannelId}`,
      ],
    );
    await prepareChannelRun(client, {
      runId,
      channelId: firstChannelId,
      candidateId: null,
      crawlMode: "full",
      contentLimit: 30,
      resultJson: { source: "first" },
    });
    await assert.rejects(
      prepareChannelRun(client, {
        runId,
        channelId: secondChannelId,
        candidateId: null,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: { source: "collision" },
      }),
      /Channel Run identity conflict/,
    );
    const stored = await client.query(
      "SELECT channel_id,result_json FROM crawler.channel_runs WHERE run_id=$1",
      [runId],
    );
    assert.equal(stored.rows[0].channel_id, firstChannelId);
    assert.deepEqual(stored.rows[0].result_json, { source: "first" });
  } finally {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
      client.release();
    }
    await pool.end();
  }
});

test("a finalized Promotion Run retry preserves its terminal state and a later latest Run", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  let client = null;
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCrunfinalized${suffix}`;
  const promotionRunId = `run:promotion:${suffix}`;
  const laterRunId = `run:repair:${suffix}`;
  try {
    client = await pool.connect();
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Finalized Run retry integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,result_json,
         finished_at,publication_finalized_status,publication_finalized_at
       ) VALUES
         ($1,$3,'done','full',30,'done','{"original":true}'::jsonb,
          now(),'ready_auto',now()),
         ($2,$3,'running','full',30,'pending','{}'::jsonb,NULL,NULL,NULL)`,
      [promotionRunId, laterRunId, channelId],
    );
    await client.query(
      "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
      [channelId, laterRunId],
    );

    await prepareChannelRun(client, {
      runId: promotionRunId,
      channelId,
      candidateId: null,
      crawlMode: "full",
      contentLimit: 50,
      resultJson: { recovered_retry: true },
    });

    const stored = await client.query(
      `SELECT channel.latest_run_id,run.status,run.detail_status,run.content_limit,
              run.result_json,run.publication_finalized_status,run.publication_finalized_at
       FROM crawler.channels AS channel
       JOIN crawler.channel_runs AS run ON run.run_id=$2
       WHERE channel.channel_id=$1`,
      [channelId, promotionRunId],
    );
    assert.equal(stored.rows[0].latest_run_id, laterRunId);
    assert.equal(stored.rows[0].status, "done");
    assert.equal(stored.rows[0].detail_status, "done");
    assert.equal(Number(stored.rows[0].content_limit), 30);
    assert.deepEqual(stored.rows[0].result_json, { original: true });
    assert.equal(stored.rows[0].publication_finalized_status, "ready_auto");
    assert.ok(stored.rows[0].publication_finalized_at);
  } finally {
    if (client) {
      await client.query("ROLLBACK").catch(() => {});
      client.release();
    }
    await pool.end();
  }
});
