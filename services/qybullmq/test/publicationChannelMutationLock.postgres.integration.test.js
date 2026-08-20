import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  lockPublicationChannelMutation,
  lockPublicationRunMutation,
} from "../src/publicationChannelMutationLock.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("a Run-derived Finalize lock conflicts with Legacy Adoption's Channel lock", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  let first = null;
  let second = null;
  let channelId = null;
  try {
    const identity = await pool.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    const suffix = randomUUID().replaceAll("-", "");
    channelId = `UClock${suffix}`;
    const runId = `run:lock:${suffix}`;
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Publication Lock Test','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (run_id,channel_id,status,crawl_mode)
       VALUES ($1,$2,'running','full')`,
      [runId, channelId],
    );

    first = await pool.connect();
    second = await pool.connect();
    await first.query("BEGIN");
    await second.query("BEGIN");
    await second.query("SET LOCAL lock_timeout = '150ms'");

    await lockPublicationChannelMutation(first, channelId);
    await assert.rejects(
      lockPublicationRunMutation(second, runId),
      (error) => error?.code === "55P03",
    );
  } finally {
    if (first) await first.query("ROLLBACK").catch(() => {});
    if (second) await second.query("ROLLBACK").catch(() => {});
    first?.release();
    second?.release();
    if (channelId) {
      await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]);
    }
    await pool.end();
  }
});
