import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("Capture blocks old source writers and accepts the declared compatible Writer", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 1 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationwriter${suffix}`;
  const streamId = randomUUID();
  const contentKey = `${channelId}:video:writer-test`;
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Before Writer Barrier','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,source_content_id,title
       ) VALUES ($1,$2,'video','writer-test','Before Writer Barrier')`,
      [contentKey, channelId],
    );
    await pool.query(
      `INSERT INTO crawler.agent_profiles (channel_id,input_url,status,metrics_json)
       VALUES ($1,$2,'success','{}'::jsonb)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.finalized_profiles (channel_id,status)
       VALUES ($1,'pending')`,
      [channelId],
    );
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,now(),
         'integration-test','Writer Barrier test','integration-test','capture enabled'
       )`,
      [streamId, `writer-barrier-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'baseline','integration-test','Writer Barrier owner')`,
      [streamId, channelId],
    );

    const protectedUpdates = [
      ["UPDATE crawler.channels SET updated_at=clock_timestamp() WHERE channel_id=$1", channelId],
      ["UPDATE crawler.contents SET last_seen_at=clock_timestamp() WHERE content_key=$1", contentKey],
      ["UPDATE crawler.agent_profiles SET updated_at=clock_timestamp() WHERE channel_id=$1", channelId],
      ["UPDATE crawler.finalized_profiles SET updated_at=clock_timestamp() WHERE channel_id=$1", channelId],
    ];
    for (const [sql, key] of protectedUpdates) {
      await assert.rejects(
        pool.query(sql, [key]),
        (error) => error?.code === "55000" && /writer version is required/.test(error.message),
      );
    }

    const oldClient = await pool.connect();
    try {
      await oldClient.query("BEGIN");
      await oldClient.query(
        "SELECT set_config('publication.writer_version',$1,true)",
        ["publication-reconciler-v0"],
      );
      await assert.rejects(
        oldClient.query(
          "UPDATE crawler.channels SET updated_at=clock_timestamp() WHERE channel_id=$1",
          [channelId],
        ),
        (error) => error?.code === "55000" && /does not satisfy required version/.test(error.message),
      );
      await oldClient.query("ROLLBACK");
    } finally {
      oldClient.release();
    }

    const currentClient = await pool.connect();
    try {
      await currentClient.query("BEGIN");
      await currentClient.query(
        "SELECT set_config('publication.writer_version',$1,true)",
        [PUBLICATION_WRITER_VERSION],
      );
      for (const [sql, key] of protectedUpdates) {
        await currentClient.query(sql, [key]);
      }
      await currentClient.query("COMMIT");
    } finally {
      currentClient.release();
    }

    const pgbouncerCompatiblePool = new Pool({
      connectionString: integrationUrl,
      application_name: PUBLICATION_WRITER_VERSION,
      max: 1,
    });
    try {
      await pgbouncerCompatiblePool.query(
        "UPDATE crawler.channels SET updated_at=clock_timestamp() WHERE channel_id=$1",
        [channelId],
      );
    } finally {
      await pgbouncerCompatiblePool.end();
    }

    const compatibility = await pool.query(
      `SELECT publication.writer_version_satisfies(
                'publication-reconciler-v2','publication-reconciler-v1'
              ) AS newer,
              publication.writer_version_satisfies(
                'other-writer-v2','publication-reconciler-v1'
              ) AS other_family`,
    );
    assert.deepEqual(compatibility.rows[0], { newer: true, other_family: false });
  } finally {
    await pool.end();
  }
});
