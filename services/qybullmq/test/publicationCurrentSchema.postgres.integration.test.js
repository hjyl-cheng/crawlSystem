import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

function lifecycleError(message) {
  return (error) => error?.code === "55000" && error.message.includes(message);
}

async function insertStream(pool, streamId, sourceKey, epoch) {
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       created_by,created_reason,status_changed_by,status_reason
     ) VALUES ($1,$2,$3::jsonb,'integration-test','lifecycle test',
               'integration-test','stream registered')`,
    [streamId, sourceKey, JSON.stringify({ database: "isolated-test", epoch })],
  );
}

test("Publication lifecycle constraints preserve Stream and Channel ownership history", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationlifecycle${suffix}`;
  const sourceKey = `publication-lifecycle-${suffix}`;
  const firstStreamId = randomUUID();
  const secondStreamId = randomUUID();
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Publication lifecycle integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await insertStream(pool, firstStreamId, sourceKey, 1);
    await insertStream(pool, secondStreamId, sourceKey, 2);

    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'baseline','integration-test','first owner')`,
      [firstStreamId, channelId],
    );
    await assert.rejects(
      pool.query(
        `INSERT INTO publication.channel_stream_state (
           publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
         ) VALUES ($1,$2,'cutover','integration-test','overlapping owner')`,
        [secondStreamId, channelId],
      ),
      (error) => error?.code === "23505"
        && error.constraint === "ux_publication_channel_stream_owned",
    );

    await pool.query(
      `UPDATE publication.channel_stream_state
       SET seed_status='complete',seed_completed_at=now(),updated_at=now()
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [firstStreamId, channelId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.channel_stream_state
         SET seed_status='pending',seed_completed_at=NULL,updated_at=now()
         WHERE publication_stream_id=$1 AND channel_id=$2`,
        [firstStreamId, channelId],
      ),
      lifecycleError("completed Publication Seed state"),
    );

    await pool.query(
      `UPDATE publication.channel_stream_state
       SET status='sealed',sealed_at=now(),
           final_version_vector='{"channel":0,"video":0,"agent":0}'::jsonb,
           state_changed_at=now(),state_changed_by='integration-test',state_reason='cutover'
       WHERE publication_stream_id=$1 AND channel_id=$2`,
      [firstStreamId, channelId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.channel_stream_state
         SET status='owned',sealed_at=NULL,final_version_vector=NULL,
             state_changed_at=now(),state_reason='reopen'
         WHERE publication_stream_id=$1 AND channel_id=$2`,
        [firstStreamId, channelId],
      ),
      lifecycleError("sealed Publication Channel ownership"),
    );

    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'cutover','integration-test','second owner')`,
      [secondStreamId, channelId],
    );

    await assert.rejects(
      pool.query(
        "UPDATE publication.stream SET source_deployment_key=$2 WHERE publication_stream_id=$1",
        [firstStreamId, `${sourceKey}-changed`],
      ),
      lifecycleError("identity and creation fields are immutable"),
    );
    await pool.query(
      `UPDATE publication.stream
       SET status='sealed',sealed_at=now(),status_changed_at=now(),
           status_changed_by='integration-test',status_reason='epoch complete'
       WHERE publication_stream_id=$1`,
      [firstStreamId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.stream
         SET status='active',sealed_at=NULL,status_changed_at=now(),status_reason='reopen'
         WHERE publication_stream_id=$1`,
        [firstStreamId],
      ),
      lifecycleError("sealed Publication Stream is immutable"),
    );
    await assert.rejects(
      pool.query("DELETE FROM publication.stream WHERE publication_stream_id=$1", [firstStreamId]),
      lifecycleError("cannot be deleted or reused"),
    );

    const baselineId = randomUUID();
    await pool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'integration-test','hold')`,
      [secondStreamId, channelId],
    );
    await pool.query(
      `UPDATE publication.channel_delivery_state
       SET mode='online',latest_successful_baseline_id=$3,
           channel_watermark_sequence=0,video_watermark_sequence=0,agent_watermark_sequence=0,
           online_at=now(),state_changed_at=now(),state_changed_by='integration-test',
           state_reason='baseline active',updated_at=now()
       WHERE destination='business' AND publication_stream_id=$1 AND channel_id=$2`,
      [secondStreamId, channelId, baselineId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.channel_delivery_state
         SET mode='hold',latest_successful_baseline_id=NULL,
             channel_watermark_sequence=NULL,video_watermark_sequence=NULL,
             agent_watermark_sequence=NULL,online_at=NULL,state_changed_at=now(),
             state_reason='return to hold',updated_at=now()
         WHERE destination='business' AND publication_stream_id=$1 AND channel_id=$2`,
        [secondStreamId, channelId],
      ),
      lifecycleError("cannot return to hold"),
    );
    await pool.query(
      `UPDATE publication.channel_delivery_state
       SET mode='sealed',sealed_at=now(),state_changed_at=now(),
           state_changed_by='integration-test',state_reason='delivery complete',updated_at=now()
       WHERE destination='business' AND publication_stream_id=$1 AND channel_id=$2`,
      [secondStreamId, channelId],
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.channel_delivery_state
         SET state_reason='changed after seal',updated_at=now()
         WHERE destination='business' AND publication_stream_id=$1 AND channel_id=$2`,
        [secondStreamId, channelId],
      ),
      lifecycleError("sealed Publication Delivery state"),
    );
  } finally {
    await pool.query(
      "DELETE FROM publication.channel_delivery_state WHERE publication_stream_id=$1 AND channel_id=$2",
      [secondStreamId, channelId],
    ).catch(() => {});
    await pool.query(
      "DELETE FROM publication.channel_stream_state WHERE channel_id=$1",
      [channelId],
    ).catch(() => {});
    await pool.query("DELETE FROM crawler.channels WHERE channel_id=$1", [channelId]).catch(() => {});
    await pool.end();
  }
});
