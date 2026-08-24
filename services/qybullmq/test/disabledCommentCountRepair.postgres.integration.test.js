import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import {
  applyDisabledCommentCountRepair,
  inspectDisabledCommentCountRepair,
} from "../src/disabledCommentCountRepair.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.DISABLED_COMMENT_COUNT_REPAIR_POSTGRES_TEST_URL;

test("disabled Comment repair converts legacy null to zero and refreshes Publication hash", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCdisabledrepair${suffix}`;
  const videoId = `disabled-repair-${suffix}`;
  const contentKey = `${channelId}:video:${videoId}`;
  let began = false;

  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    began = true;
    await client.query(
      "ALTER TABLE crawler.contents DROP CONSTRAINT contents_comment_state_shape",
    );
    await client.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Disabled repair fixture','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await client.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,content_type,content_type_source,source_content_id,
         title,url,description,description_status,description_source,
         published_at,published_at_status,published_at_source,published_at_precision,
         duration_seconds,duration_status,duration_source,
         view_count,view_count_text,view_count_status,view_count_source,
         like_count,like_count_status,like_count_source,
         comment_count,comment_count_status,comments_disabled,comment_count_source,
         access_status,access_status_source,last_enriched_at,raw_json
       ) VALUES (
         $1,$2,'video','youtube_watch_canonical',$3,
         'Disabled repair fixture',$4,'Description','exact','youtubejs_player',
         now(),'exact','youtubejs_player','second',
         60,'exact','youtubejs_player',
         100,'100','exact','youtubejs_player',
         5,'exact','youtubejs_player',
         NULL,'disabled',true,NULL,
         'public','youtubejs_player',now(),'{}'::jsonb
       )`,
      [contentKey, channelId, videoId, `https://www.youtube.com/watch?v=${videoId}`],
    );
    await client.query(
      `ALTER TABLE crawler.contents
       ADD CONSTRAINT contents_comment_state_shape CHECK (
         (comments_disabled IS TRUE AND comment_count=0 AND comment_count_status='disabled')
         OR (comments_disabled IS DISTINCT FROM TRUE AND comment_count_status<>'disabled')
       ) NOT VALID`,
    );

    const operation = {
      operator: "integration-test",
      reason: "normalize disabled comments",
    };
    const plan = await inspectDisabledCommentCountRepair(client, operation);
    assert.equal(plan.target_count, 1);
    assert.deepEqual(plan.affected_channel_ids, [channelId]);
    assert.equal(plan.missing_source_count, 1);
    assert.equal(plan.constraint_validated, false);

    const result = await applyDisabledCommentCountRepair(client, {
      expectedEvidenceHash: plan.evidence_hash,
      expectedTargetCount: 1,
      ...operation,
    });
    assert.equal(result.repaired_count, 1);
    assert.equal(result.publication_hashes.ready_count, 1);
    assert.equal(result.publication_hashes.incomplete_count, 0);

    const stored = (await client.query(
      `SELECT comment_count,comment_count_status,comments_disabled,
              comment_count_source,publication_item_hash,
              raw_json->'disabled_comment_count_repair' AS repair
       FROM crawler.contents WHERE content_key=$1`,
      [contentKey],
    )).rows[0];
    assert.equal(stored.comment_count, "0");
    assert.equal(stored.comment_count_status, "disabled");
    assert.equal(stored.comments_disabled, true);
    assert.equal(stored.comment_count_source, "stored_comments_disabled_evidence");
    assert.match(stored.publication_item_hash, /^sha256:[0-9a-f]{64}$/);
    assert.equal(stored.repair.previous_comment_count, null);
    assert.equal(stored.repair.previous_comment_count_status, "disabled");
    assert.equal(stored.repair.operator, operation.operator);
    assert.equal(stored.repair.reason, operation.reason);
    assert.equal(result.after.constraint_validated, true);
  } finally {
    if (began) await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
