import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import {
  ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT,
  applyActiveLiveContentRepair,
  inspectActiveLiveContentRepair,
} from "../src/activeLiveContentRepair.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { reconcilePublication } from "../src/publicationReconciler.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { refreshVideoPublicationItemHashes } from "../src/videoPublicationItemStore.js";

const { Pool } = pg;
const integrationUrl = process.env.ACTIVE_LIVE_CONTENT_REPAIR_POSTGRES_TEST_URL;

async function transaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

async function insertTargetChannel(pool, {
  streamId,
  channelId,
  contentId,
  observedAt,
}) {
  const runId = `active-live-repair:${channelId}`;
  const observationId = randomUUID();
  const factsHash = observationFactsHash({ channel_id: channelId, content_id: contentId });
  const contentKey = `${channelId}:live:${contentId}`;
  await pool.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
     VALUES ($1,$2,$3,'active')`,
    [channelId, `https://www.youtube.com/channel/${channelId}`, `Channel ${channelId}`],
  );
  await pool.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,status,crawl_mode,detail_status,trigger_reason,
       policy_version,crawler_version,started_at,finished_at
     ) VALUES ($1,$2,'done','full','done','migration_baseline',
               'v16-rule-6','integration-test',$3::timestamptz,$3::timestamptz)`,
    [runId, channelId, observedAt],
  );
  await pool.query(
    `INSERT INTO crawler.crawl_observations (
       observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
       trigger_reason,outcome,outcome_reason_code,result_summary_json,facts_hash,
       crawler_version,extractor_versions
     ) VALUES (
       $1,$2::timestamptz,$3,$4,'video',1,'migration_baseline','complete',
       'video_cycle_complete',
       '{"discovery":{"items":1,"pages":1,"stop_reason":"list_end","parse_gap_count":0,"detail_failure_count":0}}'::jsonb,
       $5,'integration-test','{}'::jsonb
     )`,
    [observationId, observedAt, channelId, runId, factsHash],
  );
  await pool.query(
    `INSERT INTO crawler.channel_domain_cursors (
       channel_id,observation_kind,latest_sequence,latest_observation_id,
       latest_observed_at,latest_complete_observation_id,latest_complete_observed_at,
       source_cursor,current_facts_hash
     ) VALUES ($1,'video',1,$2,$3::timestamptz,$2,$3::timestamptz,
               '{"terminal_reason":"list_end"}'::jsonb,$4)`,
    [channelId, observationId, observedAt, factsHash],
  );
  await pool.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,source_content_id,
       position,title,url,description,description_status,description_source,
       published_at,published_at_status,published_at_source,published_at_precision,
       duration_seconds,duration_status,duration_source,
       view_count,view_count_status,view_count_source,
       like_count,like_count_status,like_count_source,
       comment_count,comment_count_status,comments_disabled,comment_count_source,
       access_status,access_status_source,is_members_only,
       first_seen_at,last_seen_at,last_enriched_at,last_observation_id,
       playlist_last_seen_at,player_last_observed_at,raw_json
     ) VALUES (
       $1,$2,$3,'live','youtube_detail_live',$4,
       1,$5,$6,'','empty','youtubejs_player',
       $7::timestamptz,'exact','youtubejs_player','second',
       NULL,'unavailable','live_in_progress_not_applicable',
       NULL,'unresolved',NULL,
       NULL,'unresolved',NULL,
       NULL,'unresolved',false,NULL,
       'public','youtubejs_player',false,
       $7::timestamptz,$7::timestamptz,$7::timestamptz,$8,
       $7::timestamptz,$7::timestamptz,'{}'::jsonb
     )`,
    [
      contentKey,
      channelId,
      runId,
      contentId,
      `Active Live ${contentId}`,
      `https://www.youtube.com/watch?v=${contentId}`,
      observedAt,
      observationId,
    ],
  );
  await pool.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,title,source_url,content_type,
       type_status,type_source,detail_status,api_status,missing_fields,attempts,
       content_key,result_json,disposition,next_attempt_at,finished_at
     ) VALUES (
       $1,$2,$3,1,$4,$5,'live','resolved','youtubejs_player','done','done',
       '{}'::text[],1,$6,
       jsonb_build_object('detail',jsonb_build_object(
         'live_status','is_live','is_live',true,'was_live',false
       )),
       'stored',NULL,$7::timestamptz
     )`,
    [
      runId,
      channelId,
      contentId,
      `Active Live ${contentId}`,
      `https://www.youtube.com/watch?v=${contentId}`,
      contentKey,
      observedAt,
    ],
  );
  const hashes = await refreshVideoPublicationItemHashes(pool, [contentKey]);
  assert.equal(hashes.ready_count, 1);
  await pool.query(
    `INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
     ) VALUES ($1,$2,'bootstrap','integration-test','active-Live repair fixture')`,
    [streamId, channelId],
  );
  await pool.query(
    `INSERT INTO publication.channel_delivery_state (
       destination,publication_stream_id,channel_id,mode,latest_successful_baseline_id,
       channel_watermark_sequence,video_watermark_sequence,agent_watermark_sequence,
       online_at,state_changed_by,state_reason
     ) VALUES ('business',$1,$2,'online',$3,0,0,0,now(),
               'integration-test','active-Live repair fixture')`,
    [streamId, channelId, randomUUID()],
  );
  const bootstrap = await transaction(pool, (client) => reconcilePublication(client, {
    channelId,
    domains: ["video"],
    asOf: observedAt,
  }));
  assert.equal(bootstrap.status, "revised");
  assert.equal(bootstrap.revisions[0].revision_type, "bootstrap");
  return { channel_id: channelId, content_id: contentId };
}

test("active-Live repair preview rolls back and apply atomically retracts Crawler Current", {
  skip: !integrationUrl,
  timeout: 60_000,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 3,
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const observedAt = "2026-08-25T02:00:00.000Z";
  const operation = {
    operator: "integration-test",
    reason: "remove historically stored active broadcasts",
  };
  try {
    const database = (await pool.query("SELECT current_database() AS name")).rows[0].name;
    assert.match(database, /test/i, "integration test refuses to reset a non-test database");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
    await pool.query(schema);
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES ($1,$2,'{"database":"active-live-repair-test"}'::jsonb,$3,now(),
                 'integration-test','active-Live repair fixture',
                 'integration-test','capture enabled')`,
      [streamId, `active-live-repair-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    const targets = [];
    targets.push(await insertTargetChannel(pool, {
      streamId,
      channelId: `UCactiveliverepairA${suffix}`,
      contentId: `active-live-a-${suffix}`,
      observedAt,
    }));
    targets.push(await insertTargetChannel(pool, {
      streamId,
      channelId: `UCactiveliverepairB${suffix}`,
      contentId: `active-live-b-${suffix}`,
      observedAt,
    }));

    const plan = await inspectActiveLiveContentRepair(pool, { ...operation, targets });
    assert.equal(plan.target_count, 2);
    assert.equal(plan.target_channel_count, 2);
    assert.equal(plan.blocker_count, 0);

    const previewClient = await pool.connect();
    try {
      await previewClient.query("BEGIN");
      const preview = await applyActiveLiveContentRepair(previewClient, {
        expectedEvidenceHash: plan.evidence_hash,
        expectedTargetCount: 2,
        targets,
        ...operation,
      });
      assert.equal(preview.repaired_content_count, 2);
      assert.equal(preview.repaired_channel_count, 2);
      assert.equal(preview.after.repair_revisions, 2);
      await previewClient.query("ROLLBACK");
    } finally {
      previewClient.release();
    }

    const afterPreview = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM crawler.contents) AS contents,
         (SELECT count(*)::int FROM crawler.content_candidates
           WHERE disposition='stored') AS stored_candidates,
         (SELECT count(*)::int FROM publication.revision
           WHERE revision_type='bootstrap') AS bootstraps,
         (SELECT count(*)::int FROM publication.revision
           WHERE revision_type='repair') AS repairs`,
    );
    assert.deepEqual(afterPreview.rows[0], {
      contents: 2,
      stored_candidates: 2,
      bootstraps: 2,
      repairs: 0,
    });

    const applied = await transaction(pool, (client) => applyActiveLiveContentRepair(client, {
      expectedEvidenceHash: plan.evidence_hash,
      expectedTargetCount: 2,
      targets,
      ...operation,
    }));
    assert.equal(applied.repaired_content_count, 2);
    assert.equal(applied.excluded_candidate_count, 2);
    assert.equal(applied.repaired_channel_count, 2);
    assert.equal(applied.after.remaining_contents, 0);
    assert.equal(applied.after.current_targets, 0);
    assert.equal(applied.after.repair_revisions, 2);
    assert.equal(applied.after.publication_outbox_rows, 2);

    const stored = await pool.query(
      `SELECT candidate.disposition,candidate.content_key,candidate.next_attempt_at,
              candidate.result_json#>>'{scope,reason}' AS scope_reason,
              candidate.result_json#>>'{disposition,retry_class}' AS retry_class,
              candidate.result_json#>>'{active_live_content_repair,version}' AS repair_version,
              revision.payload_json->'retractions' AS retractions
       FROM crawler.content_candidates candidate
       JOIN publication.revision revision
         ON revision.channel_id=candidate.channel_id
        AND revision.domain='video'
        AND revision.revision_type='repair'
       ORDER BY candidate.channel_id`,
    );
    assert.equal(stored.rows.length, 2);
    for (const row of stored.rows) {
      assert.equal(row.disposition, "terminal_excluded");
      assert.equal(row.content_key, null);
      assert.equal(row.next_attempt_at.toISOString(), ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT);
      assert.equal(row.scope_reason, "live_in_progress");
      assert.equal(row.retry_class, "incremental_rediscovery");
      assert.equal(row.repair_version, "active-live-policy-removal-v1");
      assert.equal(row.retractions.length, 1);
      assert.equal(row.retractions[0].reason, "policy_removed");
    }
  } finally {
    await pool.end();
  }
});
