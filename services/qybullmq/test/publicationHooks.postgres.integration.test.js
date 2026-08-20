import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { recordAboutObservation } from "../src/aboutObservationStore.js";
import {
  classifyTerminalChannelError,
  markChannelRemoved,
} from "../src/channelLifecycle.js";
import { commitFinalizedProfile } from "../src/finalizedProfileStore.js";
import { IncrementalAgentResultStore } from "../src/incrementalAgentResultStore.js";
import { executeIncrementalVideo } from "../src/incrementalVideo.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

function publicationPool(max) {
  return new Pool({
    connectionString: integrationUrl,
    max,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
}

async function replayFinalizeEvidenceBackfill(client) {
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  const statement = schema.match(
    /UPDATE crawler\.channel_runs AS run[\s\S]*?FROM crawler\.finalized_profiles AS finalized[\s\S]*?;/,
  )?.[0];
  assert.ok(statement, "Finalize evidence backfill is missing from schema.sql");
  await client.query(statement);
}

function agentFactValue(field) {
  const values = {
    country: "Brazil",
    creator_language: "Portuguese",
    creator_gender: "brand_team",
    creator_age_range: 35,
    audience_region: [
      { region: "Brazil", percentage: 70 },
      { region: "Portugal", percentage: 10 },
      { region: "United States", percentage: 8 },
      { region: "Angola", percentage: 5 },
      { region: "Mozambique", percentage: 2 },
      { region: "Other", percentage: 5 },
    ],
    audience_language: [
      { language: "Portuguese", percentage: 90 },
      { language: "English", percentage: 7 },
      { language: "Other", percentage: 3 },
    ],
    audience_age_gender: [
      { age_range: "18-24", male: 8, female: 8 },
      { age_range: "25-34", male: 14, female: 14 },
      { age_range: "35-44", male: 10, female: 10 },
      { age_range: "45-54", male: 7, female: 7 },
      { age_range: "55-64", male: 4, female: 4 },
      { age_range: "65+", male: 7, female: 7 },
    ],
    active_subscriber_ratio: 0,
    channel_categories: {
      level_1: "Software & Internet",
      level_2: ["Artificial Intelligence"],
    },
    channel_tags: {
      tags: [
        "Artificial Intelligence",
        "Software",
        "Programming",
        "Technology",
        "Tutorials",
        "Machine Learning",
        "Developer Tools",
        "Product Reviews",
        "Industry News",
        "Digital Culture",
      ],
      top_5_distribution: [
        { tag: "Artificial Intelligence", percentage: 25 },
        { tag: "Software", percentage: 22 },
        { tag: "Programming", percentage: 18 },
        { tag: "Technology", percentage: 14 },
        { tag: "Tutorials", percentage: 11 },
        { tag: "Other", percentage: 10 },
      ],
    },
  };
  return values[field];
}

function completeAgentMetrics(channelId) {
  const fields = [
    "country",
    "creator_language",
    "creator_gender",
    "creator_age_range",
    "audience_region",
    "audience_language",
    "audience_age_gender",
    "active_subscriber_ratio",
    "channel_categories",
    "channel_tags",
  ];
  return {
    audience_profile_agent: Object.fromEntries(fields.map((field) => [field, {
      value: agentFactValue(field),
      confidence: "high",
      evidence: [`Evidence for ${field}`],
      source_urls: [`https://www.youtube.com/channel/${channelId}`],
      reason: null,
      source: "crawler",
    }])),
  };
}

function completeAboutInput({
  channelId,
  runId,
  observedAt,
  idempotencyKey,
  triggerReason = "initial_full",
  publicationReconcile = true,
  title = "Publication About Hook",
}) {
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    metadata: {
      subscriber_count_text: "1,000 subscribers",
      subscriber_count_source: "youtube_about",
      view_count_text: "50,000 views",
      view_count_source: "youtube_about",
      video_count_text: "50 videos",
      video_count_source: "youtube_about",
    },
  });
  return {
    idempotencyKey,
    channelId,
    runId,
    observedAt,
    triggerReason,
    crawlerVersion: "integration-test",
    publicationReconcile,
    about,
    current: {
      aboutDescription: "A complete About description.",
      descriptionStatus: "exact",
      country: "Brazil",
      joinedDateText: "Joined Jan 1, 2020",
      joinedAt: "2020-01-01",
      joinedAtPrecision: "date_only",
      externalLinks: [{
        title: "Website",
        target_url: "https://example.com/",
      }],
      externalLinksStatus: "observed",
      rssUrl: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
      vanityChannelUrl: "https://www.youtube.com/@publicationhook",
      isFamilySafe: true,
      isVerified: true,
      isVerifiedStatus: "verified",
      keywordsStatus: "observed",
      availableTabsStatus: "observed",
      identity: {
        title,
        handle: "@publicationhook",
        avatar_url: "https://yt3.example/about-hook.jpg",
        keywords: ["publication", "testing"],
        available_tabs: ["videos", "shorts", "live"],
        summary: "A complete About description.",
      },
    },
  };
}

async function withTransaction(pool, action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
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

test("About, Video lifecycle, and Removed hooks reconcile Publication in source transactions", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationabouthook${suffix}`;
  const runId = `incremental:about-hook:${suffix}`;
  const planId = randomUUID();
  const streamId = randomUUID();
  const observedAt = "2026-07-27T13:00:00.000Z";
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Before About','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,trigger_reason,
         policy_version,crawler_version,started_at,plan_id,plan_day,task_mask,scheduled_at
       ) VALUES ($1,$2,'running','incremental','pending','clock_due',
                 'v16-rule-6','integration-test',$3::timestamptz,$4,'2026-07-27',
                 '{"about":true,"video":true}'::jsonb,$3::timestamptz)`,
      [runId, channelId, observedAt, planId],
    );
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,'publication-reconciler-v1',now(),
         'integration-test','About Hook test','integration-test','capture enabled'
       )`,
      [streamId, `about-hook-${suffix}`],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','About Hook owner')`,
      [streamId, channelId],
    );
    await pool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'integration-test','hold for Baseline')`,
      [streamId, channelId],
    );

    const client = await pool.connect();
    let open = false;
    try {
      await client.query("BEGIN");
      open = true;
      const recorded = await recordAboutObservation(client, completeAboutInput({
        idempotencyKey: `about-hook:${suffix}`,
        channelId,
        runId,
        observedAt,
        triggerReason: "clock_due",
      }));
      assert.equal(recorded.outcome, "complete");
      await client.query("COMMIT");
      open = false;
    } finally {
      if (open) await client.query("ROLLBACK").catch(() => {});
      client.release();
    }

    const stored = await pool.query(
      `SELECT observation.outcome,run.status AS run_status,
              current.readiness_status,current.data_sequence,
              revision.revision_type,revision.source_refs #>> '{run,status}' AS source_run_status,
              outbox.status AS outbox_status
       FROM crawler.crawl_observations observation
       JOIN crawler.channel_runs run ON run.run_id=observation.run_id
       JOIN publication.domain_current current
         ON current.channel_id=observation.channel_id AND current.domain='channel'
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE observation.channel_id=$1 AND observation.observation_kind='about'`,
      [channelId],
    );
    assert.deepEqual({
      observation: stored.rows[0].outcome,
      run: stored.rows[0].run_status,
      readiness: stored.rows[0].readiness_status,
      sequence: Number(stored.rows[0].data_sequence),
      revision: stored.rows[0].revision_type,
      sourceRun: stored.rows[0].source_run_status,
      outbox: stored.rows[0].outbox_status,
    }, {
      observation: "complete",
      run: "running",
      readiness: "ready",
      sequence: 1,
      revision: "bootstrap",
      sourceRun: "running",
      outbox: "held",
    });

    const videoRecorded = await executeIncrementalVideo({
      plan: {
        job_id: `incremental__${channelId}__20260727__clock_7__hook`,
        plan_id: planId,
        plan_day: "2026-07-27",
        scheduled_at: observedAt,
        channel_id: channelId,
        capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-1" },
        planner_config_version: "video-plan-1",
      },
      runId,
      startedAt: observedAt,
      query: (sql, params) => pool.query(sql, params),
      withTransaction: (action) => withTransaction(pool, action),
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: `UU${channelId.slice(2)}`,
            entries: [],
            pages: 1,
            item_count: 0,
            parse_gap_count: 0,
            anchor_matched: false,
            matched_anchor_id: null,
            crossed_anchor_ids: [],
            stop_reason: "list_end",
            terminal_reason: "list_end",
            complete: true,
            raw: { engine: "youtubei.js@test" },
          };
        },
      }),
      fetchDetail: async () => {
        throw new Error("an empty Uploads scan must not fetch Video detail");
      },
      now: () => new Date("2026-07-27T14:00:00.000Z"),
      crawlerVersion: "integration-test",
    });
    assert.deepEqual({
      outcome: videoRecorded.outcome,
      lifecycle: videoRecorded.lifecycle_status,
    }, {
      outcome: "complete",
      lifecycle: "dormant",
    });

    const lifecycle = await pool.query(
      `SELECT current.domain,current.readiness_status,current.data_sequence,
              current.payload_json->>'lifecycle_status' AS lifecycle_status,
              revision.revision_type,revision.operation,
              observation.result_summary_json #>> '{discovery,parse_gap_count}' AS parse_gap_count
       FROM publication.domain_current current
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       LEFT JOIN crawler.crawl_observations observation
         ON observation.observation_id=(
           revision.source_refs #>> '{complete_observation,observation_id}'
         )::uuid
       WHERE current.publication_stream_id=$1 AND current.channel_id=$2
         AND current.domain IN ('channel','video')
       ORDER BY current.domain`,
      [streamId, channelId],
    );
    assert.deepEqual(lifecycle.rows.map((row) => ({
      domain: row.domain,
      readiness: row.readiness_status,
      sequence: Number(row.data_sequence),
      lifecycle: row.lifecycle_status,
      revision: row.revision_type,
      operation: row.operation,
      parseGapCount: row.parse_gap_count,
    })), [
      {
        domain: "channel",
        readiness: "ready",
        sequence: 2,
        lifecycle: "dormant",
        revision: "incremental",
        operation: "replace",
        parseGapCount: null,
      },
      {
        domain: "video",
        readiness: "ready",
        sequence: 1,
        lifecycle: null,
        revision: "bootstrap",
        operation: "replace_window",
        parseGapCount: "0",
      },
    ]);

    const removed = await withTransaction(pool, (transactionClient) => markChannelRemoved(
      transactionClient,
      {
        channelId,
        runId,
        terminal: classifyTerminalChannelError(new Error("This channel does not exist.")),
        observedAt: "2026-07-27T15:00:00.000Z",
      },
    ));
    assert.equal(removed.publication.status, "revised");

    const retracted = await pool.query(
      `SELECT channel.status AS channel_status,channel.removed_reason,
              current.readiness_status,current.data_sequence,
              current.payload_json #>> '{retraction,reason_code}' AS reason_code,
              revision.revision_type,revision.operation,outbox.status AS outbox_status
       FROM crawler.channels channel
       JOIN publication.domain_current current
         ON current.channel_id=channel.channel_id AND current.domain='channel'
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE current.publication_stream_id=$1 AND channel.channel_id=$2`,
      [streamId, channelId],
    );
    assert.deepEqual({
      channelStatus: retracted.rows[0].channel_status,
      removedReason: retracted.rows[0].removed_reason,
      readiness: retracted.rows[0].readiness_status,
      sequence: Number(retracted.rows[0].data_sequence),
      reasonCode: retracted.rows[0].reason_code,
      revision: retracted.rows[0].revision_type,
      operation: retracted.rows[0].operation,
      outbox: retracted.rows[0].outbox_status,
    }, {
      channelStatus: "removed",
      removedReason: "channel_not_found",
      readiness: "ready",
      sequence: 3,
      reasonCode: "channel_not_found",
      revision: "retraction",
      operation: "retract_channel",
      outbox: "held",
    });
  } finally {
    await pool.end();
  }
});

test("Full Finalize profile and Publication state commit or roll back together", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationfinalizehook${suffix}`;
  const runId = `full:finalize-hook:${suffix}`;
  const streamId = randomUUID();
  const observedAt = "2026-07-27T16:00:00.000Z";
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Before Full Finalize','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,trigger_reason,
         policy_version,crawler_version,started_at
       ) VALUES ($1,$2,'running','full','done','initial_full',
                 'v16-rule-6','integration-test',$3::timestamptz)`,
      [runId, channelId, observedAt],
    );
    await pool.query(
      "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
      [channelId, runId],
    );
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,'publication-reconciler-v1',now(),
         'integration-test','Full Finalize Hook test','integration-test','capture enabled'
       )`,
      [streamId, `finalize-hook-${suffix}`],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','Full Finalize Hook owner')`,
      [streamId, channelId],
    );
    await pool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'integration-test','hold for Baseline')`,
      [streamId, channelId],
    );

    await withTransaction(pool, (client) => recordAboutObservation(client, completeAboutInput({
      idempotencyKey: `full-finalize-about:${suffix}`,
      channelId,
      runId,
      observedAt,
      publicationReconcile: false,
    })));
    const beforeFinalize = await pool.query(
      "SELECT count(*)::int AS count FROM publication.domain_current WHERE publication_stream_id=$1",
      [streamId],
    );
    assert.equal(beforeFinalize.rows[0].count, 0);

    const finalizeInput = {
      channelId,
      runId,
      status: "ready_partial",
      profile: { channel: { channel_id: channelId }, quality: { quality_status: "ready_partial" } },
      quality: { quality_status: "ready_partial", source_revision: `test:${suffix}` },
      publicationAsOf: observedAt,
    };
    const rollbackClient = await pool.connect();
    try {
      await rollbackClient.query("BEGIN");
      const rolledBack = await commitFinalizedProfile(rollbackClient, finalizeInput);
      assert.equal(rolledBack.applied, true);
      assert.equal(rolledBack.publication.status, "revised");
      await rollbackClient.query("ROLLBACK");
    } finally {
      rollbackClient.release();
    }
    const afterRollback = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM crawler.finalized_profiles WHERE channel_id=$2) AS profiles,
         (SELECT count(*)::int FROM publication.domain_current
           WHERE publication_stream_id=$1 AND channel_id=$2) AS currents,
         (SELECT count(*)::int FROM publication.revision
           WHERE publication_stream_id=$1 AND channel_id=$2) AS revisions,
         (SELECT count(*)::int FROM publication.outbox outbox
           JOIN publication.revision revision USING (revision_id)
           WHERE revision.publication_stream_id=$1 AND revision.channel_id=$2) AS outbox`,
      [streamId, channelId],
    );
    assert.deepEqual(afterRollback.rows[0], {
      profiles: 0,
      currents: 0,
      revisions: 0,
      outbox: 0,
    });

    const committed = await withTransaction(
      pool,
      (client) => commitFinalizedProfile(client, finalizeInput),
    );
    assert.equal(committed.publication.status, "revised");
    const stored = await pool.query(
      `SELECT finalized.status AS finalized_status,run.status AS run_status,
              run.publication_finalized_status,run.publication_finalized_at,
              current.readiness_status,current.data_sequence,
              revision.revision_type,revision.operation,outbox.status AS outbox_status,
              (SELECT count(*)::int FROM publication.domain_current all_current
                WHERE all_current.publication_stream_id=$1 AND all_current.channel_id=$2)
                AS current_count
       FROM crawler.finalized_profiles finalized
       JOIN crawler.channel_runs run ON run.run_id=finalized.run_id
       JOIN publication.domain_current current
         ON current.publication_stream_id=$1 AND current.channel_id=finalized.channel_id
        AND current.domain='channel'
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE finalized.channel_id=$2`,
      [streamId, channelId],
    );
    assert.deepEqual({
      finalized: stored.rows[0].finalized_status,
      run: stored.rows[0].run_status,
      publicationFinalizedStatus: stored.rows[0].publication_finalized_status,
      publicationFinalized: Boolean(stored.rows[0].publication_finalized_at),
      readiness: stored.rows[0].readiness_status,
      sequence: Number(stored.rows[0].data_sequence),
      revision: stored.rows[0].revision_type,
      operation: stored.rows[0].operation,
      outbox: stored.rows[0].outbox_status,
      currentCount: stored.rows[0].current_count,
    }, {
      finalized: "ready_partial",
      run: "done",
      publicationFinalizedStatus: "ready_partial",
      publicationFinalized: true,
      readiness: "ready",
      sequence: 1,
      revision: "bootstrap",
      operation: "replace",
      outbox: "held",
      currentCount: 3,
    });
    await assert.rejects(
      pool.query(
        `UPDATE crawler.channel_runs
         SET publication_finalized_status=NULL,publication_finalized_at=NULL
         WHERE run_id=$1`,
        [runId],
      ),
      /Channel Run Publication Finalize evidence cannot regress/,
    );
    const reopenedForRepair = await pool.query(
      `UPDATE crawler.channel_runs
       SET status='running',detail_status='pending',finished_at=NULL
       WHERE run_id=$1
       RETURNING status,detail_status,finished_at,
                 publication_finalized_status,publication_finalized_at`,
      [runId],
    );
    assert.deepEqual({
      status: reopenedForRepair.rows[0].status,
      detailStatus: reopenedForRepair.rows[0].detail_status,
      finishedAt: reopenedForRepair.rows[0].finished_at,
      finalizedStatus: reopenedForRepair.rows[0].publication_finalized_status,
      finalizedAt: Boolean(reopenedForRepair.rows[0].publication_finalized_at),
    }, {
      status: "running",
      detailStatus: "pending",
      finishedAt: null,
      finalizedStatus: "ready_partial",
      finalizedAt: true,
    });
    await replayFinalizeEvidenceBackfill(pool);
    const afterSchemaReplay = await pool.query(
      `SELECT status,detail_status,finished_at,
              publication_finalized_status,publication_finalized_at
       FROM crawler.channel_runs
       WHERE run_id=$1`,
      [runId],
    );
    assert.deepEqual({
      status: afterSchemaReplay.rows[0].status,
      detailStatus: afterSchemaReplay.rows[0].detail_status,
      finishedAt: afterSchemaReplay.rows[0].finished_at,
      finalizedStatus: afterSchemaReplay.rows[0].publication_finalized_status,
      finalizedAt: Boolean(afterSchemaReplay.rows[0].publication_finalized_at),
    }, {
      status: "running",
      detailStatus: "pending",
      finishedAt: null,
      finalizedStatus: "ready_partial",
      finalizedAt: true,
    });

    const repairObservedAt = "2026-07-27T17:00:00.000Z";
    await withTransaction(pool, (client) => recordAboutObservation(client, completeAboutInput({
      idempotencyKey: `full-finalize-repair-about:${suffix}`,
      channelId,
      runId,
      observedAt: repairObservedAt,
      triggerReason: "repair",
      publicationReconcile: false,
      title: "Publication Repaired Hook",
    })));
    const repaired = await withTransaction(pool, (client) => commitFinalizedProfile(client, {
      ...finalizeInput,
      profile: {
        channel: { channel_id: channelId, title: "Publication Repaired Hook" },
        quality: { quality_status: "ready_partial" },
      },
      quality: { quality_status: "ready_partial", source_revision: `repair:${suffix}` },
      publicationAsOf: repairObservedAt,
      publicationRevisionType: "repair",
    }));
    assert.equal(repaired.publication.status, "revised");
    const repairRevision = await pool.query(
      `SELECT current.data_sequence,current.payload_json->>'title' AS title,
              revision.revision_type,revision.previous_data_sequence,
              run.status AS run_status,run.detail_status AS run_detail_status,
              run.finished_at AS run_finished_at
       FROM publication.domain_current current
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       JOIN crawler.channel_runs run ON run.run_id=$3
       WHERE current.publication_stream_id=$1 AND current.channel_id=$2
         AND current.domain='channel'`,
      [streamId, channelId, runId],
    );
    assert.deepEqual({
      sequence: Number(repairRevision.rows[0].data_sequence),
      title: repairRevision.rows[0].title,
      revision: repairRevision.rows[0].revision_type,
      previousSequence: Number(repairRevision.rows[0].previous_data_sequence),
      runStatus: repairRevision.rows[0].run_status,
      runDetailStatus: repairRevision.rows[0].run_detail_status,
      runFinished: Boolean(repairRevision.rows[0].run_finished_at),
    }, {
      sequence: 2,
      title: "Publication Repaired Hook",
      revision: "repair",
      previousSequence: 1,
      runStatus: "done",
      runDetailStatus: "done",
      runFinished: true,
    });
  } finally {
    await pool.end();
  }
});

test("Agent Success and Agent Publication commit in one transaction", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool(3);
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationagenthook${suffix}`;
  const runId = `incremental:agent-hook:${suffix}`;
  const planId = randomUUID();
  const batchId = `incremental-agent:hook:${suffix}`;
  const streamId = randomUUID();
  try {
    await pool.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,agent_status,ready_for_agent
       ) VALUES ($1,$2,'Publication Agent Hook','active',1000,'running',true)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,detail_status,plan_id,plan_day,
         trigger_reason,task_mask,scheduled_at,policy_version,crawler_version,result_json
       ) VALUES (
         $1,$2,'waiting_agent','incremental','done',$3,'2026-07-27','clock_due',
         '{"agent":true}'::jsonb,'2026-07-27T13:00:00Z','v16-rule-6','integration-test',
         '{"domains":{"agent":{"status":"queued"}}}'::jsonb
       )`,
      [runId, channelId, planId],
    );
    await pool.query(
       `INSERT INTO crawler.agent_refresh_requests (
         plan_id,plan_day,channel_id,run_id,status,batch_id,attempts,
         clock_version,policy_version,queued_at,started_at
       ) VALUES ($1,'2026-07-27',$2,$3,'running',$4,1,7,'v16-rule-6',now(),now())`,
      [planId, channelId, runId, batchId],
    );
    const templateText = "Analyze every input URL and return all V1 Agent facts.";
    const template = await pool.query(
      `INSERT INTO crawler.agent_prompt_templates (
         name,version,template_text,status,is_default
       ) VALUES ($1,1,$2,'active',false)
       RETURNING template_id`,
      [`publication-agent-hook-${suffix}`, templateText],
    );
    const config = await pool.query(
      `INSERT INTO crawler.agent_configs (
         name,provider,model,prompt_template_id,tools_json,enabled,is_default
       ) VALUES ($1,'openai-compatible','agent-test',$2,$3::jsonb,true,false)
       RETURNING config_id,provider,model,prompt_template_id,tools_json`,
      [
        `publication-agent-hook-${suffix}`,
        template.rows[0].template_id,
        JSON.stringify([{ type: "web_search" }]),
      ],
    );
    const agentConfig = { ...config.rows[0], template_text: templateText };
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,'publication-reconciler-v1',now(),
         'integration-test','Agent Hook test','integration-test','capture enabled'
       )`,
      [streamId, `agent-hook-${suffix}`],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','Agent Hook owner')`,
      [streamId, channelId],
    );
    await pool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'integration-test','hold for Baseline')`,
      [streamId, channelId],
    );

    const store = new IncrementalAgentResultStore({
      withTransaction: (action) => withTransaction(pool, action),
      maxAttempts: 3,
    });
    const recorded = await store.complete({
      batchId,
      request: {
        channel_id: channelId,
        plan_ids: [planId],
        run_ids: [runId],
        attempts: 1,
        plan_day: "2026-07-27",
        scheduled_at: "2026-07-27T13:00:00.000Z",
      },
      resolved: {
        agent_model: "agent-test",
        country_required: false,
        input_content_ids: [],
        metrics: completeAgentMetrics(channelId),
      },
      row: { input_url: `https://www.youtube.com/channel/${channelId}` },
      agentConfig,
    });
    assert.equal(recorded.outcome, "complete");

    const stored = await pool.query(
      `SELECT profile.status AS profile_status,observation.outcome,
              current.readiness_status,current.data_sequence,
              revision.revision_type,outbox.status AS outbox_status
       FROM crawler.agent_profiles profile
       JOIN crawler.crawl_observations observation
         ON observation.observation_id=profile.last_observation_id
       JOIN publication.domain_current current
         ON current.channel_id=profile.channel_id AND current.domain='agent'
       JOIN publication.revision revision ON revision.revision_id=current.current_revision_id
       JOIN publication.outbox outbox ON outbox.revision_id=revision.revision_id
       WHERE profile.channel_id=$1 AND profile.agent_mode='basic'`,
      [channelId],
    );
    assert.deepEqual({
      profile: stored.rows[0].profile_status,
      observation: stored.rows[0].outcome,
      readiness: stored.rows[0].readiness_status,
      sequence: Number(stored.rows[0].data_sequence),
      revision: stored.rows[0].revision_type,
      outbox: stored.rows[0].outbox_status,
    }, {
      profile: "success",
      observation: "complete",
      readiness: "ready",
      sequence: 1,
      revision: "bootstrap",
      outbox: "held",
    });
  } finally {
    await pool.end();
  }
});
