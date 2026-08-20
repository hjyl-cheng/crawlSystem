import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { buildAgentPublicationRun } from "../src/agentPublicationCurrent.js";
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { commitFinalizedProfile } from "../src/finalizedProfileStore.js";
import {
  loadFinalizeRecoveryCandidates,
  loadPipelineFinalizeBlockers,
} from "../src/finalizeRecoveryPolicy.js";
import { resolveFinalizeStatus } from "../src/finalizePolicy.js";
import { executeIncrementalVideo } from "../src/incrementalVideo.js";
import { recordInitialFullObservations } from "../src/initialFullObservations.js";
import { applyMigrationActivityGate } from "../src/migrationActivityGate.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { reconcilePublication } from "../src/publicationReconciler.js";
import { completePublicationOperationalFixture } from "./support/publicationOperationalFixtures.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

test("dormant Migration Finalize is durable but waits for a complete Initial Package", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const batchId = `dormant-migration-finalize-${suffix}`;
  const seedChannelId = `UCdormantseed${suffix}`;
  const channelId = `UCdormantnew${suffix}`;
  const runId = `run:dormant-new:${suffix}`;
  const observedAt = "2026-07-28T11:00:00.000Z";
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
  const pendingAbout = {
    idempotencyKey: `about:${runId}:attempt:1`,
    channelId,
    runId,
    observedAt,
    triggerReason: "initial_full",
    scheduledAt: "2026-07-28T10:00:00.000Z",
    startedAt: "2026-07-28T10:00:00.000Z",
    finishedAt: observedAt,
    crawlerVersion: "integration-test",
    extractorVersions: { youtubejs: "integration-test" },
    about,
    current: {
      aboutDescription: "Dormant but valid Channel.",
      descriptionStatus: "exact",
      country: "Brazil",
      joinedDateText: "Joined Jan 1, 2020",
      joinedAt: "2020-01-01",
      joinedAtPrecision: "date_only",
      externalLinks: [],
      externalLinksStatus: "observed",
      rssUrl: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
      vanityChannelUrl: "https://www.youtube.com/@dormant-integration",
      isFamilySafe: true,
      isVerified: false,
      isVerifiedStatus: "not_verified",
      keywordsStatus: "observed",
      availableTabsStatus: "observed",
      identity: {
        title: "Dormant Migration Channel",
        handle: "@dormant-integration",
        avatar_url: "https://yt3.example/dormant-integration.jpg",
        keywords: ["dormant", "integration"],
        available_tabs: ["videos"],
        summary: "Dormant but valid Channel.",
      },
    },
  };

  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         validation_finished_at,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted','1800-01-02T00:01:00Z','1800-01-02T00:01:00Z')
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,
         latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id,created_at
       ) VALUES
         ($1,$3,'Publication route seed','active',1000,false,'pending',NULL,NULL,NULL,
          '1800-01-01T00:00:00Z'),
         ($2,$4,'Dormant Migration Channel','active',1000,false,'pending',$5,$6,$5,
          '1800-01-02T00:00:00Z')`,
      [
        seedChannelId,
        channelId,
        `https://www.youtube.com/channel/${seedChannelId}`,
        `https://www.youtube.com/channel/${channelId}`,
        runId,
        candidateId,
      ],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         detail_status,started_at,result_json
       ) VALUES (
         $1,$2,$3,'waiting_agent','full',30,'done','2026-07-28T10:00:00Z',$4::jsonb
       )`,
      [
        runId,
        channelId,
        candidateId,
        JSON.stringify({
          dispatch_batch_id: batchId,
          migration_activity_gate: {
            required: true,
            decision: "pending",
            max_age_days: 90,
          },
          pending_initial_about_observation: pendingAbout,
        }),
      ],
    );
    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,'1800-01-01T12:00:00Z',
         'integration-test','Dormant Migration Finalize test',
         'integration-test','capture enabled'
       )`,
      [streamId, `dormant-migration-finalize-${suffix}`, PUBLICATION_WRITER_VERSION],
    );
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','online route seed')`,
      [streamId, seedChannelId],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,
         state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',now(),'integration-test','online route seed')`,
      [streamId, seedChannelId],
    );

    const gate = await applyMigrationActivityGate(client, {
      runId,
      detailStatus: "done",
      evaluatedAt: observedAt,
      activityEvidence: {
        complete: true,
        source: "uploads_publication_dates",
        referenceDay: "2026-07-28",
        recentPublishedContentCount: 0,
        uncertainContentCount: 0,
        inspectedContentCount: 30,
        excludedUpcomingCount: 0,
        newestPublishedDay: "2026-01-01",
      },
    });
    assert.equal(gate.decision, "dormant");
    const recoverableBeforeFinalize = await loadFinalizeRecoveryCandidates(
      client.query.bind(client),
      { pipelineCycleId: batchId },
    );
    assert.deepEqual(
      recoverableBeforeFinalize.map((row) => [row.channel_id, row.run_id]),
      [[channelId, runId]],
    );
    assert.deepEqual(
      await loadPipelineFinalizeBlockers(client.query.bind(client), batchId),
      { agentOpen: 0, finalOpen: 1, publicationOpen: 0 },
    );

    const observations = await recordInitialFullObservations({
      withTransaction: (action) => action(client),
      channelId,
      runId,
      observedAt,
    });
    assert.equal(observations.recorded, true);
    assert.deepEqual(Object.keys(observations.observations), ["about"]);

    const status = resolveFinalizeStatus({
      channelStatus: "dormant",
      hasAgent: false,
      missingAgentFieldCount: 9,
    });
    assert.equal(status, "ready_partial");
    const storedChannel = (await client.query(
      "SELECT to_jsonb(channel) AS value FROM crawler.channels AS channel WHERE channel_id=$1",
      [channelId],
    )).rows[0].value;
    const committed = await commitFinalizedProfile(client, {
      channelId,
      runId,
      status,
      profile: {
        channel: storedChannel,
        contents: { videos: [], shorts: [], lives: [] },
        metrics: {},
        agent_profile: null,
        quality: { quality_status: status },
      },
      quality: {
        quality_status: status,
        source_revision: `dormant-migration-finalize:${suffix}`,
        missing_agent_fields: ["creator_language"],
      },
      publicationAsOf: observedAt,
    });
    assert.equal(committed.applied, true);
    assert.equal(committed.publication.status, "not_owned");
    assert.equal(committed.publication.onboarding.status, "channel_not_eligible");
    assert.deepEqual(
      await loadFinalizeRecoveryCandidates(client.query.bind(client), { pipelineCycleId: batchId }),
      [],
    );
    assert.deepEqual(
      await loadPipelineFinalizeBlockers(client.query.bind(client), batchId),
      { agentOpen: 0, finalOpen: 0, publicationOpen: 0 },
    );

    const state = await client.query(
      `SELECT channel.status AS channel_status,channel.agent_status,
              candidate.status AS candidate_status,run.status AS run_status,
              run.detail_status,run.publication_finalized_status,
              run.publication_finalized_at,
              run.result_json ? 'pending_initial_about_observation' AS pending_about,
              finalized.status AS finalized_status,
              owner.onboarding_mode,delivery.mode AS delivery_mode,
              (SELECT count(*)::int FROM crawler.crawl_observations observation
               WHERE observation.channel_id=channel.channel_id
                 AND observation.observation_kind='about') AS about_observations,
              (SELECT count(*)::int FROM crawler.crawl_observations observation
               WHERE observation.channel_id=channel.channel_id
                 AND observation.observation_kind='video') AS video_observations,
              (SELECT count(*)::int FROM crawler.crawl_observations observation
               WHERE observation.channel_id=channel.channel_id
                 AND observation.observation_kind='agent') AS agent_observations,
              (SELECT count(*)::int FROM publication.revision revision
               WHERE revision.publication_stream_id=$4
                 AND revision.channel_id=channel.channel_id
                 AND revision.revision_type='bootstrap') AS bootstrap_revisions,
              (SELECT count(*)::int FROM publication.outbox outbox
               JOIN publication.revision revision USING (revision_id)
               WHERE revision.publication_stream_id=$4
                 AND revision.channel_id=channel.channel_id
                 AND outbox.destination='business') AS business_outbox
       FROM crawler.channels AS channel
       JOIN crawler.channel_candidates AS candidate ON candidate.candidate_id=$2
       JOIN crawler.channel_runs AS run ON run.run_id=$3
       JOIN crawler.finalized_profiles AS finalized ON finalized.channel_id=channel.channel_id
       LEFT JOIN publication.channel_stream_state AS owner
         ON owner.publication_stream_id=$4 AND owner.channel_id=channel.channel_id
       LEFT JOIN publication.channel_delivery_state AS delivery
         ON delivery.publication_stream_id=owner.publication_stream_id
        AND delivery.channel_id=owner.channel_id AND delivery.destination='business'
       WHERE channel.channel_id=$1`,
      [channelId, candidateId, runId, streamId],
    );
    const row = state.rows[0];
    assert.deepEqual({
      channelStatus: row.channel_status,
      agentStatus: row.agent_status,
      candidateStatus: row.candidate_status,
      runStatus: row.run_status,
      detailStatus: row.detail_status,
      finalizedStatus: row.finalized_status,
      publicationFinalizedStatus: row.publication_finalized_status,
      publicationFinalized: Boolean(row.publication_finalized_at),
      pendingAbout: row.pending_about,
      onboardingMode: row.onboarding_mode,
      deliveryMode: row.delivery_mode,
      aboutObservations: row.about_observations,
      videoObservations: row.video_observations,
      agentObservations: row.agent_observations,
      bootstrapRevisions: row.bootstrap_revisions,
      businessOutbox: row.business_outbox,
    }, {
      channelStatus: "dormant",
      agentStatus: "skipped",
      candidateStatus: "accepted",
      runStatus: "done",
      detailStatus: "done",
      finalizedStatus: "ready_partial",
      publicationFinalizedStatus: "ready_partial",
      publicationFinalized: true,
      pendingAbout: false,
      onboardingMode: null,
      deliveryMode: null,
      aboutObservations: 1,
      videoObservations: 1,
      agentObservations: 0,
      bootstrapRevisions: 0,
      businessOutbox: 0,
    });

    const probeRunId = `run:dormant-probe:${suffix}`;
    const probePlanId = randomUUID();
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,status,crawl_mode,content_limit,detail_status,
         plan_id,plan_day,trigger_reason,task_mask,scheduled_at,
         clock_version,policy_version,planner_config_version,capacity_version,
         crawler_version,started_at
       ) VALUES (
         $1,$2,'running','incremental',0,'pending',$3,'2026-07-28','clock_due',
         '{"video":true}'::jsonb,
         '2026-07-28T12:00:00Z',7,'v16-rule-6','video-plan-1','capacity-1',
         'integration-test','2026-07-28T12:00:00Z'
       )`,
      [probeRunId, channelId, probePlanId],
    );
    const probe = await executeIncrementalVideo({
      plan: {
        job_id: `dormant_probe__${channelId}__20260728__clock_7__integration`,
        plan_id: probePlanId,
        plan_day: "2026-07-28",
        scheduled_at: "2026-07-28T12:00:00.000Z",
        channel_id: channelId,
        capacity: { factor: 1, player_cap: 5, next_cap: 5, version: "capacity-1" },
        planner_config_version: "video-plan-1",
      },
      runId: probeRunId,
      startedAt: "2026-07-28T12:00:00.000Z",
      query: client.query.bind(client),
      withTransaction: (action) => action(client),
      getChannelSnapshot: async () => ({
        async scanUploads() {
          return {
            playlist_id: `UU${channelId.slice(2)}`,
            entries: [{
              id: `reactivated-video-${suffix}`,
              position: 1,
              content_type: "video",
              title: "Dormant Channel Reactivated",
            }],
            pages: 1,
            item_count: 1,
            parse_gap_count: 0,
            anchor_matched: false,
            matched_anchor_id: null,
            crossed_anchor_ids: [],
            stop_reason: "list_end",
            terminal_reason: "list_end",
            complete: true,
            raw: { engine: "youtubei.js@integration-test" },
          };
        },
      }),
      fetchDetail: async (videoId) => ({
        id: videoId,
        title: "Dormant Channel Reactivated",
        thumbnail_url: `https://i.ytimg.com/vi/${videoId}/hqdefault.jpg`,
        content_type_signals: {
          source: "youtubejs_player",
          canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
          is_shorts_eligible: false,
          is_live_content: false,
          is_live: false,
          is_upcoming: false,
          is_live_now: false,
        },
        description: "A complete reactivation probe Video.",
        hashtags: ["reactivated"],
        keywords: ["publication"],
        published_at: "2026-07-28T11:00:00.000Z",
        published_at_precision: "second",
        duration_seconds: 120,
        view_count: 100,
        like_count: 10,
        comment_count: 2,
        comments_disabled: false,
        availability: "public",
        extractor_version: "youtubei.js@integration-test",
      }),
      now: () => new Date("2026-07-28T12:10:00.000Z"),
      crawlerVersion: "integration-test",
    });
    assert.equal(probe.lifecycle_status, "active");
    const afterProbe = await client.query(
      "SELECT status,latest_run_id FROM crawler.channels WHERE channel_id=$1",
      [channelId],
    );
    assert.deepEqual(afterProbe.rows[0], {
      status: "active",
      latest_run_id: runId,
    });

    await client.query(
      `UPDATE crawler.channels
       SET ready_for_agent=true,agent_status='done',
           updated_at=clock_timestamp() + interval '1 second'
       WHERE channel_id=$1`,
      [channelId],
    );
    const templateText = "Analyze every input URL and return all V1 Agent facts.";
    const template = await client.query(
      `INSERT INTO crawler.agent_prompt_templates (
         name,version,template_text,status,is_default
       ) VALUES ($1,1,$2,'active',false)
       RETURNING template_id`,
      [`dormant-reactivation-${suffix}`, templateText],
    );
    const config = await client.query(
      `INSERT INTO crawler.agent_configs (
         name,provider,model,prompt_template_id,tools_json,enabled,is_default
       ) VALUES ($1,'openai-compatible','agent-test',$2,$3::jsonb,true,false)
       RETURNING config_id,provider,model,prompt_template_id,tools_json`,
      [
        `dormant-reactivation-${suffix}`,
        template.rows[0].template_id,
        JSON.stringify([{ type: "web_search" }]),
      ],
    );
    const agentConfig = { ...config.rows[0], template_text: templateText };
    const metrics = completePublicationOperationalFixture(channelId, { observedAt }).agent.metrics_json;
    const agentRun = buildAgentPublicationRun({
      agentConfig,
      agentModel: agentConfig.model,
      promptVariant: "country_resolved",
      inputContentIds: [],
    });
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,agent_model,
         agent_config_id,prompt_template_id,prompt_hash,prompt_variant,
         input_content_ids,input_content_hash,taxonomy_version,agent_version_hash,
         current_output_hash
       ) VALUES (
         $1,'basic',$2,'success',$3::jsonb,$4,$5,$6,$7,$8,$9::text[],$10,$11,$12,$13
       )`,
      [
        channelId,
        `https://www.youtube.com/channel/${channelId}`,
        JSON.stringify(metrics),
        agentRun.agent_model,
        agentRun.agent_config_id,
        agentRun.prompt_template_id,
        agentRun.prompt_hash,
        agentRun.prompt_variant,
        agentRun.input_content_ids,
        agentRun.input_content_hash,
        agentRun.taxonomy_version,
        agentRun.agent_version_hash,
        observationFactsHash(metrics),
      ],
    );
    const completedObservations = await recordInitialFullObservations({
      withTransaction: (action) => action(client),
      channelId,
      runId,
      observedAt: "2026-07-28T12:00:00.000Z",
    });
    assert.deepEqual(Object.keys(completedObservations.observations), ["agent"]);
    const recoverableAfterReactivation = await loadFinalizeRecoveryCandidates(
      client.query.bind(client),
      { pipelineCycleId: batchId },
    );
    assert.deepEqual(
      recoverableAfterReactivation.map((candidateRow) => [candidateRow.channel_id, candidateRow.run_id]),
      [[channelId, runId]],
    );

    const ownershipReference = {
      onboarding_mode: "automatic_bootstrap",
      initial_full_run_id: runId,
    };
    await client.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,ownership_reference,
         state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap',$3::jsonb,'integration-test','legacy partial Bootstrap')`,
      [streamId, channelId, JSON.stringify(ownershipReference)],
    );
    await client.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,online_at,
         source_ownership_reference,state_changed_by,state_reason
       ) VALUES (
         'business',$1,$2,'online',now(),$3::jsonb,
         'integration-test','legacy partial Bootstrap'
       )`,
      [streamId, channelId, JSON.stringify(ownershipReference)],
    );
    const legacyPartial = await reconcilePublication(client, {
      channelId,
      domains: ["channel", "video"],
      asOf: "2026-07-28T12:00:00.000Z",
      revisionType: "incremental",
    });
    assert.equal(legacyPartial.seed_status, "pending");
    assert.deepEqual(
      legacyPartial.revisions.map((revision) => [revision.domain, revision.data_sequence]),
      [["channel", 1], ["video", 1]],
    );

    const reactivated = await commitFinalizedProfile(client, {
      channelId,
      runId,
      status: "ready_auto",
      profile: {
        channel: storedChannel,
        contents: { videos: [], shorts: [], lives: [] },
        metrics: {},
        agent_profile: metrics,
        quality: { quality_status: "ready_auto" },
      },
      quality: {
        quality_status: "ready_auto",
        source_revision: `dormant-reactivation:${suffix}`,
      },
      publicationAsOf: "2026-07-28T12:00:00.000Z",
    });
    assert.equal(reactivated.applied, true);
    assert.equal(
      reactivated.publication.status,
      "revised",
      JSON.stringify(reactivated.publication),
    );
    assert.equal(reactivated.publication.onboarding.status, "recovering");
    assert.equal(reactivated.publication.seed_status, "complete");
    assert.deepEqual(
      reactivated.publication.revisions.map((revision) => [
        revision.domain,
        revision.revision_type,
        revision.data_sequence,
      ]),
      [
        ["agent", "bootstrap", 1],
      ],
    );
    const automaticBootstrap = await client.query(
      `SELECT run.publication_finalized_status,finalized.status AS finalized_status,
              owner.onboarding_mode,owner.seed_status,delivery.mode,
              array_agg(current.domain ORDER BY CASE current.domain
                WHEN 'channel' THEN 1 WHEN 'video' THEN 2 ELSE 3 END) AS domains,
              count(DISTINCT revision.revision_id)::int AS revisions,
              count(DISTINCT (outbox.destination,outbox.revision_id))::int AS outbox
       FROM crawler.channel_runs AS run
       JOIN crawler.finalized_profiles AS finalized ON finalized.run_id=run.run_id
       JOIN publication.channel_stream_state AS owner ON owner.channel_id=run.channel_id
       JOIN publication.channel_delivery_state AS delivery
         ON delivery.publication_stream_id=owner.publication_stream_id
        AND delivery.channel_id=owner.channel_id AND delivery.destination='business'
       JOIN publication.domain_current AS current
         ON current.publication_stream_id=owner.publication_stream_id
        AND current.channel_id=owner.channel_id
       JOIN publication.revision AS revision
         ON revision.publication_stream_id=owner.publication_stream_id
        AND revision.channel_id=owner.channel_id AND revision.domain=current.domain
       JOIN publication.outbox AS outbox ON outbox.revision_id=revision.revision_id
       WHERE run.run_id=$1
       GROUP BY run.publication_finalized_status,finalized.status,
                owner.onboarding_mode,owner.seed_status,delivery.mode`,
      [runId],
    );
    assert.deepEqual(automaticBootstrap.rows[0], {
      publication_finalized_status: "ready_auto",
      finalized_status: "ready_auto",
      onboarding_mode: "bootstrap",
      seed_status: "complete",
      mode: "online",
      domains: ["channel", "video", "agent"],
      revisions: 3,
      outbox: 3,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("Finalize recovery repairs a successful active profile missing its Run proof", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `active-finalize-recovery-${suffix}`;
  const channelId = `UCactivefinalize${suffix}`;
  const runId = `run:active-finalize:${suffix}`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Integration URL must target a *_test database");
    await client.query("BEGIN");
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status,discovery_closed_at
       ) VALUES ($1,$1,'validation_closed',now())`,
      [batchId],
    );
    const candidate = await client.query(
      `INSERT INTO crawler.channel_candidates (
         dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         validation_finished_at,accepted_at
       ) VALUES ($1,$1,$2,$3,'accepted',now(),now())
       RETURNING candidate_id`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const candidateId = Number(candidate.rows[0].candidate_id);
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,
         latest_run_id
       ) VALUES ($1,$2,'Active Finalize Recovery','active',1000,true,'done',$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         detail_status,started_at,finished_at,result_json
       ) VALUES ($1,$2,$3,'done','full',30,'done',now(),now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify({ pipeline_cycle_id: batchId })],
    );
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,agent_model
       ) VALUES ($1,'basic',$2,'success','{}'::jsonb,'integration-test')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now())`,
      [channelId, runId],
    );

    const recoverable = await loadFinalizeRecoveryCandidates(
      client.query.bind(client),
      { pipelineCycleId: batchId },
    );
    assert.deepEqual(
      recoverable.map((row) => [row.channel_id, row.run_id]),
      [[channelId, runId]],
    );
    assert.deepEqual(
      await loadPipelineFinalizeBlockers(client.query.bind(client), batchId),
      { agentOpen: 0, finalOpen: 1, publicationOpen: 0 },
    );

    const repaired = await commitFinalizedProfile(client, {
      channelId,
      runId,
      status: "ready_auto",
      deduplicated: true,
      publicationAsOf: "2026-07-28T12:00:00.000Z",
      publicationRevisionType: "incremental",
    });
    assert.equal(repaired.deduplicated, true);
    assert.deepEqual(
      await loadFinalizeRecoveryCandidates(client.query.bind(client), { pipelineCycleId: batchId }),
      [],
    );
    assert.deepEqual(
      await loadPipelineFinalizeBlockers(client.query.bind(client), batchId),
      { agentOpen: 0, finalOpen: 0, publicationOpen: 0 },
    );
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
