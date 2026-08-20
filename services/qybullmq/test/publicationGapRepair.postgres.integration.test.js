import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { buildAgentPublicationRun } from "../src/agentPublicationCurrent.js";
import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { commitFinalizedProfile } from "../src/finalizedProfileStore.js";
import { recordInitialFullObservations } from "../src/initialFullObservations.js";
import {
  completeAboutOnlyPublicationGapRepair,
} from "../src/publicationGapRepairExecution.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { completePublicationOperationalFixture } from "./support/publicationOperationalFixtures.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

function publicationPool() {
  return new Pool({
    connectionString: integrationUrl,
    max: 1,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
}

function aboutCommand({ channelId, runId, observedAt, suffix }) {
  const about = normalizeAboutMetrics({
    aboutObserved: true,
    locale: "en",
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
    idempotencyKey: `about:${runId}:publication-gap:${suffix}`,
    channelId,
    runId,
    observedAt,
    triggerReason: "repair",
    scheduledAt: observedAt,
    startedAt: observedAt,
    finishedAt: observedAt,
    crawlerVersion: "integration-test",
    extractorVersions: { youtubejs: "integration-test" },
    about,
    current: {
      aboutDescription: "A complete Publication Gap repair Channel.",
      descriptionStatus: "exact",
      country: "Brazil",
      joinedDateText: "Joined Jan 1, 2020",
      joinedAt: "2020-01-01",
      joinedAtPrecision: "date_only",
      externalLinks: [],
      externalLinksStatus: "observed",
      rssUrl: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
      vanityChannelUrl: `https://www.youtube.com/@publication-gap-${suffix}`,
      isFamilySafe: true,
      isVerified: false,
      isVerifiedStatus: "not_verified",
      keywordsStatus: "observed",
      availableTabsStatus: "observed",
      identity: {
        title: "Publication Gap Repair",
        handle: `@publication-gap-${suffix}`,
        avatar_url: "https://yt3.example/publication-gap.jpg",
        keywords: ["publication", "repair"],
        available_tabs: ["videos"],
        summary: "A complete Publication Gap repair Channel.",
      },
    },
  };
}

async function insertDispatchAndCandidate(client, { batchId, channelId, acceptedAt }) {
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
     ) VALUES ($1,$1,$2,$3,'accepted',$4::timestamptz,$4::timestamptz)
     RETURNING candidate_id`,
    [batchId, channelId, `https://www.youtube.com/channel/${channelId}`, acceptedAt],
  );
  return Number(candidate.rows[0].candidate_id);
}

async function insertAgentProfile(client, { channelId, observedAt, suffix }) {
  const templateText = "Analyze every input URL and return all V1 Agent facts.";
  const template = await client.query(
    `INSERT INTO crawler.agent_prompt_templates (
       name,version,template_text,status,is_default
     ) VALUES ($1,1,$2,'active',false)
     RETURNING template_id`,
    [`publication-gap-${suffix}`, templateText],
  );
  const config = await client.query(
    `INSERT INTO crawler.agent_configs (
       name,provider,model,prompt_template_id,tools_json,enabled,is_default
     ) VALUES ($1,'openai-compatible','agent-test',$2,$3::jsonb,true,false)
     RETURNING config_id,provider,model,prompt_template_id,tools_json`,
    [
      `publication-gap-${suffix}`,
      template.rows[0].template_id,
      JSON.stringify([{ type: "web_search" }]),
    ],
  );
  const agentConfig = { ...config.rows[0], template_text: templateText };
  const metrics = completePublicationOperationalFixture(channelId, { observedAt }).agent.metrics_json;
  const publicationRun = buildAgentPublicationRun({
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
       current_output_hash,updated_at
     ) VALUES (
       $1,'basic',$2,'success',$3::jsonb,$4,$5,$6,$7,$8,$9::text[],$10,$11,$12,$13,
       $14::timestamptz
     )`,
    [
      channelId,
      `https://www.youtube.com/channel/${channelId}`,
      JSON.stringify(metrics),
      publicationRun.agent_model,
      publicationRun.agent_config_id,
      publicationRun.prompt_template_id,
      publicationRun.prompt_hash,
      publicationRun.prompt_variant,
      publicationRun.input_content_ids,
      publicationRun.input_content_hash,
      publicationRun.taxonomy_version,
      publicationRun.agent_version_hash,
      observationFactsHash(metrics),
      observedAt,
    ],
  );
  return metrics;
}

async function insertObservation(client, {
  observationId,
  channelId,
  runId,
  kind,
  outcome,
  observedAt,
}) {
  const factsHash = observationFactsHash({ channel_id: channelId, kind, outcome });
  await client.query(
    `INSERT INTO crawler.crawl_observations (
       observation_id,observed_at,channel_id,run_id,observation_kind,kind_sequence,
       trigger_reason,outcome,outcome_reason_code,result_summary_json,facts_hash,
       crawler_version,extractor_versions
     ) VALUES (
       $1,$2::timestamptz,$3,$4,$5,1,'initial_full',$6,$7,'{}'::jsonb,$8,
       'integration-test','{}'::jsonb
     )`,
    [observationId, observedAt, channelId, runId, kind, outcome, `${kind}_${outcome}`, factsHash],
  );
  await client.query(
    `INSERT INTO crawler.channel_domain_cursors (
       channel_id,observation_kind,latest_sequence,latest_observation_id,latest_observed_at,
       latest_complete_observation_id,latest_complete_observed_at,current_facts_hash
     ) VALUES (
       $1,$2,1,$3,$4::timestamptz,
       CASE WHEN $5='complete' THEN $3::uuid ELSE NULL END,
       CASE WHEN $5='complete' THEN $4::timestamptz ELSE NULL END,$6
     )`,
    [channelId, kind, observationId, observedAt, outcome, factsHash],
  );
}

async function rowVersions(client, { runId, channelId }) {
  const candidates = await client.query(
    `SELECT candidate_id,xmin::text AS row_version
     FROM crawler.content_candidates WHERE run_id=$1 ORDER BY candidate_id`,
    [runId],
  );
  const contents = await client.query(
    `SELECT content_key,xmin::text AS row_version
     FROM crawler.contents WHERE run_id=$1 ORDER BY content_key`,
    [runId],
  );
  const observations = await client.query(
    `SELECT observation_kind,observation_id::text
     FROM crawler.crawl_observations
     WHERE channel_id=$1 AND run_id=$2 AND observation_kind IN ('video','agent')
     ORDER BY observation_kind,observation_id`,
    [channelId, runId],
  );
  return {
    candidates: candidates.rows,
    contents: contents.rows,
    observations: observations.rows,
  };
}

test("About-only Publication Gap Repair upgrades its Promotion Run without rewriting Video or Agent", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool();
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `about-only-gap-${suffix}`;
  const channelId = `UCaboutonlygap${suffix}`;
  const runId = `run:about-only-gap:${suffix}`;
  const observedAt = "2026-08-16T10:00:00.000Z";
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    const candidateId = await insertDispatchAndCandidate(client, {
      batchId,
      channelId,
      acceptedAt: "2026-08-16T09:00:00.000Z",
    });
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,
         latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'About-only Gap','active',1000,true,'done',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, runId, candidateId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,publication_finalized_status,
         publication_finalized_at,started_at,finished_at,result_json
       ) VALUES (
         $1,$2,$3,'done','full',30,1,'done','ready_partial',now(),
         '2026-08-16T09:00:00Z','2026-08-16T09:30:00Z',$4::jsonb
       )`,
      [
        runId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          publication_gap_repair: { status: "required", domains: ["channel"] },
        }),
      ],
    );
    await insertAgentProfile(client, { channelId, observedAt, suffix });

    const oldAboutId = randomUUID();
    const videoId = randomUUID();
    const agentId = randomUUID();
    await insertObservation(client, {
      observationId: oldAboutId,
      channelId,
      runId,
      kind: "about",
      outcome: "partial",
      observedAt: "2026-08-16T09:30:00.000Z",
    });
    await insertObservation(client, {
      observationId: videoId,
      channelId,
      runId,
      kind: "video",
      outcome: "complete",
      observedAt: "2026-08-16T09:30:00.000Z",
    });
    await insertObservation(client, {
      observationId: agentId,
      channelId,
      runId,
      kind: "agent",
      outcome: "complete",
      observedAt: "2026-08-16T09:30:00.000Z",
    });

    const sourceContentId = `video-${suffix}`;
    const contentKey = `${channelId}:video:${sourceContentId}`;
    await client.query(
      `INSERT INTO crawler.contents (
         content_key,channel_id,run_id,content_type,content_type_source,source_content_id,
         position,title,url,thumbnail_url,description,description_status,description_source,
         published_at,published_at_status,published_at_source,published_at_precision,
         duration_seconds,duration_status,duration_source,view_count_text,view_count_status,
         view_count_source,like_count,like_count_status,like_count_source,comment_count,
         comment_count_status,comment_count_source,comments_disabled,access_status,
         access_status_source,player_last_observed_at,last_observation_id
       ) VALUES (
         $1,$2,$3,'video','watch_player',$4,1,'Stable Video',$5,$6,
         'Stable description','exact','watch_player',$7::timestamptz,'exact',
         'watch_player','second',120,'exact','watch_player','100 views','exact',
         'watch_player',10,'exact','watch_player',2,'exact','watch_player',false,
         'public','watch_player',$7::timestamptz,$8
       )`,
      [
        contentKey,
        channelId,
        runId,
        sourceContentId,
        `https://www.youtube.com/watch?v=${sourceContentId}`,
        `https://i.ytimg.com/vi/${sourceContentId}/hqdefault.jpg`,
        "2026-08-15T10:00:00.000Z",
        videoId,
      ],
    );
    await client.query(
      `INSERT INTO crawler.content_candidates (
         run_id,channel_id,source_content_id,position,title,source_url,content_type,
         type_status,type_source,detail_status,api_status,missing_fields,content_key,
         result_json,finished_at
       ) VALUES (
         $1,$2,$3,1,'Stable Video',$4,'video','resolved','watch_player','done',
         'not_needed','{}'::text[],$5,'{"access":{"access_status":"public"}}'::jsonb,now()
       )`,
      [runId, channelId, sourceContentId, `https://www.youtube.com/watch?v=${sourceContentId}`, contentKey],
    );
    await client.query(
      `UPDATE crawler.agent_profiles
       SET last_observation_id=$2,last_observed_at=$3::timestamptz
       WHERE channel_id=$1 AND agent_mode='basic'`,
      [channelId, agentId, "2026-08-16T09:30:00.000Z"],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_partial','{}'::jsonb,$3::jsonb,now())`,
      [
        channelId,
        runId,
        JSON.stringify({
          initial_observations: {
            outcomes: { about: "partial", video: "complete", agent: "complete" },
          },
          unavailable_candidate_count: 0,
          detail_open_count: 0,
          api_open_count: 0,
          classified_content_count: 1,
          expected_content_count: 1,
          missing_channel_fields: [],
          missing_agent_fields: [],
          missing_content_fields: {},
        }),
      ],
    );

    const before = await rowVersions(client, { runId, channelId });
    const finalizeCalls = [];
    const command = aboutCommand({ channelId, runId, observedAt, suffix });
    const staged = await completeAboutOnlyPublicationGapRepair(client.query.bind(client), {
      jobData: {
        run_id: runId,
        candidate_id: candidateId,
        publication_gap_domains: ["channel"],
        publication_gap_root_run_id: runId,
        require_complete_about_metrics: true,
        publication_gap_scope: "about_only",
      },
      runId,
      channelId,
      aboutOutcome: "complete",
      aboutObservationCommand: command,
      enqueueFinalize: async (value) => finalizeCalls.push(value),
    });
    assert.equal(staged.scope, "about_only");
    assert.equal(finalizeCalls.length, 1);

    const repairedObservations = await recordInitialFullObservations({
      withTransaction: (action) => action(client),
      channelId,
      runId,
      observedAt,
      revisionType: "repair",
      repairId: `about-only-${suffix}`,
    });
    assert.deepEqual(Object.keys(repairedObservations.observations), ["about"]);
    assert.deepEqual(repairedObservations.outcomes, {
      about: "complete",
      video: "complete",
      agent: "complete",
    });

    const committed = await commitFinalizedProfile(client, {
      channelId,
      runId,
      status: "ready_auto",
      profile: { channel: { channel_id: channelId }, contents: {}, agent_profile: {} },
      quality: {
        quality_status: "ready_auto",
        initial_observations: { outcomes: repairedObservations.outcomes },
      },
      publicationAsOf: observedAt,
      publicationRevisionType: "repair",
    });
    assert.equal(committed.applied, true);

    const after = await rowVersions(client, { runId, channelId });
    assert.deepEqual(after, before);
    const state = await client.query(
      `SELECT run.publication_finalized_status,finalized.status AS finalized_status,
              run.result_json ? 'pending_initial_about_observation' AS pending_about,
              count(observation.*) FILTER (WHERE observation.observation_kind='about')::int
                AS about_observations,
              count(observation.*) FILTER (WHERE observation.observation_kind='video')::int
                AS video_observations,
              count(observation.*) FILTER (WHERE observation.observation_kind='agent')::int
                AS agent_observations
       FROM crawler.channel_runs AS run
       JOIN crawler.finalized_profiles AS finalized ON finalized.run_id=run.run_id
       LEFT JOIN crawler.crawl_observations AS observation ON observation.run_id=run.run_id
       WHERE run.run_id=$1
       GROUP BY run.publication_finalized_status,finalized.status,run.result_json`,
      [runId],
    );
    assert.deepEqual(state.rows[0], {
      publication_finalized_status: "ready_auto",
      finalized_status: "ready_auto",
      pending_about: false,
      about_observations: 2,
      video_observations: 1,
      agent_observations: 1,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("only an explicit Publication Gap child Repair can complete a frozen Promotion Bootstrap", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool();
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `child-gap-${suffix}`;
  const channelId = `UCchildgap${suffix}`;
  const seedChannelId = `UCchildgapseed${suffix}`;
  const promotionRunId = `run:child-gap-promotion:${suffix}`;
  const unprovenRepairRunId = `run:child-gap-unproven:${suffix}`;
  const repairRunId = `run:child-gap-repair:${suffix}`;
  const observedAt = "2026-08-16T12:00:00.000Z";
  const streamId = randomUUID();
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    const candidateId = await insertDispatchAndCandidate(client, {
      batchId,
      channelId,
      acceptedAt: "2026-08-16T09:00:00.000Z",
    });
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,
         latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES
         ($1,$3,'Publication Gap Child','active',1000,true,'done',$5,$6,$5),
         ($2,$4,'Publication route seed','active',1000,false,'pending',NULL,NULL,NULL)`,
      [
        channelId,
        seedChannelId,
        `https://www.youtube.com/channel/${channelId}`,
        `https://www.youtube.com/channel/${seedChannelId}`,
        promotionRunId,
        candidateId,
      ],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,publication_finalized_status,
         publication_finalized_at,started_at,finished_at,result_json
       ) VALUES (
         $1,$2,$3,'done','full',30,0,'done','ready_auto',now(),
         '2026-08-16T09:00:00Z','2026-08-16T10:00:00Z',$4::jsonb
       )`,
      [
        promotionRunId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          publication_gap_repair: {
            status: "required",
            reason: "quality_policy_refresh",
            domains: ["channel", "video"],
          },
        }),
      ],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now())`,
      [channelId, promotionRunId],
    );
    await client.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,$3,'2026-08-16T08:00:00Z',
         'integration-test','Publication Gap child test',
         'integration-test','capture enabled'
       )`,
      [streamId, `publication-gap-child-${suffix}`, PUBLICATION_WRITER_VERSION],
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
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,started_at,result_json
       ) VALUES (
         $1,$2,$3,'running','full',30,0,'done','2026-08-16T10:30:00Z',$4::jsonb
       )`,
      [
        unprovenRepairRunId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          final_repair: { rounds: 1, parent_run_id: promotionRunId, mode: "channel" },
        }),
      ],
    );
    await client.query("SAVEPOINT unproven_publication_gap_child");
    await assert.rejects(
      client.query(
        "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
        [channelId, unprovenRepairRunId],
      ),
      /Channel Registry promotion Run must Finalize before a later Run/,
    );
    await client.query("ROLLBACK TO SAVEPOINT unproven_publication_gap_child");
    const command = aboutCommand({ channelId, runId: repairRunId, observedAt, suffix });
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,started_at,result_json
       ) VALUES (
         $1,$2,$3,'running','full',30,0,'done','2026-08-16T11:00:00Z',$4::jsonb
       )`,
      [
        repairRunId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          final_repair: { rounds: 1, parent_run_id: promotionRunId, mode: "channel" },
          publication_gap_repair: {
            status: "required",
            reason: "inherited_publication_gap_repair",
            domains: ["channel", "video"],
            root_run_id: promotionRunId,
          },
          pending_initial_about_observation: command,
          upload_scan: {
            pages: 1,
            inspected_count: 0,
            selected_count: 0,
            requested_limit: 30,
            content_max_age_days: 90,
            scan_policy_version: "integration-test",
            parse_gap_count: 0,
            stop_reason: "list_end",
            terminal_reason: "list_end",
          },
        }),
      ],
    );
    await client.query(
      "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
      [channelId, repairRunId],
    );
    await insertAgentProfile(client, { channelId, observedAt, suffix });
    const observations = await recordInitialFullObservations({
      withTransaction: (action) => action(client),
      channelId,
      runId: repairRunId,
      observedAt,
      revisionType: "repair",
      repairId: `child-gap-${suffix}`,
    });
    assert.deepEqual(observations.outcomes, {
      about: "complete",
      video: "complete",
      agent: "complete",
    });

    const finalizeInput = {
      channelId,
      runId: repairRunId,
      status: "ready_auto",
      profile: { channel: { channel_id: channelId }, contents: {}, agent_profile: {} },
      quality: {
        quality_status: "ready_auto",
        initial_observations: { outcomes: observations.outcomes },
      },
      publicationAsOf: observedAt,
    };
    const ordinary = await commitFinalizedProfile(client, {
      ...finalizeInput,
      publicationRevisionType: "incremental",
    });
    assert.equal(ordinary.applied, true);
    assert.equal(ordinary.publication.status, "not_owned");
    assert.equal(ordinary.publication.onboarding.status, "not_initial_full_run");
    assert.equal((await client.query(
      "SELECT count(*)::int AS count FROM publication.channel_stream_state WHERE channel_id=$1",
      [channelId],
    )).rows[0].count, 0);

    const repaired = await commitFinalizedProfile(client, {
      ...finalizeInput,
      publicationRevisionType: "repair",
    });
    assert.equal(repaired.applied, true);
    assert.equal(repaired.publication.onboarding.status, "registered");
    assert.equal(repaired.publication.seed_status, "complete");

    const state = await client.query(
      `SELECT owner.seed_status,delivery.mode,
              array_agg(DISTINCT current.domain ORDER BY current.domain) AS domains,
              count(DISTINCT revision.revision_id)::int AS revisions,
              count(DISTINCT (outbox.destination,outbox.revision_id))::int AS outbox,
              bool_and(revision.revision_type='bootstrap') AS all_bootstrap
       FROM publication.channel_stream_state AS owner
       JOIN publication.channel_delivery_state AS delivery
         ON delivery.publication_stream_id=owner.publication_stream_id
        AND delivery.channel_id=owner.channel_id
       JOIN publication.domain_current AS current
         ON current.publication_stream_id=owner.publication_stream_id
        AND current.channel_id=owner.channel_id
       JOIN publication.revision AS revision
         ON revision.publication_stream_id=current.publication_stream_id
        AND revision.channel_id=current.channel_id
        AND revision.domain=current.domain
       JOIN publication.outbox AS outbox ON outbox.revision_id=revision.revision_id
       WHERE owner.publication_stream_id=$1 AND owner.channel_id=$2
       GROUP BY owner.seed_status,delivery.mode`,
      [streamId, channelId],
    );
    assert.deepEqual(state.rows[0], {
      seed_status: "complete",
      mode: "online",
      domains: ["agent", "channel", "video"],
      revisions: 3,
      outbox: 3,
      all_bootstrap: true,
    });
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});

test("a later Publication Gap Child remains rooted at the immutable Promotion Run", {
  skip: !integrationUrl,
}, async () => {
  const pool = publicationPool();
  const client = await pool.connect();
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `multi-round-gap-${suffix}`;
  const channelId = `UCmultiroundgap${suffix}`;
  const promotionRunId = `run:multi-round-promotion:${suffix}`;
  const firstChildRunId = `run:multi-round-first:${suffix}`;
  const secondChildRunId = `run:multi-round-second:${suffix}`;
  const forgedRootRunId = `run:multi-round-forged-root:${suffix}`;
  const ordinaryChildRunId = `run:multi-round-ordinary:${suffix}`;
  try {
    const identity = await client.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i);
    await client.query("BEGIN");
    const candidateId = await insertDispatchAndCandidate(client, {
      batchId,
      channelId,
      acceptedAt: "2026-08-16T09:00:00.000Z",
    });
    await client.query(
      `INSERT INTO crawler.channels (
         channel_id,channel_url,title,status,subscriber_count,ready_for_agent,agent_status,
         latest_run_id,registry_promotion_candidate_id,registry_promotion_run_id
       ) VALUES ($1,$2,'Multi-round Publication Gap','active',1000,true,'done',$3,$4,$3)`,
      [channelId, `https://www.youtube.com/channel/${channelId}`, promotionRunId, candidateId],
    );
    await client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,publication_finalized_status,
         publication_finalized_at,started_at,finished_at,result_json
       ) VALUES (
         $1,$2,$3,'done','full',30,0,'done','ready_auto',now(),
         now()-interval '3 minutes',now()-interval '2 minutes',$4::jsonb
       )`,
      [
        promotionRunId,
        channelId,
        candidateId,
        JSON.stringify({
          pipeline_cycle_id: batchId,
          publication_gap_repair: {
            status: "required",
            domains: ["channel", "video"],
          },
        }),
      ],
    );

    const childResult = (rootRunId, { includeGap = true } = {}) => ({
      pipeline_cycle_id: batchId,
      final_repair: { rounds: 1, parent_run_id: rootRunId, mode: "channel" },
      ...(includeGap ? {
        publication_gap_repair: {
          status: "required",
          reason: "inherited_publication_gap_repair",
          domains: ["channel", "video"],
          root_run_id: rootRunId,
        },
      } : {}),
    });
    const insertRunningChild = (runId, resultJson) => client.query(
      `INSERT INTO crawler.channel_runs (
         run_id,channel_id,candidate_id,status,crawl_mode,content_limit,
         expected_content_count,detail_status,started_at,result_json
       ) VALUES ($1,$2,$3,'running','full',30,0,'done',now(),$4::jsonb)`,
      [runId, channelId, candidateId, JSON.stringify(resultJson)],
    );
    const allowed = async (sourceRunId, repairRunId) => (await client.query(
      `SELECT crawler.registry_publication_gap_repair_is_allowed($1,$2,$3) AS allowed`,
      [channelId, sourceRunId, repairRunId],
    )).rows[0].allowed;

    await insertRunningChild(firstChildRunId, childResult(promotionRunId));
    assert.equal(await allowed(promotionRunId, firstChildRunId), true);
    await client.query(
      "UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1",
      [channelId, firstChildRunId],
    );
    await client.query(
      `INSERT INTO crawler.finalized_profiles (
         channel_id,run_id,status,profile_json,quality_json,finalized_at
       ) VALUES ($1,$2,'ready_auto','{}'::jsonb,'{}'::jsonb,now())`,
      [channelId, firstChildRunId],
    );
    await client.query(
      `UPDATE crawler.channel_runs
       SET status='done',publication_finalized_status='ready_auto',
           publication_finalized_at=now(),finished_at=now()
       WHERE run_id=$1`,
      [firstChildRunId],
    );

    await insertRunningChild(secondChildRunId, {
      ...childResult(promotionRunId),
      final_repair: { rounds: 2, parent_run_id: promotionRunId, mode: "channel" },
    });
    assert.equal(await allowed(firstChildRunId, secondChildRunId), true);

    await insertRunningChild(forgedRootRunId, {
      ...childResult(firstChildRunId),
      final_repair: { rounds: 2, parent_run_id: firstChildRunId, mode: "channel" },
    });
    assert.equal(await allowed(firstChildRunId, forgedRootRunId), false);

    await insertRunningChild(ordinaryChildRunId, childResult(promotionRunId, { includeGap: false }));
    assert.equal(await allowed(firstChildRunId, ordinaryChildRunId), false);
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    client.release();
    await pool.end();
  }
});
