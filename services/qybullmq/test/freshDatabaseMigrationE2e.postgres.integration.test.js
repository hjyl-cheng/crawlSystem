import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { fileURLToPath } from "node:url";
import test from "node:test";
import pg from "pg";
import { ensureLocalOfflineAgentConfig } from "../scripts/ensureLocalOfflineAgentConfig.mjs";
import { buildAgentPublicationRun } from "../src/agentPublicationCurrent.js";
import { PostgresBusinessPublicationStore } from "../src/businessPublicationIngress.js";
import { PostgresBusinessPublicationProjector } from "../src/businessPublicationProjector.js";
import { PostgresBusinessPublicationReconciler } from "../src/businessPublicationReconciler.js";
import { claimChannelRegistryPromotion } from "../src/channelRegistryPromotion.js";
import { prepareChannelRun } from "../src/channelRunBinding.js";
import {
  verifyBusinessWriterDatabase,
  verifyCrawlerWriterDatabase,
} from "../src/databaseIdentity.js";
import { commitFinalizedProfile } from "../src/finalizedProfileStore.js";
import {
  FreshPublicationBootstrapAdministrator,
  freshPublicationBootstrapConfig,
} from "../src/freshPublicationBootstrap.js";
import { recordInitialFullObservations } from "../src/initialFullObservations.js";
import { persistAgentChannelSuccess } from "../src/incrementalAgentResultStore.js";
import {
  invokeLocalProfileRuntime,
  LocalOfflineProfileExecutor,
} from "../src/localOfflineProfileExecutor.js";
import { prepareManualMigration } from "../src/manualMigrationDispatch.js";
import {
  loadMigrationSourceChannel,
  withMigrationSourceReadTransaction,
} from "../src/migrationSource.js";
import {
  PostgresPublicationOutboxStore,
  PublicationPublisher,
} from "../src/publicationPublisher.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;
const sourceUrl = process.env.FRESH_MIGRATION_SOURCE_POSTGRES_TEST_URL;
const crawlerUrl = process.env.FRESH_MIGRATION_CRAWLER_POSTGRES_TEST_URL;
const businessUrl = process.env.FRESH_MIGRATION_BUSINESS_POSTGRES_TEST_URL;
const sourceDatabaseOid = process.env.FRESH_MIGRATION_SOURCE_DATABASE_OID;
const sourceChannelId = process.env.FRESH_MIGRATION_SOURCE_CHANNEL_ID;
const pythonBin = process.env.FRESH_MIGRATION_LOCAL_PROFILE_PYTHON_BIN;
const integrationEnabled = Boolean(
  sourceUrl
  && crawlerUrl
  && businessUrl
  && sourceDatabaseOid
  && sourceChannelId
  && pythonBin,
);

function parsedDatabaseUrl(value, field) {
  const url = new URL(value);
  const database = decodeURIComponent(url.pathname.replace(/^\//, ""));
  const user = decodeURIComponent(url.username);
  if (!database || !user) throw new TypeError(`${field} must include database and user`);
  return { database, user };
}

async function transaction(pool, action) {
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

async function sourceFingerprint(pool, environment) {
  return withMigrationSourceReadTransaction(async (client) => {
    const result = await client.query(
      `SELECT md5(jsonb_build_object(
                'candidates',COALESCE((
                  SELECT jsonb_agg(to_jsonb(candidate) ORDER BY candidate.candidate_id)
                  FROM crawler.channel_candidates AS candidate
                ),'[]'::jsonb),
                'channels',COALESCE((
                  SELECT jsonb_agg(to_jsonb(channel) ORDER BY channel.channel_id)
                  FROM crawler.channels AS channel
                ),'[]'::jsonb)
              )::text) AS fingerprint,
              current_setting('default_transaction_read_only') AS role_default_read_only,
              has_table_privilege(
                current_user,'crawler.channel_candidates','INSERT,UPDATE,DELETE,TRUNCATE'
              ) AS candidate_write,
              has_table_privilege(
                current_user,'crawler.channels','INSERT,UPDATE,DELETE,TRUNCATE'
              ) AS channel_write`,
    );
    return result.rows[0];
  }, { pool, environment });
}

test("read-only Migration Source reaches fresh Crawler and Business Current without a cross-database transaction", {
  skip: !integrationEnabled,
  timeout: 600000,
}, async () => {
  const sourceIdentity = parsedDatabaseUrl(sourceUrl, "Source URL");
  const crawlerIdentity = parsedDatabaseUrl(crawlerUrl, "Crawler URL");
  const businessIdentity = parsedDatabaseUrl(businessUrl, "Business URL");
  for (const identity of [sourceIdentity, crawlerIdentity, businessIdentity]) {
    assert.match(identity.database, /_test$/i, "E2E URLs must target disposable *_test databases");
  }
  assert.notEqual(sourceIdentity.database, crawlerIdentity.database);
  assert.notEqual(crawlerIdentity.database, businessIdentity.database);

  const sourceEnvironment = {
    MIGRATION_DATABASE_URL: sourceUrl,
    MIGRATION_SOURCE_ID: "isolated-qy-migration-e2e",
    EXPECTED_MIGRATION_DATABASE: sourceIdentity.database,
    EXPECTED_MIGRATION_DATABASE_OID: sourceDatabaseOid,
    EXPECTED_MIGRATION_DATABASE_USER: sourceIdentity.user,
    EXPECTED_CRAWLER_DATABASE: crawlerIdentity.database,
    MIGRATION_POSTGRES_STATEMENT_TIMEOUT_MS: "10000",
  };
  const crawlerEnvironment = {
    EXPECTED_CRAWLER_DATABASE: crawlerIdentity.database,
    FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
  };
  const businessEnvironment = {
    EXPECTED_BUSINESS_DATABASE: businessIdentity.database,
    FORBIDDEN_BUSINESS_DATABASE: "yewu_business",
  };
  const sourcePool = new Pool({ connectionString: sourceUrl, max: 1 });
  const crawlerPool = new Pool({
    connectionString: crawlerUrl,
    max: 4,
    options: `-c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const businessPool = new Pool({ connectionString: businessUrl, max: 4 });
  const suffix = randomUUID().replaceAll("-", "");
  const batchId = `fresh-migration-e2e-${suffix}`;
  const runId = `full:fresh-migration-e2e:${suffix}`;
  const streamId = randomUUID();
  const deploymentKey = `fresh-migration-e2e-${suffix}`;
  const observedAt = new Date().toISOString();
  const contentFixtures = [
    { kind: "video", id: `video-${suffix.slice(0, 12)}`, position: 1 },
    { kind: "short", id: `short-${suffix.slice(0, 12)}`, position: 2 },
    { kind: "live", id: `live-${suffix.slice(0, 12)}`, position: 3 },
  ];

  try {
    await verifyCrawlerWriterDatabase(crawlerPool.query.bind(crawlerPool), crawlerEnvironment);
    await verifyBusinessWriterDatabase(businessPool.query.bind(businessPool), businessEnvironment);

    const sourceBefore = await sourceFingerprint(sourcePool, sourceEnvironment);
    assert.equal(sourceBefore.role_default_read_only, "on");
    assert.equal(sourceBefore.candidate_write, false);
    assert.equal(sourceBefore.channel_write, false);
    const sourceSnapshot = await loadMigrationSourceChannel({
      channelId: sourceChannelId,
      pool: sourcePool,
      environment: sourceEnvironment,
    });
    assert.equal(sourceSnapshot.channel_id, sourceChannelId);
    const publicationConfig = freshPublicationBootstrapConfig({
      CRAWLER_ADMIN_DATABASE_URL: crawlerUrl,
      BUSINESS_ADMIN_DATABASE_URL: businessUrl,
      EXPECTED_CRAWLER_DATABASE: crawlerIdentity.database,
      EXPECTED_BUSINESS_DATABASE: businessIdentity.database,
      EXPECTED_CRAWLER_CHANNEL_COUNT: "0",
      EXPECTED_BUSINESS_CHANNEL_COUNT: "0",
      PUBLICATION_STREAM_ID: streamId,
      PUBLICATION_SOURCE_DEPLOYMENT_KEY: deploymentKey,
      PUBLICATION_SOURCE_IDENTITY_JSON: JSON.stringify({
        database: crawlerIdentity.database,
        migration_source_id: sourceSnapshot.source_id,
      }),
      PUBLICATION_DESTINATION: "business",
      PUBLICATION_BOOTSTRAP_PROJECTION_MODE: "online",
      PUBLICATION_OPERATOR: "fresh-migration-e2e",
      PUBLICATION_ACTION_REASON: "isolated fresh migration E2E",
      PUBLICATION_WRITER_DEPLOYMENT_REF: `sha256:${"1".repeat(64)}`,
      PUBLICATION_RUNTIME_DEPLOYMENT_REF: `sha256:${"2".repeat(64)}`,
    });
    const crawlerAdminClient = await crawlerPool.connect();
    const businessAdminClient = await businessPool.connect();
    try {
      const administrator = new FreshPublicationBootstrapAdministrator({
        crawlerClient: crawlerAdminClient,
        businessClient: businessAdminClient,
        config: publicationConfig,
      });
      const bootstrapped = await administrator.initialize();
      assert.deepEqual(
        { business: bootstrapped.business, source: bootstrapped.source, phase: bootstrapped.phase },
        { business: 1, source: 1, phase: "complete" },
      );
      const repeatedBootstrap = await administrator.initialize();
      assert.deepEqual(
        {
          business: repeatedBootstrap.business,
          source: repeatedBootstrap.source,
          phase: repeatedBootstrap.phase,
        },
        { business: 0, source: 0, phase: "complete" },
      );
    } finally {
      crawlerAdminClient.release();
      businessAdminClient.release();
    }

    const firstIntent = await transaction(crawlerPool, (client) => prepareManualMigration(client, {
      sourceSnapshot,
      batchId,
    }));
    const repeatedIntent = await transaction(crawlerPool, (client) => prepareManualMigration(client, {
      sourceSnapshot,
      batchId,
    }));
    assert.equal(repeatedIntent.intentId, firstIntent.intentId);
    assert.equal(
      String(repeatedIntent.candidate.candidate_id),
      String(firstIntent.candidate.candidate_id),
    );
    assert.equal(repeatedIntent.shouldEnqueue, false);

    const candidateId = Number(firstIntent.candidate.candidate_id);
    const promoted = await transaction(crawlerPool, async (client) => {
      const result = await claimChannelRegistryPromotion(client, {
        candidateId,
        runId,
        channelId: sourceChannelId,
        channelUrl: sourceSnapshot.channel_url,
        handle: sourceSnapshot.handle,
        title: sourceSnapshot.title || "Fresh Migration E2E",
        country: "Brazil",
        countryCode: "BR",
        countryCanonicalName: "Brazil",
        subscriberCount: Number(sourceSnapshot.search_subscriber_count ?? 1000),
        subscriberCountText: sourceSnapshot.search_subscriber_count_text || "1,000 subscribers",
        readyForAgent: true,
        sourceJson: {
          migration_source: sourceSnapshot,
          channel_extractor: "isolated-integration-fixture",
        },
      });
      await prepareChannelRun(client, {
        runId,
        channelId: sourceChannelId,
        candidateId,
        crawlMode: "full",
        contentLimit: 30,
        resultJson: {
          migration_intent_id: firstIntent.intentId,
          upload_scan: {
            pages: 1,
            inspected_count: contentFixtures.length,
            selected_count: contentFixtures.length,
            requested_limit: 30,
            content_max_age_days: 90,
            scan_policy_version: "fresh-migration-e2e-v1",
            parse_gap_count: 0,
            stop_reason: "list_end",
            terminal_reason: "list_end",
          },
        },
      });
      await client.query(
        `UPDATE crawler.channels
         SET title=COALESCE(title,'Fresh Migration E2E'),handle=COALESCE(handle,'@freshmigratione2e'),
             avatar_url='https://yt3.example/fresh-migration-e2e.jpg',
             summary='A deterministic isolated migration channel.',
             keywords=ARRAY['migration','integration'],available_tabs=ARRAY['videos'],
             about_description='A deterministic isolated migration channel.',
             country='Brazil',country_source='youtube_about',country_code='BR',
             country_canonical_name='Brazil',joined_date_text='Joined Jan 1, 2020',
             joined_at='2020-01-01',joined_at_precision='date_only',
             external_links='[]'::jsonb,is_verified=false,is_verified_status='not_verified',
             subscriber_count=1000,subscriber_count_text='1,000 subscribers',
             subscriber_count_status='exact',subscriber_count_source='youtube_about',
             total_view_count=50000,total_view_count_text='50,000 views',
             total_view_count_status='exact',total_view_count_source='youtube_about',
             total_video_count=0,total_video_count_text='0 videos',
             total_video_count_status='exact',total_video_count_source='youtube_about',
             ready_for_agent=true,agent_status='running',updated_at=now()
         WHERE channel_id=$1`,
        [sourceChannelId],
      );
      await client.query(
        `UPDATE crawler.channel_runs
         SET status='waiting_agent',detail_status='done',crawler_version='integration-test',
             expected_content_count=$2,
             trigger_reason='initial_full',updated_at=now()
         WHERE run_id=$1`,
        [runId, contentFixtures.length],
      );
      for (const fixture of contentFixtures) {
        const contentKey = `${sourceChannelId}:${fixture.id}`;
        await client.query(
          `INSERT INTO crawler.contents (
             content_key,channel_id,run_id,content_type,content_type_source,
             source_content_id,position,title,url,published_at,published_at_status,
             published_at_source,published_at_precision,duration_seconds,duration_status,
             duration_source,view_count,view_count_status,view_count_source,
             like_count,like_count_status,like_count_source,comment_count,
             comment_count_status,comment_count_source,comments_disabled,description,
             description_status,description_source,access_status,access_status_source,
             extractor_version,last_enriched_at,player_last_observed_at,next_last_observed_at
           ) VALUES (
             $1,$2,$3,$4,'integration_fixture',$5,$6,$7,$8,$9,'exact',
             'integration_fixture','second',60,'exact','integration_fixture',100,'exact',
             'integration_fixture',0,'zero_from_empty','integration_fixture',0,
             'zero_from_surface','integration_fixture',false,'','empty',
             'integration_fixture','public','integration_fixture','integration-test',$9,$9,$9
           )`,
          [
            contentKey,
            sourceChannelId,
            runId,
            fixture.kind,
            fixture.id,
            fixture.position,
            `Fresh ${fixture.kind}`,
            `https://www.youtube.com/watch?v=${fixture.id}`,
            observedAt,
          ],
        );
        await client.query(
          `INSERT INTO crawler.content_candidates (
             run_id,channel_id,source_content_id,position,title,source_url,
             content_type,type_status,type_source,detail_status,api_status,
             content_key,disposition,attempts,finished_at
           ) VALUES (
             $1,$2,$3,$4,$5,$6,$7,'resolved','integration_fixture','done',
             'not_needed',$8,'stored',1,$9
           )`,
          [
            runId,
            sourceChannelId,
            fixture.id,
            fixture.position,
            `Fresh ${fixture.kind}`,
            `https://www.youtube.com/watch?v=${fixture.id}`,
            fixture.kind,
            contentKey,
            observedAt,
          ],
        );
      }
      return result;
    });
    assert.equal(promoted.promoted, true);

    const agentConfig = await transaction(crawlerPool, ensureLocalOfflineAgentConfig);
    const localAgentEnvironment = {
      ...process.env,
      LOCAL_PROFILE_PYTHON_BIN: pythonBin,
      LOCAL_PROFILE_MODEL_BUNDLE: process.env.FRESH_MIGRATION_LOCAL_PROFILE_MODEL_BUNDLE
        || fileURLToPath(new URL("../../local-agent/models", import.meta.url)),
      LOCAL_PROFILE_DATABASE_URL: crawlerUrl,
      EXPECTED_CRAWLER_DATABASE: crawlerIdentity.database,
      FORBIDDEN_CRAWLER_DATABASE: "bullmq_crawler_migration",
      PYTHONPATH: [
        fileURLToPath(new URL("../../local-agent/src", import.meta.url)),
        process.env.PYTHONPATH,
      ].filter(Boolean).join(":"),
    };
    const executor = new LocalOfflineProfileExecutor({
      invokeRuntime: (request, { config }) => invokeLocalProfileRuntime(request, {
        config,
        environment: localAgentEnvironment,
      }),
      loadLatestRunIds: async (channelIds) => {
        const rows = await crawlerPool.query(
          "SELECT channel_id,latest_run_id FROM crawler.channels WHERE channel_id=ANY($1::text[])",
          [channelIds],
        );
        return new Map(rows.rows.map((row) => [row.channel_id, row.latest_run_id]));
      },
    });
    const execution = await executor.execute({
      config: agentConfig,
      channelIds: [sourceChannelId],
    });
    assert.deepEqual(execution.errors, []);
    const resolved = execution.results.get(sourceChannelId);
    assert.ok(resolved, "Local Agent must return the migrated Channel");
    const publicationRun = buildAgentPublicationRun({
      agentConfig,
      agentModel: resolved.agent_model,
      promptVariant: "local_offline",
      executionVariant: resolved.execution_variant,
      runtimeIdentity: resolved.metrics.profile_processing_context,
      inputContentIds: resolved.input_content_ids,
      taxonomyVersion: resolved.metrics.profile_processing_context.taxonomy_version,
    });
    await persistAgentChannelSuccess({
      withTransaction: (action) => transaction(crawlerPool, action),
      channelId: sourceChannelId,
      inputUrl: sourceSnapshot.channel_url,
      metrics: resolved.metrics,
      publicationRun,
    });

    const observations = await recordInitialFullObservations({
      withTransaction: (action) => transaction(crawlerPool, action),
      channelId: sourceChannelId,
      runId,
      observedAt,
      crawlerVersion: "integration-test",
    });
    assert.equal(observations.recorded, true);
    assert.deepEqual(observations.outcomes, {
      about: "complete",
      video: "complete",
      agent: "complete",
    });

    const crawlerState = (await crawlerPool.query(
      `SELECT to_jsonb(channel) AS channel,profile.metrics_json
       FROM crawler.channels AS channel
       JOIN crawler.agent_profiles AS profile USING(channel_id)
       WHERE channel.channel_id=$1`,
      [sourceChannelId],
    )).rows[0];
    const finalizedContents = (await crawlerPool.query(
      `SELECT * FROM crawler.contents WHERE channel_id=$1 AND run_id=$2 ORDER BY position`,
      [sourceChannelId, runId],
    )).rows;
    const finalized = await transaction(crawlerPool, (client) => commitFinalizedProfile(client, {
      channelId: sourceChannelId,
      runId,
      status: "ready_auto",
      profile: {
        channel: crawlerState.channel,
        contents: {
          videos: finalizedContents.filter((row) => row.content_type === "video"),
          shorts: finalizedContents.filter((row) => row.content_type === "short"),
          lives: finalizedContents.filter((row) => row.content_type === "live"),
        },
        metrics: crawlerState.metrics_json,
        agent_profile: crawlerState.metrics_json,
        quality: { quality_status: "ready_auto" },
      },
      quality: {
        quality_status: "ready_auto",
        data_complete: true,
        expected_content_count: contentFixtures.length,
        candidate_count: contentFixtures.length,
        classified_content_count: contentFixtures.length,
        source_revision: `fresh-migration-e2e:${suffix}`,
      },
      publicationAsOf: observedAt,
    }));
    assert.equal(finalized.applied, true);
    assert.equal(finalized.publication.status, "revised");
    assert.equal(finalized.publication.onboarding.status, "registered");
    assert.equal(finalized.publication.onboarding.publication_stream_id, streamId);
    assert.deepEqual(
      finalized.publication.revisions.map((revision) => revision.domain),
      ["channel", "video", "agent"],
    );

    const businessStore = new PostgresBusinessPublicationStore(businessPool);
    const publisher = new PublicationPublisher({
      store: new PostgresPublicationOutboxStore({
        query: crawlerPool.query.bind(crawlerPool),
        withTransaction: (action) => transaction(crawlerPool, action),
      }),
      ingress: businessStore,
      destination: "business",
      leaseOwner: `fresh-migration-e2e-${suffix}`,
      batchSize: 10,
      leaseSeconds: 60,
      logger: { info() {}, error() {} },
    });
    const published = await publisher.runOnce();
    assert.equal(published.claimed, 3);
    assert.equal(published.delivered, 3);

    const reconciler = new PostgresBusinessPublicationReconciler(businessPool, {
      workerId: `fresh-migration-e2e-reconciler-${suffix}`,
      batchSize: 10,
      concurrency: 1,
    });
    const reconciled = await reconciler.runOnce();
    assert.equal(reconciled.claimed, 1);
    assert.equal(reconciled.outcomes.activated, 1);
    assert.equal(reconciled.applied_revisions, 3);

    const projector = new PostgresBusinessPublicationProjector(businessPool, {
      workerId: `fresh-migration-e2e-projector-${suffix}`,
      batchSize: 10,
      leaseSeconds: 60,
    });
    const projected = await projector.runOnce();
    assert.equal(projected.outcome, "published");
    assert.equal(projected.projected, 1);
    assert.equal(projected.delivered, 1);

    const businessCurrent = (await businessPool.query(
      `SELECT
         (SELECT count(*)::int FROM result.entity_current WHERE channel_id=$1) AS entity_current,
         (SELECT count(*)::int FROM result.video_current WHERE channel_id=$1) AS video_current,
         (SELECT count(*)::int FROM result.agent_current WHERE channel_id=$1) AS agent_current,
         (SELECT count(*)::int FROM public.channel_snapshots WHERE channel_id=$1) AS snapshots,
         (SELECT count(*)::int FROM public.creator_search_live WHERE channel_id=$1) AS search_current,
         (SELECT count(*)::int FROM public.content_items WHERE channel_id=$1) AS content_items,
         (SELECT count(*)::int FROM public.content_snapshots WHERE channel_id=$1) AS content_snapshots`,
      [sourceChannelId],
    )).rows[0];
    assert.deepEqual(businessCurrent, {
      entity_current: 1,
      video_current: 1,
      agent_current: 1,
      snapshots: 1,
      search_current: 1,
      content_items: contentFixtures.length,
      content_snapshots: contentFixtures.length,
    });

    const sourceAfter = await sourceFingerprint(sourcePool, sourceEnvironment);
    assert.equal(sourceAfter.fingerprint, sourceBefore.fingerprint);
    assert.equal(sourceAfter.role_default_read_only, "on");
    assert.equal(sourceAfter.candidate_write, false);
    assert.equal(sourceAfter.channel_write, false);
  } finally {
    await Promise.all([
      sourcePool.end(),
      crawlerPool.end(),
      businessPool.end(),
    ]);
  }
});
