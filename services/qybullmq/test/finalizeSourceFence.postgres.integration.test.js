import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";

import { normalizeAboutMetrics } from "../src/aboutMetrics.js";
import { lockCheckpointRepairTarget } from "../src/checkpointRepair.js";
import { finalizeDispatchRevision } from "../src/finalizePolicy.js";
import {
  finalizeDispatchStateFromSource,
  lockFinalizeCommitSource,
  readFinalizeSource,
} from "../src/finalizeSourceFence.js";
import {
  lockGenericFinalizeAgainstMigrationSystemRetry,
  lockMigrationSystemRetryFinalizeJobFence,
} from "../src/migrationSystemRetryRecovery.js";
import { recordInitialFullObservations } from "../src/initialFullObservations.js";
import { lockPublicationChannelMutation } from "../src/publicationChannelMutationLock.js";
import { crawlerRuntimeSchema } from "../src/publicationCurrentSchema.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Client } = pg;
const databaseUrl = String(
  process.env.FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL
    ?? process.env.MANAGED_JOB_TEST_DATABASE_URL
    ?? "",
).trim();
const channelId = "UCFinalizeSourceFence";
const runId = "finalize-source-fence-run";
const batchId = "finalize-source-fence-batch";

function assertDedicatedLocalTestDatabase(value) {
  const url = new URL(value);
  assert.match(decodeURIComponent(url.pathname), /test/i);
  assert.ok(["127.0.0.1", "localhost", "::1"].includes(url.hostname));
}

async function transaction(client, action) {
  await client.query("BEGIN");
  try {
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    const value = await action(client);
    await client.query("COMMIT");
    return value;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

function pendingAboutCommand(observedAt) {
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
    idempotencyKey: `about:${runId}:first-finalize`,
    channelId,
    runId,
    observedAt,
    triggerReason: "initial_full",
    scheduledAt: observedAt,
    startedAt: observedAt,
    finishedAt: observedAt,
    crawlerVersion: "finalize-source-fence-test",
    extractorVersions: { youtubejs: "integration-test" },
    about,
    current: {
      aboutDescription: "Finalize source fence test Channel.",
      descriptionStatus: "exact",
      country: "Brazil",
      joinedDateText: "Joined Jan 1, 2020",
      joinedAt: "2020-01-01",
      joinedAtPrecision: "date_only",
      externalLinks: [],
      externalLinksStatus: "observed",
      rssUrl: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
      vanityChannelUrl: "https://www.youtube.com/@finalize-source-fence",
      isFamilySafe: true,
      isVerified: false,
      isVerifiedStatus: "not_verified",
      keywordsStatus: "observed",
      availableTabsStatus: "observed",
      identity: {
        title: "Finalize Source Fence",
        handle: "@finalize-source-fence",
        avatar_url: "https://yt3.example/finalize-source-fence.jpg",
        keywords: ["finalize", "fence"],
        available_tabs: ["videos"],
        summary: "Finalize source fence test Channel.",
      },
    },
  };
}

async function waitFor(check, label, timeoutMs = 10_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await check();
    if (value) return value;
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  throw new Error(`timed out waiting for ${label}`);
}

async function initializeSchema(client) {
  await client.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await client.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  const schema = await readFile(new URL("../src/schema.sql", import.meta.url), "utf8");
  await client.query(crawlerRuntimeSchema(schema));
}

async function seedFinalizeSource(client, { recovery = false } = {}) {
  if (recovery) {
    await client.query(
      `INSERT INTO crawler.query_dispatch_batches (
         dispatch_batch_id,pipeline_cycle_id,status
       ) VALUES ($1,$1,'completed')`,
      [batchId],
    );
    await client.query(
      `INSERT INTO crawler.channel_candidates (
         candidate_id,dispatch_batch_id,pipeline_cycle_id,channel_id,channel_url,status,
         snapshot_dispatch_generation,snapshot_json,source_json,accepted_at
       ) VALUES (
         801,$1,$1,$2,$3,'accepted',2,'{}'::jsonb,
         '{"source":"legacy_results_db"}'::jsonb,now()
       )`,
      [batchId, channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await client.query(
      `INSERT INTO crawler.migration_channel_intents (
         source_id,source_database,source_database_oid,source_candidate_id,
         channel_id,source_snapshot,snapshot_sha256,target_candidate_id,
         first_dispatch_batch_id,dispatch_attempts,last_dispatch_at
       ) VALUES (
         'finalize-fence-source',current_database(),
         (SELECT oid FROM pg_database WHERE datname=current_database()),1,
         $1,'{}'::jsonb,repeat('a',64),801,$2,2,now()
       )`,
      [channelId, batchId],
    );
  }
  await client.query(
    `INSERT INTO crawler.channels (
       channel_id,channel_url,title,subscriber_count,status,ready_for_agent,agent_status
     ) VALUES ($1,$2,'Finalize Source Fence',1000,$3,true,$4)`,
    [
      channelId,
      `https://www.youtube.com/channel/${channelId}`,
      "active",
      recovery ? "done" : "pending",
    ],
  );
  await client.query(
    `INSERT INTO crawler.channel_runs (
       run_id,channel_id,candidate_id,status,crawl_mode,detail_status,
       expected_content_count,started_at,result_json
     ) VALUES ($1,$2,$3,'finalizing','full','done',1,now(),$4::jsonb)`,
    [
      runId,
      channelId,
      recovery ? 801 : null,
      JSON.stringify({ pipeline_cycle_id: batchId, dispatch_batch_id: batchId }),
    ],
  );
  await client.query(
    `UPDATE crawler.channels SET latest_run_id=$2 WHERE channel_id=$1`,
    [channelId, runId],
  );
  await client.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,content_type,type_status,type_source,
       detail_status,api_status,missing_fields,result_json,disposition
     ) VALUES (
       $1,$2,'finalize-video',1,'video','resolved','youtubejs','done','not_needed',
       '{}'::text[],'{}'::jsonb,'stored'
     )`,
    [runId, channelId],
  );
  await client.query(
    `INSERT INTO crawler.contents (
       content_key,channel_id,run_id,content_type,content_type_source,source_content_id,
       position,title,url,description,description_status,description_source,
       published_at_status,published_at_precision,duration_status,view_count_status,
       like_count_status,comment_count_status,access_status,raw_json
     ) VALUES (
       $1,$2,$3,'video','youtubejs','finalize-video',1,'Source S0',$4,
       'source-s0','exact','youtubejs','unresolved','unknown','unresolved','unresolved',
       'unresolved','unresolved','public','{}'::jsonb
     )`,
    [
      `${channelId}:video:finalize-video`,
      channelId,
      runId,
      "https://www.youtube.com/watch?v=finalize-video",
    ],
  );
  if (recovery) {
    await client.query(
      `INSERT INTO crawler.agent_profiles (
         channel_id,agent_mode,input_url,status,metrics_json,prompt_variant,
         input_content_ids,input_content_hash,taxonomy_version,agent_version_hash,attempts
       ) VALUES (
         $1,'basic',$2,'success','{}'::jsonb,'local_offline','{}'::text[],
         'sha256:' || repeat('a',64),'qy-taxonomy-v1',
         'sha256:' || repeat('b',64),1
       )`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
  }
  if (recovery) {
    const intentId = (await client.query(
      `SELECT migration_intent_id FROM crawler.migration_channel_intents
       WHERE target_candidate_id=801`,
    )).rows[0].migration_intent_id;
    await client.query(
      `INSERT INTO crawler.migration_system_retry_items (
         migration_intent_id,candidate_id,failed_dispatch_batch_id,
         failed_dispatch_generation,failed_job_id,failed_job_attempt,
         failure_code,failure_category,failure_evidence,status,
         retry_dispatch_generation,recovery_run_id,dispatched_at
       ) VALUES (
         $1,801,$2,1,'failed-channel-job',1,'LEASE_CONFLICT','lease','{}'::jsonb,
         'dispatched',2,$3,now()
       )`,
      [intentId, batchId, runId],
    );
  }
}

async function waitForAdvisoryWait(observer, applicationName) {
  return waitFor(async () => Number((await observer.query(
    `SELECT count(*)::int AS waiting
     FROM pg_stat_activity
     WHERE application_name=$1 AND wait_event_type='Lock' AND wait_event='advisory'`,
    [applicationName],
  )).rows[0].waiting) === 1, `${applicationName} advisory wait`);
}

test("generic F_old cannot overwrite S1 after F_new commits first", {
  skip: databaseUrl ? false : "FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, application_name: "finalize-fence-setup" });
  const blocker = new Client({ connectionString: databaseUrl, application_name: "finalize-fence-new" });
  const oldFinalize = new Client({ connectionString: databaseUrl, application_name: "finalize-fence-old" });
  const observer = new Client({ connectionString: databaseUrl, application_name: "finalize-fence-observer" });
  t.after(async () => Promise.all([setup, blocker, oldFinalize, observer]
    .map((client) => client.end().catch(() => {}))));
  await Promise.all([setup.connect(), blocker.connect(), oldFinalize.connect(), observer.connect()]);
  await initializeSchema(setup);
  await seedFinalizeSource(setup);
  const oldSource = await readFinalizeSource(setup.query.bind(setup), { channelId, runId });

  await blocker.query("BEGIN");
  await lockPublicationChannelMutation(blocker, channelId);
  const oldResultPromise = transaction(oldFinalize, (client) => lockFinalizeCommitSource(client, {
    channelId,
    runId,
    expectedSourceRevision: oldSource.sourceRevision,
    expectedDispatchRevision: oldSource.dispatchRevision,
    transactionGuard: (activeClient) => lockGenericFinalizeAgainstMigrationSystemRetry(
      activeClient,
      { channelId, runId },
    ),
  }));
  await waitForAdvisoryWait(observer, "finalize-fence-old");

  await blocker.query(
    `UPDATE crawler.contents
     SET description='source-s1',last_enriched_at=now()
     WHERE channel_id=$1 AND run_id=$2`,
    [channelId, runId],
  );
  const newSource = await readFinalizeSource(blocker.query.bind(blocker), { channelId, runId });
  await blocker.query(
    "SELECT set_config('publication.writer_version',$1,true)",
    [PUBLICATION_WRITER_VERSION],
  );
  await blocker.query(
    `INSERT INTO crawler.finalized_profiles (
       channel_id,run_id,status,profile_json,quality_json,updated_at
     ) VALUES ($1,$2,'pending_agent',$3::jsonb,$4::jsonb,now())`,
    [
      channelId,
      runId,
      JSON.stringify({ marker: "F_new", description: "source-s1" }),
      JSON.stringify({ source_revision: newSource.sourceRevision }),
    ],
  );
  await blocker.query("COMMIT");

  const oldResult = await oldResultPromise;
  assert.equal(oldResult.accepted, false);
  assert.equal(oldResult.reason, "source_revision_stale");
  const retained = (await observer.query(
    `SELECT profile_json,quality_json FROM crawler.finalized_profiles WHERE channel_id=$1`,
    [channelId],
  )).rows[0];
  assert.equal(retained.profile_json.marker, "F_new");
  assert.equal(retained.quality_json.source_revision, newSource.sourceRevision);
});

test("recovery Finalize rejects a final_repair change committed while it waits for Publication", {
  skip: databaseUrl ? false : "FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, application_name: "finalize-recovery-setup" });
  const blocker = new Client({ connectionString: databaseUrl, application_name: "finalize-recovery-new" });
  const recovery = new Client({ connectionString: databaseUrl, application_name: "finalize-recovery-old" });
  const observer = new Client({ connectionString: databaseUrl, application_name: "finalize-recovery-observer" });
  t.after(async () => Promise.all([setup, blocker, recovery, observer]
    .map((client) => client.end().catch(() => {}))));
  await Promise.all([setup.connect(), blocker.connect(), recovery.connect(), observer.connect()]);
  await initializeSchema(setup);
  await seedFinalizeSource(setup, { recovery: true });
  const source = await readFinalizeSource(setup.query.bind(setup), { channelId, runId });
  const systemRetryId = Number((await setup.query(
    "SELECT system_retry_id FROM crawler.migration_system_retry_items",
  )).rows[0].system_retry_id);
  const fence = {
    systemRetryId,
    candidateId: 801,
    dispatchGeneration: 2,
    dispatchBatchId: batchId,
    runId,
    channelId,
    sourceRevision: finalizeDispatchRevision(finalizeDispatchStateFromSource(source)),
  };

  await blocker.query("BEGIN");
  await lockPublicationChannelMutation(blocker, channelId);
  const recoveryResultPromise = transaction(recovery, (client) => lockFinalizeCommitSource(client, {
    channelId,
    runId,
    expectedSourceRevision: source.sourceRevision,
    expectedDispatchRevision: source.dispatchRevision,
    transactionGuard: (activeClient) => lockMigrationSystemRetryFinalizeJobFence(
      activeClient,
      fence,
    ),
  }));
  await waitForAdvisoryWait(observer, "finalize-recovery-old");
  await blocker.query(
    `UPDATE crawler.channel_runs
     SET result_json=jsonb_set(
       result_json,'{final_repair}',
       '{"parent_run_id":"older-run","rounds":2,"mode":"channel"}'::jsonb,true
     ),updated_at=now()
     WHERE run_id=$1`,
    [runId],
  );
  await blocker.query("COMMIT");

  const recoveryResult = await recoveryResultPromise;
  assert.equal(recoveryResult.accepted, false);
  assert.equal(recoveryResult.reason, "transaction_guard_rejected");
  assert.equal((await observer.query(
    "SELECT count(*)::int AS count FROM crawler.finalized_profiles",
  )).rows[0].count, 0);
});

test("Finalize source locks prevent a new FK child from entering the committed snapshot", {
  skip: databaseUrl ? false : "FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, application_name: "finalize-fk-setup" });
  const finalize = new Client({ connectionString: databaseUrl, application_name: "finalize-fk-holder" });
  const inserter = new Client({ connectionString: databaseUrl, application_name: "finalize-fk-inserter" });
  const observer = new Client({ connectionString: databaseUrl, application_name: "finalize-fk-observer" });
  t.after(async () => Promise.all([setup, finalize, inserter, observer]
    .map((client) => client.end().catch(() => {}))));
  await Promise.all([setup.connect(), finalize.connect(), inserter.connect(), observer.connect()]);
  await initializeSchema(setup);
  await seedFinalizeSource(setup);
  const source = await readFinalizeSource(setup.query.bind(setup), { channelId, runId });

  await finalize.query("BEGIN");
  const locked = await lockFinalizeCommitSource(finalize, {
    channelId,
    runId,
    expectedSourceRevision: source.sourceRevision,
    expectedDispatchRevision: source.dispatchRevision,
    transactionGuard: (activeClient) => lockGenericFinalizeAgainstMigrationSystemRetry(
      activeClient,
      { channelId, runId },
    ),
  });
  assert.equal(locked.accepted, true);
  const insertPromise = inserter.query(
    `INSERT INTO crawler.content_candidates (
       run_id,channel_id,source_content_id,position,detail_status,api_status,result_json
     ) VALUES ($1,$2,'late-finalize-video',2,'done','not_needed','{}'::jsonb)`,
    [runId, channelId],
  );
  await waitFor(async () => Number((await observer.query(
    `SELECT count(*)::int AS waiting
     FROM pg_stat_activity
     WHERE application_name='finalize-fk-inserter' AND wait_event_type='Lock'`,
  )).rows[0].waiting) === 1, "late Finalize source insert to wait on FK parent lock");
  await finalize.query("ROLLBACK");
  await insertPromise;
  assert.equal((await observer.query(
    "SELECT count(*)::int AS count FROM crawler.content_candidates WHERE run_id=$1",
    [runId],
  )).rows[0].count, 2);
});

test("Checkpoint Repair takes the parent Fence before child writes and cannot deadlock Finalize", {
  skip: databaseUrl ? false : "FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, application_name: "checkpoint-finalize-setup" });
  const checkpoint = new Client({ connectionString: databaseUrl, application_name: "checkpoint-finalize-writer" });
  const finalize = new Client({ connectionString: databaseUrl, application_name: "checkpoint-finalize-reader" });
  const observer = new Client({ connectionString: databaseUrl, application_name: "checkpoint-finalize-observer" });
  t.after(async () => Promise.all([setup, checkpoint, finalize, observer]
    .map((client) => client.end().catch(() => {}))));
  await Promise.all([setup.connect(), checkpoint.connect(), finalize.connect(), observer.connect()]);
  await initializeSchema(setup);
  await seedFinalizeSource(setup);
  const source = await readFinalizeSource(setup.query.bind(setup), { channelId, runId });

  await checkpoint.query("BEGIN");
  assert.equal(await lockCheckpointRepairTarget(checkpoint, {
    targetRunId: runId,
    channelId,
  }), true);
  await checkpoint.query(
    `UPDATE crawler.content_candidates
     SET detail_status='queued',updated_at=now()
     WHERE run_id=$1`,
    [runId],
  );

  const finalizePromise = transaction(finalize, (client) => lockFinalizeCommitSource(client, {
    channelId,
    runId,
    expectedSourceRevision: source.sourceRevision,
    expectedDispatchRevision: source.dispatchRevision,
    transactionGuard: (activeClient) => lockGenericFinalizeAgainstMigrationSystemRetry(
      activeClient,
      { channelId, runId },
    ),
  }));
  await waitFor(async () => Number((await observer.query(
    `SELECT count(*)::int AS waiting
     FROM pg_stat_activity
     WHERE application_name='checkpoint-finalize-reader' AND wait_event_type='Lock'`,
  )).rows[0].waiting) === 1, "Finalize to wait on the Checkpoint Repair parent Fence");

  await checkpoint.query(
    `DELETE FROM crawler.contents
     WHERE channel_id=$1 AND run_id=$2`,
    [channelId, runId],
  );
  await checkpoint.query("COMMIT");
  const finalized = await finalizePromise;
  assert.equal(finalized.accepted, false);
  assert.equal(finalized.reason, "source_revision_stale");
});

test("the first pending About observation does not invalidate its own Finalize dispatch", {
  skip: databaseUrl ? false : "FINALIZE_SOURCE_FENCE_TEST_DATABASE_URL is not configured",
  timeout: 30_000,
}, async (t) => {
  assertDedicatedLocalTestDatabase(databaseUrl);
  const setup = new Client({ connectionString: databaseUrl, application_name: "finalize-about-setup" });
  const executor = new Client({ connectionString: databaseUrl, application_name: "finalize-about-executor" });
  t.after(async () => Promise.all([setup, executor]
    .map((client) => client.end().catch(() => {}))));
  await Promise.all([setup.connect(), executor.connect()]);
  await initializeSchema(setup);
  await seedFinalizeSource(setup, { recovery: true });
  const observedAt = "2026-08-31T12:00:00.000Z";
  await setup.query(
    `UPDATE crawler.channel_runs
     SET result_json=result_json || jsonb_build_object(
       'pending_initial_about_observation',$2::jsonb
     ),updated_at=now()
     WHERE run_id=$1`,
    [runId, JSON.stringify(pendingAboutCommand(observedAt))],
  );
  const queuedSource = await readFinalizeSource(setup.query.bind(setup), { channelId, runId });
  const systemRetryId = Number((await setup.query(
    "SELECT system_retry_id FROM crawler.migration_system_retry_items",
  )).rows[0].system_retry_id);
  const fence = {
    systemRetryId,
    candidateId: 801,
    dispatchGeneration: 2,
    dispatchBatchId: batchId,
    runId,
    channelId,
    sourceRevision: queuedSource.dispatchRevision,
  };

  const observations = await recordInitialFullObservations({
    withTransaction: (action) => transaction(executor, action),
    channelId,
    runId,
    observedAt,
    transactionGuard: (client) => lockMigrationSystemRetryFinalizeJobFence(client, fence),
  });
  assert.equal(observations.fenceRejected, undefined);
  assert.equal(observations.recorded, true);
  assert.equal((await setup.query(
    `SELECT result_json ? 'pending_initial_about_observation' AS pending
     FROM crawler.channel_runs WHERE run_id=$1`,
    [runId],
  )).rows[0].pending, false);

  const postObservationSource = await readFinalizeSource(
    setup.query.bind(setup),
    { channelId, runId },
  );
  assert.notEqual(postObservationSource.sourceRevision, queuedSource.sourceRevision);
  assert.equal(postObservationSource.dispatchRevision, queuedSource.dispatchRevision);
  const accepted = await transaction(executor, (client) => lockFinalizeCommitSource(client, {
    channelId,
    runId,
    expectedSourceRevision: postObservationSource.sourceRevision,
    expectedDispatchRevision: queuedSource.dispatchRevision,
    transactionGuard: (activeClient) => lockMigrationSystemRetryFinalizeJobFence(
      activeClient,
      fence,
    ),
  }));
  assert.equal(accepted.accepted, true);
});
