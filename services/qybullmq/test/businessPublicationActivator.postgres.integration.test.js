import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { PostgresBusinessPublicationActivator } from "../src/businessPublicationActivator.js";
import { PostgresBusinessPublicationAuditor } from "../src/businessPublicationAuditor.js";
import { PostgresBusinessPublicationStore } from "../src/businessPublicationIngress.js";
import {
  PostgresBusinessPublicationReconciler,
} from "../src/businessPublicationReconciler.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { publicationResultHash } from "../src/publicationResultHash.js";
import {
  buildPublicationShard,
  publicationEnvelopeFromRow,
} from "../src/publicationTransport.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_BUSINESS_ACTIVATION_POSTGRES_TEST_URL;
const reconciliationIntegrationUrl = process.env.PUBLICATION_BUSINESS_RECONCILIATION_POSTGRES_TEST_URL;
const FACT_KEYS = [
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

function channelPayload(channelId, title) {
  return {
    channel_id: channelId,
    title,
    canonical_url: `https://www.youtube.com/channel/${channelId}`,
    vanity_channel_url: null,
    handle: null,
    avatar: [],
    rss_url: `https://www.youtube.com/feeds/videos.xml?channel_id=${channelId}`,
    keywords: [],
    is_family_safe: true,
    is_verified: false,
    is_verified_status: "observed_false",
    has_videos: true,
    has_shorts: false,
    has_live_streams: false,
    description: "Business Activation integration fixture",
    subscriber_count: 1,
    subscriber_count_status: "exact",
    total_video_count: 2,
    total_video_count_status: "exact",
    total_view_count: 10,
    total_view_count_status: "exact",
    joined_date: "2020-01-01",
    joined_date_status: "exact",
    joined_date_raw: "Joined Jan 1, 2020",
    country_code: "US",
    country_name: "United States",
    links: [],
    lifecycle_status: "active",
  };
}

function envelope({
  streamId,
  channelId,
  domain,
  sequence,
  payload,
  resultHash,
  previousResultHash = null,
  revisionId = randomUUID(),
}) {
  const bootstrap = sequence === 1;
  const operation = domain === "video"
    ? bootstrap ? "replace_window" : "apply_window_delta"
    : "replace";
  return publicationEnvelopeFromRow({
    revision_id: revisionId,
    publication_stream_id: streamId,
    revision_type: bootstrap ? "bootstrap" : "incremental",
    channel_id: channelId,
    domain,
    data_sequence: sequence,
    previous_data_sequence: bootstrap ? null : sequence - 1,
    operation,
    contract_version: 1,
    policy_version: domain === "video" ? "video-window-v1" : "publication-policy-v1",
    occurred_at: `2026-07-27T20:00:${String(sequence).padStart(2, "0")}.000Z`,
    source_refs: { integration_test: true },
    previous_result_hash: bootstrap ? null : previousResultHash,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

function channelRevision({ streamId, channelId, sequence, previousResultHash = null, title }) {
  const payload = channelPayload(channelId, title);
  return envelope({
    streamId,
    channelId,
    domain: "channel",
    sequence,
    previousResultHash,
    payload,
    resultHash: observationFactsHash(payload),
  });
}

function videoItem(channelId, contentId, position, title) {
  const payload = {
    content_id: contentId,
    content_key: `${channelId}:${contentId}`,
    kind: "video",
    title,
    url: `https://www.youtube.com/watch?v=${contentId}`,
    thumbnail_url: null,
    published_at: "2026-07-20T12:00:00.000Z",
    published_date: "2026-07-20",
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "youtubejs",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "youtubejs",
    view_count: 10,
    view_count_status: "exact",
    view_count_source: "youtubejs",
    view_count_observed_at: "2026-07-27T20:00:00.000Z",
    like_count: 2,
    like_count_status: "exact",
    like_count_source: "youtubejs",
    like_count_observed_at: "2026-07-27T20:00:00.000Z",
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "youtubejs",
    comment_count_observed_at: "2026-07-27T20:00:00.000Z",
    comments_disabled: false,
    description: "Video Activation fixture",
    description_status: "exact",
    description_source: "youtubejs",
    hashtags: [],
    keywords: [],
    access_status: "public",
    access_status_source: "youtubejs",
    is_members_only: false,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "integration-test",
  };
  const hashValue = Object.fromEntries(Object.entries(payload).filter(([key]) => (
    !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return { position, item_hash: observationFactsHash(hashValue), ...payload };
}

function windowPolicy() {
  return {
    policy_version: "video-window-v1",
    as_of: "2026-07-27T20:00:00.000Z",
    cutoff_at: "2026-04-28T20:00:00.000Z",
    cutoff_date: "2026-04-28",
    max_age_days: 90,
    max_items: 30,
  };
}

function windowProof(selectedCount) {
  return {
    complete: true,
    terminal_condition: "list_end_confirmed",
    catalog_candidate_count: selectedCount,
    qualified_count: selectedCount,
    selected_count: selectedCount,
    excluded_count: 0,
    latest_scan_items: selectedCount,
    latest_scan_pages: 1,
    latest_scan_stop_reason: "list_end",
    latest_scan_detail_failure_count: 0,
  };
}

function videoRevision({
  streamId,
  channelId,
  sequence,
  previousResultHash = null,
  targetItems,
  upserts = [],
  windowExits = [],
  retractions = [],
}) {
  const policy = windowPolicy();
  const resultHash = publicationResultHash("video", {
    channel_id: channelId,
    window_policy: policy,
    items: targetItems,
  });
  const payload = sequence === 1 ? {
    channel_id: channelId,
    window_policy: policy,
    window_proof: windowProof(targetItems.length),
    items: targetItems,
    result_hash: resultHash,
  } : {
    channel_id: channelId,
    window_policy: policy,
    window_proof: windowProof(targetItems.length),
    upserts,
    window_exits: windowExits,
    retractions,
    result_hash: resultHash,
  };
  return envelope({
    streamId,
    channelId,
    domain: "video",
    sequence,
    previousResultHash,
    payload,
    resultHash,
  });
}

function agentRevision(streamId, channelId) {
  const fact = {
    value: "integration-value",
    confidence: "high",
    evidence: ["integration evidence"],
    source_urls: ["https://www.youtube.com/"],
    reason: null,
    source: "integration-test",
  };
  const inputIds = ["video-activation-a", "video-activation-b"];
  const payload = {
    channel_id: channelId,
    agent_mode: "basic",
    input_url: `https://www.youtube.com/channel/${channelId}`,
    facts: Object.fromEntries(FACT_KEYS.map((key) => [key, fact])),
    agent_model: "integration-model",
    agent_config_id: 1,
    prompt_template_id: 1,
    prompt_hash: "a".repeat(64),
    prompt_variant: "with_country",
    agent_version_hash: `sha256:${"b".repeat(64)}`,
    output_hash: `sha256:${"c".repeat(64)}`,
    input_content_ids: inputIds,
    input_content_hash: observationFactsHash(inputIds),
    taxonomy_version: "qy-taxonomy-v1",
  };
  return envelope({
    streamId,
    channelId,
    domain: "agent",
    sequence: 1,
    payload,
    resultHash: observationFactsHash(payload),
  });
}

async function registerStream(pool, streamId, key) {
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       registered_by,registered_reason,status_changed_by,status_reason
     ) VALUES ($1,$2,'{"database":"activation-integration-test"}'::jsonb,
               'integration-test','isolated test','integration-test','stream registered')`,
    [streamId, key],
  );
}

async function ownChannel(
  pool,
  streamId,
  channelId,
  projectionMode = "held_shadow",
  onboardingMode = null,
) {
  await pool.query(
    `INSERT INTO publication.channel_ownership (
       channel_id,active_publication_stream_id,projection_mode,ownership_reference,
       state_changed_by,state_reason
     ) VALUES ($1,$2,$3,$4::jsonb,'integration-test','Activation ownership')`,
    [
      channelId,
      streamId,
      projectionMode,
      JSON.stringify(onboardingMode ? { onboarding_mode: onboardingMode } : {}),
    ],
  );
}

async function deliver(store, revisions) {
  return store.acceptShard(buildPublicationShard(revisions));
}

test("activateReady owns atomic Initial Package, gaps, Current, audit, and rollback", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 6 });
  const suffix = randomUUID().replaceAll("-", "");
  try {
    const identity = await pool.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Activation URL must target a *_test database");
    const [ingressSchema, activationSchema] = await Promise.all([
      readFile(new URL("../src/businessPublicationSchema.sql", import.meta.url), "utf8"),
      readFile(new URL("../src/businessPublicationActivationSchema.sql", import.meta.url), "utf8"),
    ]);
    await pool.query("DROP SCHEMA IF EXISTS result CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query(ingressSchema);
    await pool.query(activationSchema);

    const store = new PostgresBusinessPublicationStore(pool);
    const activator = new PostgresBusinessPublicationActivator(pool);
    const streamId = randomUUID();
    const channelId = `UCbusinessactivation${suffix}`;
    await registerStream(pool, streamId, `activation-${suffix}`);
    await ownChannel(pool, streamId, channelId, "held_shadow", "automatic_bootstrap");

    const channel1 = channelRevision({
      streamId,
      channelId,
      sequence: 1,
      title: "Activation Sequence 1",
    });
    const itemA = videoItem(channelId, "video-activation-a", 1, "Video A");
    const itemB = videoItem(channelId, "video-activation-b", 2, "Video B");
    const video1 = videoRevision({
      streamId,
      channelId,
      sequence: 1,
      targetItems: [itemA, itemB],
    });
    assert.equal((await deliver(store, [channel1])).receipts[0].status, "accepted");
    assert.equal((await activator.activateReady(channelId)).status, "waiting_initial_package");
    assert.equal((await deliver(store, [video1])).receipts[0].status, "accepted");
    assert.equal((await activator.activateReady(channelId)).status, "waiting_initial_package");
    const waitingState = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.consumer_cursor WHERE channel_id=$1) AS cursor_count,
         (SELECT count(*)::int FROM result.content_current
          WHERE channel_id=$1 AND window_status='active') AS active_content_count,
         (SELECT count(*)::int FROM publication.projection_outbox
          WHERE channel_id=$1) AS projection_count`,
      [channelId],
    );
    assert.deepEqual(waitingState.rows[0], {
      cursor_count: 0,
      active_content_count: 0,
      projection_count: 0,
    });

    const agent1 = agentRevision(streamId, channelId);
    assert.equal((await deliver(store, [agent1])).receipts[0].status, "accepted");
    const initialPackage = await activator.activateReady(channelId);
    assert.equal(initialPackage.status, "activated");
    assert.deepEqual(initialPackage.applied.map((item) => item.domain), ["channel", "video", "agent"]);
    assert.equal(initialPackage.projection_status, "held_shadow");
    assert.equal(initialPackage.after_version_vector.channel.sequence, 1);
    assert.equal(initialPackage.after_version_vector.video.sequence, 1);
    assert.equal(initialPackage.after_version_vector.agent.sequence, 1);
    assert.equal((await activator.activateReady(channelId)).status, "idle");
    const storedAgent = await pool.query(
      "SELECT active_sequence,is_retracted FROM result.agent_current WHERE channel_id=$1",
      [channelId],
    );
    assert.deepEqual({
      sequence: Number(storedAgent.rows[0].active_sequence),
      retracted: storedAgent.rows[0].is_retracted,
    }, { sequence: 1, retracted: false });

    const legacyChannelId = `UCbusinesslegacy${suffix}`;
    await ownChannel(
      pool,
      streamId,
      legacyChannelId,
      "held_shadow",
      "legacy_tracked_adoption",
    );
    const legacyChannel1 = channelRevision({
      streamId,
      channelId: legacyChannelId,
      sequence: 1,
      title: "Independent Legacy Channel",
    });
    assert.equal((await deliver(store, [legacyChannel1])).receipts[0].status, "accepted");
    const legacyChannelActivation = await activator.activateReady(legacyChannelId);
    assert.equal(legacyChannelActivation.status, "activated");
    assert.deepEqual(
      legacyChannelActivation.applied.map((item) => item.domain),
      ["channel"],
    );
    assert.equal(legacyChannelActivation.after_version_vector.channel.sequence, 1);
    assert.equal(legacyChannelActivation.after_version_vector.video, null);
    assert.equal(legacyChannelActivation.after_version_vector.agent, null);

    const legacyItem = videoItem(
      legacyChannelId,
      "video-legacy-independent",
      1,
      "Independent Legacy Video",
    );
    const legacyVideo1 = videoRevision({
      streamId,
      channelId: legacyChannelId,
      sequence: 1,
      targetItems: [legacyItem],
    });
    assert.equal((await deliver(store, [legacyVideo1])).receipts[0].status, "accepted");
    const legacyVideoActivation = await activator.activateReady(legacyChannelId);
    assert.equal(legacyVideoActivation.status, "activated");
    assert.deepEqual(
      legacyVideoActivation.applied.map((item) => item.domain),
      ["video"],
    );
    assert.equal(legacyVideoActivation.after_version_vector.channel.sequence, 1);
    assert.equal(legacyVideoActivation.after_version_vector.video.sequence, 1);
    assert.equal(legacyVideoActivation.after_version_vector.agent, null);

    const channel2 = channelRevision({
      streamId,
      channelId,
      sequence: 2,
      previousResultHash: channel1.result_hash,
      title: "Activation Sequence 2",
    });
    const channel3 = channelRevision({
      streamId,
      channelId,
      sequence: 3,
      previousResultHash: channel2.result_hash,
      title: "Activation Sequence 3",
    });
    assert.equal((await deliver(store, [channel3])).receipts[0].status, "waiting_gap");
    assert.equal((await activator.activateReady(channelId)).status, "waiting_gap");
    assert.equal((await deliver(store, [channel2])).receipts[0].status, "accepted");
    const gapFilled = await activator.activateReady(channelId);
    assert.deepEqual(gapFilled.applied.map((item) => item.data_sequence), [2, 3]);
    const channelRevisionStates = await pool.query(
      `SELECT data_sequence,activation_status
       FROM publication.revision
       WHERE publication_stream_id=$1 AND channel_id=$2 AND domain='channel'
       ORDER BY data_sequence`,
      [streamId, channelId],
    );
    assert.deepEqual(channelRevisionStates.rows.map((row) => ({
      sequence: Number(row.data_sequence),
      status: row.activation_status,
    })), [
      { sequence: 1, status: "superseded" },
      { sequence: 2, status: "superseded" },
      { sequence: 3, status: "active" },
    ]);

    await pool.query(
      "UPDATE publication.channel_ownership SET projection_mode='online',updated_at=now() WHERE channel_id=$1",
      [channelId],
    );
    const itemB2 = videoItem(channelId, "video-activation-b", 2, "Video B updated");
    const video2 = videoRevision({
      streamId,
      channelId,
      sequence: 2,
      previousResultHash: video1.result_hash,
      targetItems: [itemA, itemB2],
      upserts: [itemB2],
    });
    assert.equal((await deliver(store, [video2])).receipts[0].status, "accepted");
    const videoActivation = await activator.activateReady(channelId);
    assert.equal(videoActivation.projection_status, "pending");
    const videoState = await pool.query(
      `SELECT content_id,position,payload_json->>'title' AS title
       FROM result.content_current
       WHERE channel_id=$1 AND window_status='active'
       ORDER BY position`,
      [channelId],
    );
    assert.deepEqual(videoState.rows.map((row) => ({
      id: row.content_id,
      position: row.position,
      title: row.title,
    })), [
      { id: "video-activation-a", position: 1, title: "Video A" },
      { id: "video-activation-b", position: 2, title: "Video B updated" },
    ]);

    const itemB3 = videoItem(channelId, "video-activation-b", 1, "Video B updated");
    const video3 = videoRevision({
      streamId,
      channelId,
      sequence: 3,
      previousResultHash: video2.result_hash,
      targetItems: [itemB3],
      upserts: [itemB3],
      windowExits: [{ content_id: itemA.content_id, reason: "aged_out" }],
    });
    await deliver(store, [video3]);
    assert.equal((await activator.activateReady(channelId)).status, "activated");
    const video4 = videoRevision({
      streamId,
      channelId,
      sequence: 4,
      previousResultHash: video3.result_hash,
      targetItems: [],
      retractions: [{ content_id: itemB3.content_id, reason: "source_deleted" }],
    });
    await deliver(store, [video4]);
    assert.equal((await activator.activateReady(channelId)).status, "activated");
    const removedContent = await pool.query(
      `SELECT content_id,window_status,state_reason,position
       FROM result.content_current WHERE channel_id=$1 ORDER BY content_id`,
      [channelId],
    );
    assert.deepEqual(removedContent.rows, [
      {
        content_id: "video-activation-a",
        window_status: "window_exit",
        state_reason: "aged_out",
        position: null,
      },
      {
        content_id: "video-activation-b",
        window_status: "retracted",
        state_reason: "source_deleted",
        position: null,
      },
    ]);

    const channel4 = channelRevision({
      streamId,
      channelId,
      sequence: 4,
      previousResultHash: channel3.result_hash,
      title: "Activation Sequence 4",
    });
    const channel5Bad = channelRevision({
      streamId,
      channelId,
      sequence: 5,
      previousResultHash: `sha256:${"f".repeat(64)}`,
      title: "Activation Sequence 5 invalid chain",
    });
    assert.equal((await deliver(store, [channel5Bad])).receipts[0].status, "waiting_gap");
    assert.equal((await deliver(store, [channel4])).receipts[0].status, "accepted");
    const quarantineAdvance = await activator.activateReady(channelId);
    assert.deepEqual(quarantineAdvance.applied.map((item) => item.data_sequence), [4]);
    const quarantined = await pool.query(
      `SELECT validation_status,activation_status
       FROM publication.revision WHERE revision_id=$1`,
      [channel5Bad.revision_id],
    );
    assert.deepEqual(quarantined.rows[0], {
      validation_status: "quarantined",
      activation_status: "quarantined",
    });

    const otherStream = randomUUID();
    await registerStream(pool, otherStream, `activation-other-${suffix}`);
    const otherChannel = channelRevision({
      streamId: otherStream,
      channelId,
      sequence: 1,
      title: "Other Stream must not activate",
    });
    const otherVideo = videoRevision({
      streamId: otherStream,
      channelId,
      sequence: 1,
      targetItems: [itemA, itemB2],
    });
    assert.deepEqual(
      (await deliver(store, [otherChannel, otherVideo])).receipts.map((item) => item.status),
      ["waiting_gap", "waiting_gap"],
    );
    assert.equal((await activator.activateReady(channelId)).status, "idle");
    const otherStates = await pool.query(
      `SELECT DISTINCT activation_status
       FROM publication.revision WHERE publication_stream_id=$1 AND channel_id=$2`,
      [otherStream, channelId],
    );
    assert.deepEqual(otherStates.rows.map((row) => row.activation_status), ["superseded"]);

    const ownershipWaitChannel = `UCbusinessactivationownership${suffix}`;
    const ownershipWaitStream = randomUUID();
    await registerStream(pool, ownershipWaitStream, `activation-ownership-wait-${suffix}`);
    const ownershipWaitChannelRevision = channelRevision({
      streamId: ownershipWaitStream,
      channelId: ownershipWaitChannel,
      sequence: 1,
      title: "Ownership arrives after Ingress",
    });
    const ownershipWaitItem = videoItem(
      ownershipWaitChannel,
      "video-ownership-wait",
      1,
      "Ownership Wait Video",
    );
    const ownershipWaitVideoRevision = videoRevision({
      streamId: ownershipWaitStream,
      channelId: ownershipWaitChannel,
      sequence: 1,
      targetItems: [ownershipWaitItem],
    });
    assert.deepEqual(
      (await deliver(store, [ownershipWaitChannelRevision, ownershipWaitVideoRevision]))
        .receipts.map((item) => item.status),
      ["waiting_gap", "waiting_gap"],
    );
    assert.equal(
      (await activator.activateReady(ownershipWaitChannel)).status,
      "waiting_ownership",
    );
    await ownChannel(pool, ownershipWaitStream, ownershipWaitChannel);
    assert.equal((await activator.activateReady(ownershipWaitChannel)).status, "activated");
    const noChange = channelRevision({
      streamId: ownershipWaitStream,
      channelId: ownershipWaitChannel,
      sequence: 2,
      previousResultHash: ownershipWaitChannelRevision.result_hash,
      title: "Ownership arrives after Ingress",
    });
    assert.equal((await deliver(store, [noChange])).receipts[0].status, "accepted");
    assert.equal((await activator.activateReady(ownershipWaitChannel)).status, "waiting_gap");
    const noChangeState = await pool.query(
      `SELECT validation_status,activation_status
       FROM publication.revision WHERE revision_id=$1`,
      [noChange.revision_id],
    );
    assert.deepEqual(noChangeState.rows[0], {
      validation_status: "quarantined",
      activation_status: "quarantined",
    });

    const rollbackChannel = `UCbusinessactivationrollback${suffix}`;
    await ownChannel(pool, streamId, rollbackChannel);
    const rollbackChannelRevision = channelRevision({
      streamId,
      channelId: rollbackChannel,
      sequence: 1,
      title: "Activation rollback",
    });
    const rollbackItem = videoItem(rollbackChannel, "video-rollback", 1, "Rollback Video");
    const rollbackVideoRevision = videoRevision({
      streamId,
      channelId: rollbackChannel,
      sequence: 1,
      targetItems: [rollbackItem],
    });
    await deliver(store, [rollbackChannelRevision, rollbackVideoRevision]);
    await pool.query(
      `CREATE OR REPLACE FUNCTION publication.fail_activation_projection_test()
       RETURNS TRIGGER LANGUAGE plpgsql AS $activation_failure$
       BEGIN
         IF NEW.channel_id=$$${rollbackChannel}$$ THEN
           RAISE EXCEPTION 'intentional Activation projection failure';
         END IF;
         RETURN NEW;
       END
       $activation_failure$;
       CREATE TRIGGER trg_activation_projection_test_failure
       BEFORE INSERT ON publication.projection_outbox
       FOR EACH ROW EXECUTE FUNCTION publication.fail_activation_projection_test()`,
    );
    try {
      await assert.rejects(
        activator.activateReady(rollbackChannel),
        /intentional Activation projection failure/,
      );
    } finally {
      await pool.query(
        "DROP TRIGGER IF EXISTS trg_activation_projection_test_failure ON publication.projection_outbox",
      );
      await pool.query("DROP FUNCTION IF EXISTS publication.fail_activation_projection_test()");
    }
    const rollbackState = await pool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.consumer_cursor WHERE channel_id=$1) AS cursors,
         (SELECT count(*)::int FROM result.entity_current WHERE channel_id=$1) AS entity_rows,
         (SELECT count(*)::int FROM publication.activation WHERE channel_id=$1) AS activations`,
      [rollbackChannel],
    );
    assert.deepEqual(rollbackState.rows[0], { cursors: 0, entity_rows: 0, activations: 0 });

    const concurrent = await Promise.all([
      activator.activateReady(rollbackChannel),
      activator.activateReady(rollbackChannel),
    ]);
    assert.deepEqual(concurrent.map((item) => item.status).sort(), ["activated", "idle"]);
    const oneActivation = await pool.query(
      "SELECT count(*)::int AS count FROM publication.activation WHERE channel_id=$1",
      [rollbackChannel],
    );
    assert.equal(oneActivation.rows[0].count, 1);

    await pool.query(
      `UPDATE publication.channel_ownership
       SET previous_publication_stream_id=active_publication_stream_id,
           active_publication_stream_id=$2,status='active',
           state_changed_by='integration-test',state_reason='unfinalized cutover',
           state_changed_at=now(),updated_at=now()
       WHERE channel_id=$1`,
      [channelId, otherStream],
    );
    assert.equal((await activator.activateReady(channelId)).status, "cutover_required");
    const protectedCurrent = await pool.query(
      "SELECT publication_stream_id FROM result.entity_current WHERE channel_id=$1",
      [channelId],
    );
    assert.equal(String(protectedCurrent.rows[0].publication_stream_id), streamId);

    const recoveryStream = randomUUID();
    await registerStream(pool, recoveryStream, `activation-recovery-${suffix}`);
    const recoveryChannel = channelRevision({
      streamId: recoveryStream,
      channelId,
      sequence: 1,
      title: "Explicit dead-letter recovery",
    });
    const recoveryItem = videoItem(channelId, "video-recovery", 1, "Recovery Video");
    const recoveryVideo = videoRevision({
      streamId: recoveryStream,
      channelId,
      sequence: 1,
      targetItems: [recoveryItem],
    });
    const recoveryAgent = agentRevision(recoveryStream, channelId);
    assert.equal((await deliver(store, [recoveryChannel])).receipts[0].status, "waiting_gap");
    const previousCursorRows = await pool.query(
      `SELECT domain,publication_stream_id,active_sequence,active_revision_id,active_result_hash
       FROM publication.consumer_cursor WHERE channel_id=$1 ORDER BY domain`,
      [channelId],
    );
    const previousVersionVector = Object.fromEntries(["channel", "video", "agent"].map((domain) => {
      const cursor = previousCursorRows.rows.find((row) => row.domain === domain);
      return [domain, cursor ? {
        publication_stream_id: String(cursor.publication_stream_id),
        sequence: Number(cursor.active_sequence),
        revision_id: String(cursor.active_revision_id),
        result_hash: cursor.active_result_hash,
      } : null];
    }));
    await pool.query(
      `UPDATE publication.channel_ownership
       SET previous_publication_stream_id=$2,
           active_publication_stream_id=$3,status='cutover_pending',
           ownership_reference=$4::jsonb,
           state_changed_by='integration-test',state_reason='approved dead-letter recovery',
           state_changed_at=now(),updated_at=now()
       WHERE channel_id=$1`,
      [
        channelId,
        streamId,
        recoveryStream,
        JSON.stringify({
          onboarding_mode: "dead_letter_recovery_cutover",
          evidence_hash: `sha256:${"9".repeat(64)}`,
          previous_version_vector: previousVersionVector,
        }),
      ],
    );
    assert.equal((await activator.activateReady(channelId)).status, "waiting_cutover_package");
    const waitingCutoverCurrent = await pool.query(
      "SELECT publication_stream_id FROM result.entity_current WHERE channel_id=$1",
      [channelId],
    );
    assert.equal(String(waitingCutoverCurrent.rows[0].publication_stream_id), streamId);
    assert.deepEqual(
      (await deliver(store, [recoveryVideo, recoveryAgent])).receipts.map((item) => item.status),
      ["waiting_gap", "waiting_gap"],
    );
    const recoveredCutover = await activator.activateReady(channelId);
    assert.equal(recoveredCutover.status, "cutover_activated");
    assert.equal(recoveredCutover.publication_stream_id, recoveryStream);
    assert.deepEqual(
      recoveredCutover.applied.map((item) => [item.domain, item.data_sequence]).sort(),
      [["agent", 1], ["channel", 1], ["video", 1]],
    );
    const cutoverState = await pool.query(
      `SELECT
         (SELECT status FROM publication.channel_ownership WHERE channel_id=$1) AS owner_status,
         (SELECT active_publication_stream_id::text FROM publication.channel_ownership
          WHERE channel_id=$1) AS owner_stream,
         (SELECT previous_publication_stream_id::text FROM publication.channel_ownership
          WHERE channel_id=$1) AS previous_stream,
         (SELECT count(*)::int FROM publication.consumer_cursor
          WHERE channel_id=$1 AND publication_stream_id=$2::uuid AND active_sequence=1) AS cursors,
         (SELECT count(*)::int FROM publication.revision
          WHERE channel_id=$1 AND publication_stream_id=$3::uuid
            AND activation_status='active') AS old_active,
         (SELECT count(*)::int FROM publication.revision
          WHERE channel_id=$1 AND publication_stream_id=$2::uuid
            AND activation_status='active') AS new_active`,
      [channelId, recoveryStream, streamId],
    );
    assert.deepEqual(cutoverState.rows[0], {
      owner_status: "active",
      owner_stream: recoveryStream,
      previous_stream: streamId,
      cursors: 3,
      old_active: 0,
      new_active: 3,
    });

    await assert.rejects(
      pool.query(
        `UPDATE publication.consumer_cursor
         SET active_sequence=active_sequence-1
         WHERE channel_id=$1 AND domain='channel'`,
        [channelId],
      ),
      /must advance monotonically/,
    );
    await assert.rejects(
      pool.query(
        "UPDATE publication.activation SET reason='tampered' WHERE activation_id=$1",
        [initialPackage.activation_id],
      ),
      /Activation audit is immutable/,
    );
  } finally {
    await pool.end();
  }
});

test("runOnce leases, activates, recovers, and reports durable inconsistencies", {
  skip: !reconciliationIntegrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: reconciliationIntegrationUrl, max: 16 });
  const suffix = randomUUID().replaceAll("-", "");
  try {
    const identity = await pool.query("SELECT current_database() AS database_name");
    assert.match(
      identity.rows[0].database_name,
      /_test$/i,
      "Reconciliation URL must target a *_test database",
    );
    const [ingressSchema, activationSchema, reconciliationSchema] = await Promise.all([
      readFile(new URL("../src/businessPublicationSchema.sql", import.meta.url), "utf8"),
      readFile(new URL("../src/businessPublicationActivationSchema.sql", import.meta.url), "utf8"),
      readFile(new URL("../src/businessPublicationReconciliationSchema.sql", import.meta.url), "utf8"),
    ]);
    await pool.query("DROP SCHEMA IF EXISTS result CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query(ingressSchema);
    await pool.query(activationSchema);
    await pool.query(reconciliationSchema);

    await assert.rejects(
      pool.query(
        `INSERT INTO publication.reconciliation_state (channel_id,lease_expires_at)
         VALUES ('invalid-half-lease',now())`,
      ),
      /reconciliation_state.*check/i,
    );
    await assert.rejects(
      pool.query(
        `INSERT INTO publication.reconciliation_state (channel_id,last_outcome)
         VALUES ('invalid-error-state','error')`,
      ),
      /reconciliation_state.*check/i,
    );

    const store = new PostgresBusinessPublicationStore(pool);
    const streamId = randomUUID();
    const channelId = `UCbusinessreconcile${suffix}`;
    await registerStream(pool, streamId, `reconciliation-${suffix}`);
    await ownChannel(pool, streamId, channelId);
    const channel1 = channelRevision({
      streamId,
      channelId,
      sequence: 1,
      title: "Reconciliation Bootstrap",
    });
    const item1 = videoItem(channelId, "video-reconciliation-1", 1, "Reconciliation Video");
    const video1 = videoRevision({
      streamId,
      channelId,
      sequence: 1,
      targetItems: [item1],
    });
    await deliver(store, [channel1, video1]);

    const workerOptions = {
      batchSize: 10,
      concurrency: 2,
      auditIntervalSeconds: 0,
      gapAlertSeconds: 0,
      projectionStuckSeconds: 0,
      blockedRetrySeconds: 1,
      errorRetrySeconds: 1,
    };
    const firstWorker = new PostgresBusinessPublicationReconciler(pool, {
      ...workerOptions,
      workerId: `reconciliation-a-${suffix}`,
    });
    const secondWorker = new PostgresBusinessPublicationReconciler(pool, {
      ...workerOptions,
      workerId: `reconciliation-b-${suffix}`,
    });
    const concurrent = await Promise.all([firstWorker.runOnce(), secondWorker.runOnce()]);
    assert.deepEqual(concurrent.map((summary) => summary.claimed).sort(), [0, 1]);
    const activationSummary = concurrent.find((summary) => summary.claimed === 1);
    assert.equal(activationSummary.outcomes.activated, 1);
    assert.equal(activationSummary.applied_revisions, 2);
    assert.equal(activationSummary.audit.issue_count, 0);
    const firstState = await pool.query(
      `SELECT attempt_count,last_outcome,last_error,lease_owner,lease_expires_at
       FROM publication.reconciliation_state WHERE channel_id=$1`,
      [channelId],
    );
    assert.deepEqual({
      attempts: Number(firstState.rows[0].attempt_count),
      outcome: firstState.rows[0].last_outcome,
      error: firstState.rows[0].last_error,
      leaseOwner: firstState.rows[0].lease_owner,
      leaseExpiresAt: firstState.rows[0].lease_expires_at,
    }, {
      attempts: 1,
      outcome: "activated",
      error: null,
      leaseOwner: null,
      leaseExpiresAt: null,
    });
    assert.equal((await firstWorker.runOnce()).claimed, 0);

    const failureChannel = `UCbusinessreconcilefailure${suffix}`;
    await ownChannel(pool, streamId, failureChannel);
    const failureChannelRevision = channelRevision({
      streamId,
      channelId: failureChannel,
      sequence: 1,
      title: "Reconciliation Failure Recovery",
    });
    const failureItem = videoItem(
      failureChannel,
      "video-reconciliation-failure",
      1,
      "Failure Recovery Video",
    );
    const failureVideoRevision = videoRevision({
      streamId,
      channelId: failureChannel,
      sequence: 1,
      targetItems: [failureItem],
    });
    await deliver(store, [failureChannelRevision, failureVideoRevision]);
    await pool.query(
      `INSERT INTO publication.reconciliation_state (
         channel_id,lease_owner,lease_expires_at,next_attempt_at
       ) VALUES ($1,'stopped-worker',now()-interval '1 minute',now()-interval '1 minute')`,
      [failureChannel],
    );
    await pool.query(
      `CREATE OR REPLACE FUNCTION publication.fail_reconciliation_projection_test()
       RETURNS TRIGGER LANGUAGE plpgsql AS $reconciliation_failure$
       BEGIN
         IF NEW.channel_id=$$${failureChannel}$$ THEN
           RAISE EXCEPTION 'intentional Reconciliation activation failure';
         END IF;
         RETURN NEW;
       END
       $reconciliation_failure$;
       CREATE TRIGGER trg_reconciliation_projection_test_failure
       BEFORE INSERT ON publication.projection_outbox
       FOR EACH ROW EXECUTE FUNCTION publication.fail_reconciliation_projection_test()`,
    );
    const failed = await firstWorker.runOnce();
    assert.equal(failed.claimed, 1);
    assert.equal(failed.outcomes.error, 1);
    assert.match(failed.channels[0].error, /intentional Reconciliation activation failure/);
    const failedState = await pool.query(
      `SELECT attempt_count,last_outcome,consecutive_error_count,lease_owner
       FROM publication.reconciliation_state WHERE channel_id=$1`,
      [failureChannel],
    );
    assert.deepEqual({
      attempts: Number(failedState.rows[0].attempt_count),
      outcome: failedState.rows[0].last_outcome,
      errors: failedState.rows[0].consecutive_error_count,
      leaseOwner: failedState.rows[0].lease_owner,
    }, { attempts: 1, outcome: "error", errors: 1, leaseOwner: null });
    await pool.query(
      "DROP TRIGGER trg_reconciliation_projection_test_failure ON publication.projection_outbox",
    );
    await pool.query("DROP FUNCTION publication.fail_reconciliation_projection_test() ");
    await pool.query(
      "UPDATE publication.reconciliation_state SET next_attempt_at=now()-interval '1 second' WHERE channel_id=$1",
      [failureChannel],
    );
    const recovered = await firstWorker.runOnce();
    assert.equal(recovered.outcomes.activated, 1);
    const recoveredState = await pool.query(
      `SELECT last_outcome,last_error,consecutive_error_count
       FROM publication.reconciliation_state WHERE channel_id=$1`,
      [failureChannel],
    );
    assert.deepEqual(recoveredState.rows[0], {
      last_outcome: "activated",
      last_error: null,
      consecutive_error_count: 0,
    });

    await pool.query(
      "UPDATE result.entity_current SET result_hash=$2 WHERE channel_id=$1",
      [failureChannel, `sha256:${"e".repeat(64)}`],
    );
    const channel3 = channelRevision({
      streamId,
      channelId,
      sequence: 3,
      previousResultHash: `sha256:${"f".repeat(64)}`,
      title: "Reconciliation Sequence Gap",
    });
    await deliver(store, [channel3]);
    await pool.query(
      `INSERT INTO publication.quarantine (
         quarantine_id,revision_id,issue_code,issue_hash,details_json
       ) VALUES ($1,$2,'reconciliation_test_issue',$3,'{}'::jsonb)`,
      [randomUUID(), channel3.revision_id, `sha256:${"d".repeat(64)}`],
    );
    const otherStream = randomUUID();
    await registerStream(pool, otherStream, `reconciliation-cutover-${suffix}`);
    await pool.query(
      `UPDATE publication.channel_ownership
       SET previous_publication_stream_id=active_publication_stream_id,
           active_publication_stream_id=$2,state_changed_by='integration-test',
           state_reason='unfinished reconciliation cutover',state_changed_at=now(),updated_at=now()
       WHERE channel_id=$1`,
      [channelId, otherStream],
    );
    await pool.query(
      `UPDATE publication.projection_outbox
       SET status='pending',next_attempt_at=now()-interval '1 minute',updated_at=now()-interval '1 minute'
       WHERE channel_id=$1`,
      [failureChannel],
    );
    await pool.query(
      `UPDATE publication.projection_outbox
       SET status='dead_letter',last_error='integration dead letter',updated_at=now()
       WHERE channel_id=$1`,
      [channelId],
    );
    const orphanRevisionId = randomUUID();
    const orphanReceiptId = randomUUID();
    const orphanChannel = `UCbusinessreconcileorphan${suffix}`;
    const orphanHash = `sha256:${"a".repeat(64)}`;
    await pool.query(
      `INSERT INTO publication.inbox (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         payload_hash,envelope_hash,received_envelope,receipt_id,receive_status
       ) VALUES ($1,$2,$3,'channel',1,$4,$4,'{}'::jsonb,$5,'accepted')`,
      [orphanRevisionId, streamId, orphanChannel, orphanHash, orphanReceiptId],
    );
    await pool.query(
      `INSERT INTO publication.revision (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         previous_data_sequence,revision_type,operation,contract_version,policy_version,
         occurred_at,source_json,previous_result_hash,result_hash,payload_hash,
         payload_json,envelope_hash,ingress_status,activation_status
       ) VALUES (
         $1,$2,$3,'channel',1,NULL,'bootstrap','replace',1,'integration-policy',
         now(),'{}'::jsonb,NULL,$4,$4,'{}'::jsonb,$4,'accepted','active'
       )`,
      [orphanRevisionId, streamId, orphanChannel, orphanHash],
    );

    const activelyLeasedChannel = `UCbusinessreconcileleased${suffix}`;
    await ownChannel(pool, streamId, activelyLeasedChannel);
    await deliver(store, [channelRevision({
      streamId,
      channelId: activelyLeasedChannel,
      sequence: 1,
      title: "Actively Leased Reconciliation",
    })]);
    await pool.query(
      `INSERT INTO publication.reconciliation_state (
         channel_id,attempt_count,consecutive_error_count,last_outcome,last_error,
         last_attempted_at,next_attempt_at,lease_owner,lease_expires_at
       ) VALUES ($1,1,1,'error','previous transient error',now(),now(),
                 'other-active-worker',now()+interval '5 minutes')`,
      [activelyLeasedChannel],
    );

    await pool.query(
      "UPDATE publication.reconciliation_state SET next_attempt_at=now()-interval '1 second' WHERE channel_id=$1",
      [channelId],
    );
    const audited = await firstWorker.runOnce();
    assert.equal(audited.outcomes.cutover_required, 1);
    const findings = audited.audit.findings;
    assert.equal(findings.cursor_current_mismatch.count, 1);
    assert.equal(findings.cursor_revision_mismatch.count, 0);
    assert.equal(findings.ownership_cursor_mismatch.count, 2);
    assert.equal(findings.long_lived_gap.count, 1);
    assert.equal(findings.open_quarantine.count, 1);
    assert.equal(findings.old_stream_pending.count, 1);
    assert.equal(findings.active_without_activation.count, 1);
    assert.equal(findings.projection_stuck.count, 1);
    assert.equal(findings.projection_dead_letter.count, 1);
    assert.equal(findings.blocked_activation.count, 1);
    assert.equal(findings.activation_error.count, 0);
    assert.ok(audited.audit.issue_count >= 10);

    const auditSettings = [];
    const auditPlanNodes = [];
    const planCheckingPool = {
      async connect() {
        const client = await pool.connect();
        return {
          async query(sql, params) {
            const statement = String(sql);
            if (statement.includes("business-publication-auditor:")) {
              const setting = await client.query(
                `SELECT current_setting('max_parallel_workers_per_gather') AS workers,
                        current_setting('debug_parallel_query') AS debug`,
              );
              auditSettings.push(setting.rows[0]);
              const explained = await client.query(`EXPLAIN (FORMAT JSON) ${statement}`, params);
              const visit = (node) => {
                auditPlanNodes.push(node["Node Type"]);
                for (const child of node.Plans ?? []) visit(child);
              };
              visit(explained.rows[0]["QUERY PLAN"][0].Plan);
            }
            return client.query(sql, params);
          },
          release() { client.release(); },
        };
      },
    };
    const guardedAudit = await new PostgresBusinessPublicationAuditor(planCheckingPool, {
      auditIntervalSeconds: 0,
      gapAlertSeconds: 0,
      projectionStuckSeconds: 0,
    }).runIfDue();
    assert.equal(guardedAudit.status, "succeeded");
    assert.deepEqual(
      auditSettings,
      Array.from({ length: 11 }, () => ({ workers: "0", debug: "off" })),
    );
    assert.deepEqual(
      auditPlanNodes.filter((node) => ["Gather", "Gather Merge", "Parallel Hash"].includes(node)),
      [],
    );
  } finally {
    await pool.end();
  }
});
