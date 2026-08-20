import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { PostgresBusinessPublicationActivator } from "../src/businessPublicationActivator.js";
import { PostgresBusinessPublicationStore } from "../src/businessPublicationIngress.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  PublicationDeadLetterRecoveryAdministrator,
  planPublicationDeadLetterRecovery,
} from "../src/publicationDeadLetterRecovery.js";
import {
  PostgresPublicationOutboxStore,
  PublicationPublisher,
} from "../src/publicationPublisher.js";
import { publicationResultHash } from "../src/publicationResultHash.js";
import { publicationEnvelopeFromRow } from "../src/publicationTransport.js";

const { Pool } = pg;
const crawlerUrl = process.env.PUBLICATION_DEAD_LETTER_RECOVERY_CRAWLER_TEST_URL;
const businessUrl = process.env.PUBLICATION_DEAD_LETTER_RECOVERY_BUSINESS_TEST_URL;
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

function channelPayload(channelId) {
  return {
    channel_id: channelId,
    title: "Dead-letter recovery fixture",
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
    description: "Recovery integration fixture",
    subscriber_count: 1000,
    subscriber_count_status: "exact",
    total_video_count: 2,
    total_video_count_status: "exact",
    total_view_count: 10000,
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

function videoItem(channelId, contentId, position, accessStatus, membersOnly) {
  const item = {
    position,
    content_id: contentId,
    content_key: `${channelId}:${contentId}`,
    kind: "video",
    title: contentId,
    url: `https://www.youtube.com/watch?v=${contentId}`,
    thumbnail_url: null,
    published_at: "2026-08-01T00:00:00.000Z",
    published_date: "2026-08-01",
    published_at_precision: "second",
    published_at_status: "exact",
    published_at_source: "youtube_player",
    duration_seconds: 60,
    duration_status: "exact",
    duration_source: "youtube_player",
    view_count: 100,
    view_count_status: "exact",
    view_count_source: "youtube_player",
    view_count_observed_at: "2026-08-03T04:00:00.000Z",
    like_count: 10,
    like_count_status: "exact",
    like_count_source: "youtube_player",
    like_count_observed_at: "2026-08-03T04:00:00.000Z",
    comment_count: 1,
    comment_count_status: "exact",
    comment_count_source: "youtube_next",
    comment_count_observed_at: "2026-08-03T04:00:00.000Z",
    comments_disabled: false,
    description: "Recovery fixture",
    description_status: "exact",
    description_source: "youtube_player",
    hashtags: [],
    keywords: [],
    access_status: accessStatus,
    access_status_source: "youtube_player",
    is_members_only: membersOnly,
    live_scheduled_at: null,
    live_started_at: null,
    live_ended_at: null,
    extractor_version: "recovery-integration",
  };
  const businessValue = Object.fromEntries(Object.entries(item).filter(([key]) => (
    key !== "position"
    && !key.endsWith("_observed_at")
    && !key.endsWith("_source")
    && key !== "extractor_version"
  )));
  return { ...item, item_hash: observationFactsHash(businessValue) };
}

function videoCurrentPayload(channelId) {
  const items = [
    videoItem(channelId, "public-video", 1, "public", false),
    videoItem(channelId, "members-video", 2, "members_only", true),
  ];
  return {
    channel_id: channelId,
    window_policy: {
      policy_version: "video-window-v1",
      as_of: "2026-08-03T04:00:00.000Z",
      cutoff_at: "2026-05-05T04:00:00.000Z",
      cutoff_date: "2026-05-05",
      max_age_days: 90,
      max_items: 30,
    },
    window_proof: {
      complete: true,
      terminal_condition: "list_end_confirmed",
      catalog_candidate_count: 2,
      qualified_count: 2,
      selected_count: 2,
      excluded_count: 0,
      latest_scan_items: 2,
      latest_scan_pages: 1,
      latest_scan_stop_reason: "list_end",
      latest_scan_detail_failure_count: 0,
    },
    items,
  };
}

function agentPayload(channelId) {
  const fact = {
    value: "fixture",
    confidence: "high",
    evidence: ["integration fixture"],
    source_urls: ["https://www.youtube.com/"],
    reason: null,
    source: "integration-test",
  };
  const inputIds = ["members-video", "public-video"];
  return {
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
}

function bootstrapEnvelope(streamId, channelId, domain, currentPayload) {
  const resultHash = publicationResultHash(domain, currentPayload);
  const payload = domain === "video"
    ? { ...currentPayload, result_hash: resultHash }
    : currentPayload;
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: streamId,
    revision_type: "bootstrap",
    channel_id: channelId,
    domain,
    data_sequence: 1,
    previous_data_sequence: null,
    operation: domain === "video" ? "replace_window" : "replace",
    contract_version: 1,
    policy_version: domain === "video" ? "video-window-v1" : "publication-policy-v1",
    occurred_at: "2026-08-03T04:00:00.000Z",
    source_refs: { integration_test: true, domain },
    previous_result_hash: null,
    result_hash: resultHash,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

async function applySchemas(crawlerPool, businessPool) {
  const [crawlerSchema, businessSchema, activationSchema] = await Promise.all([
    readFile(new URL("../src/schema.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/businessPublicationSchema.sql", import.meta.url), "utf8"),
    readFile(new URL("../src/businessPublicationActivationSchema.sql", import.meta.url), "utf8"),
  ]);
  await crawlerPool.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await crawlerPool.query("DROP SCHEMA IF EXISTS crawler CASCADE");
  await crawlerPool.query(crawlerSchema);
  await businessPool.query("DROP SCHEMA IF EXISTS result CASCADE");
  await businessPool.query("DROP SCHEMA IF EXISTS publication CASCADE");
  await businessPool.query("DROP SCHEMA IF EXISTS public CASCADE");
  await businessPool.query("CREATE SCHEMA public");
  await businessPool.query(businessSchema);
  await businessPool.query(activationSchema);
}

async function insertSourceBootstrap(crawlerPool, envelope, currentPayload) {
  await crawlerPool.query(
    `INSERT INTO publication.revision (
       revision_id,publication_stream_id,channel_id,domain,data_sequence,
       previous_data_sequence,revision_type,operation,contract_version,policy_version,
       occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
     ) VALUES ($1,$2,$3,$4,1,NULL,'bootstrap',$5,$6,$7,$8,$9::jsonb,NULL,$10,$11,$12::jsonb)`,
    [
      envelope.revision_id,
      envelope.publication_stream_id,
      envelope.channel_id,
      envelope.domain,
      envelope.operation,
      envelope.contract_version,
      envelope.policy_version,
      envelope.occurred_at,
      JSON.stringify(envelope.source),
      envelope.result_hash,
      envelope.payload_hash,
      JSON.stringify(envelope.payload),
    ],
  );
  await crawlerPool.query(
    `INSERT INTO publication.domain_current (
       publication_stream_id,channel_id,domain,contract_version,policy_version,
       readiness_status,readiness_reasons,payload_json,result_hash,source_refs,
       complete_observed_at,data_sequence,current_revision_id
     ) VALUES ($1,$2,$3,$4,$5,'ready','[]'::jsonb,$6::jsonb,$7,$8::jsonb,$9,1,$10)`,
    [
      envelope.publication_stream_id,
      envelope.channel_id,
      envelope.domain,
      envelope.contract_version,
      envelope.policy_version,
      JSON.stringify(currentPayload),
      envelope.result_hash,
      JSON.stringify(envelope.source),
      envelope.occurred_at,
      envelope.revision_id,
    ],
  );
  await crawlerPool.query(
    "INSERT INTO publication.outbox (destination,revision_id,status) VALUES ('business',$1,'pending')",
    [envelope.revision_id],
  );
}

async function insertHistoricalContractDeadLetter(crawlerPool, businessPool, envelope) {
  const receiptId = randomUUID();
  const envelopeHash = observationFactsHash(envelope);
  const errorCode = "payload_contract_invalid";
  const errorMessage = "video payload.upserts may contain only public Content";
  const persistedAt = new Date().toISOString();
  const receipt = {
    receipt_id: receiptId,
    revision_id: envelope.revision_id,
    status: "rejected",
    persisted_at: persistedAt,
    payload_hash: envelope.payload_hash,
    error_code: errorCode,
    error_message: errorMessage,
  };
  const details = {
    code: errorCode,
    message: errorMessage,
    envelope_hash: envelopeHash,
  };
  const businessClient = await businessPool.connect();
  try {
    await businessClient.query("BEGIN");
    await businessClient.query(
      `INSERT INTO publication.inbox (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         payload_hash,envelope_hash,received_envelope,receipt_id,
         receive_status,error_code,error_message
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,'rejected',$10,$11)`,
      [
        envelope.revision_id,
        envelope.publication_stream_id,
        envelope.channel_id,
        envelope.domain,
        envelope.data_sequence,
        envelope.payload_hash,
        envelopeHash,
        JSON.stringify(envelope),
        receiptId,
        errorCode,
        errorMessage,
      ],
    );
    await businessClient.query(
      `INSERT INTO publication.quarantine (
         quarantine_id,revision_id,issue_code,issue_hash,details_json
       ) VALUES ($1,$2,$3,$4,$5::jsonb)`,
      [
        randomUUID(),
        envelope.revision_id,
        errorCode,
        observationFactsHash(details),
        JSON.stringify(details),
      ],
    );
    await businessClient.query("COMMIT");
  } catch (error) {
    await businessClient.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    businessClient.release();
  }
  const source = await crawlerPool.query(
    `UPDATE publication.outbox
     SET status='dead_letter',attempts=1,
         last_error='Business Ingress rejected: payload_contract_invalid',
         receipt_id=$2,receipt_status='rejected',receipt_received_at=$3::timestamptz,
         receipt_json=$4::jsonb,updated_at=now()
     WHERE destination='business' AND revision_id=$1 AND status='pending'
     RETURNING revision_id`,
    [envelope.revision_id, receiptId, persistedAt, JSON.stringify(receipt)],
  );
  assert.equal(source.rows.length, 1);
}

function sourceOutboxStore(pool) {
  return new PostgresPublicationOutboxStore({
    query: pool.query.bind(pool),
    withTransaction: async (action) => {
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
    },
  });
}

test("dead-letter recovery creates a clean Stream Bootstrap and atomically cuts over", {
  skip: !crawlerUrl || !businessUrl,
}, async () => {
  const crawlerPool = new Pool({ connectionString: crawlerUrl, max: 8 });
  const businessPool = new Pool({ connectionString: businessUrl, max: 8 });
  const suffix = randomUUID().replaceAll("-", "");
  const oldStreamId = randomUUID();
  const channelId = `UCrecovery${suffix}`;
  try {
    const [crawlerIdentity, businessIdentity] = await Promise.all([
      crawlerPool.query("SELECT current_database() AS database_name"),
      businessPool.query("SELECT current_database() AS database_name"),
    ]);
    assert.match(crawlerIdentity.rows[0].database_name, /_test$/i);
    assert.match(businessIdentity.rows[0].database_name, /_test$/i);
    assert.notEqual(crawlerIdentity.rows[0].database_name, businessIdentity.rows[0].database_name);
    await applySchemas(crawlerPool, businessPool);

    await crawlerPool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status,agent_status)
       VALUES ($1,$2,'Recovery fixture','active','done')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    const sourceIdentity = { database: "recovery-source", stream_role: "primary" };
    await crawlerPool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES ($1,$2,$3::jsonb,'publication-writer-v1','2026-08-01T00:00:00Z',
                 'integration-test','fixture','integration-test','fixture')`,
      [oldStreamId, `recovery-old-${suffix}`, JSON.stringify(sourceIdentity)],
    );
    const sourceOwnership = { onboarding_mode: "legacy_tracked_adoption", fixture: true };
    await crawlerPool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,seed_status,ownership_reference,
         seed_completed_at,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','complete',$3::jsonb,now(),'integration-test','fixture')`,
      [oldStreamId, channelId, JSON.stringify(sourceOwnership)],
    );
    await crawlerPool.query(
      `INSERT INTO publication.channel_delivery_state (
         destination,publication_stream_id,channel_id,mode,source_ownership_reference,
         online_at,state_changed_by,state_reason
       ) VALUES ('business',$1,$2,'online',$3::jsonb,now(),'integration-test','fixture')`,
      [oldStreamId, channelId, JSON.stringify(sourceOwnership)],
    );

    const channelCurrent = channelPayload(channelId);
    const videoCurrent = videoCurrentPayload(channelId);
    const agentCurrent = agentPayload(channelId);
    const oldEnvelopes = [
      bootstrapEnvelope(oldStreamId, channelId, "channel", channelCurrent),
      bootstrapEnvelope(oldStreamId, channelId, "video", videoCurrent),
      bootstrapEnvelope(oldStreamId, channelId, "agent", agentCurrent),
    ];
    await insertSourceBootstrap(crawlerPool, oldEnvelopes[0], channelCurrent);
    await insertSourceBootstrap(crawlerPool, oldEnvelopes[1], videoCurrent);
    await insertSourceBootstrap(crawlerPool, oldEnvelopes[2], agentCurrent);

    await businessPool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         registered_by,registered_reason,status_changed_by,status_reason
       ) VALUES ($1,$2,$3::jsonb,'integration-test','fixture','integration-test','fixture')`,
      [oldStreamId, `recovery-old-${suffix}`, JSON.stringify(sourceIdentity)],
    );
    await businessPool.query(
      `INSERT INTO publication.channel_ownership (
         channel_id,active_publication_stream_id,status,ownership_reference,
         projection_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'active',$3::jsonb,'online','integration-test','fixture')`,
      [channelId, oldStreamId, JSON.stringify(sourceOwnership)],
    );
    await insertHistoricalContractDeadLetter(
      crawlerPool,
      businessPool,
      oldEnvelopes[1],
    );

    const businessStore = new PostgresBusinessPublicationStore(businessPool);
    const publisher = new PublicationPublisher({
      store: sourceOutboxStore(crawlerPool),
      ingress: businessStore,
      destination: "business",
      leaseOwner: `recovery-publisher-${suffix}`,
      batchSize: 100,
      logger: { info() {}, error() {} },
    });
    const initialDelivery = await publisher.runOnce();
    assert.equal(initialDelivery.delivered, 2);
    assert.equal(initialDelivery.dead_lettered, 0);
    const activator = new PostgresBusinessPublicationActivator(businessPool, {
      actor: "integration-test",
      reason: "dead-letter recovery integration",
    });
    assert.equal((await activator.activateReady(channelId)).status, "activated");
    await businessPool.query(
      `UPDATE publication.projection_outbox
       SET status='delivered',delivered_at=now(),updated_at=now()
       WHERE channel_id=$1 AND status='pending'`,
      [channelId],
    );

    const evidence = await planPublicationDeadLetterRecovery({
      crawlerPool,
      businessPool,
      destination: "business",
      now: () => new Date("2026-08-03T05:00:00.000Z"),
    });
    assert.deepEqual(evidence.summary, {
      channel_count: 1,
      dead_letter_count: 1,
      bootstrap_revision_count: 3,
      removed_unpublishable_content_count: 0,
    });

    const interruptedAdministrator = new PublicationDeadLetterRecoveryAdministrator({
      crawlerPool,
      businessPool,
      activator,
      evidence,
      actor: "integration-test",
      reason: "interrupted dead-letter recovery",
      deliveryTimeoutMs: 5000,
      projectionTimeoutMs: 1000,
      pollMs: 50,
      afterSourcePrepared: async () => {
        const delivered = await publisher.runOnce();
        assert.equal(delivered.delivered, 3);
        assert.equal(delivered.dead_lettered, 0);
        const background = await activator.activateReady(channelId);
        assert.equal(background.status, "idle");
        const protectedRecovery = await businessPool.query(
          `SELECT activation_status,count(*)::int AS count
           FROM publication.revision
           WHERE publication_stream_id=$1::uuid AND channel_id=$2
           GROUP BY activation_status`,
          [evidence.recovery_stream.publication_stream_id, channelId],
        );
        assert.deepEqual(protectedRecovery.rows, [{
          activation_status: "waiting_ownership",
          count: 3,
        }]);
        await businessPool.query(
          `UPDATE publication.revision
           SET activation_status='superseded',updated_at=now()
           WHERE publication_stream_id=$1::uuid AND channel_id=$2`,
          [evidence.recovery_stream.publication_stream_id, channelId],
        );
      },
    });
    await assert.rejects(
      interruptedAdministrator.apply(),
      /timed out waiting for recovery Projection/,
    );

    const interrupted = await businessPool.query(
      `SELECT owner.status,owner.active_publication_stream_id::text AS owner_stream,
              projection.status AS projection_status
       FROM publication.channel_ownership owner
       JOIN publication.activation activation
         ON activation.channel_id=owner.channel_id
        AND activation.publication_stream_id=owner.active_publication_stream_id
       JOIN publication.projection_outbox projection USING(activation_id)
       WHERE owner.channel_id=$1`,
      [channelId],
    );
    assert.deepEqual(interrupted.rows, [{
      status: "active",
      owner_stream: evidence.recovery_stream.publication_stream_id,
      projection_status: "pending",
    }]);

    const administrator = new PublicationDeadLetterRecoveryAdministrator({
      crawlerPool,
      businessPool,
      activator,
      evidence,
      actor: "integration-test",
      reason: "resume approved dead-letter recovery",
      deliveryTimeoutMs: 5000,
      projectionTimeoutMs: 5000,
      pollMs: 50,
      afterBusinessActivated: async ({ activations }) => {
        assert.equal(activations.length, 1);
        const projected = await businessPool.query(
          `UPDATE publication.projection_outbox
           SET status='delivered',delivered_at=now(),updated_at=now()
           WHERE activation_id=ANY($1::uuid[]) AND status='pending'
           RETURNING activation_id`,
          [activations],
        );
        assert.equal(projected.rows.length, activations.length);
      },
    });
    const result = await administrator.apply();
    assert.deepEqual(result.summary, {
      recovered_channels: 1,
      resolved_dead_letters: 1,
      unresolved_dead_letters: 0,
    });
    const quarantine = await businessPool.query(
      `SELECT status,resolved_by,resolution_reason
       FROM publication.quarantine WHERE revision_id=$1::uuid`,
      [oldEnvelopes[1].revision_id],
    );
    assert.deepEqual(quarantine.rows, [{
      status: "resolved",
      resolved_by: "integration-test",
      resolution_reason: `dead-letter recovery ${evidence.evidence_hash}`,
    }]);

    const final = await businessPool.query(
      `SELECT
         (SELECT active_publication_stream_id::text FROM publication.channel_ownership
          WHERE channel_id=$1) AS owner_stream,
         (SELECT count(*)::int FROM publication.consumer_cursor
          WHERE channel_id=$1 AND publication_stream_id=$2::uuid) AS new_cursors,
         (SELECT count(*)::int FROM result.content_current
          WHERE channel_id=$1 AND window_status='active') AS active_content,
         (SELECT count(*)::int FROM result.content_current
          WHERE channel_id=$1 AND window_status='active'
            AND (payload_json->>'access_status'<>'public'
              OR coalesce((payload_json->>'is_members_only')::boolean,false))) AS non_public_content`,
      [channelId, evidence.recovery_stream.publication_stream_id],
    );
    assert.deepEqual(final.rows[0], {
      owner_stream: evidence.recovery_stream.publication_stream_id,
      new_cursors: 3,
      active_content: 2,
      non_public_content: 1,
    });
    const sourceAudit = await crawlerPool.query(
      `SELECT
         (SELECT count(*)::int FROM publication.outbox WHERE status='dead_letter') AS historical,
         (SELECT status FROM publication.channel_stream_state
          WHERE publication_stream_id=$1::uuid AND channel_id=$2) AS old_status,
         (SELECT status FROM publication.channel_stream_state
          WHERE publication_stream_id=$3::uuid AND channel_id=$2) AS new_status`,
      [oldStreamId, channelId, evidence.recovery_stream.publication_stream_id],
    );
    assert.deepEqual(sourceAudit.rows[0], {
      historical: 1,
      old_status: "sealed",
      new_status: "owned",
    });
    const resolution = await crawlerPool.query(
      `SELECT cutover_reference->>'recovery_completed' AS recovery_completed,
              cutover_reference->>'evidence_hash' AS evidence_hash
       FROM publication.channel_delivery_state
       WHERE publication_stream_id=$1::uuid AND channel_id=$2 AND destination='business'`,
      [evidence.recovery_stream.publication_stream_id, channelId],
    );
    assert.deepEqual(resolution.rows, [{
      recovery_completed: "true",
      evidence_hash: evidence.evidence_hash,
    }]);
    const replay = await new PublicationDeadLetterRecoveryAdministrator({
      crawlerPool,
      businessPool,
      activator,
      evidence,
      actor: "integration-test",
      reason: "idempotent recovery replay",
      deliveryTimeoutMs: 5000,
      projectionTimeoutMs: 5000,
      pollMs: 50,
    }).apply();
    assert.deepEqual(replay.completed, [{
      channel_id: channelId,
      status: "already_completed",
    }]);
    assert.deepEqual(replay.summary, {
      recovered_channels: 1,
      resolved_dead_letters: 1,
      unresolved_dead_letters: 0,
    });
    await assert.rejects(
      planPublicationDeadLetterRecovery({
        crawlerPool,
        businessPool,
        destination: "business",
      }),
      /no recoverable Publication dead letters were found/,
    );
  } finally {
    await Promise.all([crawlerPool.end(), businessPool.end()]);
  }
});
