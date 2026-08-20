import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import pg from "pg";
import { PostgresBusinessPublicationStore } from "../src/businessPublicationIngress.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  PostgresPublicationOutboxStore,
  PublicationPublisher,
} from "../src/publicationPublisher.js";
import {
  buildPublicationShard,
  publicationEnvelopeFromRow,
} from "../src/publicationTransport.js";

const { Pool } = pg;
const businessIntegrationUrl = process.env.PUBLICATION_BUSINESS_POSTGRES_TEST_URL;
const crawlerIntegrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

function channelPayload(channelId, title = "Business Publication Integration") {
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
    description: "Business Publication integration fixture",
    subscriber_count: 1,
    subscriber_count_status: "exact",
    total_video_count: 1,
    total_video_count_status: "exact",
    total_view_count: 1,
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

function channelEnvelope({
  streamId,
  channelId,
  revisionId = randomUUID(),
  sequence = 1,
  previousResultHash = null,
  title,
  payload: payloadOverride,
}) {
  const payload = payloadOverride ?? channelPayload(channelId, title);
  return publicationEnvelopeFromRow({
    revision_id: revisionId,
    publication_stream_id: streamId,
    revision_type: sequence === 1 ? "bootstrap" : "incremental",
    channel_id: channelId,
    domain: "channel",
    data_sequence: sequence,
    previous_data_sequence: sequence === 1 ? null : sequence - 1,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: { integration_test: true },
    previous_result_hash: sequence === 1 ? null : previousResultHash,
    result_hash: observationFactsHash(payload),
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

async function registerStream(pool, streamId, sourceKey) {
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       registered_by,registered_reason,status_changed_by,status_reason
     ) VALUES ($1,$2,'{"database":"business-integration-test"}'::jsonb,
               'integration-test','isolated test','integration-test','stream registered')`,
    [streamId, sourceKey],
  );
}

async function ownChannel(pool, streamId, channelId) {
  await pool.query(
    `INSERT INTO publication.channel_ownership (
       channel_id,active_publication_stream_id,state_changed_by,state_reason
     ) VALUES ($1,$2,'integration-test','isolated test ownership')`,
    [channelId, streamId],
  );
}

async function crawlerTransaction(pool, action) {
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

async function verifyPublisherRetry(businessPool, businessStore, suffix) {
  if (!crawlerIntegrationUrl) return;
  const crawlerPool = new Pool({ connectionString: crawlerIntegrationUrl, max: 2 });
  const streamId = randomUUID();
  const channelId = `UCbusinesspublisher${suffix}`;
  const revisionId = randomUUID();
  const destination = `business-e2e-${suffix}`;
  const payload = channelPayload(channelId, "Publisher response-loss integration");
  try {
    await registerStream(businessPool, streamId, `publisher-${suffix}`);
    await ownChannel(businessPool, streamId, channelId);
    await crawlerPool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Business Publisher Integration','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await crawlerPool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"crawler-integration-test"}'::jsonb,
         'publication-reconciler-v1',NULL,
         'integration-test','Business Publisher test','integration-test','Capture disabled'
       )`,
      [streamId, `business-publisher-${suffix}`],
    );
    await crawlerPool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','Publisher owner')`,
      [streamId, channelId],
    );
    await crawlerPool.query(
      `INSERT INTO publication.revision (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         previous_data_sequence,revision_type,operation,contract_version,policy_version,
         occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
       ) VALUES (
         $1,$2,$3,'channel',1,NULL,'bootstrap','replace',1,'publication-policy-v1',
         now(),'{"integration_test":true}'::jsonb,NULL,$4,$5,$6::jsonb
       )`,
      [
        revisionId,
        streamId,
        channelId,
        observationFactsHash(payload),
        observationFactsHash(payload),
        JSON.stringify(payload),
      ],
    );
    await crawlerPool.query(
      `INSERT INTO publication.outbox (destination,revision_id,status,next_attempt_at)
       VALUES ($1,$2,'pending',now())`,
      [destination, revisionId],
    );

    const crawlerStore = new PostgresPublicationOutboxStore({
      query: crawlerPool.query.bind(crawlerPool),
      withTransaction: (action) => crawlerTransaction(crawlerPool, action),
    });
    let ingressCalls = 0;
    const publisher = new PublicationPublisher({
      store: crawlerStore,
      ingress: {
        async acceptShard(shard) {
          const result = await businessStore.acceptShard(shard);
          ingressCalls += 1;
          if (ingressCalls === 1) throw new Error("simulated response loss after Business commit");
          return result;
        },
      },
      destination,
      leaseOwner: `publisher-${suffix}`,
      retryDelay: () => 1,
      logger: { info() {}, error() {} },
    });

    const first = await publisher.runOnce();
    assert.deepEqual(first, {
      claimed: 1,
      shards: 1,
      delivered: 0,
      retried: 1,
      dead_lettered: 0,
      lease_lost: 0,
    });
    await crawlerPool.query(
      `UPDATE publication.outbox SET next_attempt_at=now()-interval '1 second'
       WHERE destination=$1 AND revision_id=$2`,
      [destination, revisionId],
    );
    const second = await publisher.runOnce();
    assert.equal(second.delivered, 1);
    assert.equal(ingressCalls, 2);

    const crawlerState = await crawlerPool.query(
      `SELECT status,attempts,receipt_status
       FROM publication.outbox WHERE destination=$1 AND revision_id=$2`,
      [destination, revisionId],
    );
    assert.deepEqual({
      status: crawlerState.rows[0].status,
      attempts: Number(crawlerState.rows[0].attempts),
      receiptStatus: crawlerState.rows[0].receipt_status,
    }, { status: "delivered", attempts: 2, receiptStatus: "duplicate" });
    const businessState = await businessPool.query(
      `SELECT receive_count FROM publication.inbox WHERE revision_id=$1`,
      [revisionId],
    );
    assert.equal(Number(businessState.rows[0].receive_count), 2);
  } finally {
    await crawlerPool.end();
  }
}

test("Business Ingress persists idempotency, conflicts, gaps, and whole-Shard atomicity", {
  skip: !businessIntegrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: businessIntegrationUrl, max: 3 });
  const suffix = randomUUID().replaceAll("-", "");
  try {
    const identity = await pool.query("SELECT current_database() AS database_name");
    assert.match(identity.rows[0].database_name, /_test$/i, "Business integration URL must target a *_test database");
    const schema = await readFile(new URL("../src/businessPublicationSchema.sql", import.meta.url), "utf8");
    await pool.query("DROP SCHEMA IF EXISTS result CASCADE");
    await pool.query("DROP SCHEMA IF EXISTS publication CASCADE");
    await pool.query(schema);

    const store = new PostgresBusinessPublicationStore(pool);
    const streamId = randomUUID();
    await registerStream(pool, streamId, `ingress-${suffix}`);

    const acceptedChannel = `UCbusinessaccepted${suffix}`;
    await ownChannel(pool, streamId, acceptedChannel);
    const accepted = channelEnvelope({ streamId, channelId: acceptedChannel });
    const first = await store.acceptShard(buildPublicationShard([accepted]));
    assert.equal(first.receipts[0].status, "accepted");
    const repeated = await store.acceptShard(buildPublicationShard([accepted]));
    assert.equal(repeated.receipts[0].status, "duplicate");
    assert.equal(repeated.receipts[0].receipt_id, first.receipts[0].receipt_id);
    assert.equal(repeated.receipts[0].persisted_at, first.receipts[0].persisted_at);

    const changed = channelEnvelope({
      streamId,
      channelId: acceptedChannel,
      revisionId: accepted.revision_id,
      title: "Conflicting immutable Payload",
    });
    const revisionConflict = await store.acceptShard(buildPublicationShard([changed]));
    assert.equal(revisionConflict.receipts[0].status, "conflict");
    assert.equal(revisionConflict.receipts[0].error_code, "revision_envelope_conflict");

    const sequenceCollision = channelEnvelope({
      streamId,
      channelId: acceptedChannel,
      title: "Different Revision with the same Sequence",
    });
    const sequenceConflict = await store.acceptShard(buildPublicationShard([sequenceCollision]));
    assert.equal(sequenceConflict.receipts[0].status, "conflict");
    assert.equal(sequenceConflict.receipts[0].error_code, "stream_sequence_conflict");

    const gapChannel = `UCbusinessgap${suffix}`;
    await ownChannel(pool, streamId, gapChannel);
    const gap = channelEnvelope({
      streamId,
      channelId: gapChannel,
      sequence: 2,
      previousResultHash: `sha256:${"a".repeat(64)}`,
    });
    const waiting = await store.acceptShard(buildPublicationShard([gap]));
    assert.equal(waiting.receipts[0].status, "waiting_gap");
    assert.equal(waiting.receipts[0].error_code, "waiting_sequence_gap");

    const unknown = channelEnvelope({
      streamId: randomUUID(),
      channelId: `UCbusinessunknown${suffix}`,
    });
    const rejected = await store.acceptShard(buildPublicationShard([unknown]));
    assert.equal(rejected.receipts[0].status, "rejected");
    assert.equal(rejected.receipts[0].error_code, "unknown_publication_stream");

    const automaticChannelId = `UCbusinessautomatic${suffix}`;
    const automatic = channelEnvelope({
      streamId,
      channelId: automaticChannelId,
    });
    const automaticReceipt = await store.acceptShard(buildPublicationShard([automatic]));
    assert.equal(automaticReceipt.receipts[0].status, "accepted");
    const automaticOwnership = await pool.query(
      `SELECT active_publication_stream_id,status,projection_mode,ownership_reference
       FROM publication.channel_ownership WHERE channel_id=$1`,
      [automaticChannelId],
    );
    assert.equal(String(automaticOwnership.rows[0].active_publication_stream_id), streamId);
    assert.equal(automaticOwnership.rows[0].status, "active");
    assert.equal(automaticOwnership.rows[0].projection_mode, "held_shadow");
    assert.equal(
      automaticOwnership.rows[0].ownership_reference.onboarding_mode,
      "automatic_bootstrap",
    );

    const cutoverStreamId = randomUUID();
    await registerStream(pool, cutoverStreamId, `ingress-cutover-${suffix}`);
    const cutoverSeedChannelId = `UCbusinesscutoverseed${suffix}`;
    await ownChannel(pool, cutoverStreamId, cutoverSeedChannelId);
    await pool.query(
      `UPDATE publication.channel_ownership
       SET status='cutover_pending',projection_mode='held_shadow',
           state_changed_by='integration-test',state_reason='cutover is held',updated_at=now()
       WHERE channel_id=$1`,
      [cutoverSeedChannelId],
    );
    const cutoverAutomaticChannelId = `UCbusinesscutoverautomatic${suffix}`;
    const cutoverAutomatic = channelEnvelope({
      streamId: cutoverStreamId,
      channelId: cutoverAutomaticChannelId,
    });
    const cutoverReceipt = await store.acceptShard(buildPublicationShard([cutoverAutomatic]));
    assert.equal(cutoverReceipt.receipts[0].status, "accepted");
    const cutoverOwnership = await pool.query(
      `SELECT status,projection_mode
       FROM publication.channel_ownership WHERE channel_id=$1`,
      [cutoverAutomaticChannelId],
    );
    assert.deepEqual(cutoverOwnership.rows[0], {
      status: "active",
      projection_mode: "held_shadow",
    });

    const emptyStreamId = randomUUID();
    await registerStream(pool, emptyStreamId, `ingress-empty-${suffix}`);
    const unowned = channelEnvelope({
      streamId: emptyStreamId,
      channelId: `UCbusinessunowned${suffix}`,
    });
    const ownershipWait = await store.acceptShard(buildPublicationShard([unowned]));
    assert.equal(ownershipWait.receipts[0].status, "waiting_gap");
    assert.equal(ownershipWait.receipts[0].error_code, "waiting_ownership");
    const ownershipState = await pool.query(
      `SELECT inbox.receive_status,revision.ingress_status
       FROM publication.inbox AS inbox
       JOIN publication.revision AS revision USING (revision_id)
       WHERE inbox.revision_id=$1`,
      [unowned.revision_id],
    );
    assert.deepEqual(ownershipState.rows[0], {
      receive_status: "waiting_ownership",
      ingress_status: "waiting_ownership",
    });

    const invalidChannel = `UCbusinessinvalid${suffix}`;
    await ownChannel(pool, streamId, invalidChannel);
    const invalidPayload = channelPayload(invalidChannel);
    delete invalidPayload.links;
    const invalid = channelEnvelope({
      streamId,
      channelId: invalidChannel,
      payload: invalidPayload,
    });
    const invalidReceipt = await store.acceptShard(buildPublicationShard([invalid]));
    assert.equal(invalidReceipt.receipts[0].status, "rejected");
    assert.equal(invalidReceipt.receipts[0].error_code, "payload_contract_invalid");

    const rollbackFirstChannel = `UCbusinessrollbacka${suffix}`;
    const rollbackFailureChannel = `UCbusinessrollbackb${suffix}`;
    await ownChannel(pool, streamId, rollbackFirstChannel);
    await ownChannel(pool, streamId, rollbackFailureChannel);
    const rollbackFirst = channelEnvelope({ streamId, channelId: rollbackFirstChannel });
    const rollbackFailure = channelEnvelope({ streamId, channelId: rollbackFailureChannel });
    await pool.query(
      `CREATE OR REPLACE FUNCTION publication.fail_business_ingress_test()
       RETURNS TRIGGER LANGUAGE plpgsql AS $test_failure$
       BEGIN
         IF NEW.channel_id=$$${rollbackFailureChannel}$$ THEN
           RAISE EXCEPTION 'intentional Business Ingress integration failure';
         END IF;
         RETURN NEW;
       END
       $test_failure$;
       CREATE TRIGGER trg_business_ingress_test_failure
       BEFORE INSERT ON publication.inbox
       FOR EACH ROW EXECUTE FUNCTION publication.fail_business_ingress_test()` ,
    );
    try {
      await assert.rejects(
        store.acceptShard(buildPublicationShard([rollbackFirst, rollbackFailure])),
        /intentional Business Ingress integration failure/,
      );
    } finally {
      await pool.query("DROP TRIGGER IF EXISTS trg_business_ingress_test_failure ON publication.inbox");
      await pool.query("DROP FUNCTION IF EXISTS publication.fail_business_ingress_test()");
    }
    const rolledBack = await pool.query(
      "SELECT count(*)::int AS count FROM publication.inbox WHERE revision_id=ANY($1::uuid[])",
      [[rollbackFirst.revision_id, rollbackFailure.revision_id]],
    );
    assert.equal(rolledBack.rows[0].count, 0);

    await assert.rejects(
      pool.query(
        `UPDATE publication.inbox
         SET received_envelope=received_envelope || '{"tampered":true}'::jsonb
         WHERE revision_id=$1`,
        [accepted.revision_id],
      ),
      /Inbox identity and evidence are immutable/,
    );
    await assert.rejects(
      pool.query(
        `UPDATE publication.revision
         SET payload_json=payload_json || '{"tampered":true}'::jsonb
         WHERE revision_id=$1`,
        [accepted.revision_id],
      ),
      /Revision Envelope is immutable/,
    );
    const durableEvidence = await pool.query(
      `SELECT inbox.receive_count,
              (SELECT count(*)::int FROM publication.inbox_conflict
               WHERE revision_id=$1) AS conflict_count,
              (SELECT count(*)::int FROM publication.quarantine
               WHERE revision_id IN ($1,$2,$3)) AS quarantine_count
       FROM publication.inbox AS inbox WHERE inbox.revision_id=$1`,
      [accepted.revision_id, sequenceCollision.revision_id, invalid.revision_id],
    );
    assert.equal(Number(durableEvidence.rows[0].receive_count), 3);
    assert.equal(durableEvidence.rows[0].conflict_count, 1);
    assert.ok(durableEvidence.rows[0].quarantine_count >= 3);

    await verifyPublisherRetry(pool, store, suffix);
  } finally {
    await pool.end();
  }
});
