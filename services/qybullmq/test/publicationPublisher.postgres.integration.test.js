import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { PostgresPublicationOutboxStore } from "../src/publicationPublisher.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;

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

test("Publication Outbox lease and Durable Receipt transitions use PostgreSQL as authority", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  const suffix = randomUUID().replaceAll("-", "");
  const channelId = `UCpublicationpublisher${suffix}`;
  const streamId = randomUUID();
  const revisions = [
    { id: randomUUID(), domain: "channel", operation: "replace", status: "pending" },
    { id: randomUUID(), domain: "video", operation: "replace_window", status: "held" },
    { id: randomUUID(), domain: "agent", operation: "replace", status: "retry_wait" },
  ];
  try {
    await pool.query(
      `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
       VALUES ($1,$2,'Publication Publisher','active')`,
      [channelId, `https://www.youtube.com/channel/${channelId}`],
    );
    await pool.query(
      `INSERT INTO publication.stream (
         publication_stream_id,source_deployment_key,source_identity_json,
         minimum_writer_version,capture_enabled_at,created_by,created_reason,
         status_changed_by,status_reason
       ) VALUES (
         $1,$2,'{"database":"isolated-test"}'::jsonb,
         'publication-reconciler-v1',NULL,
         'integration-test','Publisher test','integration-test','Capture disabled'
       )`,
      [streamId, `publisher-${suffix}`],
    );
    await pool.query(
      `INSERT INTO publication.channel_stream_state (
         publication_stream_id,channel_id,onboarding_mode,state_changed_by,state_reason
       ) VALUES ($1,$2,'bootstrap','integration-test','Publisher owner')`,
      [streamId, channelId],
    );
    for (const revision of revisions) {
      const payload = { channel_id: channelId, domain: revision.domain };
      await pool.query(
        `INSERT INTO publication.revision (
           revision_id,publication_stream_id,channel_id,domain,data_sequence,
           previous_data_sequence,revision_type,operation,contract_version,policy_version,
           occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
         ) VALUES (
           $1,$2,$3,$4,1,NULL,'bootstrap',$5,1,'publication-policy-v1',
           now(),'{}'::jsonb,NULL,$6,$7,$8::jsonb
         )`,
        [
          revision.id,
          streamId,
          channelId,
          revision.domain,
          revision.operation,
          `sha256:${({ channel: "c", video: "d", agent: "a" })[revision.domain].repeat(64)}`,
          observationFactsHash(payload),
          JSON.stringify(payload),
        ],
      );
      await pool.query(
        `INSERT INTO publication.outbox (
           destination,revision_id,status,next_attempt_at
         ) VALUES ('business',$1,$2,
                   CASE WHEN $2='retry_wait' THEN now()-interval '1 second' ELSE now() END)`,
        [revision.id, revision.status],
      );
    }

    const store = new PostgresPublicationOutboxStore({
      query: pool.query.bind(pool),
      withTransaction: (action) => transaction(pool, action),
    });
    const claimed = await store.claimBatch({
      destination: "business",
      leaseOwner: "publisher-integration",
      batchSize: 10,
      leaseSeconds: 60,
    });
    assert.deepEqual(
      claimed.map((row) => row.domain).sort(),
      ["agent", "channel"],
    );

    const channelRevision = claimed.find((row) => row.domain === "channel");
    assert.equal(await store.markDelivered({
      destination: "business",
      revisionId: channelRevision.revision_id,
      leaseOwner: "publisher-integration",
      receipt: {
        receipt_id: `receipt-${channelRevision.revision_id}`,
        revision_id: channelRevision.revision_id,
        status: "accepted",
        persisted_at: "2026-07-27T21:00:00.000Z",
      },
    }), "delivered");

    const agentRevision = claimed.find((row) => row.domain === "agent");
    assert.equal(await store.markFailed({
      destination: "business",
      revisionId: agentRevision.revision_id,
      leaseOwner: "publisher-integration",
      deadLetter: false,
      retryDelayMs: 60000,
      error: new Error("temporary ingress outage"),
    }), "retry_wait");

    await pool.query(
      `UPDATE publication.outbox
       SET next_attempt_at=now()-interval '1 second'
       WHERE destination='business' AND revision_id=$1`,
      [agentRevision.revision_id],
    );
    const retried = await store.claimBatch({
      destination: "business",
      leaseOwner: "publisher-integration",
      batchSize: 10,
      leaseSeconds: 60,
    });
    assert.deepEqual(retried.map((row) => row.domain), ["agent"]);
    assert.equal(Number(retried[0].attempts), 2);
    assert.equal(await store.markDelivered({
      destination: "business",
      revisionId: agentRevision.revision_id,
      leaseOwner: "publisher-integration",
      receipt: {
        receipt_id: `receipt-${agentRevision.revision_id}`,
        revision_id: agentRevision.revision_id,
        status: "duplicate",
        persisted_at: "2026-07-27T21:01:00.000Z",
      },
    }), "delivered");

    const stored = await pool.query(
      `SELECT revision.domain,outbox.status,outbox.attempts,outbox.receipt_status,
              outbox.lease_owner
       FROM publication.outbox AS outbox
       JOIN publication.revision AS revision USING (revision_id)
       WHERE revision.publication_stream_id=$1
       ORDER BY revision.domain`,
      [streamId],
    );
    assert.deepEqual(stored.rows.map((item) => ({
      domain: item.domain,
      status: item.status,
      attempts: Number(item.attempts),
      receipt: item.receipt_status,
      leaseOwner: item.lease_owner,
    })), [
      { domain: "agent", status: "delivered", attempts: 2, receipt: "duplicate", leaseOwner: null },
      { domain: "channel", status: "delivered", attempts: 1, receipt: "accepted", leaseOwner: null },
      { domain: "video", status: "held", attempts: 0, receipt: null, leaseOwner: null },
    ]);
  } finally {
    await pool.end();
  }
});
