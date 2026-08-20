import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import pg from "pg";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  PublicationCohortAdministrator,
  buildPublicationCohort,
} from "../src/publicationCohortAdmin.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import {
  publicationPayloadFixture,
  publicationPayloadResultHash,
  publicationPolicyVersionFixture,
} from "./support/publicationPayloadFixtures.js";

const { Pool } = pg;
const integrationUrl = process.env.PUBLICATION_POSTGRES_TEST_URL;
const RUNTIME_DIGEST = `sha256:${"b".repeat(64)}`;

function queryResult(rows = []) {
  return { rows, rowCount: rows.length };
}

class BusinessBarrierClient {
  constructor({ config, cohort, failBarrierCommit = false }) {
    this.config = config;
    this.cohort = cohort;
    this.failBarrierCommit = failBarrierCommit;
    this.barrierLocked = false;
    this.failureInjected = false;
    this.barrierRolledBack = false;
    this.events = [];
  }

  async query(statement) {
    const sql = String(statement?.text ?? statement).trim();
    if (sql.startsWith("BEGIN TRANSACTION")) return queryResult();
    if (sql.startsWith("SET LOCAL")) return queryResult();
    if (sql.startsWith("SELECT pg_advisory_xact_lock")) return queryResult([{}]);
    if (sql.startsWith("LOCK TABLE public.channels")) {
      this.barrierLocked = true;
      this.events.push("barrier_lock");
      return queryResult();
    }
    if (sql === "COMMIT") {
      if (this.barrierLocked && this.failBarrierCommit && !this.failureInjected) {
        this.failureInjected = true;
        this.events.push("barrier_commit_failed");
        throw new Error("simulated Business barrier commit failure");
      }
      this.barrierLocked = false;
      return queryResult();
    }
    if (sql === "ROLLBACK") {
      if (this.barrierLocked) this.barrierRolledBack = true;
      this.barrierLocked = false;
      return queryResult();
    }
    if (sql.includes("inet_server_addr()")) {
      return queryResult([{
        database_name: this.config.expectedBusinessDatabase,
        server_address: "127.0.0.1",
        server_port: 5432,
        channels_ready: true,
        stream_ready: true,
        ownership_ready: true,
        inbox_ready: true,
        revision_ready: true,
        activation_ready: true,
        reconciliation_ready: true,
        projection_mode_ready: true,
      }]);
    }
    if (sql.includes("SELECT count(*)::int AS count FROM public.channels")) {
      return queryResult([{ count: this.config.expectedBusinessChannelCount }]);
    }
    if (sql.startsWith("SELECT channel_id FROM public.channels")) {
      return queryResult(this.cohort.channelIds.map((channelId) => ({ channel_id: channelId })));
    }
    if (sql.includes("FROM publication.stream")) {
      return queryResult([{
        publication_stream_id: this.config.streamId,
        source_deployment_key: this.config.sourceDeploymentKey,
        source_identity_json: this.config.sourceIdentity,
        status: "active",
        accepted_contract_versions: [1],
      }]);
    }
    if (sql.includes("FROM publication.channel_ownership")) {
      return queryResult(this.cohort.channelIds.map((channelId) => ({
        channel_id: channelId,
        active_publication_stream_id: this.config.streamId,
        status: "active",
        previous_publication_stream_id: null,
        ownership_reference: this.cohort.ownershipReference,
        projection_mode: "held_shadow",
      })));
    }
    if (sql.includes("FROM result.entity_current")) {
      return queryResult([{
        inbox: 0,
        revision: 0,
        activation: 0,
        activation_item: 0,
        consumer_cursor: 0,
        projection_outbox: 0,
        reconciliation_state: 0,
        inbox_conflict: 0,
        open_quarantine: 0,
        entity_current: 0,
        video_current: 0,
        content_current: 0,
        agent_current: 0,
      }]);
    }
    if (sql.includes("FROM publication.inbox WHERE publication_stream_id")) {
      return queryResult([{
        inbox: 0,
        revision: 0,
        activation: 0,
        consumer_cursor: 0,
      }]);
    }
    throw new Error(`unexpected fake Business query: ${sql.slice(0, 120)}`);
  }
}

class SourceCommitAcknowledgementLossClient {
  constructor(client) {
    this.client = client;
    this.releaseUpdated = false;
    this.failureInjected = false;
  }

  async query(statement, values) {
    const sql = String(statement?.text ?? statement).trim();
    const result = await this.client.query(statement, values);
    if (sql.startsWith("UPDATE publication.channel_delivery_state")) {
      this.releaseUpdated = true;
    }
    if (sql === "COMMIT" && this.releaseUpdated && !this.failureInjected) {
      this.failureInjected = true;
      throw new Error("simulated lost Source COMMIT acknowledgement");
    }
    return result;
  }
}

async function createReleaseFixture(pool) {
  const suffix = randomUUID().replaceAll("-", "");
  const streamId = randomUUID();
  const channelId = `UCdeliveryrelease${suffix}`;
  const sourceIdentity = { database: "isolated-test", fixture: suffix };
  await pool.query(
    `INSERT INTO crawler.channels (channel_id,channel_url,title,status)
     VALUES ($1,$2,'Delivery Release','active')`,
    [channelId, `https://www.youtube.com/channel/${channelId}`],
  );
  const crawlerChannelCount = Number((await pool.query(
    "SELECT count(*)::int AS count FROM crawler.channels",
  )).rows[0].count);
  const config = {
    command: "release-delivery",
    expectedCrawlerDatabase: "publication_test",
    expectedBusinessDatabase: "business_test",
    expectedCrawlerChannelCount: crawlerChannelCount,
    expectedBusinessChannelCount: 1,
    streamId,
    sourceDeploymentKey: `delivery-release-${suffix}`,
    sourceIdentity,
    cohortKey: `delivery-release-${suffix}`,
    destination: "business",
    actor: "delivery-release-integration",
    reason: "controlled integration release",
    writerVersion: PUBLICATION_WRITER_VERSION,
    runtimeDeploymentRef: RUNTIME_DIGEST,
  };
  const cohort = buildPublicationCohort(config, [channelId]);
  const evidence = {
    report_version: "publication-readiness-report-v1",
    report_as_of: "2026-07-27T09:02:29.741Z",
    evidence_hash: observationFactsHash({ fixture: suffix }),
    channels: [{ channel_id: channelId, result_hashes: {} }],
  };
  await pool.query(
    `INSERT INTO publication.stream (
       publication_stream_id,source_deployment_key,source_identity_json,
       minimum_writer_version,capture_enabled_at,
       created_by,created_reason,status_changed_by,status_reason
     ) VALUES ($1,$2,$3::jsonb,$4,now(),
               'integration-test','Delivery release test',
               'integration-test','Delivery release test')`,
    [streamId, config.sourceDeploymentKey, JSON.stringify(sourceIdentity), config.writerVersion],
  );
  await pool.query(
    `INSERT INTO publication.channel_stream_state (
       publication_stream_id,channel_id,onboarding_mode,seed_status,seed_completed_at,
       ownership_reference,state_changed_by,state_reason
     ) VALUES ($1,$2,'bootstrap','complete',now(),$3::jsonb,
               'integration-test','Seed complete')`,
    [streamId, channelId, JSON.stringify(cohort.ownershipReference)],
  );
  await pool.query(
    `INSERT INTO publication.channel_delivery_state (
       destination,publication_stream_id,channel_id,mode,source_ownership_reference,
       state_changed_by,state_reason
     ) VALUES ('business',$1,$2,'hold',$3::jsonb,'integration-test','Held for release')`,
    [streamId, channelId, JSON.stringify(cohort.ownershipReference)],
  );
  for (const domain of ["channel", "video", "agent"]) {
    const revisionId = randomUUID();
    const payload = publicationPayloadFixture(domain, channelId);
    const resultHash = publicationPayloadResultHash(domain, payload);
    const policyVersion = publicationPolicyVersionFixture(domain);
    evidence.channels[0].result_hashes[domain] = resultHash;
    await pool.query(
      `INSERT INTO publication.revision (
         revision_id,publication_stream_id,channel_id,domain,data_sequence,
         previous_data_sequence,revision_type,operation,contract_version,policy_version,
         occurred_at,source_refs,previous_result_hash,result_hash,payload_hash,payload_json
       ) VALUES ($1,$2,$3,$4,1,NULL,'bootstrap',$5,1,$6,
                 now(),'{}'::jsonb,NULL,$7,$8,$9::jsonb)`,
      [
        revisionId,
        streamId,
        channelId,
        domain,
        domain === "video" ? "replace_window" : "replace",
        policyVersion,
        resultHash,
        observationFactsHash(payload),
        JSON.stringify(payload),
      ],
    );
    await pool.query(
      `INSERT INTO publication.domain_current (
         publication_stream_id,channel_id,domain,contract_version,policy_version,
         readiness_status,payload_json,result_hash,source_refs,complete_observed_at,
         data_sequence,current_revision_id
       ) VALUES ($1,$2,$3,1,$4,'ready',$5::jsonb,$6,
                 '{}'::jsonb,now(),1,$7)`,
      [
        streamId,
        channelId,
        domain,
        policyVersion,
        JSON.stringify(payload),
        resultHash,
        revisionId,
      ],
    );
    await pool.query(
      "INSERT INTO publication.outbox (destination,revision_id,status) VALUES ('business',$1,'held')",
      [revisionId],
    );
  }
  return { config, cohort, evidence };
}

async function storedReleaseState(pool, config, cohort) {
  const result = await pool.query(
    `SELECT delivery.mode,delivery.online_at,delivery.state_changed_at,
            delivery.state_changed_by,delivery.state_reason,
            array_agg(outbox.status ORDER BY revision.domain) AS outbox_statuses,
            bool_and(outbox.next_attempt_at=delivery.online_at) AS one_release_boundary
     FROM publication.channel_delivery_state AS delivery
     JOIN publication.revision AS revision
       ON revision.publication_stream_id=delivery.publication_stream_id
      AND revision.channel_id=delivery.channel_id
     JOIN publication.outbox AS outbox
       ON outbox.destination=delivery.destination
      AND outbox.revision_id=revision.revision_id
     WHERE delivery.publication_stream_id=$1 AND delivery.channel_id=$2
     GROUP BY delivery.mode,delivery.online_at,delivery.state_changed_at,
              delivery.state_changed_by,delivery.state_reason`,
    [config.streamId, cohort.channelIds[0]],
  );
  return result.rows[0];
}

async function administrator(pool, fixture, business, { loseSourceCommitAck = false } = {}) {
  const sourceConnection = await pool.connect();
  const crawlerClient = loseSourceCommitAck
    ? new SourceCommitAcknowledgementLossClient(sourceConnection)
    : sourceConnection;
  return {
    sourceConnection,
    value: new PublicationCohortAdministrator({
      crawlerClient,
      businessClient: business,
      config: fixture.config,
      cohort: fixture.cohort,
    }),
  };
}

test("Delivery release uses the Business barrier, is idempotent, and reports partial commits", {
  skip: !integrationUrl,
}, async () => {
  const pool = new Pool({ connectionString: integrationUrl, max: 2 });
  try {
    const successFixture = await createReleaseFixture(pool);
    const successBusiness = new BusinessBarrierClient(successFixture);
    const success = await administrator(pool, successFixture, successBusiness);
    try {
      const released = await success.value.releaseDelivery(successFixture.evidence);
      assert.equal(released.delivery, 1);
      assert.equal(released.outbox, 3);
      assert.match(released.releasedAt, /^20[0-9]{2}-/);
      assert.deepEqual(successBusiness.events, ["barrier_lock"]);
      const stored = await storedReleaseState(pool, successFixture.config, successFixture.cohort);
      assert.equal(stored.mode, "online");
      assert.equal(stored.state_changed_by, successFixture.config.actor);
      assert.deepEqual(stored.outbox_statuses, ["pending", "pending", "pending"]);
      assert.equal(stored.one_release_boundary, true);
      const reason = JSON.parse(stored.state_reason);
      assert.equal(reason.readiness_evidence_hash, successFixture.evidence.evidence_hash);
      assert.equal(reason.runtime_deployment_ref, RUNTIME_DIGEST);

      const repeated = await success.value.releaseDelivery(successFixture.evidence);
      assert.equal(repeated.delivery, 0);
      assert.equal(repeated.outbox, 0);
    } finally {
      success.sourceConnection.release();
    }

    const businessFailureFixture = await createReleaseFixture(pool);
    const failingBusiness = new BusinessBarrierClient({
      ...businessFailureFixture,
      failBarrierCommit: true,
    });
    const businessFailure = await administrator(pool, businessFailureFixture, failingBusiness);
    try {
      await assert.rejects(
        businessFailure.value.releaseDelivery(businessFailureFixture.evidence),
        /committed on Crawler but the Business release barrier failed/,
      );
      assert.equal(failingBusiness.barrierRolledBack, true);
      const stored = await storedReleaseState(
        pool,
        businessFailureFixture.config,
        businessFailureFixture.cohort,
      );
      assert.equal(stored.mode, "online");
      assert.deepEqual(stored.outbox_statuses, ["pending", "pending", "pending"]);
    } finally {
      businessFailure.sourceConnection.release();
    }

    const acknowledgementFixture = await createReleaseFixture(pool);
    const acknowledgementBusiness = new BusinessBarrierClient(acknowledgementFixture);
    const acknowledgementFailure = await administrator(
      pool,
      acknowledgementFixture,
      acknowledgementBusiness,
      { loseSourceCommitAck: true },
    );
    try {
      await assert.rejects(
        acknowledgementFailure.value.releaseDelivery(acknowledgementFixture.evidence),
        /may have committed on Crawler.*rerun the read-only release plan/,
      );
      assert.equal(acknowledgementBusiness.barrierRolledBack, true);
      const stored = await storedReleaseState(
        pool,
        acknowledgementFixture.config,
        acknowledgementFixture.cohort,
      );
      assert.equal(stored.mode, "online");
      assert.deepEqual(stored.outbox_statuses, ["pending", "pending", "pending"]);
    } finally {
      acknowledgementFailure.sourceConnection.release();
    }
  } finally {
    await pool.end();
  }
});
