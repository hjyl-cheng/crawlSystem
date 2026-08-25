import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { createServer } from "node:http";
import test from "node:test";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  PostgresBusinessPublicationStore,
  createBusinessPublicationIngressApp,
} from "../src/businessPublicationIngress.js";
import {
  buildPublicationShard,
  publicationEnvelopeFromRow,
} from "../src/publicationTransport.js";
import {
  publicationPayloadFixture,
  publicationPolicyVersionFixture,
} from "./support/publicationPayloadFixtures.js";

const TOKEN = "business-ingress-test-token";

function envelope() {
  const payload = { channel_id: "UCbusinesshttp", title: "Business HTTP" };
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "bootstrap",
    channel_id: payload.channel_id,
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: null,
    result_hash: observationFactsHash(payload),
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

function acceptedEnvelope() {
  const channelId = "UCbusinesspointer";
  const payload = publicationPayloadFixture("channel", channelId);
  return publicationEnvelopeFromRow({
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "bootstrap",
    channel_id: channelId,
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: publicationPolicyVersionFixture("channel"),
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: { pointer_test: true },
    previous_result_hash: null,
    result_hash: observationFactsHash(payload),
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
  });
}

function ingressPool(envelopeValue, { streamExists = true } = {}) {
  const calls = [];
  const client = {
    async query(sql, parameters = []) {
      calls.push({ sql, parameters });
      if (sql.includes("business-publication-ingress:existing-inbox")) return { rows: [] };
      if (sql.includes("business-publication-ingress:stream")) {
        return { rows: streamExists ? [{ status: "active", accepted_contract_versions: [1, 2] }] : [] };
      }
      if (sql.includes("business-publication-ingress:sequence-collision")) return { rows: [] };
      if (sql.includes("business-publication-ownership:find")) {
        return { rows: [{
          channel_id: envelopeValue.channel_id,
          active_publication_stream_id: envelopeValue.publication_stream_id,
          status: "active",
          projection_mode: "online",
          ownership_reference: {},
          state_changed_at: "2026-07-27T19:00:00.000Z",
        }] };
      }
      if (sql.includes("business-publication-ingress:ownership")) {
        return { rows: [{
          active_publication_stream_id: envelopeValue.publication_stream_id,
          status: "active",
        }] };
      }
      if (sql.includes("business-publication-ingress:insert-inbox")) {
        return { rows: [{
          receipt_id: parameters[8],
          receive_status: parameters[9],
          error_code: parameters[10],
          error_message: parameters[11],
          first_received_at: "2026-07-27T20:01:00.000Z",
        }] };
      }
      return { rows: [], rowCount: 0 };
    },
    release() {},
  };
  return {
    calls,
    pool: { async connect() { return client; } },
  };
}

async function serve(app) {
  const server = createServer(app);
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      server.off("error", reject);
      resolve();
    });
  });
  const address = server.address();
  return {
    url: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolve) => server.close(resolve)),
  };
}

function request(url, shard, { token = TOKEN } = {}) {
  return fetch(`${url}/internal/publications/v1/shards`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...(token == null ? {} : { authorization: `Bearer ${token}` }),
    },
    body: JSON.stringify(shard),
  });
}

test("Business Publication HTTP Ingress authenticates and validates before durable acceptance", async () => {
  const accepted = [];
  const store = {
    async ping() {},
    async acceptShard(shard) {
      accepted.push(shard);
      return { shard_id: shard.shard_id, receipts: [] };
    },
  };
  const app = createBusinessPublicationIngressApp({ store, token: TOKEN, maximumBodyBytes: 1024 });
  const server = await serve(app);
  try {
    const shard = buildPublicationShard([envelope()]);

    for (const token of [null, "wrong-token"]) {
      const response = await request(server.url, shard, { token });
      assert.equal(response.status, 401);
    }
    assert.equal(accepted.length, 0);

    const valid = await request(server.url, shard);
    assert.equal(valid.status, 200);
    assert.deepEqual(await valid.json(), { shard_id: shard.shard_id, receipts: [] });
    assert.equal(accepted.length, 1);

    const badManifest = await request(server.url, {
      ...shard,
      manifest_hash: `sha256:${"f".repeat(64)}`,
    });
    assert.equal(badManifest.status, 400);
    assert.match((await badManifest.json()).error, /manifest_hash/);
    assert.equal(accepted.length, 1);

    const oversized = await fetch(`${server.url}/internal/publications/v1/shards`, {
      method: "POST",
      headers: {
        authorization: `Bearer ${TOKEN}`,
        "content-type": "application/json",
      },
      body: JSON.stringify({ padding: "x".repeat(2048) }),
    });
    assert.equal(oversized.status, 413);
    assert.deepEqual(await oversized.json(), { error: "request body too large" });

    const health = await fetch(`${server.url}/health`);
    assert.equal(health.status, 200);
    assert.deepEqual(await health.json(), { overall: "ok", postgres: "ok" });
  } finally {
    await server.close();
  }
});

test("Business Publication HTTP Ingress reports database failure without a false Receipt", async () => {
  const logs = [];
  const shard = buildPublicationShard([envelope()]);
  const app = createBusinessPublicationIngressApp({
    store: {
      async acceptShard() {
        throw new Error("database unavailable");
      },
    },
    token: TOKEN,
    logger: { error: (line) => logs.push(JSON.parse(line)) },
  });
  const server = await serve(app);
  try {
    const response = await request(server.url, shard);
    assert.equal(response.status, 503);
    assert.deepEqual(await response.json(), { error: "publication ingress unavailable" });
    assert.equal(logs.length, 1);
    assert.equal(logs[0].shard_id, shard.shard_id);
    assert.equal(logs[0].item_count, 1);
    assert.equal(JSON.stringify(logs[0]).includes(JSON.stringify(shard.items[0].payload)), false);
  } finally {
    await server.close();
  }
});

test("Business Ingress stores normal Inbox receipts as Revision pointers", async () => {
  const item = acceptedEnvelope();
  const fixture = ingressPool(item);
  const store = new PostgresBusinessPublicationStore(fixture.pool);
  const result = await store.acceptShard(buildPublicationShard([item]));
  assert.equal(result.receipts[0].status, "accepted");
  const inboxInsert = fixture.calls.find(({ sql }) => (
    sql.includes("business-publication-ingress:insert-inbox")
  ));
  const revisionInsert = fixture.calls.find(({ sql }) => (
    sql.includes("business-publication-ingress:insert-revision")
  ));
  assert.equal(inboxInsert.parameters[7], null);
  assert.deepEqual(JSON.parse(revisionInsert.parameters[15]), item.payload);
});

test("Business Ingress keeps the full envelope when no Revision can be stored", async () => {
  const item = acceptedEnvelope();
  const fixture = ingressPool(item, { streamExists: false });
  const store = new PostgresBusinessPublicationStore(fixture.pool);
  const result = await store.acceptShard(buildPublicationShard([item]));
  assert.equal(result.receipts[0].status, "rejected");
  const inboxInsert = fixture.calls.find(({ sql }) => (
    sql.includes("business-publication-ingress:insert-inbox")
  ));
  assert.deepEqual(JSON.parse(inboxInsert.parameters[7]), item);
  assert.ok(!fixture.calls.some(({ sql }) => (
    sql.includes("business-publication-ingress:insert-revision")
  )));
});
