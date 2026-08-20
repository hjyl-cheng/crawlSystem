import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import {
  HttpPublicationIngressAdapter,
  PublicationEnvelopeConflict,
  buildPublicationShard,
  normalizePublicationReceiptResponse,
  normalizePublicationShard,
  planPublicationShards,
  publicationEnvelopeFromRow,
  validatePublicationIngressTarget,
} from "../src/publicationTransport.js";

function revisionRow(overrides = {}) {
  const payload = overrides.payload_json ?? {
    channel_id: "UCtransport",
    title: "Publication Transport",
  };
  return {
    destination: "business",
    revision_id: randomUUID(),
    publication_stream_id: randomUUID(),
    revision_type: "bootstrap",
    channel_id: "UCtransport",
    domain: "channel",
    data_sequence: "1",
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: { observation_id: randomUUID() },
    previous_result_hash: null,
    result_hash: `sha256:${"a".repeat(64)}`,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
    attempts: 1,
    ...overrides,
  };
}

test("Revision rows become complete deterministic Publication Envelopes", () => {
  const row = revisionRow();
  const envelope = publicationEnvelopeFromRow(row);

  assert.deepEqual(envelope, {
    revision_id: row.revision_id,
    publication_stream_id: row.publication_stream_id,
    revision_type: "bootstrap",
    channel_id: "UCtransport",
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source: row.source_refs,
    previous_result_hash: null,
    result_hash: row.result_hash,
    payload_hash: row.payload_hash,
    payload: row.payload_json,
  });
});

test("Envelope construction rejects hash and Sequence corruption", () => {
  assert.throws(
    () => publicationEnvelopeFromRow(revisionRow({ payload_hash: `sha256:${"b".repeat(64)}` })),
    PublicationEnvelopeConflict,
  );
  assert.throws(
    () => publicationEnvelopeFromRow(revisionRow({
      revision_type: "incremental",
      data_sequence: 3,
      previous_data_sequence: 1,
      previous_result_hash: `sha256:${"c".repeat(64)}`,
    })),
    /previous Sequence/,
  );
});

test("Envelope transport accepts Channel Contract V2 without changing the Shard contract", () => {
  const envelope = publicationEnvelopeFromRow(revisionRow({ contract_version: 2 }));
  const shard = buildPublicationShard([envelope]);
  assert.equal(envelope.contract_version, 2);
  assert.equal(shard.contract_version, 1);
  assert.throws(
    () => publicationEnvelopeFromRow(revisionRow({ contract_version: 3 })),
    /unsupported contract_version/,
  );
});

test("Shard planning honors item and exact UTF-8 byte limits", () => {
  const envelopes = [revisionRow(), revisionRow(), revisionRow()].map(publicationEnvelopeFromRow);
  const split = planPublicationShards(envelopes, {
    maxItems: 1,
    shardId: () => randomUUID(),
    createdAt: "2026-07-27T20:00:00.000Z",
  });
  assert.equal(split.shards.length, 3);
  assert.equal(split.oversized.length, 0);

  const oversized = planPublicationShards([envelopes[0]], {
    maxBytes: 1,
    createdAt: "2026-07-27T20:00:00.000Z",
  });
  assert.equal(oversized.shards.length, 0);
  assert.equal(oversized.oversized[0].envelope.revision_id, envelopes[0].revision_id);
  assert.ok(oversized.oversized[0].bytes > 1);
});

test("Durable Receipt validation binds responses to the requested Shard", () => {
  const envelope = publicationEnvelopeFromRow(revisionRow());
  const shard = buildPublicationShard([envelope], {
    shardId: randomUUID(),
    createdAt: "2026-07-27T20:00:00.000Z",
  });
  const receipts = normalizePublicationReceiptResponse({
    shard_id: shard.shard_id,
    receipts: [{
      receipt_id: "receipt-1",
      revision_id: envelope.revision_id,
      status: "waiting_gap",
      persisted_at: "2026-07-27T20:01:00.000Z",
      payload_hash: envelope.payload_hash,
    }],
  }, shard);
  assert.equal(receipts.get(envelope.revision_id).status, "waiting_gap");

  assert.throws(() => normalizePublicationReceiptResponse({
    shard_id: randomUUID(),
    receipts: [],
  }, shard), /Shard ID/);
  assert.throws(() => normalizePublicationReceiptResponse({
    shard_id: shard.shard_id,
    receipts: [{
      receipt_id: "receipt-2",
      revision_id: envelope.revision_id,
      status: "accepted",
      persisted_at: "2026-07-27T20:01:00.000Z",
      payload_hash: `sha256:${"f".repeat(64)}`,
    }],
  }, shard), /Payload Hash/);
});

test("Business-side Shard normalization verifies the manifest and exact Envelope surface", () => {
  const envelope = publicationEnvelopeFromRow(revisionRow());
  const shard = buildPublicationShard([envelope]);
  assert.deepEqual(normalizePublicationShard(JSON.parse(JSON.stringify(shard))), shard);

  assert.throws(() => normalizePublicationShard({
    ...shard,
    manifest_hash: `sha256:${"f".repeat(64)}`,
  }), /manifest_hash/);
  assert.throws(() => normalizePublicationShard({
    ...shard,
    items: [{ ...envelope, unexpected: true }],
  }), /unsupported fields/);
});

test("remote Ingress requires HTTPS and a token while loopback remains usable", () => {
  assert.deepEqual(
    validatePublicationIngressTarget(
      "http://127.0.0.1:8081/internal/publications/v1/shards",
      null,
    ),
    {
      endpoint: "http://127.0.0.1:8081/internal/publications/v1/shards",
      token: null,
    },
  );
  assert.throws(
    () => validatePublicationIngressTarget(
      "http://business.internal/internal/publications/v1/shards",
      "token",
    ),
    /HTTPS/,
  );
  assert.throws(
    () => validatePublicationIngressTarget(
      "https://business.internal/internal/publications/v1/shards",
      null,
    ),
    /token/,
  );
});

test("explicit internal hostname may use authenticated HTTP without weakening other hosts", () => {
  assert.deepEqual(
    validatePublicationIngressTarget(
      "http://business-publication-ingress:8081/internal/publications/v1/shards",
      "internal-token",
      { trustedInternalHttpHostname: "business-publication-ingress" },
    ),
    {
      endpoint: "http://business-publication-ingress:8081/internal/publications/v1/shards",
      token: "internal-token",
    },
  );

  assert.throws(
    () => validatePublicationIngressTarget(
      "http://business-publication-ingress:8081/internal/publications/v1/shards",
      null,
      { trustedInternalHttpHostname: "business-publication-ingress" },
    ),
    /token/,
  );
  assert.throws(
    () => validatePublicationIngressTarget(
      "http://business-publication-ingress.attacker.invalid/internal/publications/v1/shards",
      "internal-token",
      { trustedInternalHttpHostname: "business-publication-ingress" },
    ),
    /HTTPS/,
  );
  assert.throws(
    () => validatePublicationIngressTarget(
      "http://business-publication-ingress:8081/internal/publications/v1/shards",
      "internal-token",
      { trustedInternalHttpHostname: "different-service" },
    ),
    /HTTPS/,
  );
});

test("HTTP Adapter sends auth and Shard idempotency without interpreting 2xx as a Receipt", async () => {
  const envelope = publicationEnvelopeFromRow(revisionRow());
  const shard = buildPublicationShard([envelope]);
  let request;
  const adapter = new HttpPublicationIngressAdapter({
    endpoint: "https://business.internal/internal/publications/v1/shards",
    token: "secret-token",
    fetchImpl: async (url, init) => {
      request = { url, init };
      return {
        ok: true,
        status: 200,
        async text() {
          return JSON.stringify({ shard_id: shard.shard_id, receipts: [] });
        },
      };
    },
  });

  const response = await adapter.acceptShard(shard);
  assert.equal(request.url, "https://business.internal/internal/publications/v1/shards");
  assert.equal(request.init.headers.authorization, "Bearer secret-token");
  assert.equal(request.init.headers["idempotency-key"], shard.shard_id);
  assert.deepEqual(response.receipts, []);
});
