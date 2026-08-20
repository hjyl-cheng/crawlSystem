import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { PublicationPublisher, publicationRetryDelayMs } from "../src/publicationPublisher.js";

function row(attempts = 1, overrides = {}) {
  const revisionId = overrides.revision_id ?? randomUUID();
  const payload = overrides.payload_json ?? {
    channel_id: "UCpublisher",
    title: `Revision ${revisionId}`,
  };
  return {
    destination: "business",
    revision_id: revisionId,
    publication_stream_id: randomUUID(),
    revision_type: "bootstrap",
    channel_id: "UCpublisher",
    domain: "channel",
    data_sequence: 1,
    previous_data_sequence: null,
    operation: "replace",
    contract_version: 1,
    policy_version: "publication-policy-v1",
    occurred_at: "2026-07-27T20:00:00.000Z",
    source_refs: {},
    previous_result_hash: null,
    result_hash: `sha256:${"a".repeat(64)}`,
    payload_hash: observationFactsHash(payload),
    payload_json: payload,
    attempts,
    ...overrides,
  };
}

function storeFixture(rows) {
  const calls = { delivered: [], rejected: [], failed: [] };
  return {
    calls,
    async claimBatch() { return rows; },
    async markDelivered(value) { calls.delivered.push(value); return "delivered"; },
    async markRejected(value) { calls.rejected.push(value); return "dead_letter"; },
    async markFailed(value) {
      calls.failed.push(value);
      return value.deadLetter ? "dead_letter" : "retry_wait";
    },
  };
}

function receipt(shard, item, status) {
  return {
    receipt_id: `receipt-${item.revision_id}`,
    revision_id: item.revision_id,
    status,
    persisted_at: "2026-07-27T20:01:00.000Z",
    payload_hash: item.payload_hash,
    ...(status === "rejected" || status === "conflict"
      ? { error_code: `business_${status}` }
      : {}),
  };
}

test("Publisher settles each row only from its Durable Receipt", async () => {
  const rows = [row(), row(), row()];
  const store = storeFixture(rows);
  const ingress = {
    async acceptShard(shard) {
      return {
        shard_id: shard.shard_id,
        receipts: [
          receipt(shard, shard.items[0], "accepted"),
          receipt(shard, shard.items[1], "conflict"),
        ],
      };
    },
  };
  const publisher = new PublicationPublisher({
    store,
    ingress,
    leaseOwner: "publisher-1",
    retryDelay: () => 1234,
    logger: {},
  });

  assert.deepEqual(await publisher.runOnce(), {
    claimed: 3,
    shards: 1,
    delivered: 1,
    retried: 1,
    dead_lettered: 1,
    lease_lost: 0,
  });
  assert.equal(store.calls.delivered[0].receipt.status, "accepted");
  assert.equal(store.calls.rejected[0].receipt.status, "conflict");
  assert.equal(store.calls.failed[0].retryDelayMs, 1234);
});

test("accepted, duplicate, and waiting_gap are all durable delivery outcomes", async () => {
  const rows = [row(), row(), row()];
  const statuses = ["accepted", "duplicate", "waiting_gap"];
  const store = storeFixture(rows);
  const publisher = new PublicationPublisher({
    store,
    ingress: {
      async acceptShard(shard) {
        return {
          shard_id: shard.shard_id,
          receipts: shard.items.map((item, index) => receipt(shard, item, statuses[index])),
        };
      },
    },
    leaseOwner: "publisher-1",
    logger: {},
  });

  const summary = await publisher.runOnce();
  assert.equal(summary.delivered, 3);
  assert.deepEqual(store.calls.delivered.map((call) => call.receipt.status), statuses);
});

test("network or malformed Receipt responses retry and dead-letter only at the attempt cap", async () => {
  const retryStore = storeFixture([row(2)]);
  const retryPublisher = new PublicationPublisher({
    store: retryStore,
    ingress: { async acceptShard() { throw new Error("business unavailable"); } },
    leaseOwner: "publisher-1",
    maxAttempts: 3,
    logger: {},
  });
  assert.equal((await retryPublisher.runOnce()).retried, 1);
  assert.equal(retryStore.calls.failed[0].deadLetter, false);

  const deadStore = storeFixture([row(3)]);
  const deadPublisher = new PublicationPublisher({
    store: deadStore,
    ingress: { async acceptShard() { return { shard_id: "wrong", receipts: [] }; } },
    leaseOwner: "publisher-1",
    maxAttempts: 3,
    logger: {},
  });
  assert.equal((await deadPublisher.runOnce()).dead_lettered, 1);
  assert.equal(deadStore.calls.failed[0].deadLetter, true);
});

test("a corrupt or oversized local Envelope is dead-lettered without contacting Ingress", async () => {
  const corrupt = row(1, { payload_hash: `sha256:${"f".repeat(64)}` });
  const store = storeFixture([corrupt]);
  let contacted = false;
  const publisher = new PublicationPublisher({
    store,
    ingress: { async acceptShard() { contacted = true; } },
    leaseOwner: "publisher-1",
    logger: {},
  });
  const result = await publisher.runOnce();
  assert.equal(result.dead_lettered, 1);
  assert.equal(result.shards, 0);
  assert.equal(contacted, false);
  assert.equal(store.calls.failed[0].deadLetter, true);

  const largePayload = { channel_id: "UCpublisher", title: "x".repeat(10000) };
  const largeStore = storeFixture([row(1, {
    payload_json: largePayload,
    payload_hash: observationFactsHash(largePayload),
  })]);
  const large = new PublicationPublisher({
    store: largeStore,
    ingress: { async acceptShard() { contacted = true; } },
    leaseOwner: "publisher-1",
    maximumShardBytes: 1000,
    logger: {},
  });
  assert.equal((await large.runOnce()).dead_lettered, 1);
});

test("Publication retry delay is bounded and deterministic", () => {
  assert.equal(publicationRetryDelayMs(1, { baseMs: 1000, random: () => 0.5 }), 1000);
  assert.equal(publicationRetryDelayMs(4, { baseMs: 1000, random: () => 0.5 }), 8000);
  assert.equal(
    publicationRetryDelayMs(30, { baseMs: 1000, maximumMs: 9000, random: () => 0.5 }),
    9000,
  );
});

test("invalid Shard byte configuration fails before any Outbox row is claimed", () => {
  assert.throws(() => new PublicationPublisher({
    store: storeFixture([]),
    ingress: { async acceptShard() {} },
    leaseOwner: "publisher-1",
    maximumShardBytes: "invalid",
  }), /maximumShardBytes/);
});
