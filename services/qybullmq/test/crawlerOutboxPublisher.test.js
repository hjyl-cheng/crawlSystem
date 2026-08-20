import assert from "node:assert/strict";
import test from "node:test";
import { CrawlerOutboxPublisher, outboxRetryDelayMs } from "../src/crawlerOutboxPublisher.js";
import { OutboxEnvelopeConflict } from "../src/featureTransport.js";

function row(attempts = 1) {
  const payload = {
    event_id: "fded31f0-6724-4ceb-867d-16b9a2631d72",
    event_type: "crawler.observation.recorded",
    event_version: 1,
    observation_id: "db86c563-2a86-4f92-b468-b7e75282cd79",
    channel_id: "UCpublisher",
    observation_kind: "about",
    kind_sequence: 1,
    observed_at: "2026-07-20T00:00:00.000Z",
    outcome: "complete",
    crawler_version: "qy-v16",
    payload_hash: "sha256:publisher",
    payload: {},
  };
  return {
    ...payload,
    aggregate_key: "UCpublisher:about",
    payload_json: payload,
    attempts,
  };
}

function storeFixture(claimed) {
  const calls = { published: [], failed: [] };
  return {
    calls,
    async claimBatch() { return claimed; },
    async markPublished(value) { calls.published.push(value); return true; },
    async markFailed(value) {
      calls.failed.push(value);
      return value.deadLetter ? "dead_letter" : "pending";
    },
  };
}

function queueFixture(error = null) {
  const jobs = new Map();
  return {
    async getJob(id) { return jobs.get(id) ?? null; },
    async add(name, data, options) {
      if (error) throw error;
      const job = { id: options.jobId, name, data };
      jobs.set(options.jobId, job);
      return job;
    },
  };
}

test("Outbox Publisher marks a row only after BullMQ accepts it", async () => {
  const store = storeFixture([row()]);
  const publisher = new CrawlerOutboxPublisher({
    store,
    queue: queueFixture(),
    leaseOwner: "publisher-1",
    logger: {},
  });
  const result = await publisher.runOnce();
  assert.deepEqual(result, {
    claimed: 1, published: 1, retried: 0, dead_lettered: 0, lease_lost: 0,
  });
  assert.equal(store.calls.published.length, 1);
  assert.equal(store.calls.failed.length, 0);
});

test("Outbox Publisher retries transient delivery and dead-letters at its cap", async () => {
  const retryStore = storeFixture([row(2)]);
  const retryPublisher = new CrawlerOutboxPublisher({
    store: retryStore,
    queue: queueFixture(new Error("redis unavailable")),
    leaseOwner: "publisher-1",
    maxAttempts: 3,
    retryDelay: () => 1234,
    logger: {},
  });
  const retried = await retryPublisher.runOnce();
  assert.equal(retried.retried, 1);
  assert.equal(retryStore.calls.failed[0].retryDelayMs, 1234);
  assert.equal(retryStore.calls.failed[0].deadLetter, false);

  const deadStore = storeFixture([row(3)]);
  const deadPublisher = new CrawlerOutboxPublisher({
    store: deadStore,
    queue: queueFixture(new Error("redis unavailable")),
    leaseOwner: "publisher-1",
    maxAttempts: 3,
    logger: {},
  });
  const dead = await deadPublisher.runOnce();
  assert.equal(dead.dead_lettered, 1);
  assert.equal(deadStore.calls.failed[0].deadLetter, true);
});

test("Outbox retry delay is bounded and deterministic with injected random", () => {
  assert.equal(outboxRetryDelayMs(1, { baseMs: 1000, random: () => 0.5 }), 1000);
  assert.equal(outboxRetryDelayMs(4, { baseMs: 1000, random: () => 0.5 }), 8000);
  assert.equal(
    outboxRetryDelayMs(30, { baseMs: 1000, maximumMs: 9000, random: () => 0.5 }),
    9000,
  );
});

test("A corrupt Outbox envelope is dead-lettered immediately", async () => {
  const corrupt = row(1);
  corrupt.payload_json = { ...corrupt.payload_json, payload_hash: "sha256:different" };
  const store = storeFixture([corrupt]);
  const publisher = new CrawlerOutboxPublisher({
    store,
    queue: queueFixture(),
    leaseOwner: "publisher-1",
    maxAttempts: 12,
    logger: {},
  });
  const result = await publisher.runOnce();
  assert.equal(result.dead_lettered, 1);
  assert.equal(store.calls.failed[0].deadLetter, true);
  assert.ok(store.calls.failed[0].error instanceof OutboxEnvelopeConflict);
});
