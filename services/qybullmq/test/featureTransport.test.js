import assert from "node:assert/strict";
import test from "node:test";
import {
  createFeatureRecalcProcessor,
  FeatureIngestPermanentError,
  featureRecalcJobId,
  OutboxEnvelopeConflict,
  publishCrawlerOutboxRow,
  validateCrawlerOutboxRow,
  validateFeatureIngestTarget,
} from "../src/featureTransport.js";

function fixture() {
  const event = {
    event_id: "3e3cfbbf-9ef6-4ccb-90cb-136f005dc011",
    event_type: "crawler.observation.recorded",
    event_version: 1,
    observation_id: "4f379f17-c52f-4220-b056-d229ed4b342d",
    channel_id: "UCtransport",
    observation_kind: "about",
    kind_sequence: 3,
    observed_at: "2026-07-20T00:00:00.000Z",
    outcome: "complete",
    crawler_version: "qy-v16",
    payload_hash: "sha256:example",
    payload: {
      subscriber_count: 100,
      subscriber_count_status: "exact",
      total_view_count: 200,
      total_view_count_status: "exact",
      total_video_count: 3,
      total_video_count_status: "exact",
    },
  };
  return {
    row: {
      event_id: event.event_id,
      observation_id: event.observation_id,
      observed_at: new Date(event.observed_at),
      event_type: event.event_type,
      event_version: event.event_version,
      aggregate_key: `${event.channel_id}:${event.observation_kind}`,
      kind_sequence: event.kind_sequence,
      payload_json: event,
      payload_hash: event.payload_hash,
      attempts: 1,
    },
    event,
  };
}

function fakeQueue() {
  const jobs = new Map();
  return {
    jobs,
    adds: 0,
    async getJob(id) { return jobs.get(id) ?? null; },
    async add(name, data, options) {
      this.adds += 1;
      const existing = jobs.get(options.jobId);
      if (existing) return existing;
      const job = { id: options.jobId, name, data, opts: options };
      jobs.set(options.jobId, job);
      return job;
    },
  };
}

test("Crawler Outbox row and payload envelope must agree", () => {
  const { row, event } = fixture();
  assert.deepEqual(validateCrawlerOutboxRow(row), event);
  assert.throws(
    () => validateCrawlerOutboxRow({ ...row, kind_sequence: 4 }),
    OutboxEnvelopeConflict,
  );
});

test("Publisher reuses an identical deterministic BullMQ job", async () => {
  const { row, event } = fixture();
  const queue = fakeQueue();
  const first = await publishCrawlerOutboxRow(queue, row);
  const duplicate = await publishCrawlerOutboxRow(queue, row);
  assert.equal(first.job_id, featureRecalcJobId(event.event_id));
  assert.equal(duplicate.job_id, first.job_id);
  assert.equal(queue.adds, 1);
});

test("Publisher rejects an existing BullMQ job with a different envelope", async () => {
  const { row, event } = fixture();
  const queue = fakeQueue();
  queue.jobs.set(featureRecalcJobId(event.event_id), {
    id: featureRecalcJobId(event.event_id),
    data: { ...event, outcome: "partial" },
  });
  await assert.rejects(publishCrawlerOutboxRow(queue, row), OutboxEnvelopeConflict);
});

test("Feature Relay sends auth and the event idempotency key", async () => {
  const { event } = fixture();
  let request = null;
  const processor = createFeatureRecalcProcessor({
    endpoint: "http://feature-engine:8090/v1/crawler-observations",
    token: "secret",
    fetchImpl: async (url, options) => {
      request = { url, options };
      return new Response(JSON.stringify({ ok: true, status: "applied" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    },
  });
  const result = await processor({ data: event });
  assert.equal(result.status, "applied");
  assert.equal(request.options.headers.authorization, "Bearer secret");
  assert.equal(request.options.headers["idempotency-key"], event.event_id);
});

test("Feature Relay treats permanent 4xx differently from retryable 5xx", async () => {
  const { event } = fixture();
  const permanent = createFeatureRecalcProcessor({
    endpoint: "http://127.0.0.1:8090/v1/crawler-observations",
    fetchImpl: async () => new Response(JSON.stringify({ error: "event conflict" }), { status: 409 }),
  });
  const retryable = createFeatureRecalcProcessor({
    endpoint: "http://127.0.0.1:8090/v1/crawler-observations",
    fetchImpl: async () => new Response(JSON.stringify({ error: "database unavailable" }), { status: 503 }),
  });
  await assert.rejects(permanent({ data: event }), FeatureIngestPermanentError);
  await assert.rejects(retryable({ data: event }), (error) => (
    !(error instanceof FeatureIngestPermanentError) && /database unavailable/.test(error.message)
  ));
});

test("A non-loopback Feature endpoint requires a token", () => {
  assert.throws(
    () => validateFeatureIngestTarget("http://feature-engine:8090/v1/crawler-observations", null),
    /FEATURE_INGEST_TOKEN/,
  );
  assert.equal(
    validateFeatureIngestTarget("http://127.0.0.1:8090/v1/crawler-observations", null).token,
    null,
  );
});

test("An invalid BullMQ event is a permanent Relay failure", async () => {
  const processor = createFeatureRecalcProcessor({
    endpoint: "http://127.0.0.1:8090/v1/crawler-observations",
    fetchImpl: async () => {
      throw new Error("fetch must not be called");
    },
  });
  await assert.rejects(processor({ data: {} }), FeatureIngestPermanentError);
});
