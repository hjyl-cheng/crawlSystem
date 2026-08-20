import { createHash } from "node:crypto";

export const FEATURE_RECALC_QUEUE = "feature-recalc";

export class OutboxEnvelopeConflict extends Error {
  constructor(message) {
    super(message);
    this.name = "OutboxEnvelopeConflict";
  }
}

export class FeatureIngestPermanentError extends Error {
  constructor(message, status = null) {
    super(message);
    this.name = "FeatureIngestPermanentError";
    this.status = status;
  }
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new OutboxEnvelopeConflict(`${field} is required`);
  return output;
}

function eventObject(value) {
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch (error) {
      throw new OutboxEnvelopeConflict(`payload_json is not valid JSON: ${error.message}`);
    }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new OutboxEnvelopeConflict("payload_json must be an object");
  }
  return value;
}

function timestamp(value, field) {
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) throw new OutboxEnvelopeConflict(`${field} is invalid`);
  return parsed.toISOString();
}

function sameNumber(left, right) {
  return Number(left) === Number(right) && Number.isSafeInteger(Number(left));
}

export function validateCrawlerOutboxRow(row) {
  const event = eventObject(row?.payload_json);
  const checks = [
    [requiredText(row?.event_id, "row.event_id"), requiredText(event.event_id, "event.event_id"), "event_id"],
    [
      requiredText(row?.observation_id, "row.observation_id"),
      requiredText(event.observation_id, "event.observation_id"),
      "observation_id",
    ],
    [requiredText(row?.event_type, "row.event_type"), requiredText(event.event_type, "event.event_type"), "event_type"],
    [requiredText(row?.payload_hash, "row.payload_hash"), requiredText(event.payload_hash, "event.payload_hash"), "payload_hash"],
  ];
  for (const [left, right, field] of checks) {
    if (left !== right) throw new OutboxEnvelopeConflict(`${field} differs between row and payload`);
  }
  if (!sameNumber(row.event_version, event.event_version)) {
    throw new OutboxEnvelopeConflict("event_version differs between row and payload");
  }
  if (!sameNumber(row.kind_sequence, event.kind_sequence)) {
    throw new OutboxEnvelopeConflict("kind_sequence differs between row and payload");
  }
  if (timestamp(row.observed_at, "row.observed_at") !== timestamp(event.observed_at, "event.observed_at")) {
    throw new OutboxEnvelopeConflict("observed_at differs between row and payload");
  }
  const expectedAggregate = `${requiredText(event.channel_id, "event.channel_id")}:${requiredText(
    event.observation_kind,
    "event.observation_kind",
  )}`;
  if (requiredText(row.aggregate_key, "row.aggregate_key") !== expectedAggregate) {
    throw new OutboxEnvelopeConflict("aggregate_key differs from the event route");
  }
  return event;
}

export function featureRecalcJobId(eventId) {
  const normalized = requiredText(eventId, "event_id").replace(/[^a-zA-Z0-9_-]+/g, "_");
  const digest = createHash("sha256").update(String(eventId)).digest("hex").slice(0, 12);
  return `crawler_observation__${normalized.slice(0, 180)}__${digest}`;
}

function transportIdentity(event) {
  return JSON.stringify({
    event_id: event?.event_id ?? null,
    event_type: event?.event_type ?? null,
    event_version: event?.event_version ?? null,
    observation_id: event?.observation_id ?? null,
    plan_id: event?.plan_id ?? null,
    channel_id: event?.channel_id ?? null,
    observation_kind: event?.observation_kind ?? null,
    kind_sequence: event?.kind_sequence ?? null,
    observed_at: timestamp(event?.observed_at, "event.observed_at"),
    outcome: event?.outcome ?? null,
    crawler_version: event?.crawler_version ?? null,
    payload_hash: event?.payload_hash ?? null,
  });
}

export async function publishCrawlerOutboxRow(queue, row, options = {}) {
  const event = validateCrawlerOutboxRow(row);
  const jobId = featureRecalcJobId(event.event_id);
  let job = await queue.getJob(jobId);
  if (!job) {
    job = await queue.add(event.event_type, event, {
      jobId,
      attempts: options.attempts ?? 8,
      backoff: options.backoff ?? { type: "exponential", delay: 5000, jitter: 0.5 },
      removeOnComplete: options.removeOnComplete ?? { age: 1209600, count: 100000 },
      removeOnFail: options.removeOnFail ?? { age: 2592000, count: 100000 },
    });
  }
  if (transportIdentity(job.data) !== transportIdentity(event)) {
    throw new OutboxEnvelopeConflict(`BullMQ job ${jobId} already contains a different event envelope`);
  }
  return { event, job_id: String(job.id) };
}

function loopbackHostname(hostname) {
  return hostname === "127.0.0.1" || hostname === "localhost" || hostname === "::1";
}

export function validateFeatureIngestTarget(endpoint, token) {
  const url = new URL(endpoint);
  if (!["http:", "https:"].includes(url.protocol)) throw new TypeError("FEATURE_INGEST_URL must use HTTP(S)");
  const normalizedToken = String(token ?? "").trim();
  if (!normalizedToken && !loopbackHostname(url.hostname)) {
    throw new TypeError("FEATURE_INGEST_TOKEN is required for a non-loopback Feature endpoint");
  }
  return { endpoint: url.toString(), token: normalizedToken || null };
}

export function createFeatureRecalcProcessor({
  endpoint,
  token = null,
  timeoutMs = 15000,
  fetchImpl = globalThis.fetch,
}) {
  const target = validateFeatureIngestTarget(endpoint, token);
  if (typeof fetchImpl !== "function") throw new TypeError("fetch implementation is required");
  return async function processFeatureRecalc(job) {
    const event = job?.data;
    let eventId;
    try {
      eventId = requiredText(event?.event_id, "event_id");
    } catch (error) {
      throw new FeatureIngestPermanentError(error.message);
    }
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), Math.max(100, Number(timeoutMs) || 15000));
    try {
      const response = await fetchImpl(target.endpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json",
          "idempotency-key": eventId,
          ...(target.token ? { authorization: `Bearer ${target.token}` } : {}),
        },
        body: JSON.stringify(event),
        signal: controller.signal,
      });
      const body = await response.text();
      let result = null;
      try {
        result = body ? JSON.parse(body) : {};
      } catch {
        result = { error: body.slice(0, 500) };
      }
      if (response.ok) return result;
      const message = String(result?.error || `Feature ingest returned HTTP ${response.status}`).slice(0, 1000);
      if (response.status >= 400 && response.status < 500 && ![408, 429].includes(response.status)) {
        throw new FeatureIngestPermanentError(message, response.status);
      }
      throw new Error(message);
    } finally {
      clearTimeout(timer);
    }
  };
}
