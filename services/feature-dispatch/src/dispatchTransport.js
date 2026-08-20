import { createHash } from "node:crypto";

export const INCREMENTAL_QUEUE = "youtube-channel-incremental";
export const INCREMENTAL_JOB_NAME = "channel.incremental.plan";
export const UTC_CLOCK_WINDOW_START_MINUTE = 30;
export const UTC_CLOCK_WINDOW_END_MINUTE = (21 * 60) + 30;

export class DispatchEnvelopeConflict extends Error {
  constructor(message) {
    super(message);
    this.name = "DispatchEnvelopeConflict";
  }
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new DispatchEnvelopeConflict(`${field} is required`);
  return output;
}

function exactKeys(value, expected, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new DispatchEnvelopeConflict(`${field} must be an object`);
  }
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (JSON.stringify(actual) !== JSON.stringify(wanted)) {
    throw new DispatchEnvelopeConflict(`${field} keys differ from the contract`);
  }
}

function canonicalValue(value, field = "payload") {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new DispatchEnvelopeConflict(`${field} contains a non-finite number`);
    return value;
  }
  if (Array.isArray(value)) return value.map((item, index) => canonicalValue(item, `${field}[${index}]`));
  if (!value || typeof value !== "object") {
    throw new DispatchEnvelopeConflict(`${field} contains a non-JSON value`);
  }
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalValue(value[key], `${field}.${key}`)]),
  );
}

export function canonicalJson(value) {
  return JSON.stringify(canonicalValue(value));
}

export function dispatchPayloadHash(payload) {
  return `sha256:${createHash("sha256").update(canonicalJson(payload)).digest("hex")}`;
}

function payloadObject(value) {
  if (typeof value === "string") {
    try {
      return JSON.parse(value);
    } catch (error) {
      throw new DispatchEnvelopeConflict(`payload_json is not valid JSON: ${error.message}`);
    }
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new DispatchEnvelopeConflict("payload_json must be an object");
  }
  return value;
}

function uuid(value, field) {
  const output = requiredText(value, field).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(output)) {
    throw new DispatchEnvelopeConflict(`${field} must be a UUID`);
  }
  return output;
}

function booleanMask(value, expected, field) {
  exactKeys(value, expected, field);
  for (const key of expected) {
    if (typeof value[key] !== "boolean") {
      throw new DispatchEnvelopeConflict(`${field}.${key} must be boolean`);
    }
  }
}

function nonNegativeInteger(value, field) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new DispatchEnvelopeConflict(`${field} must be a non-negative integer`);
  }
  return value;
}

function positiveInteger(value, field) {
  if (nonNegativeInteger(value, field) === 0) {
    throw new DispatchEnvelopeConflict(`${field} must be positive`);
  }
  return value;
}

function timestamp(value, field) {
  if (value instanceof Date) {
    if (Number.isNaN(value.getTime())) {
      throw new DispatchEnvelopeConflict(`${field} must be an ISO timestamp`);
    }
    return value.toISOString();
  }
  const text = requiredText(value, field);
  if (!/(?:Z|[+-]\d{2}:\d{2})$/i.test(text)) {
    throw new DispatchEnvelopeConflict(`${field} must include a timezone`);
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    throw new DispatchEnvelopeConflict(`${field} must be an ISO timestamp`);
  }
  return parsed.toISOString();
}

export function timestampInUtcClockWindow(value, field = "timestamp") {
  const parsed = new Date(timestamp(value, field));
  const minute = (parsed.getUTCHours() * 60) + parsed.getUTCMinutes();
  return minute >= UTC_CLOCK_WINDOW_START_MINUTE
    && minute < UTC_CLOCK_WINDOW_END_MINUTE;
}

export function validateDispatchOutboxRow(row) {
  const payload = payloadObject(row?.payload_json);
  const baseKeys = [
    "schema_version",
    "job_id",
    "plan_id",
    "plan_mode",
    "plan_day",
    "scheduled_at",
    "channel_id",
    "task_mask",
    "capacity",
    "clock_version",
    "policy_version",
    "planner_config_version",
  ];
  exactKeys(payload, baseKeys, "payload");
  if (payload.schema_version !== 4) {
    throw new DispatchEnvelopeConflict("schema_version must be 4");
  }
  uuid(row?.dispatch_event_id, "row.dispatch_event_id");
  const planId = uuid(row?.plan_id, "row.plan_id");
  if (uuid(payload.plan_id, "payload.plan_id") !== planId) {
    throw new DispatchEnvelopeConflict("plan_id differs between row and payload");
  }
  const jobId = requiredText(row?.job_id, "row.job_id");
  if (requiredText(payload.job_id, "payload.job_id") !== jobId) {
    throw new DispatchEnvelopeConflict("job_id differs between row and payload");
  }
  if (jobId.includes(":")) throw new DispatchEnvelopeConflict("BullMQ job_id cannot contain ':'");
  const queueName = requiredText(row?.queue_name, "row.queue_name");
  if (queueName !== INCREMENTAL_QUEUE) {
    throw new DispatchEnvelopeConflict(`queue_name must be ${INCREMENTAL_QUEUE}`);
  }
  requiredText(payload.channel_id, "payload.channel_id");
  requiredText(payload.policy_version, "payload.policy_version");
  requiredText(payload.planner_config_version, "payload.planner_config_version");
  const planDay = requiredText(payload.plan_day, "payload.plan_day");
  const parsedPlanDay = new Date(`${planDay}T00:00:00.000Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(planDay)
      || Number.isNaN(parsedPlanDay.getTime())
      || parsedPlanDay.toISOString().slice(0, 10) !== planDay) {
    throw new DispatchEnvelopeConflict("payload.plan_day must be an ISO date");
  }
  const scheduledAt = timestamp(payload.scheduled_at, "payload.scheduled_at");
  if (!timestampInUtcClockWindow(scheduledAt, "payload.scheduled_at")) {
    throw new DispatchEnvelopeConflict(
      "payload.scheduled_at must be inside the UTC Clock window",
    );
  }
  if (scheduledAt.slice(0, 10) !== planDay) {
    throw new DispatchEnvelopeConflict(
      "payload.scheduled_at UTC day differs from plan_day",
    );
  }
  if (row?.plan_scheduled_at != null
      && timestamp(row.plan_scheduled_at, "row.plan_scheduled_at") !== scheduledAt) {
    throw new DispatchEnvelopeConflict("scheduled_at differs between Plan and payload");
  }
  positiveInteger(payload.clock_version, "payload.clock_version");

  booleanMask(
    payload.task_mask,
    ["about", "video", "agent"],
    "payload.task_mask",
  );
  if (!Object.values(payload.task_mask).some(Boolean)) {
    throw new DispatchEnvelopeConflict("task_mask cannot be empty");
  }
  const planMode = requiredText(payload.plan_mode, "payload.plan_mode");
  if (!["standard", "dormant_probe"].includes(planMode)) {
    throw new DispatchEnvelopeConflict("payload.plan_mode is invalid");
  }
  if (planMode === "dormant_probe" && (
    payload.task_mask.about
    || !payload.task_mask.video
    || payload.task_mask.agent
  )) {
    throw new DispatchEnvelopeConflict("dormant_probe must contain only the Video task");
  }

  exactKeys(payload.capacity, ["factor", "player_cap", "next_cap", "version"], "payload.capacity");
  if (typeof payload.capacity.factor !== "number"
      || !Number.isFinite(payload.capacity.factor)
      || payload.capacity.factor < 0
      || payload.capacity.factor > 1) {
    throw new DispatchEnvelopeConflict("payload.capacity.factor must be between 0 and 1");
  }
  nonNegativeInteger(payload.capacity.player_cap, "payload.capacity.player_cap");
  nonNegativeInteger(payload.capacity.next_cap, "payload.capacity.next_cap");
  requiredText(payload.capacity.version, "payload.capacity.version");

  const expectedHash = dispatchPayloadHash(payload);
  if (requiredText(row?.payload_hash, "row.payload_hash") !== expectedHash) {
    throw new DispatchEnvelopeConflict("payload_hash does not match payload_json");
  }
  return payload;
}

export async function publishDispatchOutboxRow(queue, row, options = {}) {
  const payload = validateDispatchOutboxRow(row);
  if (requiredText(queue?.name, "queue.name") !== requiredText(row.queue_name, "row.queue_name")) {
    throw new DispatchEnvelopeConflict("queue_name differs from the configured BullMQ queue");
  }
  const jobId = payload.job_id;
  let job = await queue.getJob(jobId);
  if (!job) {
    job = await queue.add(INCREMENTAL_JOB_NAME, payload, {
      jobId,
      attempts: options.attempts ?? 5,
      backoff: options.backoff ?? { type: "exponential", delay: 5000, jitter: 0.5 },
      removeOnComplete: options.removeOnComplete ?? { age: 1209600, count: 100000 },
      removeOnFail: options.removeOnFail ?? { age: 2592000, count: 100000 },
    });
  }
  if (canonicalJson(job.data) !== canonicalJson(payload)) {
    throw new DispatchEnvelopeConflict(`BullMQ job ${jobId} already contains a different Plan`);
  }
  return { payload, job_id: String(job.id) };
}
