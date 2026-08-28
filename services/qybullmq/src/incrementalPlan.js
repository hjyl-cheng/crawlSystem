import { createHash } from "node:crypto";

export const INCREMENTAL_QUEUE = "youtube-channel-incremental";
export const INCREMENTAL_JOB_NAME = "channel.incremental.plan";

const PAYLOAD_KEYS = [
  "schema_version",
  "dispatch_generation",
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

export class IncrementalPlanContractError extends Error {
  constructor(message) {
    super(message);
    this.name = "IncrementalPlanContractError";
  }
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new IncrementalPlanContractError(`${field} is required`);
  return output;
}

function exactKeys(value, expected, field) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new IncrementalPlanContractError(`${field} must be an object`);
  }
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (JSON.stringify(actual) !== JSON.stringify(wanted)) {
    throw new IncrementalPlanContractError(`${field} keys differ from the contract`);
  }
}

function uuid(value, field) {
  const output = requiredText(value, field).toLowerCase();
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(output)) {
    throw new IncrementalPlanContractError(`${field} must be a UUID`);
  }
  return output;
}

function booleanMask(value, keys, field) {
  exactKeys(value, keys, field);
  for (const key of keys) {
    if (typeof value[key] !== "boolean") {
      throw new IncrementalPlanContractError(`${field}.${key} must be boolean`);
    }
  }
  return Object.fromEntries(keys.map((key) => [key, value[key]]));
}

function nonNegativeInteger(value, field) {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new IncrementalPlanContractError(`${field} must be a non-negative integer`);
  }
  return value;
}

function timestamp(value, field) {
  const text = requiredText(value, field);
  if (!/(?:Z|[+-]\d{2}:\d{2})$/i.test(text)) {
    throw new IncrementalPlanContractError(`${field} must include a timezone`);
  }
  const parsed = new Date(text);
  if (Number.isNaN(parsed.getTime())) {
    throw new IncrementalPlanContractError(`${field} must be an ISO timestamp`);
  }
  return parsed.toISOString();
}

function canonicalValue(value, field = "payload") {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new IncrementalPlanContractError(`${field} contains a non-finite number`);
    }
    return value;
  }
  if (Array.isArray(value)) {
    return value.map((item, index) => canonicalValue(item, `${field}[${index}]`));
  }
  if (!value || typeof value !== "object") {
    throw new IncrementalPlanContractError(`${field} contains a non-JSON value`);
  }
  return Object.fromEntries(
    Object.keys(value).sort().map((key) => [key, canonicalValue(value[key], `${field}.${key}`)]),
  );
}

export function canonicalIncrementalJson(value) {
  return JSON.stringify(canonicalValue(value));
}

export function incrementalPlanHash(plan) {
  return `sha256:${createHash("sha256").update(canonicalIncrementalJson(plan)).digest("hex")}`;
}

export function incrementalRunId(planId) {
  return `incremental:${uuid(planId, "plan_id")}`;
}

export function validateIncrementalPlan(value) {
  exactKeys(value, PAYLOAD_KEYS, "payload");
  if (value.schema_version !== 5) {
    throw new IncrementalPlanContractError("schema_version must be 5");
  }
  const planId = uuid(value.plan_id, "payload.plan_id");
  const planDay = requiredText(value.plan_day, "payload.plan_day");
  const parsedPlanDay = new Date(`${planDay}T00:00:00.000Z`);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(planDay)
      || Number.isNaN(parsedPlanDay.getTime())
      || parsedPlanDay.toISOString().slice(0, 10) !== planDay) {
    throw new IncrementalPlanContractError("payload.plan_day must be an ISO date");
  }
  const jobId = requiredText(value.job_id, "payload.job_id");
  if (jobId.includes(":")) {
    throw new IncrementalPlanContractError("BullMQ job_id cannot contain ':'");
  }
  const taskMask = booleanMask(
    value.task_mask,
    ["about", "video", "agent"],
    "payload.task_mask",
  );
  if (!Object.values(taskMask).some(Boolean)) {
    throw new IncrementalPlanContractError("task_mask cannot be empty");
  }
  exactKeys(value.capacity, ["factor", "player_cap", "next_cap", "version"], "payload.capacity");
  const factor = Number(value.capacity.factor);
  if (typeof value.capacity.factor !== "number" || !Number.isFinite(factor) || factor < 0 || factor > 1) {
    throw new IncrementalPlanContractError("payload.capacity.factor must be between 0 and 1");
  }
  const clockVersion = nonNegativeInteger(value.clock_version, "payload.clock_version");
  if (clockVersion === 0) throw new IncrementalPlanContractError("payload.clock_version must be positive");
  const dispatchGeneration = nonNegativeInteger(
    value.dispatch_generation,
    "payload.dispatch_generation",
  );
  if (dispatchGeneration === 0) {
    throw new IncrementalPlanContractError("payload.dispatch_generation must be positive");
  }
  const scheduledAt = timestamp(value.scheduled_at, "payload.scheduled_at");
  if (scheduledAt.slice(0, 10) !== planDay) {
    throw new IncrementalPlanContractError("payload.scheduled_at must fall inside plan_day UTC");
  }
  const planMode = requiredText(value.plan_mode, "payload.plan_mode");
  if (!["standard", "dormant_probe"].includes(planMode)) {
    throw new IncrementalPlanContractError("payload.plan_mode is invalid");
  }
  if (planMode === "dormant_probe" && (
    taskMask.about || !taskMask.video || taskMask.agent
  )) {
    throw new IncrementalPlanContractError("dormant_probe must contain only the Video task");
  }

  return Object.freeze({
    schema_version: value.schema_version,
    dispatch_generation: dispatchGeneration,
    job_id: jobId,
    plan_id: planId,
    plan_mode: planMode,
    plan_day: planDay,
    scheduled_at: scheduledAt,
    channel_id: requiredText(value.channel_id, "payload.channel_id"),
    task_mask: Object.freeze(taskMask),
    capacity: Object.freeze({
      factor,
      player_cap: nonNegativeInteger(value.capacity.player_cap, "payload.capacity.player_cap"),
      next_cap: nonNegativeInteger(value.capacity.next_cap, "payload.capacity.next_cap"),
      version: requiredText(value.capacity.version, "payload.capacity.version"),
    }),
    clock_version: clockVersion,
    policy_version: requiredText(value.policy_version, "payload.policy_version"),
    planner_config_version: requiredText(
      value.planner_config_version,
      "payload.planner_config_version",
    ),
  });
}

export function validateIncrementalJob(job) {
  if (job?.queueName !== INCREMENTAL_QUEUE) {
    throw new IncrementalPlanContractError(`queue must be ${INCREMENTAL_QUEUE}`);
  }
  if (job?.name !== INCREMENTAL_JOB_NAME) {
    throw new IncrementalPlanContractError(`job name must be ${INCREMENTAL_JOB_NAME}`);
  }
  const plan = validateIncrementalPlan(job.data);
  if (String(job.id) !== plan.job_id) {
    throw new IncrementalPlanContractError("BullMQ job id differs from payload.job_id");
  }
  return plan;
}
