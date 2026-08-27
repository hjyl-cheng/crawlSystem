import { createHash } from "node:crypto";
import {
  INCREMENTAL_QUEUE,
  dispatchPayloadHash,
} from "./dispatchTransport.js";

export const CHANNEL_CRAWL_QUEUE = "youtube-channel-crawl";
export const AGENT_INCREMENTAL_QUEUE = "youtube-agent-incremental";
export const QUEUE_PRESSURE_STATES = Object.freeze([
  "waiting",
  "active",
  "delayed",
  "prioritized",
  "paused",
  "waiting-children",
]);

const DYNAMIC_DISPATCH_LOCK_ID = 741603221;
const DISPATCH_NAMESPACE = "336343f8-4684-4468-8cb2-2042ad252eb7";

function boundedInteger(value, fallback, minimum, maximum) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isSafeInteger(parsed)) return fallback;
  return Math.max(minimum, Math.min(maximum, parsed));
}

function boundedNumber(value, fallback, minimum, maximum) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(minimum, Math.min(maximum, parsed));
}

function isoDay(value, field) {
  if (value instanceof Date && !Number.isNaN(value.getTime())) {
    return value.toISOString().slice(0, 10);
  }
  const text = String(value ?? "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) throw new Error(`${field} must be an ISO date`);
  return text;
}

function uuidBytes(value) {
  const hex = String(value).replaceAll("-", "");
  if (!/^[0-9a-f]{32}$/i.test(hex)) throw new Error("UUID is invalid");
  return Buffer.from(hex, "hex");
}

export function uuidV5(namespace, name) {
  const bytes = createHash("sha1")
    .update(Buffer.concat([uuidBytes(namespace), Buffer.from(String(name), "utf8")]))
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function safeJobComponent(value) {
  return String(value ?? "")
    .replace(/[^a-zA-Z0-9_-]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 120) || "channel";
}

export function insideUtcDispatchWindow(value) {
  const now = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(now.getTime())) return false;
  const minute = (now.getUTCHours() * 60) + now.getUTCMinutes();
  return minute >= 30 && minute < ((21 * 60) + 30);
}

export function queuePressure(counts = {}) {
  return QUEUE_PRESSURE_STATES.reduce(
    (total, state) => total + Math.max(0, Number(counts[state] ?? 0)),
    0,
  );
}

function queueCapacity(snapshot, proxyReady = null) {
  if (!snapshot || snapshot.paused === true) return 0;
  const workers = Math.max(0, Math.floor(Number(snapshot.workers) || 0));
  if (workers === 0) return 0;
  const limits = [workers];
  const globalConcurrency = Number(snapshot.global_concurrency);
  if (Number.isFinite(globalConcurrency) && globalConcurrency > 0) {
    limits.push(Math.floor(globalConcurrency));
  }
  if (proxyReady !== null && proxyReady !== undefined && Number.isFinite(Number(proxyReady))) {
    limits.push(Math.max(0, Math.floor(Number(proxyReady))));
  }
  return Math.min(...limits);
}

export function computeDispatchBudget(telemetry, {
  releaseBatchSize = 50,
  bufferPerWorker = 2,
  maximumQueueBuffer = 100,
  minimumIncrementalShare = 0.25,
  agentShare = 0.25,
  agentBatchSize = 30,
  agentBufferBatches = 1,
} = {}) {
  const incremental = telemetry?.incremental ?? {};
  const full = telemetry?.channel_crawl ?? {};
  const agent = telemetry?.agent_incremental ?? {};
  const channelCapacity = queueCapacity(incremental, telemetry?.proxy_channel_ready);
  const channelTarget = Math.min(
    boundedInteger(maximumQueueBuffer, 100, 1, 100000),
    channelCapacity * boundedInteger(bufferPerWorker, 2, 1, 20),
  );
  const incrementalPressure = queuePressure(incremental.counts);
  const competingPressure = full.paused === true ? 0 : queuePressure(full.counts);
  const minimumShare = boundedNumber(minimumIncrementalShare, 0.25, 0, 1);
  const incrementalFloor = competingPressure > 0
    ? Math.ceil(channelTarget * minimumShare)
    : channelTarget;
  const incrementalTarget = Math.max(
    incrementalFloor,
    channelTarget - Math.min(channelTarget, competingPressure),
  );
  const totalLimit = Math.min(
    boundedInteger(releaseBatchSize, 50, 1, 500),
    Math.max(0, incrementalTarget - incrementalPressure),
  );

  const agentCapacity = queueCapacity(agent);
  const agentFraction = boundedNumber(agentShare, 0.25, 0, 1);
  const agentOnlyLimit = totalLimit > 0 && agentCapacity > 0
    ? Math.max(1, Math.min(totalLimit, Math.floor(totalLimit * agentFraction)))
    : 0;
  const agentPendingTarget = boundedInteger(agentBatchSize, 30, 1, 100)
    * Math.max(1, agentCapacity * boundedInteger(agentBufferBatches, 1, 1, 10));

  return {
    total_limit: totalLimit,
    agent_only_limit: agentOnlyLimit,
    agent_pending_target: agentPendingTarget,
    channel_capacity: channelCapacity,
    channel_target: channelTarget,
    incremental_target: incrementalTarget,
    incremental_minimum_share: minimumShare,
    incremental_pressure: incrementalPressure,
    competing_channel_pressure: competingPressure,
    agent_capacity: agentCapacity,
  };
}

async function queueSnapshot(queue) {
  const [counts, workers, globalConcurrency, paused] = await Promise.all([
    queue.getJobCounts(...QUEUE_PRESSURE_STATES),
    queue.getWorkersCount(),
    queue.getGlobalConcurrency(),
    queue.isPaused(),
  ]);
  return {
    name: queue.name,
    counts,
    workers: Number(workers) || 0,
    global_concurrency: Number.isFinite(Number(globalConcurrency))
      ? Number(globalConcurrency)
      : null,
    paused: Boolean(paused),
  };
}

export class BullMqCapacityProbe {
  constructor({
    incrementalQueue,
    channelCrawlQueue,
    agentIncrementalQueue,
    proxyCapacityUrl = "",
    proxyCapacityToken = "",
    fetchImpl = globalThis.fetch,
    fetchTimeoutMs = 2000,
  }) {
    if (!incrementalQueue || !channelCrawlQueue || !agentIncrementalQueue) {
      throw new TypeError("all three BullMQ queues are required");
    }
    this.incrementalQueue = incrementalQueue;
    this.channelCrawlQueue = channelCrawlQueue;
    this.agentIncrementalQueue = agentIncrementalQueue;
    this.proxyCapacityUrl = String(proxyCapacityUrl ?? "").replace(/\/+$/, "");
    this.proxyCapacityToken = String(proxyCapacityToken ?? "").trim();
    this.fetchImpl = fetchImpl;
    this.fetchTimeoutMs = boundedInteger(fetchTimeoutMs, 2000, 100, 30000);
  }

  async proxyChannelReady() {
    if (!this.proxyCapacityUrl || typeof this.fetchImpl !== "function") return null;
    try {
      const request = {
        signal: AbortSignal.timeout(this.fetchTimeoutMs),
      };
      if (this.proxyCapacityToken) {
        request.headers = { Authorization: `Bearer ${this.proxyCapacityToken}` };
      }
      const response = await this.fetchImpl(`${this.proxyCapacityUrl}/capacity`, request);
      if (!response.ok) return null;
      const payload = await response.json();
      const ready = Number(payload?.roles?.channel?.ready);
      return Number.isFinite(ready) ? Math.max(0, Math.floor(ready)) : null;
    } catch {
      return null;
    }
  }

  async sample() {
    const [incremental, channelCrawl, agentIncremental, proxyChannelReady] = await Promise.all([
      queueSnapshot(this.incrementalQueue),
      queueSnapshot(this.channelCrawlQueue),
      queueSnapshot(this.agentIncrementalQueue),
      this.proxyChannelReady(),
    ]);
    return {
      sampled_at: new Date().toISOString(),
      incremental,
      channel_crawl: channelCrawl,
      agent_incremental: agentIncremental,
      proxy_channel_ready: proxyChannelReady,
    };
  }
}

export function buildDispatchEnvelope(plan, releasedAt) {
  const scheduledAt = releasedAt instanceof Date ? releasedAt : new Date(releasedAt);
  if (Number.isNaN(scheduledAt.getTime())) throw new Error("releasedAt must be a timestamp");
  const planDay = isoDay(plan.plan_day, "plan.plan_day");
  if (scheduledAt.toISOString().slice(0, 10) !== planDay) {
    throw new Error("only a Plan for the current UTC day can be released");
  }
  const planId = String(plan.plan_id);
  const channelId = String(plan.channel_id);
  const channelDigest = createHash("sha256").update(channelId).digest("hex").slice(0, 12);
  const jobId = `incremental__${safeJobComponent(channelId)}__${planDay.replaceAll("-", "")}__clock_${Number(plan.source_clock_version)}__${channelDigest}`;
  const taskMask = {
    about: Boolean(plan.run_about),
    video: Boolean(plan.run_video),
    agent: Boolean(plan.run_agent),
  };
  if (!Object.values(taskMask).some(Boolean)) {
    throw new Error("Plan has no active About, Video, or Agent task");
  }
  const payload = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: jobId,
    plan_id: planId,
    plan_mode: String(plan.plan_mode || "standard"),
    plan_day: planDay,
    scheduled_at: scheduledAt.toISOString(),
    channel_id: channelId,
    task_mask: taskMask,
    capacity: {
      factor: Number(plan.capacity_factor),
      player_cap: Number(plan.player_cap),
      next_cap: Number(plan.next_cap),
      version: String(plan.capacity_version),
    },
    clock_version: Number(plan.source_clock_version),
    policy_version: String(plan.policy_version),
    planner_config_version: String(plan.planner_config_version),
  };
  return {
    dispatch_event_id: uuidV5(DISPATCH_NAMESPACE, planId),
    plan_id: planId,
    job_id: jobId,
    queue_name: INCREMENTAL_QUEUE,
    payload,
    payload_hash: dispatchPayloadHash(payload),
  };
}

export class PostgresDynamicDispatchStore {
  constructor({ withTransaction }) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  async stageBatch({
    releasedAt,
    totalLimit,
    agentOnlyLimit,
    agentPendingTarget,
    executionTimeoutMinutes = 1440,
  }) {
    const releaseTime = releasedAt instanceof Date ? releasedAt : new Date(releasedAt);
    if (!insideUtcDispatchWindow(releaseTime)) {
      return { staged: 0, channel: 0, agent_only: 0, pending_outbox: 0 };
    }
    const requestedTotal = boundedInteger(totalLimit, 0, 0, 500);
    if (requestedTotal === 0) {
      return { staged: 0, channel: 0, agent_only: 0, pending_outbox: 0 };
    }
    const requestedAgent = boundedInteger(agentOnlyLimit, 0, 0, requestedTotal);
    const pendingTarget = boundedInteger(agentPendingTarget, 30, 1, 100000);
    const timeoutMinutes = boundedInteger(executionTimeoutMinutes, 1440, 1, 10080);

    return this.withTransaction(async (client) => {
      await client.query("SELECT pg_advisory_xact_lock($1)", [DYNAMIC_DISPATCH_LOCK_ID]);
      const pendingResult = await client.query(
        `SELECT count(*)::int AS total,
                count(*) FILTER (
                  WHERE plan.run_agent
                    AND NOT plan.run_about AND NOT plan.run_video
                )::int AS agent_only
         FROM feature_clock.dispatch_outbox outbox
         JOIN feature_clock.daily_channel_plans plan ON plan.plan_id=outbox.plan_id
         WHERE outbox.status IN ('pending','publishing')
           AND plan.scheduled_at<=$1`,
        [releaseTime],
      );
      const pendingOutbox = Math.max(0, Number(pendingResult.rows[0]?.total) || 0);
      const pendingAgentOutbox = Math.max(0, Number(pendingResult.rows[0]?.agent_only) || 0);
      const availableTotal = Math.max(0, requestedTotal - pendingOutbox);
      if (availableTotal === 0) {
        return { staged: 0, channel: 0, agent_only: 0, pending_outbox: pendingOutbox };
      }

      const agentOutstandingResult = await client.query(
        `SELECT count(*)::int AS count
         FROM feature_clock.daily_channel_plans plan
         WHERE plan.run_agent
           AND plan.status IN ('planned','dispatching','dispatched','running')
           AND (plan.status<>'planned' OR plan.scheduled_at IS NOT NULL)
           AND NOT EXISTS (
             SELECT 1
             FROM feature_clock.crawler_event_inbox inbox
             WHERE inbox.plan_id=plan.plan_id
               AND inbox.observation_kind='agent'
               AND inbox.status='applied'
           )`,
      );
      const agentOutstanding = Math.max(
        0,
        Number(agentOutstandingResult.rows[0]?.count) || 0,
      );
      const availableAgent = Math.min(
        availableTotal,
        Math.max(0, requestedAgent - pendingAgentOutbox),
        Math.max(0, pendingTarget - agentOutstanding),
      );

      const commonWhere = `
        status='planned'
        AND scheduled_at IS NULL
        AND plan_day=($1::timestamptz AT TIME ZONE 'UTC')::date
        AND eligible_at<=$1::timestamptz`;
      const agentRows = availableAgent === 0 ? { rows: [] } : await client.query(
        `SELECT *
         FROM feature_clock.daily_channel_plans
         WHERE ${commonWhere}
           AND run_agent AND NOT run_about AND NOT run_video
         ORDER BY due_day,dispatch_slot,channel_id
         FOR UPDATE SKIP LOCKED
         LIMIT $2`,
        [releaseTime, availableAgent],
      );
      const regularLimit = availableTotal - agentRows.rows.length;
      const regularRows = regularLimit === 0 ? { rows: [] } : await client.query(
        `SELECT *
         FROM feature_clock.daily_channel_plans
         WHERE ${commonWhere}
           AND (run_about OR run_video)
         ORDER BY due_day,dispatch_slot,channel_id
         FOR UPDATE SKIP LOCKED
         LIMIT $2`,
        [releaseTime, regularLimit],
      );
      const plans = [...agentRows.rows, ...regularRows.rows];
      for (const plan of plans) {
        const envelope = buildDispatchEnvelope(plan, releaseTime);
        const updated = await client.query(
          `UPDATE feature_clock.daily_channel_plans
           SET scheduled_at=$2::timestamptz,
               execution_deadline_at=$2::timestamptz+($3::int*interval '1 minute'),
               updated_at=now()
           WHERE plan_id=$1 AND status='planned' AND scheduled_at IS NULL
           RETURNING plan_id`,
          [plan.plan_id, releaseTime, timeoutMinutes],
        );
        if (updated.rowCount !== 1) throw new Error("Dynamic Dispatcher lost its Plan lock");
        await client.query(
          `INSERT INTO feature_clock.dispatch_outbox (
             dispatch_event_id,plan_id,job_id,queue_name,payload_json,payload_hash,
             status,next_attempt_at
           ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,'pending',$7)`,
          [
            envelope.dispatch_event_id,
            envelope.plan_id,
            envelope.job_id,
            envelope.queue_name,
            JSON.stringify(envelope.payload),
            envelope.payload_hash,
            releaseTime,
          ],
        );
      }
      return {
        staged: plans.length,
        channel: regularRows.rows.length,
        agent_only: agentRows.rows.length,
        pending_outbox: pendingOutbox,
      };
    });
  }
}

export class DynamicDispatcher {
  constructor({
    probe,
    store,
    publisher,
    budgetOptions = {},
    executionTimeoutMinutes = 1440,
    now = () => new Date(),
  }) {
    if (!probe || !store || !publisher) throw new TypeError("probe, store and publisher are required");
    this.probe = probe;
    this.store = store;
    this.publisher = publisher;
    this.budgetOptions = budgetOptions;
    this.executionTimeoutMinutes = executionTimeoutMinutes;
    this.now = now;
  }

  async runOnce() {
    const releasedAt = this.now();
    if (!insideUtcDispatchWindow(releasedAt)) {
      return {
        window_open: false,
        budget: { total_limit: 0, agent_only_limit: 0 },
        staged: { staged: 0, channel: 0, agent_only: 0, pending_outbox: 0 },
        published: await this.publisher.runOnce({ batchSize: 0 }),
      };
    }
    const telemetry = await this.probe.sample();
    const budget = computeDispatchBudget(telemetry, this.budgetOptions);
    const staged = await this.store.stageBatch({
      releasedAt,
      totalLimit: budget.total_limit,
      agentOnlyLimit: budget.agent_only_limit,
      agentPendingTarget: budget.agent_pending_target,
      executionTimeoutMinutes: this.executionTimeoutMinutes,
    });
    const published = await this.publisher.runOnce({ batchSize: budget.total_limit });
    return { window_open: true, telemetry, budget, staged, published };
  }
}
