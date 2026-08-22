import { performance } from "node:perf_hooks";
import { inspectBusinessPublication } from "./businessPublicationAuditQueries.js";

function integerOption(value, fallback, field, { minimum, maximum }) {
  if (value == null || String(value).trim() === "") return fallback;
  const output = Number(value);
  if (!Number.isSafeInteger(output) || output < minimum || output > maximum) {
    throw new TypeError(`${field} must be an integer between ${minimum} and ${maximum}`);
  }
  return output;
}

function instant(value) {
  const output = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(output.getTime())) throw new TypeError("clock must return a valid Date");
  return output;
}

function errorText(error) {
  return String(error?.message || error).slice(0, 2000);
}

function auditFailureKind(error) {
  const message = errorText(error);
  if (
    /shared memory segment/i.test(message)
    && /No space left on device/i.test(message)
  ) {
    return "dynamic_shared_memory_exhausted";
  }
  return "audit_query_failed";
}

function isoTimestamp(value) {
  return value == null ? null : new Date(value).toISOString();
}

export class PostgresBusinessPublicationAuditor {
  constructor(pool, {
    auditIntervalSeconds = 300,
    auditSampleSize = 10,
    gapAlertSeconds = 900,
    projectionStuckSeconds = 300,
    errorRetrySeconds = 30,
    maximumErrorRetrySeconds = 300,
    clock = () => new Date(),
    monotonicClock = () => performance.now(),
  } = {}) {
    if (!pool || typeof pool.connect !== "function") {
      throw new TypeError("a PostgreSQL Pool is required");
    }
    if (typeof clock !== "function") throw new TypeError("clock must be a function");
    if (typeof monotonicClock !== "function") {
      throw new TypeError("monotonicClock must be a function");
    }
    this.pool = pool;
    this.auditIntervalMs = integerOption(
      auditIntervalSeconds,
      300,
      "auditIntervalSeconds",
      { minimum: 0, maximum: 86400 },
    ) * 1000;
    this.auditSampleSize = integerOption(
      auditSampleSize,
      10,
      "auditSampleSize",
      { minimum: 1, maximum: 100 },
    );
    this.gapAlertSeconds = integerOption(
      gapAlertSeconds,
      900,
      "gapAlertSeconds",
      { minimum: 0, maximum: 2592000 },
    );
    this.projectionStuckSeconds = integerOption(
      projectionStuckSeconds,
      300,
      "projectionStuckSeconds",
      { minimum: 0, maximum: 2592000 },
    );
    this.errorRetrySeconds = integerOption(
      errorRetrySeconds,
      30,
      "errorRetrySeconds",
      { minimum: 1, maximum: 3600 },
    );
    this.maximumErrorRetrySeconds = integerOption(
      maximumErrorRetrySeconds,
      300,
      "maximumErrorRetrySeconds",
      { minimum: this.errorRetrySeconds, maximum: 86400 },
    );
    this.clock = clock;
    this.monotonicClock = monotonicClock;
    this.lastAttemptAtMs = null;
    this.lastSuccessAtMs = null;
    this.nextAttemptAtMs = null;
    this.consecutiveFailures = 0;
    this.attemptsTotal = 0;
    this.successesTotal = 0;
    this.failuresTotal = 0;
    this.sharedMemoryFailuresTotal = 0;
    this.inFlight = null;
  }

  #state() {
    return {
      consecutive_failures: this.consecutiveFailures,
      last_attempt_at: isoTimestamp(this.lastAttemptAtMs),
      last_success_at: isoTimestamp(this.lastSuccessAtMs),
      next_attempt_at: isoTimestamp(this.nextAttemptAtMs),
      parallel_workers_per_gather: 0,
      debug_parallel_query: "off",
      attempts_total: this.attemptsTotal,
      successes_total: this.successesTotal,
      failures_total: this.failuresTotal,
      shared_memory_failures_total: this.sharedMemoryFailuresTotal,
    };
  }

  #skipped(status) {
    return {
      performed: false,
      status,
      issue_count: null,
      findings: null,
      query_count: 0,
      query_duration_ms: null,
      failed_query: null,
      duration_ms: 0,
      error: null,
      failure_kind: null,
      ...this.#state(),
    };
  }

  async #perform(attemptedAt) {
    this.lastAttemptAtMs = attemptedAt.getTime();
    this.attemptsTotal += 1;
    const monotonicStartedAt = this.monotonicClock();
    let client;
    try {
      client = await this.pool.connect();
      await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
      await client.query("SET LOCAL max_parallel_workers_per_gather = 0");
      await client.query("SET LOCAL debug_parallel_query = off");
      const inspection = await inspectBusinessPublication(client, {
        sampleSize: this.auditSampleSize,
        gapAlertSeconds: this.gapAlertSeconds,
        projectionStuckSeconds: this.projectionStuckSeconds,
        monotonicClock: this.monotonicClock,
      });
      await client.query("COMMIT");
      const finishedAt = instant(this.clock());
      this.lastSuccessAtMs = finishedAt.getTime();
      this.nextAttemptAtMs = finishedAt.getTime() + this.auditIntervalMs;
      this.consecutiveFailures = 0;
      this.successesTotal += 1;
      return {
        performed: true,
        status: "succeeded",
        started_at: attemptedAt.toISOString(),
        finished_at: finishedAt.toISOString(),
        duration_ms: Math.max(0, Math.round(this.monotonicClock() - monotonicStartedAt)),
        error: null,
        failure_kind: null,
        failed_query: null,
        ...inspection,
        ...this.#state(),
      };
    } catch (error) {
      if (client) await client.query("ROLLBACK").catch(() => {});
      const finishedAt = instant(this.clock());
      const failureKind = auditFailureKind(error);
      this.consecutiveFailures += 1;
      this.failuresTotal += 1;
      if (failureKind === "dynamic_shared_memory_exhausted") {
        this.sharedMemoryFailuresTotal += 1;
      }
      const retrySeconds = this.consecutiveFailures >= 4
        ? this.maximumErrorRetrySeconds
        : Math.min(
          this.maximumErrorRetrySeconds,
          this.errorRetrySeconds * (2 ** (this.consecutiveFailures - 1)),
        );
      this.nextAttemptAtMs = finishedAt.getTime() + retrySeconds * 1000;
      return {
        performed: true,
        status: "failed",
        started_at: attemptedAt.toISOString(),
        finished_at: finishedAt.toISOString(),
        duration_ms: Math.max(0, Math.round(this.monotonicClock() - monotonicStartedAt)),
        issue_count: null,
        findings: null,
        query_count: Number(error?.completedQueryCount ?? 0),
        query_duration_ms: error?.queryDurationMs ?? {},
        failed_query: error?.failedQuery ?? null,
        error: errorText(error),
        failure_kind: failureKind,
        ...this.#state(),
      };
    } finally {
      client?.release();
    }
  }

  async runIfDue() {
    const now = instant(this.clock());
    if (this.inFlight) return this.#skipped("in_progress");
    if (this.nextAttemptAtMs != null && now.getTime() < this.nextAttemptAtMs) {
      return this.#skipped("not_due");
    }
    const attempt = this.#perform(now);
    this.inFlight = attempt;
    try {
      return await attempt;
    } finally {
      if (this.inFlight === attempt) this.inFlight = null;
    }
  }
}
