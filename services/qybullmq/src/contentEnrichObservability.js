const OPEN_QUEUE_STATES = Object.freeze([
  "waiting",
  "active",
  "delayed",
  "paused",
  "prioritized",
  "waiting-children",
]);

const TASK_STATUSES = Object.freeze([
  "queued",
  "leased",
  "running",
  "failed",
  "terminal",
  "dead_letter",
  "done",
  "skipped",
]);

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) return fallback;
  return Math.min(number, maximum);
}

function nonnegativeInteger(value) {
  const number = Number(value);
  return Number.isSafeInteger(number) && number >= 0 ? number : 0;
}

function observedDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) throw new TypeError("now must return a valid date");
  return date;
}

function normalizedCounts(value, fields) {
  return Object.fromEntries(fields.map((field) => [field, nonnegativeInteger(value?.[field])]));
}

function perMinute(count, windowMs) {
  return count / (windowMs / 60_000);
}

function ratio(count, claimed) {
  return claimed > 0 ? count / claimed : 0;
}

export class PostgresContentEnrichObservabilityRepository {
  constructor({ queryFn } = {}) {
    if (typeof queryFn !== "function") throw new TypeError("queryFn is required");
    this.query = queryFn;
  }

  async loadSnapshot({ windowMs = 5 * 60_000 } = {}) {
    const durationMs = positiveInteger(windowMs, 5 * 60_000, 24 * 60 * 60_000);
    const result = await this.query(
      `WITH task_metrics AS (
         SELECT
           count(*) FILTER (WHERE status='queued')::bigint AS queued,
           count(*) FILTER (WHERE status='leased')::bigint AS leased,
           count(*) FILTER (WHERE status='running')::bigint AS running,
           count(*) FILTER (WHERE status='failed')::bigint AS failed,
           count(*) FILTER (WHERE status='terminal')::bigint AS terminal,
           count(*) FILTER (WHERE status='dead_letter')::bigint AS dead_letter,
           count(*) FILTER (WHERE status='done')::bigint AS done,
           count(*) FILTER (WHERE status='skipped')::bigint AS skipped,
           min(created_at) FILTER (WHERE status='queued') AS oldest_queued_at
         FROM crawler.content_enrich_tasks
         WHERE job_type='player-refresh'
       ), event_payloads AS (
         SELECT payload_json AS result_json
         FROM crawler.task_events
         WHERE queue_name='youtube-content-enrich'
           AND status IN ('claimed','checkpointed')
           AND created_at>=clock_timestamp()-($1::bigint*interval '1 millisecond')
       ), event_metrics AS (
         SELECT
           COALESCE(sum((result_json->>'claimed')::bigint),0)::bigint AS claimed,
           COALESCE(sum((result_json->>'done')::bigint),0)::bigint AS success,
           COALESCE(sum((result_json->>'retryable')::bigint),0)::bigint AS retry,
           COALESCE(sum((result_json->>'terminal')::bigint),0)::bigint AS outcome_terminal,
           COALESCE(sum((result_json->>'dead_letter')::bigint),0)::bigint AS outcome_dead_letter
         FROM event_payloads
       )
       SELECT task_metrics.*,event_metrics.*
       FROM task_metrics CROSS JOIN event_metrics`,
      [durationMs],
    );
    const row = result.rows[0] ?? {};
    const oldestQueuedAtMs = Date.parse(String(row.oldest_queued_at ?? ""));
    return {
      task_counts: normalizedCounts(row, TASK_STATUSES),
      oldest_queued_at: Number.isFinite(oldestQueuedAtMs)
        ? new Date(oldestQueuedAtMs).toISOString()
        : null,
      outcome_counts: {
        claimed: nonnegativeInteger(row.claimed),
        success: nonnegativeInteger(row.success),
        retry: nonnegativeInteger(row.retry),
        terminal: nonnegativeInteger(row.outcome_terminal),
        dead_letter: nonnegativeInteger(row.outcome_dead_letter),
      },
    };
  }
}

export class ContentEnrichMonitor {
  constructor({
    repository,
    now = () => new Date(),
    windowMs = 5 * 60_000,
    sampleIntervalMs = 60_000,
    backlogAlertThreshold = 10_000,
    queuedAgeAlertSeconds = 24 * 60 * 60,
    alertRepeatMs = 15 * 60_000,
  } = {}) {
    if (!repository || typeof repository.loadSnapshot !== "function") {
      throw new TypeError("a Content Enrich observability repository is required");
    }
    if (typeof now !== "function") throw new TypeError("now must be a function");
    this.repository = repository;
    this.now = now;
    this.windowMs = positiveInteger(windowMs, 5 * 60_000, 24 * 60 * 60_000);
    this.sampleIntervalMs = positiveInteger(sampleIntervalMs, 60_000, 24 * 60 * 60_000);
    this.backlogAlertThreshold = nonnegativeInteger(backlogAlertThreshold);
    this.queuedAgeAlertSeconds = nonnegativeInteger(queuedAgeAlertSeconds);
    this.alertRepeatMs = positiveInteger(alertRepeatMs, 15 * 60_000, 24 * 60 * 60_000);
    this.alertState = new Map();
    this.sample = null;
  }

  #alerts({ taskBacklog, oldestQueuedAgeSeconds, observedAt }) {
    const active = [];
    if (this.backlogAlertThreshold > 0 && taskBacklog >= this.backlogAlertThreshold) {
      active.push({
        code: "content_enrich_backlog_high",
        value: taskBacklog,
        threshold: this.backlogAlertThreshold,
      });
    }
    if (
      this.queuedAgeAlertSeconds > 0
      && oldestQueuedAgeSeconds != null
      && oldestQueuedAgeSeconds >= this.queuedAgeAlertSeconds
    ) {
      active.push({
        code: "content_enrich_queued_age_high",
        value: oldestQueuedAgeSeconds,
        threshold: this.queuedAgeAlertSeconds,
      });
    }

    const notifications = [];
    const activeCodes = new Set(active.map((alert) => alert.code));
    for (const alert of active) {
      const previous = this.alertState.get(alert.code);
      if (!previous) {
        notifications.push({ ...alert, state: "raised" });
        this.alertState.set(alert.code, { alert, notifiedAt: observedAt.getTime() });
      } else if (observedAt.getTime() - previous.notifiedAt >= this.alertRepeatMs) {
        notifications.push({ ...alert, state: "reminder" });
        this.alertState.set(alert.code, { alert, notifiedAt: observedAt.getTime() });
      } else {
        this.alertState.set(alert.code, { ...previous, alert });
      }
    }
    for (const [code, previous] of this.alertState) {
      if (activeCodes.has(code)) continue;
      notifications.push({ ...previous.alert, state: "resolved" });
      this.alertState.delete(code);
    }
    return { active, notifications };
  }

  async observe({ queueCounts = {}, dispatchSummary = null, dispatchOk = true } = {}) {
    const calledAt = observedDate(this.now());
    const sampleAgeMs = this.sample == null
      ? Number.POSITIVE_INFINITY
      : calledAt.getTime() - this.sample.observedAt.getTime();
    if (sampleAgeMs < 0 || sampleAgeMs >= this.sampleIntervalMs) {
      const snapshot = await this.repository.loadSnapshot({
        windowMs: this.windowMs,
        observedAt: calledAt,
      });
      this.sample = { observedAt: calledAt, snapshot };
    }
    const { observedAt, snapshot } = this.sample;
    const taskCounts = normalizedCounts(snapshot?.task_counts, TASK_STATUSES);
    const outcomeCounts = normalizedCounts(snapshot?.outcome_counts, [
      "claimed",
      "success",
      "retry",
      "terminal",
      "dead_letter",
    ]);
    const taskBacklog = taskCounts.queued
      + taskCounts.leased
      + taskCounts.running
      + taskCounts.failed;
    const oldestQueuedAtMs = Date.parse(String(snapshot?.oldest_queued_at ?? ""));
    const oldestQueuedAgeSeconds = Number.isFinite(oldestQueuedAtMs)
      ? Math.max(0, Math.floor((observedAt.getTime() - oldestQueuedAtMs) / 1_000))
      : null;
    const queueOpenJobs = OPEN_QUEUE_STATES.reduce(
      (total, state) => total + nonnegativeInteger(queueCounts?.[state]),
      0,
    );
    const claimed = outcomeCounts.claimed;
    const outcomeRates = {
      window_seconds: this.windowMs / 1_000,
      ...outcomeCounts,
      claimed_per_minute: perMinute(claimed, this.windowMs),
      success_per_minute: perMinute(outcomeCounts.success, this.windowMs),
      retry_per_minute: perMinute(outcomeCounts.retry, this.windowMs),
      terminal_per_minute: perMinute(outcomeCounts.terminal, this.windowMs),
      dead_letter_per_minute: perMinute(outcomeCounts.dead_letter, this.windowMs),
      success_ratio: ratio(outcomeCounts.success, claimed),
      retry_ratio: ratio(outcomeCounts.retry, claimed),
      terminal_ratio: ratio(outcomeCounts.terminal, claimed),
      dead_letter_ratio: ratio(outcomeCounts.dead_letter, claimed),
    };

    return {
      observed_at: observedAt.toISOString(),
      task_counts: taskCounts,
      task_backlog: taskBacklog,
      oldest_queued_at: Number.isFinite(oldestQueuedAtMs)
        ? new Date(oldestQueuedAtMs).toISOString()
        : null,
      oldest_queued_age_seconds: oldestQueuedAgeSeconds,
      queue_open_jobs: queueOpenJobs,
      mutex_contention: dispatchSummary?.reason === "dispatch_locked",
      dispatch_ok: dispatchOk === true,
      outcome_rates: outcomeRates,
      alerts: this.#alerts({ taskBacklog, oldestQueuedAgeSeconds, observedAt }),
    };
  }
}
