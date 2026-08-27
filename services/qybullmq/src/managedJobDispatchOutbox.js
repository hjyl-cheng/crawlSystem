const JOB_NAMES = Object.freeze({
  discover_page: "discover-page",
  migration_retry: "channel-snapshot-recovery",
  query_quality_chunk: "score-query-quality",
});

function errorMessage(error) {
  return String(error?.message ?? error ?? "unknown dispatch error").slice(0, 2000);
}

function defaultRetryDelay(attempt) {
  return Math.min(300_000, 1000 * (2 ** Math.max(0, Number(attempt) - 1)));
}

function canonical(value) {
  if (Array.isArray(value)) return value.map(canonical);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.keys(value).sort().map((key) => [key, canonical(value[key])]));
}

function samePayload(left, right) {
  return JSON.stringify(canonical(left)) === JSON.stringify(canonical(right));
}

function assertMatchingQueueJob(job, row, jobName) {
  if (String(job?.id ?? "") !== String(row.deterministic_job_id)
      || job?.name !== jobName
      || !samePayload(job?.data, row.payload_json)) {
    throw new Error(`deterministic BullMQ Job conflicts with managed dispatch ${row.dispatch_id}`);
  }
  return job;
}

async function recoverAmbiguousQueueAdd(queue, row, jobName) {
  if (typeof queue?.getJob !== "function") return null;
  const existing = await queue.getJob(row.deterministic_job_id);
  if (!existing) return null;
  return assertMatchingQueueJob(existing, row, jobName);
}

export class ManagedJobOutboxDispatcher {
  constructor({
    repository,
    queues,
    maxAttempts = 8,
    retryDelayMs = defaultRetryDelay,
    now = () => new Date(),
  } = {}) {
    if (!repository || typeof repository.claimNext !== "function"
        || typeof repository.markSent !== "function"
        || typeof repository.markFailed !== "function") {
      throw new TypeError("a managed dispatch repository is required");
    }
    if (!queues || typeof queues !== "object") throw new TypeError("queues are required");
    this.repository = repository;
    this.queues = queues;
    this.maxAttempts = Math.max(1, Number(maxAttempts) || 8);
    this.retryDelayMs = retryDelayMs;
    this.now = now;
  }

  async dispatchAvailable({ limit = 100 } = {}) {
    const summary = { claimed: 0, sent: 0, failed: 0, dead: 0 };
    const boundedLimit = Math.max(0, Math.min(1000, Number(limit) || 0));
    for (let index = 0; index < boundedLimit; index += 1) {
      const row = await this.repository.claimNext({ now: this.now() });
      if (!row) break;
      summary.claimed += 1;
      const queue = this.queues[row.queue_registry_key];
      const jobName = JOB_NAMES[row.aggregate_kind];
      let job;
      try {
        if (!queue || typeof queue.add !== "function") {
          throw new Error(`unknown managed queue registry key: ${row.queue_registry_key}`);
        }
        if (!jobName) throw new Error(`unsupported managed aggregate kind: ${row.aggregate_kind}`);
        const options = { jobId: row.deterministic_job_id };
        if (Number.isFinite(Number(row.job_priority))) {
          options.priority = Number(row.job_priority);
        }
        job = assertMatchingQueueJob(
          await queue.add(jobName, row.payload_json, options),
          row,
          jobName,
        );
      } catch (error) {
        try {
          job = await recoverAmbiguousQueueAdd(queue, row, jobName);
        } catch (lookupError) {
          error = lookupError;
        }
        if (job) {
          await this.repository.markSent({
            dispatchId: row.dispatch_id,
            attempt: Number(row.attempts),
            jobId: String(job.id),
          });
          summary.sent += 1;
          continue;
        }
        const terminal = Number(row.attempts) >= this.maxAttempts;
        const delayMs = Math.max(0, Number(this.retryDelayMs(row.attempts, error)) || 0);
        await this.repository.markFailed({
          dispatchId: row.dispatch_id,
          attempt: Number(row.attempts),
          error: errorMessage(error),
          terminal,
          nextAttemptAt: terminal ? null : new Date(this.now().getTime() + delayMs),
        });
        if (terminal) summary.dead += 1;
        else summary.failed += 1;
        continue;
      }
      await this.repository.markSent({
        dispatchId: row.dispatch_id,
        attempt: Number(row.attempts),
        jobId: String(job?.id ?? row.deterministic_job_id),
      });
      summary.sent += 1;
    }
    return summary;
  }
}

function assertFencedUpdate(result, dispatchId, action) {
  if (result.rowCount !== 1) {
    throw new Error(`managed dispatch ${dispatchId} lost its fence while ${action}`);
  }
}

async function updateAggregate(client, row, { status, jobId = null, reason = null }) {
  if (row.aggregate_kind === "discover_page") {
    await client.query(
      `UPDATE crawler.query_pages
       SET dispatch_status=$2,dispatched_job_id=COALESCE($3,dispatched_job_id),
           dispatch_reason=$4,updated_at=now()
       WHERE page_id=$1 AND page_intent_hash=$5 AND dispatch_status<>'terminal'`,
      [row.aggregate_id, status, jobId, reason, row.intent_hash],
    );
    return;
  }
  if (row.aggregate_kind === "query_quality_chunk") {
    await client.query(
      `UPDATE crawler.query_quality_chunks
       SET dispatch_status=$2,dispatched_job_id=COALESCE($3,dispatched_job_id),
           dispatch_reason=$4,
           status=CASE WHEN status='pending' AND $2='enqueued' THEN 'queued' ELSE status END,
           updated_at=now()
       WHERE quality_chunk_id=$1 AND chunk_intent_hash=$5 AND dispatch_status<>'terminal'`,
      [row.aggregate_id, status, jobId, reason, row.intent_hash],
    );
    return;
  }
  if (row.aggregate_kind === "migration_retry") {
    const updated = await client.query(
      `UPDATE crawler.migration_retry_intents
       SET dispatch_status=$2,
           status=CASE
             WHEN $2='enqueued' AND status IN ('requested','dispatched') THEN 'dispatched'
             WHEN $2='terminal' AND status NOT IN ('finished','failed') THEN 'failed'
             ELSE status
           END,
           dispatched_at=CASE
             WHEN $2='enqueued' THEN COALESCE(dispatched_at,now())
             ELSE dispatched_at
           END,
           finished_at=CASE WHEN $2='terminal' THEN COALESCE(finished_at,now()) ELSE finished_at END,
           last_error=$3,updated_at=now()
       WHERE retry_intent_id=$1 AND intent_hash=$4
         AND status NOT IN ('finished','failed')
       RETURNING candidate_id`,
      [row.aggregate_id, status, reason, row.intent_hash],
    );
    if (status === "terminal" && updated.rows[0]) {
      await client.query(
        `UPDATE crawler.channel_candidates
         SET status=CASE WHEN status='queued' THEN 'failed' ELSE status END,
             error_message=CASE WHEN status='queued' THEN $2 ELSE error_message END,
             validation_finished_at=CASE WHEN status='queued' THEN now() ELSE validation_finished_at END,
             updated_at=now()
         WHERE candidate_id=$1`,
        [updated.rows[0].candidate_id, reason],
      );
    }
    return;
  }
  throw new Error(`unsupported managed aggregate kind: ${row.aggregate_kind}`);
}

export class PostgresManagedJobDispatchRepository {
  constructor({ withTransaction, sendingTimeoutMs = 60_000 } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
    this.sendingTimeoutMs = Math.max(1000, Number(sendingTimeoutMs) || 60_000);
  }

  claimNext({ now = new Date() } = {}) {
    const staleBefore = new Date(now.getTime() - this.sendingTimeoutMs);
    return this.withTransaction(async (client) => {
      const claimed = await client.query(
        `WITH candidate AS (
           SELECT dispatch_id
           FROM crawler.proxy_job_dispatch_outbox
           WHERE (status='pending' AND (next_attempt_at IS NULL OR next_attempt_at<=$1))
              OR (status='sending' AND updated_at<=$2)
           ORDER BY created_at,dispatch_id
           FOR UPDATE SKIP LOCKED
           LIMIT 1
         )
         UPDATE crawler.proxy_job_dispatch_outbox outbox
         SET status='sending',attempts=outbox.attempts+1,last_error=NULL,updated_at=now()
         FROM candidate
         WHERE outbox.dispatch_id=candidate.dispatch_id
         RETURNING outbox.*`,
        [now, staleBefore],
      );
      const row = claimed.rows[0];
      if (!row) return null;
      if (row.aggregate_kind === "discover_page") {
        const page = await client.query(
          "SELECT priority FROM crawler.query_pages WHERE page_id=$1",
          [row.aggregate_id],
        );
        row.job_priority = page.rows[0]?.priority ?? null;
      } else if (row.aggregate_kind === "migration_retry") {
        const candidate = await client.query(
          `SELECT candidate.priority
           FROM crawler.migration_retry_intents intent
           JOIN crawler.channel_candidates candidate ON candidate.candidate_id=intent.candidate_id
           WHERE intent.retry_intent_id=$1`,
          [row.aggregate_id],
        );
        row.job_priority = candidate.rows[0]?.priority ?? null;
      }
      return row;
    });
  }

  markSent({ dispatchId, attempt, jobId }) {
    return this.withTransaction(async (client) => {
      const updated = await client.query(
        `UPDATE crawler.proxy_job_dispatch_outbox
         SET status='sent',sent_at=COALESCE(sent_at,now()),next_attempt_at=NULL,
             last_error=NULL,updated_at=now()
         WHERE dispatch_id=$1 AND status='sending' AND attempts=$2
         RETURNING *`,
        [dispatchId, attempt],
      );
      assertFencedUpdate(updated, dispatchId, "marking sent");
      await updateAggregate(client, updated.rows[0], {
        status: "enqueued",
        jobId,
        reason: null,
      });
    });
  }

  markFailed({ dispatchId, attempt, error, terminal, nextAttemptAt }) {
    return this.withTransaction(async (client) => {
      const updated = await client.query(
        `UPDATE crawler.proxy_job_dispatch_outbox
         SET status=CASE WHEN $3::boolean THEN 'dead' ELSE 'pending' END,
             next_attempt_at=$4,last_error=$5,updated_at=now()
         WHERE dispatch_id=$1 AND status='sending' AND attempts=$2
         RETURNING *`,
        [dispatchId, attempt, terminal, nextAttemptAt, error],
      );
      assertFencedUpdate(updated, dispatchId, "marking failed");
      await updateAggregate(client, updated.rows[0], {
        status: terminal ? "terminal" : "deferred",
        reason: error,
      });
    });
  }
}

export class InMemoryManagedJobDispatchRepository {
  constructor({ rows = [] } = {}) {
    this.rows = new Map(rows.map((row) => [row.dispatch_id, structuredClone(row)]));
    this.aggregates = new Map();
    this.failNextMarkSent = false;
  }

  async claimNext({ now = new Date() } = {}) {
    const row = [...this.rows.values()]
      .filter((value) => value.status === "pending"
        && (!value.next_attempt_at || new Date(value.next_attempt_at) <= now))
      .sort((left, right) => left.dispatch_id.localeCompare(right.dispatch_id))[0];
    if (!row) return null;
    row.status = "sending";
    row.attempts = Number(row.attempts ?? 0) + 1;
    return structuredClone(row);
  }

  async markSent({ dispatchId, attempt, jobId }) {
    if (this.failNextMarkSent) {
      this.failNextMarkSent = false;
      throw new Error("simulated mark-sent crash");
    }
    const row = this.#fenced(dispatchId, attempt);
    row.status = "sent";
    row.sent_at = new Date().toISOString();
    this.aggregates.set(`${row.aggregate_kind}:${row.aggregate_id}`, {
      dispatch_status: "enqueued",
      dispatched_job_id: jobId,
    });
  }

  async markFailed({ dispatchId, attempt, error, terminal, nextAttemptAt }) {
    const row = this.#fenced(dispatchId, attempt);
    row.status = terminal ? "dead" : "pending";
    row.last_error = error;
    row.next_attempt_at = nextAttemptAt;
    this.aggregates.set(`${row.aggregate_kind}:${row.aggregate_id}`, {
      dispatch_status: terminal ? "terminal" : "deferred",
      dispatch_reason: error,
    });
  }

  recoverSending(dispatchId) {
    const row = this.rows.get(dispatchId);
    if (row?.status === "sending") row.status = "pending";
  }

  #fenced(dispatchId, attempt) {
    const row = this.rows.get(dispatchId);
    if (!row || row.status !== "sending" || Number(row.attempts) !== Number(attempt)) {
      throw new Error(`managed dispatch ${dispatchId} lost its fence`);
    }
    return row;
  }
}
