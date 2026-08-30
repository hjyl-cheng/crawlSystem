import { canonicalJsonEqual } from "./canonicalJson.js";
import { channelSnapshotQueueJobIdentityMatches } from "./channelSnapshotDispatch.js";

const JOB_NAMES = Object.freeze({
  channel_snapshot: "channel-snapshot",
  discover_page: "discover-page",
  migration_retry: "channel-snapshot-recovery",
  query_quality_chunk: "score-query-quality",
});

const QUEUE_KEYS = Object.freeze({
  channel_snapshot: "youtube-channel-crawl",
  discover_page: "youtube-discover-page",
  migration_retry: "youtube-channel-crawl",
  query_quality_chunk: "youtube-query-quality",
});

const PAYLOAD_KEYS = Object.freeze({
  channel_snapshot: Object.freeze([
    "candidate_id",
    "channel_id",
    "channel_url",
    "crawl_mode",
    "dispatch_batch_id",
    "dispatch_generation",
    "enforce_min_subscribers",
    "min_subscriber_count",
    "pipeline_cycle_id",
    "query_id",
    "query_text",
    "reject_if_no_recent_content",
  ]),
  discover_page: Object.freeze(["dispatch_generation", "intent_schema_version", "page_id"]),
  query_quality_chunk: Object.freeze([
    "dispatch_generation", "intent_schema_version", "quality_chunk_id",
  ]),
  migration_retry: Object.freeze([
    "candidate_id",
    "channel_id",
    "channel_url",
    "crawl_mode",
    "dispatch_batch_id",
    "dispatch_generation",
    "enforce_min_subscribers",
    "min_subscriber_count",
    "pipeline_cycle_id",
    "query_id",
    "query_text",
    "recovery_business_run_id",
    "recovery_reason",
    "reject_if_no_recent_content",
    "retry_intent_id",
  ]),
});

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`managed dispatch payload ${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`managed dispatch payload ${field} must be a positive integer`);
  }
  return parsed;
}

function assertExactPayloadKeys(payload, aggregateKind) {
  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    throw new TypeError(`managed dispatch ${aggregateKind} payload must be an object`);
  }
  const actual = Object.keys(payload).sort();
  const expected = PAYLOAD_KEYS[aggregateKind];
  const optional = aggregateKind === "channel_snapshot" && actual.includes("migration_intent_id")
    ? ["migration_intent_id"]
    : [];
  const allowed = [...(expected ?? []), ...optional].sort();
  if (!expected || actual.length !== allowed.length
      || actual.some((key, index) => key !== allowed[index])) {
    throw new TypeError(`managed dispatch ${aggregateKind} payload has invalid fields`);
  }
}

function assertDispatchPayload(row) {
  const aggregateKind = String(row?.aggregate_kind ?? "");
  const expectedQueue = QUEUE_KEYS[aggregateKind];
  if (!expectedQueue) throw new TypeError(`unsupported managed aggregate kind: ${aggregateKind}`);
  if (row.queue_registry_key !== expectedQueue) {
    throw new TypeError(`managed dispatch ${aggregateKind} has an invalid queue registry key`);
  }
  const payload = row.payload_json;
  assertExactPayloadKeys(payload, aggregateKind);

  if (aggregateKind === "discover_page") {
    if (requiredText(payload.page_id, "page_id") !== String(row.aggregate_id)) {
      throw new TypeError("managed dispatch discover_page payload conflicts with aggregate_id");
    }
    if (positiveInteger(payload.intent_schema_version, "intent_schema_version") !== 1) {
      throw new TypeError("managed dispatch discover_page payload has an unsupported schema version");
    }
    positiveInteger(payload.dispatch_generation, "dispatch_generation");
    return payload;
  }
  if (aggregateKind === "query_quality_chunk") {
    if (requiredText(payload.quality_chunk_id, "quality_chunk_id") !== String(row.aggregate_id)) {
      throw new TypeError("managed dispatch query_quality_chunk payload conflicts with aggregate_id");
    }
    if (positiveInteger(payload.intent_schema_version, "intent_schema_version") !== 1) {
      throw new TypeError("managed dispatch query_quality_chunk payload has an unsupported schema version");
    }
    positiveInteger(payload.dispatch_generation, "dispatch_generation");
    return payload;
  }

  if (aggregateKind === "channel_snapshot") {
    if (positiveInteger(payload.candidate_id, "candidate_id") !== Number(row.aggregate_id)) {
      throw new TypeError("managed dispatch channel_snapshot payload conflicts with aggregate_id");
    }
    positiveInteger(payload.dispatch_generation, "dispatch_generation");
    if (payload.migration_intent_id != null) {
      positiveInteger(payload.migration_intent_id, "migration_intent_id");
    }
    positiveInteger(payload.min_subscriber_count, "min_subscriber_count");
    if (payload.query_id !== null) positiveInteger(payload.query_id, "query_id");
    for (const field of [
      "channel_id", "channel_url", "dispatch_batch_id", "pipeline_cycle_id", "query_text",
    ]) requiredText(payload[field], field);
    if (payload.crawl_mode !== "full" || payload.enforce_min_subscribers !== true
        || typeof payload.reject_if_no_recent_content !== "boolean") {
      throw new TypeError("managed dispatch channel_snapshot payload has invalid controls");
    }
    return payload;
  }

  if (requiredText(payload.retry_intent_id, "retry_intent_id") !== String(row.aggregate_id)) {
    throw new TypeError("managed dispatch migration_retry payload conflicts with aggregate_id");
  }
  positiveInteger(payload.candidate_id, "candidate_id");
  positiveInteger(payload.dispatch_generation, "dispatch_generation");
  positiveInteger(payload.min_subscriber_count, "min_subscriber_count");
  for (const field of [
    "channel_id", "channel_url", "dispatch_batch_id", "pipeline_cycle_id", "query_text",
    "recovery_business_run_id", "recovery_reason",
  ]) requiredText(payload[field], field);
  if (payload.crawl_mode !== "full" || payload.query_id !== null
      || payload.enforce_min_subscribers !== true
      || typeof payload.reject_if_no_recent_content !== "boolean") {
    throw new TypeError("managed dispatch migration_retry payload has invalid recovery controls");
  }
  return payload;
}

function errorMessage(error) {
  return String(error?.message ?? error ?? "unknown dispatch error").slice(0, 2000);
}

function defaultRetryDelay(attempt) {
  return Math.min(300_000, 1000 * (2 ** Math.max(0, Number(attempt) - 1)));
}

function samePayload(left, right) {
  return canonicalJsonEqual(left, right);
}

function assertMatchingQueueJob(job, row, jobName) {
  const matches = row.aggregate_kind === "channel_snapshot"
    ? channelSnapshotQueueJobIdentityMatches(job, {
      jobId: row.deterministic_job_id,
      payload: row.payload_json,
      intentHash: row.intent_hash,
    })
    : String(job?.id ?? "") === String(row.deterministic_job_id)
      && job?.name === jobName
      && samePayload(job?.data, row.payload_json);
  if (!matches) {
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

async function persistedQueueJob(queue, row, jobName) {
  if (typeof queue?.getJob !== "function") {
    throw new TypeError("managed BullMQ queue.getJob is required");
  }
  const persisted = await queue.getJob(row.deterministic_job_id);
  if (!persisted) {
    throw new Error(`managed BullMQ Job was not persisted for dispatch ${row.dispatch_id}`);
  }
  return assertMatchingQueueJob(persisted, row, jobName);
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
      let addAttempted = false;
      try {
        if (!queue || typeof queue.add !== "function") {
          throw new Error(`unknown managed queue registry key: ${row.queue_registry_key}`);
        }
        if (!jobName) throw new Error(`unsupported managed aggregate kind: ${row.aggregate_kind}`);
        assertDispatchPayload(row);
        const options = { jobId: row.deterministic_job_id };
        if (Number.isFinite(Number(row.job_priority))) {
          options.priority = Number(row.job_priority);
        }
        addAttempted = true;
        await queue.add(jobName, row.payload_json, options);
        job = await persistedQueueJob(queue, row, jobName);
      } catch (error) {
        if (addAttempted) {
          try {
            job = await recoverAmbiguousQueueAdd(queue, row, jobName);
          } catch (lookupError) {
            error = lookupError;
          }
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
       RETURNING candidate_id,dispatch_generation`,
      [row.aggregate_id, status, reason, row.intent_hash],
    );
    if (status === "terminal" && updated.rows[0]) {
      const candidate = await client.query(
        `UPDATE crawler.channel_candidates
         SET status='failed',error_message=$2,validation_finished_at=now(),
             updated_at=now()
         WHERE candidate_id=$1 AND snapshot_dispatch_generation=$3
           AND snapshot_active_job_id IS NULL AND snapshot_active_job_attempt IS NULL
           AND status='queued'`,
        [updated.rows[0].candidate_id, reason, updated.rows[0].dispatch_generation],
      );
      if (candidate.rowCount !== 1) {
        throw new Error(`Recovery Intent ${row.aggregate_id} lost its Candidate fence`);
      }
    }
    return;
  }
  if (row.aggregate_kind === "channel_snapshot") {
    if (status !== "terminal") return;
    const candidate = await client.query(
      `UPDATE crawler.channel_candidates
       SET status=CASE WHEN status='accepted' THEN 'accepted' ELSE 'failed' END,
           error_message=$2,
           validation_finished_at=CASE WHEN status='accepted' THEN validation_finished_at ELSE now() END,
           snapshot_json=COALESCE(snapshot_json,'{}'::jsonb) || jsonb_build_object(
             'failure_type','retryable_system_failure',
             'system_failure',jsonb_build_object(
               'category','outbox','code','OUTBOX_DELIVERY_EXHAUSTED',
               'name','ManagedJobOutboxDispatchError','message',$2::text,
               'retryable',true
             )
           ),
           updated_at=now()
       WHERE candidate_id=$1 AND snapshot_dispatch_generation=$3
         AND snapshot_active_job_id=$4 AND snapshot_active_job_attempt=0
         AND status IN ('queued','accepted')`,
      [
        Number(row.aggregate_id),
        reason,
        Number(row.payload_json?.dispatch_generation),
        row.deterministic_job_id,
      ],
    );
    if (candidate.rowCount !== 1) {
      throw new Error(`Channel snapshot dispatch ${row.dispatch_id} lost its Candidate fence`);
    }
    await client.query(
      `WITH requeued_retry AS (
         UPDATE crawler.migration_system_retry_items
         SET failed_dispatch_batch_id=COALESCE(failed_dispatch_batch_id,$7),
             status='pending',failure_code='OUTBOX_DELIVERY_EXHAUSTED',
             failure_category='outbox',resolution=NULL,resolved_at=NULL,
             failure_evidence=COALESCE(failure_evidence,'{}'::jsonb)
               || jsonb_build_object(
                 'retry_delivery_failure',jsonb_build_object(
                   'category','outbox','code','OUTBOX_DELIVERY_EXHAUSTED',
                   'name','ManagedJobOutboxDispatchError','message',$3::text,
                   'dispatch_id',$4::text,'job_id',$5::text,'attempts',$6::int,
                   'retryable',true
                 )
               ),
             updated_at=now()
         WHERE candidate_id=$1 AND retry_dispatch_generation=$2
           AND status='dispatched'
           AND (failed_dispatch_batch_id IS NULL OR failed_dispatch_batch_id=$7)
         RETURNING system_retry_id
       ), inserted_retry AS (
         INSERT INTO crawler.migration_system_retry_items AS existing_retry (
           migration_intent_id,candidate_id,failed_dispatch_batch_id,failed_dispatch_generation,
           failed_job_id,failed_job_attempt,failure_code,failure_category,
           failure_evidence,status,updated_at
         )
         SELECT intent.migration_intent_id,$1,$7,$2,$5,0,
                'OUTBOX_DELIVERY_EXHAUSTED','outbox',
                jsonb_build_object(
                  'failure_type','retryable_system_failure',
                  'system_failure',jsonb_build_object(
                    'category','outbox','code','OUTBOX_DELIVERY_EXHAUSTED',
                    'name','ManagedJobOutboxDispatchError','message',$3::text,
                    'dispatch_id',$4::text,'job_id',$5::text,'attempts',$6::int,
                    'dispatch_batch_id',$7::text,
                    'retryable',true
                  )
                ),
                'pending',now()
         FROM crawler.migration_channel_intents intent
         WHERE intent.target_candidate_id=$1
           AND NOT EXISTS (SELECT 1 FROM requeued_retry)
         ON CONFLICT (
           migration_intent_id,failed_dispatch_generation,failed_job_id,failed_job_attempt
         ) DO UPDATE
         SET failed_dispatch_batch_id=COALESCE(
               existing_retry.failed_dispatch_batch_id,
               EXCLUDED.failed_dispatch_batch_id
             ),
             failure_code=EXCLUDED.failure_code,
             failure_category=EXCLUDED.failure_category,
             failure_evidence=EXCLUDED.failure_evidence,
             status='pending',resolution=NULL,resolved_at=NULL,updated_at=now()
         WHERE existing_retry.failed_dispatch_batch_id IS NULL
            OR existing_retry.failed_dispatch_batch_id=EXCLUDED.failed_dispatch_batch_id
         RETURNING system_retry_id
       )
       SELECT system_retry_id FROM requeued_retry
       UNION ALL
       SELECT system_retry_id FROM inserted_retry`,
      [
        Number(row.aggregate_id),
        Number(row.payload_json?.dispatch_generation),
        reason,
        String(row.dispatch_id),
        String(row.deterministic_job_id),
        Number(row.attempts),
        requiredText(row.payload_json?.dispatch_batch_id, "dispatch_batch_id"),
      ],
    );
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
      } else if (row.aggregate_kind === "channel_snapshot") {
        const candidate = await client.query(
          "SELECT priority FROM crawler.channel_candidates WHERE candidate_id=$1",
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
