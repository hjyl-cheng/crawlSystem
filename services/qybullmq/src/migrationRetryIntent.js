import { createHash, randomUUID as nodeRandomUUID } from "node:crypto";
import { canonicalJsonString } from "./canonicalJson.js";
import { queuesByRole, safeJobId } from "./queues.js";

export class MigrationRetryIntentConflictError extends Error {
  constructor(requestKey, message = "Recovery Intent conflicts with persisted state") {
    super(`${message}: ${requestKey}`);
    this.name = "MigrationRetryIntentConflictError";
    this.code = "MIGRATION_RETRY_INTENT_CONFLICT";
    this.requestKey = requestKey;
  }
}

function requiredText(value, field) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${field} is required`);
  return normalized;
}

function positiveInteger(value, field) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${field} must be a positive integer`);
  }
  return parsed;
}

function intentHash(value) {
  return `sha256:${createHash("sha256")
    .update(canonicalJsonString(value))
    .digest("hex")}`;
}

function normalizedInput(input) {
  return {
    requestKey: requiredText(input?.requestKey, "requestKey"),
    candidateId: positiveInteger(input?.candidateId, "candidateId"),
    previousBusinessRunId: requiredText(
      input?.previousBusinessRunId,
      "previousBusinessRunId",
    ),
    reason: requiredText(input?.reason, "reason"),
    minSubscriberCount: positiveInteger(input?.minSubscriberCount ?? 1000, "minSubscriberCount"),
  };
}

function assertReplayMatches(intent, input) {
  const payload = intent.job_payload_json ?? intent.payload;
  if (Number(intent.candidate_id) !== input.candidateId
      || intent.previous_business_run_id !== input.previousBusinessRunId
      || intent.reason !== input.reason
      || Number(payload?.min_subscriber_count) !== input.minSubscriberCount) {
    throw new MigrationRetryIntentConflictError(input.requestKey);
  }
}

function recoveryPayload(target, input, {
  retryIntentId,
  businessRunId,
  dispatchGeneration,
}) {
  return {
    candidate_id: input.candidateId,
    retry_intent_id: retryIntentId,
    recovery_business_run_id: businessRunId,
    dispatch_generation: dispatchGeneration,
    dispatch_batch_id: requiredText(target.dispatch_batch_id, "candidate.dispatch_batch_id"),
    channel_id: requiredText(target.channel_id, "candidate.channel_id"),
    channel_url: requiredText(target.channel_url, "candidate.channel_url"),
    crawl_mode: "full",
    query_id: null,
    query_text: "controlled migration recovery",
    pipeline_cycle_id: requiredText(target.pipeline_cycle_id, "candidate.pipeline_cycle_id"),
    enforce_min_subscribers: true,
    min_subscriber_count: input.minSubscriberCount,
    reject_if_no_recent_content: target.source_json?.source === "legacy_results_db",
    recovery_reason: input.reason,
  };
}

export class MigrationRetryIntentStore {
  constructor({
    repository,
    randomUUID = nodeRandomUUID,
    now = () => new Date(),
  } = {}) {
    if (!repository || typeof repository.transaction !== "function") {
      throw new TypeError("repository.transaction is required");
    }
    this.repository = repository;
    this.randomUUID = randomUUID;
    this.now = now;
  }

  async prepare(value) {
    const input = normalizedInput(value);
    return this.repository.transaction(async (transaction) => {
      const existing = await transaction.findByRequestKey(input.requestKey);
      if (existing) {
        assertReplayMatches(existing.intent, input);
        if (!existing.outbox) {
          throw new MigrationRetryIntentConflictError(
            input.requestKey,
            "Recovery Intent Outbox is missing",
          );
        }
        return { created: false, intent: existing.intent, outbox: existing.outbox };
      }

      const target = await transaction.loadRecoveryTarget({
        candidateId: input.candidateId,
        previousBusinessRunId: input.previousBusinessRunId,
      });
      if (!target
          || target.status !== "failed"
          || target.binding_status !== "terminal") {
        throw new MigrationRetryIntentConflictError(
          input.requestKey,
          "Candidate and previous Binding are not terminal",
        );
      }
      const active = await transaction.findActiveForCandidate(input.candidateId);
      if (active) {
        throw new MigrationRetryIntentConflictError(
          input.requestKey,
          "Candidate already has an active Recovery Intent",
        );
      }

      const retryIntentId = requiredText(this.randomUUID(), "retryIntentId");
      const businessRunId = `run:${requiredText(this.randomUUID(), "businessRunId")}`;
      const dispatchGeneration = Number(target.snapshot_dispatch_generation ?? 0) + 1;
      positiveInteger(dispatchGeneration, "dispatchGeneration");
      const businessRunKey = `full-candidate:${input.candidateId}:recovery:${retryIntentId}`;
      const jobId = safeJobId(
        "channel-recovery",
        input.candidateId,
        retryIntentId,
        `g${dispatchGeneration}`,
      );
      const requestedAt = new Date(this.now());
      if (Number.isNaN(requestedAt.getTime())) throw new TypeError("now must be a valid date");
      const payload = recoveryPayload(target, input, {
        retryIntentId,
        businessRunId,
        dispatchGeneration,
      });
      const immutable = {
        retry_intent_id: retryIntentId,
        request_key: input.requestKey,
        candidate_id: input.candidateId,
        previous_business_run_id: input.previousBusinessRunId,
        new_business_run_id: businessRunId,
        new_business_run_key: businessRunKey,
        new_job_id: jobId,
        dispatch_generation: dispatchGeneration,
        reason: input.reason,
        payload,
      };
      const hash = intentHash(immutable);
      const intent = {
        ...immutable,
        intent_hash: hash,
        status: "requested",
        dispatch_status: "pending",
        requested_at: requestedAt.toISOString(),
        dispatched_at: null,
        finished_at: null,
        last_error: null,
      };
      const outbox = {
        dispatch_id: `migration-retry-dispatch:${retryIntentId}`,
        aggregate_kind: "migration_retry",
        aggregate_id: retryIntentId,
        intent_hash: hash,
        queue_registry_key: queuesByRole.channelCrawl,
        deterministic_job_id: jobId,
        payload_json: payload,
        status: "pending",
      };
      await transaction.persist(intent, outbox);
      return { created: true, intent, outbox };
    });
  }
}

class PostgresMigrationRetryIntentTransaction {
  constructor(client) {
    this.client = client;
  }

  async findByRequestKey(requestKey) {
    await this.client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
      `migration-retry-request:${requestKey}`,
    ]);
    const result = await this.client.query(
      `SELECT *
       FROM crawler.migration_retry_intents
       WHERE request_key=$1
       FOR UPDATE`,
      [requestKey],
    );
    const intent = result.rows[0];
    if (!intent) return null;
    const dispatched = await this.client.query(
      `SELECT *
       FROM crawler.proxy_job_dispatch_outbox
       WHERE aggregate_kind='migration_retry'
         AND aggregate_id=$1 AND intent_hash=$2`,
      [intent.retry_intent_id, intent.intent_hash],
    );
    return { intent, outbox: dispatched.rows[0] ?? null };
  }

  async loadRecoveryTarget({ candidateId, previousBusinessRunId }) {
    await this.client.query("SELECT pg_advisory_xact_lock(hashtext($1))", [
      `migration-retry-candidate:${candidateId}`,
    ]);
    const result = await this.client.query(
      `SELECT candidate.*,
              binding.status AS binding_status,
              binding.terminal_reason AS binding_terminal_reason
       FROM crawler.channel_candidates candidate
       JOIN crawler.business_run_bindings binding
         ON binding.business_run_id=$2
        AND binding.candidate_id=candidate.candidate_id
        AND binding.channel_id=candidate.channel_id
       WHERE candidate.candidate_id=$1
       FOR UPDATE OF candidate,binding`,
      [candidateId, previousBusinessRunId],
    );
    return result.rows[0] ?? null;
  }

  async findActiveForCandidate(candidateId) {
    const result = await this.client.query(
      `SELECT retry_intent_id,request_key,status
       FROM crawler.migration_retry_intents
       WHERE candidate_id=$1 AND status IN ('requested','dispatched','running')
       FOR UPDATE`,
      [candidateId],
    );
    return result.rows[0] ?? null;
  }

  async persist(intent, outbox) {
    const candidate = await this.client.query(
      `UPDATE crawler.channel_candidates
       SET status='queued',snapshot_dispatch_generation=$2,
           snapshot_active_job_id=NULL,snapshot_active_job_attempt=NULL,
           reject_reason=NULL,error_message=NULL,next_retry_at=NULL,
           validation_started_at=NULL,validation_finished_at=NULL,accepted_at=NULL,
           updated_at=now()
       WHERE candidate_id=$1 AND status='failed'
         AND snapshot_dispatch_generation=$2-1
       RETURNING candidate_id`,
      [intent.candidate_id, intent.dispatch_generation],
    );
    if (candidate.rowCount !== 1) {
      throw new MigrationRetryIntentConflictError(
        intent.request_key,
        "Candidate changed before Recovery Intent commit",
      );
    }
    await this.client.query(
      `INSERT INTO crawler.migration_retry_intents (
         retry_intent_id,request_key,candidate_id,previous_business_run_id,
         new_business_run_id,new_business_run_key,new_job_id,dispatch_generation,
         reason,intent_hash,job_payload_json,status,dispatch_status,requested_at,
         dispatched_at,finished_at,last_error,updated_at
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13,$14,$15,$16,$17,now()
       )`,
      [
        intent.retry_intent_id,
        intent.request_key,
        intent.candidate_id,
        intent.previous_business_run_id,
        intent.new_business_run_id,
        intent.new_business_run_key,
        intent.new_job_id,
        intent.dispatch_generation,
        intent.reason,
        intent.intent_hash,
        JSON.stringify(intent.payload),
        intent.status,
        intent.dispatch_status,
        intent.requested_at,
        intent.dispatched_at,
        intent.finished_at,
        intent.last_error,
      ],
    );
    await this.client.query(
      `INSERT INTO crawler.proxy_job_dispatch_outbox (
         dispatch_id,aggregate_kind,aggregate_id,intent_hash,queue_registry_key,
         deterministic_job_id,payload_json,status,updated_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,'pending',now())`,
      [
        outbox.dispatch_id,
        outbox.aggregate_kind,
        outbox.aggregate_id,
        outbox.intent_hash,
        outbox.queue_registry_key,
        outbox.deterministic_job_id,
        JSON.stringify(outbox.payload_json),
      ],
    );
  }
}

export class PostgresMigrationRetryIntentRepository {
  constructor({ withTransaction } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    this.withTransaction = withTransaction;
  }

  transaction(action) {
    return this.withTransaction((client) => action(new PostgresMigrationRetryIntentTransaction(client)));
  }

  loadActive({ limit = 100 } = {}) {
    const boundedLimit = Math.max(1, Math.min(1000, Number(limit) || 100));
    return this.withTransaction(async (client) => {
      const result = await client.query(
        `SELECT retry_intent_id,new_job_id,dispatch_generation,status
         FROM crawler.migration_retry_intents
         WHERE status IN ('requested','dispatched','running')
           AND dispatch_status='enqueued'
         ORDER BY updated_at,retry_intent_id
         LIMIT $1`,
        [boundedLimit],
      );
      return result.rows;
    });
  }

  markRunning(intent) {
    return this.withTransaction((client) => markMigrationRetryIntentRunning(
      client.query.bind(client),
      migrationRetryJobFence(intent),
    ));
  }

  finish(intent, outcome) {
    return this.withTransaction((client) => finishMigrationRetryIntent(
      client.query.bind(client),
      migrationRetryJobFence(intent),
      outcome,
    ));
  }
}

function migrationRetryJobFence(intent) {
  return {
    id: requiredText(intent?.new_job_id, "intent.new_job_id"),
    data: {
      retry_intent_id: requiredText(intent?.retry_intent_id, "intent.retry_intent_id"),
      dispatch_generation: positiveInteger(
        intent?.dispatch_generation,
        "intent.dispatch_generation",
      ),
    },
  };
}

function assertRecoveryJobMatchesIntent(job, intent) {
  const expected = migrationRetryJobFence(intent);
  if (String(job?.id ?? "") !== expected.id
      || String(job?.data?.retry_intent_id ?? "") !== expected.data.retry_intent_id
      || Number(job?.data?.dispatch_generation) !== expected.data.dispatch_generation) {
    throw new MigrationRetryIntentConflictError(
      expected.data.retry_intent_id,
      `BullMQ Job does not match Recovery Intent ${expected.id}`,
    );
  }
}

export class MigrationRetryIntentJobReconciler {
  constructor({ repository, queue } = {}) {
    if (!repository || typeof repository.loadActive !== "function"
        || typeof repository.markRunning !== "function"
        || typeof repository.finish !== "function") {
      throw new TypeError("a Recovery Intent lifecycle repository is required");
    }
    if (!queue || typeof queue.getJob !== "function") {
      throw new TypeError("a BullMQ Channel queue is required");
    }
    this.repository = repository;
    this.queue = queue;
  }

  async reconcileAvailable({ limit = 100 } = {}) {
    const intents = await this.repository.loadActive({ limit });
    const summary = { scanned: 0, finished: 0, failed: 0, pending: 0, missing: 0 };
    for (const intent of intents) {
      summary.scanned += 1;
      const job = await this.queue.getJob(intent.new_job_id);
      if (!job) {
        summary.missing += 1;
        continue;
      }
      assertRecoveryJobMatchesIntent(job, intent);
      const state = await job.getState();
      if (state === "completed") {
        if (!await this.repository.finish(intent, { outcome: "finished" })) {
          throw new MigrationRetryIntentConflictError(
            intent.retry_intent_id,
            "Recovery Intent lost its terminal replay fence",
          );
        }
        summary.finished += 1;
      } else if (state === "failed") {
        const error = new Error(String(job.failedReason || "BullMQ Recovery Job failed"));
        if (!await this.repository.finish(intent, { outcome: "failed", error })) {
          throw new MigrationRetryIntentConflictError(
            intent.retry_intent_id,
            "Recovery Intent lost its terminal replay fence",
          );
        }
        summary.failed += 1;
      } else {
        if (state === "active" && intent.status !== "running") {
          await this.repository.markRunning(intent);
        }
        summary.pending += 1;
      }
    }
    return summary;
  }
}

function recoveryJobFence(job) {
  const retryIntentId = String(job?.data?.retry_intent_id ?? "").trim();
  if (!retryIntentId) return null;
  return {
    retryIntentId,
    jobId: requiredText(job?.id, "job.id"),
    dispatchGeneration: positiveInteger(
      job?.data?.dispatch_generation,
      "job.data.dispatch_generation",
    ),
  };
}

export async function markMigrationRetryIntentRunning(query, job) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = recoveryJobFence(job);
  if (!fence) return false;
  const result = await query(
    `UPDATE crawler.migration_retry_intents
     SET status='running',dispatch_status='enqueued',
         dispatched_at=COALESCE(dispatched_at,now()),last_error=NULL,updated_at=now()
     WHERE retry_intent_id=$1 AND new_job_id=$2 AND dispatch_generation=$3
       AND status IN ('requested','dispatched','running')
     RETURNING retry_intent_id`,
    [fence.retryIntentId, fence.jobId, fence.dispatchGeneration],
  );
  return result.rowCount === 1;
}

export async function finishMigrationRetryIntent(query, job, {
  outcome,
  error = null,
} = {}) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const fence = recoveryJobFence(job);
  if (!fence) return false;
  const normalizedOutcome = String(outcome ?? "").trim();
  if (!["finished", "failed"].includes(normalizedOutcome)) {
    throw new TypeError("outcome must be finished or failed");
  }
  const message = error == null ? null : String(error?.message ?? error).slice(0, 2000);
  const result = await query(
    `UPDATE crawler.migration_retry_intents
     SET status=$4,dispatch_status='terminal',finished_at=COALESCE(finished_at,now()),
         last_error=$5,updated_at=now()
     WHERE retry_intent_id=$1 AND new_job_id=$2 AND dispatch_generation=$3
       AND (status IN ('requested','dispatched','running') OR status=$4)
     RETURNING retry_intent_id`,
    [
      fence.retryIntentId,
      fence.jobId,
      fence.dispatchGeneration,
      normalizedOutcome,
      message,
    ],
  );
  return result.rowCount === 1;
}

export class InMemoryMigrationRetryIntentRepository {
  constructor({ candidates = [], bindings = [] } = {}) {
    this.candidates = new Map(candidates.map((row) => [Number(row.candidate_id), structuredClone(row)]));
    this.bindings = new Map(bindings.map((row) => [row.business_run_id, structuredClone(row)]));
    this.intents = new Map();
    this.outbox = new Map();
  }

  transaction(action) {
    return action({
      findByRequestKey: async (requestKey) => {
        const intent = [...this.intents.values()].find((row) => row.request_key === requestKey);
        if (!intent) return null;
        const outbox = [...this.outbox.values()].find(
          (row) => row.aggregate_id === intent.retry_intent_id,
        );
        return { intent: structuredClone(intent), outbox: structuredClone(outbox) };
      },
      loadRecoveryTarget: async ({ candidateId, previousBusinessRunId }) => {
        const candidate = this.candidates.get(candidateId);
        const binding = this.bindings.get(previousBusinessRunId);
        if (!candidate || !binding
            || Number(binding.candidate_id) !== candidateId
            || binding.channel_id !== candidate.channel_id) return null;
        return structuredClone({
          ...candidate,
          binding_status: binding.status,
          binding_terminal_reason: binding.terminal_reason,
        });
      },
      findActiveForCandidate: async (candidateId) => [...this.intents.values()].find(
        (row) => Number(row.candidate_id) === candidateId
          && ["requested", "dispatched", "running"].includes(row.status),
      ) ?? null,
      persist: async (intent, outbox) => {
        const candidate = this.candidates.get(Number(intent.candidate_id));
        if (!candidate || candidate.status !== "failed") {
          throw new MigrationRetryIntentConflictError(intent.request_key);
        }
        candidate.status = "queued";
        candidate.snapshot_dispatch_generation = Number(intent.dispatch_generation);
        candidate.snapshot_active_job_id = null;
        candidate.snapshot_active_job_attempt = null;
        candidate.error_message = null;
        candidate.next_retry_at = null;
        candidate.validation_started_at = null;
        candidate.validation_finished_at = null;
        this.intents.set(intent.retry_intent_id, structuredClone(intent));
        this.outbox.set(outbox.dispatch_id, structuredClone(outbox));
      },
    });
  }
}
