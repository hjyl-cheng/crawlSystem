import {
  contentEnrichDetailOutcome,
  contentEnrichFailureOutcome,
  contentEnrichRetryDelayMs,
} from "./contentEnrichPolicy.js";
import { retryableRotaFailure } from "./managedWorkerExecution.js";

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) return fallback;
  return Math.min(number, maximum);
}

function observedDate(value, field = "now") {
  const observedAt = new Date(value);
  if (Number.isNaN(observedAt.getTime())) throw new TypeError(`${field} must return a valid date`);
  return observedAt;
}

function emptySettlementSummary() {
  return {
    done: 0,
    terminal: 0,
    retryable: 0,
    dead_letter: 0,
    skipped: 0,
  };
}

function combineSettlementSummaries(...summaries) {
  return summaries.reduce((combined, summary) => {
    for (const key of Object.keys(combined)) combined[key] += Number(summary?.[key] ?? 0);
    return combined;
  }, emptySettlementSummary());
}

function taskReferences(job) {
  const channelId = requiredText(job?.data?.channel_id, "job.data.channel_id");
  const values = Array.isArray(job?.data?.tasks) ? job.data.tasks : [];
  if (values.length === 0) throw new TypeError("job.data.tasks is required");
  const tasks = values.map((value) => ({
    task_id: requiredText(value?.task_id, "job.data.tasks[].task_id"),
    dispatch_generation: positiveInteger(value?.dispatch_generation, null),
  }));
  if (tasks.some((task) => task.dispatch_generation == null)) {
    throw new TypeError("job.data.tasks[].dispatch_generation must be a positive integer");
  }
  if (new Set(tasks.map((task) => task.task_id)).size !== tasks.length) {
    throw new TypeError("job.data.tasks must not contain duplicate Task IDs");
  }
  return { channelId, tasks };
}

export function contentEnrichResultFromError(error) {
  const pending = [error];
  const seen = new Set();
  while (pending.length > 0 && seen.size < 20) {
    const value = pending.shift();
    if (!value || (typeof value !== "object" && typeof value !== "function")) continue;
    if (seen.has(value)) continue;
    seen.add(value);
    const result = value.content_enrich_result;
    if (result && typeof result === "object" && !Array.isArray(result)) return result;
    if (value.cause != null) pending.push(value.cause);
    if (Array.isArray(value.errors)) pending.push(...value.errors);
  }
  return null;
}

export { contentEnrichRetryDelayMs } from "./contentEnrichPolicy.js";

export class ContentEnrichExecutor {
  constructor({
    repository,
    fetchDetail,
    now = () => new Date(),
    leaseDurationMs = 15 * 60_000,
    heartbeatIntervalMs,
    setHeartbeatTimeout = setTimeout,
    clearHeartbeatTimeout = clearTimeout,
    retryBaseMs = 30_000,
    retryMaxMs = 6 * 60 * 60_000,
    maxAttempts = 8,
  } = {}) {
    if (!repository
        || typeof repository.claimBatch !== "function"
        || typeof repository.renewBatch !== "function"
        || typeof repository.settleBatch !== "function") {
      throw new TypeError("a Content Enrich execution repository is required");
    }
    if (typeof fetchDetail !== "function") throw new TypeError("fetchDetail is required");
    if (typeof setHeartbeatTimeout !== "function" || typeof clearHeartbeatTimeout !== "function") {
      throw new TypeError("heartbeat timer functions are required");
    }
    this.repository = repository;
    this.fetchDetail = fetchDetail;
    this.now = now;
    this.leaseDurationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    const maximumHeartbeatMs = Math.max(1, Math.floor(this.leaseDurationMs / 2));
    const defaultHeartbeatMs = Math.min(
      maximumHeartbeatMs,
      Math.max(1, Math.floor(this.leaseDurationMs / 3)),
    );
    this.heartbeatIntervalMs = positiveInteger(
      heartbeatIntervalMs,
      defaultHeartbeatMs,
      maximumHeartbeatMs,
    );
    this.setHeartbeatTimeout = setHeartbeatTimeout;
    this.clearHeartbeatTimeout = clearHeartbeatTimeout;
    this.retryOptions = {
      baseMs: retryBaseMs,
      maxMs: retryMaxMs,
      maxAttempts: positiveInteger(maxAttempts, 8, 100),
    };
  }

  #startHeartbeat({ jobId, channelId, tasks }) {
    const abortController = new AbortController();
    const clearHeartbeatTimer = this.clearHeartbeatTimeout;
    let stopped = false;
    let timer = null;
    let inFlight = null;
    let renewals = 0;
    let failures = 0;
    let leaseLost = false;
    const markLeaseLost = (cause) => {
      leaseLost = true;
      if (!abortController.signal.aborted) {
        abortController.abort(cause instanceof Error
          ? cause
          : new Error("Content Enrich Worker lease ownership was lost"));
      }
    };
    const schedule = () => {
      if (stopped || leaseLost) return;
      timer = this.setHeartbeatTimeout(() => {
        if (stopped || leaseLost) return;
        const heartbeatAt = new Date(this.now());
        const leaseExpiresAt = new Date(heartbeatAt.getTime() + this.leaseDurationMs);
        inFlight = (async () => {
          try {
            const renewed = Number(await this.repository.renewBatch({
              jobId,
              channelId,
              tasks,
              now: heartbeatAt,
              leaseExpiresAt,
              leaseDurationMs: this.leaseDurationMs,
            }));
            if (!Number.isSafeInteger(renewed) || renewed < 0) {
              throw new TypeError("renewBatch must return a non-negative integer");
            }
            renewals += 1;
            if (renewed !== tasks.length) {
              markLeaseLost(new Error(
                `Content Enrich heartbeat renewed ${renewed} of ${tasks.length} Tasks`,
              ));
            }
          } catch (error) {
            failures += 1;
            markLeaseLost(error);
          }
        })()
          .finally(() => {
            inFlight = null;
            schedule();
          });
      }, this.heartbeatIntervalMs);
      timer.unref?.();
    };
    schedule();
    return {
      signal: abortController.signal,
      async stop() {
        stopped = true;
        if (timer) clearHeartbeatTimer(timer);
        if (inFlight) await inFlight;
        return {
          renewals,
          failures,
          lease_lost: leaseLost,
        };
      },
    };
  }

  async execute(job) {
    const jobId = requiredText(job?.id, "job.id");
    const { channelId, tasks } = taskReferences(job);
    const observedAt = observedDate(this.now());
    const leaseExpiresAt = new Date(observedAt.getTime() + this.leaseDurationMs);
    const claimed = await this.repository.claimBatch({
      jobId,
      channelId,
      tasks,
      now: observedAt,
      leaseExpiresAt,
      leaseDurationMs: this.leaseDurationMs,
    });
    if (!Array.isArray(claimed)) throw new TypeError("claimBatch must return an array");
    if (claimed.length === 0) {
      return {
        requested: tasks.length,
        claimed: 0,
        attempted: 0,
        done: 0,
        terminal: 0,
        retryable: 0,
        dead_letter: 0,
        skipped: tasks.length,
      };
    }

    const claimedReferences = claimed.map((task) => ({
      task_id: task.task_id,
      dispatch_generation: task.dispatch_generation,
    }));
    const heartbeatControl = this.#startHeartbeat({
      jobId,
      channelId,
      tasks: claimedReferences,
    });
    const outcomes = [];
    let fetchAttempts = 0;
    let routeError = null;
    let heartbeat = null;
    try {
      for (const task of claimed) {
        if (heartbeatControl.signal.aborted) break;
        fetchAttempts += 1;
        try {
          const detail = await this.fetchDetail(task.source_content_id, {
            signal: heartbeatControl.signal,
          });
          if (heartbeatControl.signal.aborted) break;
          const completedAt = observedDate(this.now());
          outcomes.push(contentEnrichDetailOutcome(task, detail, completedAt, this.retryOptions));
        } catch (error) {
          if (heartbeatControl.signal.aborted) break;
          const completedAt = observedDate(this.now());
          outcomes.push(contentEnrichFailureOutcome(task, error, completedAt, this.retryOptions));
          if (retryableRotaFailure(error)) {
            routeError = error;
            break;
          }
        }
      }
    } finally {
      heartbeat = await heartbeatControl.stop();
    }
    if (heartbeat.lease_lost) {
      outcomes.length = 0;
      routeError = null;
    }
    const attemptedIds = new Set(outcomes.map((outcome) => outcome.task_id));
    const unattemptedTasks = claimed
      .filter((task) => !attemptedIds.has(task.task_id))
      .map((task) => ({
        task_id: task.task_id,
        dispatch_generation: task.dispatch_generation,
      }));
    const settledAt = observedDate(this.now());
    let settled;
    try {
      settled = await this.repository.settleBatch({
        jobId,
        channelId,
        outcomes,
        unattemptedTasks,
        observedAt,
        settledAt,
      });
    } catch (error) {
      if (error && (typeof error === "object" || typeof error === "function")) {
        const persisted = error.content_enrich_persisted_result ?? emptySettlementSummary();
        error.content_enrich_result = {
          requested: tasks.length,
          claimed: claimed.length,
          attempted: fetchAttempts,
          done: Number(persisted.done ?? 0),
          terminal: Number(persisted.terminal ?? 0),
          retryable: Number(persisted.retryable ?? 0),
          dead_letter: Number(persisted.dead_letter ?? 0),
          skipped: (tasks.length - claimed.length) + Number(persisted.skipped ?? 0),
        };
      }
      throw error;
    }
    const result = {
      requested: tasks.length,
      claimed: claimed.length,
      attempted: fetchAttempts,
      done: Number(settled?.done ?? 0),
      terminal: Number(settled?.terminal ?? 0),
      retryable: Number(settled?.retryable ?? 0),
      dead_letter: Number(settled?.dead_letter ?? 0),
      skipped: (tasks.length - claimed.length) + (
        heartbeat.lease_lost ? claimed.length : Number(settled?.skipped ?? 0)
      ),
      heartbeat,
    };
    if (routeError) {
      const expectedCheckpoints = outcomes.filter(
        (outcome) => ["retryable", "dead_letter"].includes(outcome.kind),
      ).length;
      const persistedCheckpoints = result.retryable + result.dead_letter;
      routeError.content_enrich_checkpoint_persisted = expectedCheckpoints > 0
        && persistedCheckpoints === expectedCheckpoints;
      routeError.content_enrich_result = result;
      throw routeError;
    }
    return result;
  }
}

export class PostgresContentEnrichExecutionRepository {
  constructor({ withTransaction, applyDetail, refreshHashes, reconcilePublication } = {}) {
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    if (typeof applyDetail !== "function") throw new TypeError("applyDetail is required");
    if (typeof refreshHashes !== "function") throw new TypeError("refreshHashes is required");
    if (typeof reconcilePublication !== "function") {
      throw new TypeError("reconcilePublication is required");
    }
    this.withTransaction = withTransaction;
    this.applyDetail = applyDetail;
    this.refreshHashes = refreshHashes;
    this.reconcilePublication = reconcilePublication;
  }

  claimBatch({ jobId, channelId, tasks, leaseDurationMs }) {
    const durationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    return this.withTransaction(async (client) => {
      const selected = await client.query(
        `WITH requested AS (
           SELECT * FROM jsonb_to_recordset($3::jsonb)
             AS item(task_id text,dispatch_generation bigint,ordinal integer)
         )
         SELECT requested.ordinal,to_jsonb(task) AS task,to_jsonb(content) AS content
         FROM requested
         JOIN crawler.content_enrich_tasks task
           ON task.task_id=requested.task_id
          AND task.dispatch_generation=requested.dispatch_generation
         JOIN crawler.contents content ON content.content_key=task.content_key
         WHERE task.channel_id=$2
           AND task.job_type='player-refresh'
           AND task.status='leased'
           AND task.lease_owner=$1
           AND task.lease_expires_at>clock_timestamp()
         ORDER BY requested.ordinal
         FOR UPDATE OF task SKIP LOCKED`,
        [
          jobId,
          channelId,
          JSON.stringify(tasks.map((task, ordinal) => ({ ...task, ordinal }))),
        ],
      );
      if (selected.rows.length === 0) return [];
      const references = selected.rows.map((row) => ({
        task_id: row.task.task_id,
        dispatch_generation: Number(row.task.dispatch_generation),
      }));
      const claimed = await client.query(
        `WITH input AS (
           SELECT * FROM jsonb_to_recordset($3::jsonb)
             AS item(task_id text,dispatch_generation bigint)
           )
           UPDATE crawler.content_enrich_tasks task
           SET status='running',last_attempt_at=clock_timestamp(),
               lease_expires_at=clock_timestamp()+($4::bigint*interval '1 millisecond'),
               updated_at=clock_timestamp()
         FROM input
         WHERE task.task_id=input.task_id
           AND task.dispatch_generation=input.dispatch_generation
           AND task.channel_id=$2
             AND task.job_type='player-refresh'
             AND task.status='leased'
             AND task.lease_owner=$1
             AND task.lease_expires_at>clock_timestamp()
           RETURNING task.task_id`,
        [jobId, channelId, JSON.stringify(references), durationMs],
      );
      const claimedIds = new Set(claimed.rows.map((row) => row.task_id));
      return selected.rows
        .filter((row) => claimedIds.has(row.task.task_id))
        .map((row) => ({ ...row.content, ...row.task, dispatch_generation: Number(row.task.dispatch_generation) }));
    });
  }

  renewBatch({ jobId, channelId, tasks, leaseDurationMs }) {
    const durationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    return this.withTransaction(async (client) => {
      const serializedTasks = JSON.stringify(tasks);
      await client.query(
        `WITH requested AS (
           SELECT * FROM jsonb_to_recordset($3::jsonb)
             AS item(task_id text,dispatch_generation bigint)
         )
         SELECT task.task_id
         FROM requested
         JOIN crawler.content_enrich_tasks task
           ON task.task_id=requested.task_id
          AND task.dispatch_generation=requested.dispatch_generation
         WHERE task.channel_id=$2
           AND task.job_type='player-refresh'
           AND task.status='running'
           AND task.lease_owner=$1
         ORDER BY task.task_id
         FOR UPDATE OF task`,
        [jobId, channelId, serializedTasks],
      );
      const renewed = await client.query(
        `WITH requested AS (
           SELECT * FROM jsonb_to_recordset($3::jsonb)
             AS item(task_id text,dispatch_generation bigint)
           )
           UPDATE crawler.content_enrich_tasks task
           SET lease_expires_at=clock_timestamp()+($4::bigint*interval '1 millisecond'),
               updated_at=clock_timestamp()
         FROM requested
         WHERE task.task_id=requested.task_id
           AND task.dispatch_generation=requested.dispatch_generation
           AND task.channel_id=$2
             AND task.job_type='player-refresh'
             AND task.status='running'
             AND task.lease_owner=$1
             AND task.lease_expires_at>clock_timestamp()`,
        [jobId, channelId, serializedTasks, durationMs],
      );
      return Number(renewed.rowCount ?? 0);
    });
  }

  async settleBatch({
    jobId,
    channelId,
    outcomes,
    unattemptedTasks,
    observedAt,
  }) {
    const checkpointOutcomes = outcomes.filter((outcome) => outcome.detail == null);
    const publicationOutcomes = outcomes.filter((outcome) => outcome.detail != null);
    const checkpoint = checkpointOutcomes.length > 0 || unattemptedTasks.length > 0
      ? await this.#settleBatchTransaction({
          jobId,
          channelId,
          outcomes: checkpointOutcomes,
          unattemptedTasks,
          observedAt,
        })
      : emptySettlementSummary();
    let publication;
    try {
      publication = publicationOutcomes.length > 0
        ? await this.#settleBatchTransaction({
            jobId,
            channelId,
            outcomes: publicationOutcomes,
            unattemptedTasks: [],
            observedAt,
          })
        : emptySettlementSummary();
    } catch (error) {
      if (error && (typeof error === "object" || typeof error === "function")) {
        error.content_enrich_persisted_result = checkpoint;
      }
      throw error;
    }
    return combineSettlementSummaries(checkpoint, publication);
  }

  #settleBatchTransaction({
    jobId,
    channelId,
    outcomes,
    unattemptedTasks,
    observedAt,
  }) {
    return this.withTransaction(async (client) => {
      const outcomeReferences = outcomes.map((outcome) => ({
        task_id: outcome.task_id,
        dispatch_generation: outcome.dispatch_generation,
        disposition: "outcome",
      }));
      const allReferences = [
        ...outcomeReferences,
        ...unattemptedTasks.map((task) => ({
          task_id: task.task_id,
          dispatch_generation: task.dispatch_generation,
          disposition: "unattempted",
        })),
      ];
      const serializedReferences = JSON.stringify(allReferences);
      const locked = allReferences.length === 0
        ? { rows: [] }
        : await client.query(
            `WITH requested AS (
               SELECT * FROM jsonb_to_recordset($3::jsonb)
                 AS item(task_id text,dispatch_generation bigint,disposition text)
             )
             SELECT requested.disposition,to_jsonb(task) AS task,to_jsonb(content) AS content
             FROM requested
             JOIN crawler.content_enrich_tasks task
               ON task.task_id=requested.task_id
              AND task.dispatch_generation=requested.dispatch_generation
             JOIN crawler.contents content ON content.content_key=task.content_key
             WHERE task.channel_id=$2
               AND task.job_type='player-refresh'
               AND task.status='running'
               AND task.lease_owner=$1
             ORDER BY task.task_id
             FOR UPDATE OF task`,
            [jobId, channelId, serializedReferences],
      );
      const live = allReferences.length === 0
        ? { rows: [] }
        : await client.query(
            `WITH requested AS (
               SELECT * FROM jsonb_to_recordset($3::jsonb)
                 AS item(task_id text,dispatch_generation bigint)
             )
             SELECT task.task_id
             FROM requested
             JOIN crawler.content_enrich_tasks task
               ON task.task_id=requested.task_id
              AND task.dispatch_generation=requested.dispatch_generation
             WHERE task.channel_id=$2
               AND task.job_type='player-refresh'
               AND task.status='running'
               AND task.lease_owner=$1
               AND task.lease_expires_at>clock_timestamp()`,
            [jobId, channelId, serializedReferences],
      );
      const liveIds = new Set(live.rows.map((row) => row.task_id));
      const rowsById = new Map(locked.rows
        .filter((row) => row.disposition === "outcome" && liveIds.has(row.task.task_id))
        .map((row) => [row.task.task_id, row]));
      const changedKeys = [];
      const summary = emptySettlementSummary();
      for (const outcome of outcomes) {
        const lockedRow = rowsById.get(outcome.task_id);
        if (!lockedRow) {
          summary.skipped += 1;
          continue;
        }
        if (outcome.detail) {
          await this.applyDetail(client, {
            row: lockedRow.content,
            detail: outcome.detail,
            observedAt: observedAt.toISOString(),
            observationId: null,
            collectNext: false,
            detailMetadataKey: "content_enrich_detail",
          });
          changedKeys.push(lockedRow.task.content_key);
        }
        const resultJson = JSON.stringify({
          kind: outcome.kind,
          observed_at: outcome.observed_at,
          access_status: outcome.access_status,
          failure_decision: outcome.failure_decision ?? null,
        });
        const updated = ["retryable", "dead_letter"].includes(outcome.kind)
          ? await client.query(
              `UPDATE crawler.content_enrich_tasks
               SET status=$5,attempts=attempts+1,next_retry_at=$6,
                   error_message=$7,result_json=result_json || jsonb_build_object('last_outcome',$8::jsonb),
                   lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
               WHERE task_id=$1 AND channel_id=$2 AND job_type='player-refresh'
                 AND status='running' AND lease_owner=$3 AND dispatch_generation=$4`,
              [
                outcome.task_id,
                channelId,
                jobId,
                outcome.dispatch_generation,
                outcome.kind === "dead_letter" ? "dead_letter" : "failed",
                outcome.next_retry_at,
                outcome.error_message,
                resultJson,
              ],
            )
          : await client.query(
              `UPDATE crawler.content_enrich_tasks
               SET status=$5,next_retry_at=$6,error_message=$7,
                   last_success_at=CASE WHEN $8::boolean THEN $9 ELSE last_success_at END,
                   result_json=result_json || jsonb_build_object('last_outcome',$10::jsonb),
                   lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
               WHERE task_id=$1 AND channel_id=$2 AND job_type='player-refresh'
                 AND status='running' AND lease_owner=$3 AND dispatch_generation=$4`,
              [
                outcome.task_id,
                channelId,
                jobId,
                outcome.dispatch_generation,
                outcome.kind,
                outcome.next_retry_at ?? null,
                outcome.error_message,
                outcome.detail != null,
                observedAt,
                resultJson,
              ],
            );
        if (updated.rowCount !== 1) throw new Error(`lost Enrich Task fence: ${outcome.task_id}`);
        summary[outcome.kind] += 1;
      }
      for (const task of unattemptedTasks) {
        if (!liveIds.has(task.task_id)) continue;
        await client.query(
          `UPDATE crawler.content_enrich_tasks
           SET status='leased',updated_at=now()
           WHERE task_id=$1 AND channel_id=$2 AND job_type='player-refresh'
             AND status='running' AND lease_owner=$3 AND dispatch_generation=$4`,
          [task.task_id, channelId, jobId, task.dispatch_generation],
        );
      }
      if (changedKeys.length > 0) {
        await this.refreshHashes(client, changedKeys);
        await this.reconcilePublication(client, {
          channelId,
          domains: ["video"],
          asOf: observedAt.toISOString(),
        });
      }
      return summary;
    });
  }
}
