import { createHash, randomUUID } from "node:crypto";
import {
  CONTENT_ENRICH_DISPATCH_LOCK_KEY,
  CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY,
  loadContentEnrichMode,
} from "./contentEnrichMode.js";
import { DORMANT_WINDOW_DAYS } from "./channelDormancy.js";
import { safeJobId } from "./queues.js";

const OPEN_QUEUE_STATES = Object.freeze([
  "waiting",
  "active",
  "delayed",
  "paused",
  "prioritized",
  "waiting-children",
]);
function positiveInteger(value, fallback, maximum = Number.MAX_SAFE_INTEGER) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number <= 0) return fallback;
  return Math.min(number, maximum);
}

function requiredText(value, field) {
  const output = String(value ?? "").trim();
  if (!output) throw new TypeError(`${field} is required`);
  return output;
}

function normalizedTaskReferences(tasks) {
  const references = (Array.isArray(tasks) ? tasks : []).map((task) => ({
    task_id: requiredText(task?.task_id, "task.task_id"),
    dispatch_generation: positiveInteger(
      task?.dispatch_generation,
      null,
    ),
  }));
  if (references.length === 0) throw new TypeError("at least one Enrich Task is required");
  if (references.some((task) => task.dispatch_generation == null)) {
    throw new TypeError("task.dispatch_generation must be a positive integer");
  }
  const taskIds = new Set(references.map((task) => task.task_id));
  if (taskIds.size !== references.length) throw new TypeError("Enrich Task IDs must be unique");
  return references.sort((left, right) => left.task_id.localeCompare(right.task_id));
}

export function contentEnrichJobId({ channelId, tasks } = {}) {
  const channel = requiredText(channelId, "channelId");
  const references = normalizedTaskReferences(tasks);
  const digest = createHash("sha256")
    .update(references.map((task) => `${task.task_id}:${task.dispatch_generation}`).join("\n"))
    .digest("hex")
    .slice(0, 24);
  return safeJobId("content_enrich", channel, digest);
}

function batchJobData(batch) {
  const tasks = normalizedTaskReferences(batch.tasks);
  return {
    channel_id: requiredText(batch.channel_id, "batch.channel_id"),
    task_ids: tasks.map((task) => task.task_id),
    tasks,
  };
}

function queueBacklog(counts = {}) {
  return OPEN_QUEUE_STATES.reduce((total, state) => total + Number(counts[state] ?? 0), 0);
}

class ContentEnrichDispatchLockLostError extends Error {
  constructor(message = "Content Enrich dispatch mutex ownership was lost") {
    super(message);
    this.name = "ContentEnrichDispatchLockLostError";
  }
}

function assertDispatchOwned(lock) {
  lock?.assertOwned?.();
}

export class ContentEnrichDispatcher {
  constructor({
    repository,
    queue,
    enabled = false,
    highWater = 20,
    refill = 10,
    batchSize = 5,
    leaseDurationMs = 15 * 60_000,
    now = () => new Date(),
  } = {}) {
    if (!repository) throw new TypeError("repository is required");
    if (!queue || typeof queue.add !== "function" || typeof queue.getJobCounts !== "function") {
      throw new TypeError("a BullMQ-compatible queue is required");
    }
    this.repository = repository;
    this.queue = queue;
    this.enabled = enabled === true;
    this.highWater = positiveInteger(highWater, 20, 10_000);
    this.refill = positiveInteger(refill, 10, this.highWater);
    this.batchSize = positiveInteger(batchSize, 5, 100);
    this.leaseDurationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    this.now = now;
  }

  async dispatchAvailable() {
    const summary = {
      enqueued: 0,
      recovered: 0,
      existing: 0,
      failed: 0,
      released: 0,
    };
    if (!this.enabled) return { ...summary, reason: "disabled" };
    if (typeof this.repository.withDispatchLock === "function") {
      const locked = await this.repository.withDispatchLock(
        (repository, lock) => this.#dispatchAvailable(repository, summary, lock),
      );
      if (locked?.acquired !== true) return { ...summary, reason: "dispatch_locked" };
      return locked.result;
    }
    return this.#dispatchAvailable(this.repository, summary);
  }

  async #dispatchAvailable(repository, summary, dispatchLock = null) {
    assertDispatchOwned(dispatchLock);
    if (typeof repository.dispatchMode === "function") {
      const mode = await repository.dispatchMode();
      assertDispatchOwned(dispatchLock);
      if (mode !== "queue") return { ...summary, reason: `mode_${mode || "clock"}` };
    }

    const counts = await this.queue.getJobCounts(...OPEN_QUEUE_STATES);
    assertDispatchOwned(dispatchLock);
    const backlog = queueBacklog(counts);
    const startedAt = new Date(this.now());
    const leaseExpiresAt = new Date(startedAt.getTime() + this.leaseDurationMs);
    let addBudget = Math.max(0, Math.min(this.refill, this.highWater - backlog));
    const dispatchedChannelIds = new Set();
    const leasedBatches = await repository.listLeasedBatches({
      limit: this.highWater,
      now: startedAt,
    });
    assertDispatchOwned(dispatchLock);

    for (const batch of leasedBatches) {
      assertDispatchOwned(dispatchLock);
      const jobId = requiredText(batch.job_id, "batch.job_id");
      const data = batchJobData(batch);
      const taskIds = data.task_ids;
      const existing = typeof this.queue.getJob === "function" ? await this.queue.getJob(jobId) : null;
      assertDispatchOwned(dispatchLock);
      const state = existing && typeof existing.getState === "function"
        ? await existing.getState()
        : existing?.state ?? null;
      assertDispatchOwned(dispatchLock);
      if (existing && ["completed", "failed"].includes(state)) {
        await repository.releaseLease({ jobId, taskIds });
        assertDispatchOwned(dispatchLock);
        summary.released += 1;
        continue;
      }
      if (existing) {
        await repository.refreshLease({
          jobId,
          taskIds,
          leaseExpiresAt,
          leaseDurationMs: this.leaseDurationMs,
          dispatchOwner: dispatchLock?.owner ?? null,
        });
        assertDispatchOwned(dispatchLock);
        summary.existing += 1;
        continue;
      }
      if (addBudget <= 0) continue;
      if (dispatchedChannelIds.has(data.channel_id)) continue;
      addBudget -= 1;
      dispatchedChannelIds.add(data.channel_id);
      await repository.refreshLease({
        jobId,
        taskIds,
        leaseExpiresAt,
        leaseDurationMs: this.leaseDurationMs,
        dispatchOwner: dispatchLock?.owner ?? null,
      });
      assertDispatchOwned(dispatchLock);
      try {
        await this.queue.add("content-enrich", data, { jobId });
        assertDispatchOwned(dispatchLock);
        summary.recovered += 1;
      } catch (error) {
        assertDispatchOwned(dispatchLock);
        summary.failed += 1;
      }
    }

    if (addBudget > 0) {
      const batches = await repository.leaseFairBatches({
        maxJobs: addBudget,
        batchSize: this.batchSize,
        leaseExpiresAt,
        leaseDurationMs: this.leaseDurationMs,
        now: startedAt,
        dispatchOwner: dispatchLock?.owner ?? null,
        excludeChannelIds: [...dispatchedChannelIds],
      });
      assertDispatchOwned(dispatchLock);
      for (const batch of batches) {
        assertDispatchOwned(dispatchLock);
        const data = batchJobData(batch);
        try {
          await this.queue.add("content-enrich", data, { jobId: batch.job_id });
          assertDispatchOwned(dispatchLock);
          summary.enqueued += 1;
        } catch (error) {
          assertDispatchOwned(dispatchLock);
          summary.failed += 1;
        }
      }
    }

    return {
      ...summary,
      ...(backlog >= this.highWater ? { reason: "high_water" } : {}),
      backlog,
      high_water: this.highWater,
      batch_size: this.batchSize,
    };
  }
}

export class PostgresContentEnrichDispatchRepository {
  constructor({
    queryFn,
    withTransaction,
    dispatchLockDurationMs = 60_000,
    dispatchLockOwner = () => `content-enrich-controller:${randomUUID()}`,
    setDispatchLockTimeout = setTimeout,
    clearDispatchLockTimeout = clearTimeout,
  } = {}) {
    if (typeof queryFn !== "function") throw new TypeError("queryFn is required");
    if (typeof withTransaction !== "function") throw new TypeError("withTransaction is required");
    if (typeof dispatchLockOwner !== "function") throw new TypeError("dispatchLockOwner is required");
    if (typeof setDispatchLockTimeout !== "function" || typeof clearDispatchLockTimeout !== "function") {
      throw new TypeError("dispatch mutex timer functions are required");
    }
    this.query = queryFn;
    this.withTransaction = withTransaction;
    this.dispatchLockDurationMs = positiveInteger(dispatchLockDurationMs, 60_000, 15 * 60_000);
    this.dispatchLockOwner = dispatchLockOwner;
    this.setDispatchLockTimeout = setDispatchLockTimeout;
    this.clearDispatchLockTimeout = clearDispatchLockTimeout;
  }

  async #acquireDispatchLock(owner) {
    return this.withTransaction(async (client) => {
      await client.query(
        `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
         VALUES ($1,'{"owner":null,"expires_at":null}'::jsonb,clock_timestamp())
         ON CONFLICT (setting_key) DO NOTHING`,
        [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY],
      );
      const acquired = await client.query(
        `UPDATE crawler.settings
         SET value_json=jsonb_build_object(
               'owner',$2::text,
               'expires_at',clock_timestamp()+($3::bigint*interval '1 millisecond')
             ),
             updated_at=clock_timestamp()
         WHERE setting_key=$1
           AND (
             NULLIF(value_json->>'owner','') IS NULL
             OR NULLIF(value_json->>'expires_at','')::timestamptz<=clock_timestamp()
           )
         RETURNING value_json`,
        [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY, owner, this.dispatchLockDurationMs],
      );
      return acquired.rowCount === 1;
    });
  }

  #startDispatchLockHeartbeat(owner) {
    const intervalMs = Math.max(1, Math.floor(this.dispatchLockDurationMs / 3));
    const clearTimer = this.clearDispatchLockTimeout;
    let stopped = false;
    let lost = false;
    let timer = null;
    let inFlight = null;
    const schedule = () => {
      if (stopped || lost) return;
      timer = this.setDispatchLockTimeout(() => {
        if (stopped || lost) return;
        inFlight = this.query(
          `UPDATE crawler.settings
           SET value_json=value_json || jsonb_build_object(
                 'expires_at',clock_timestamp()+($3::bigint*interval '1 millisecond')
               ),
               updated_at=clock_timestamp()
           WHERE setting_key=$1
             AND value_json->>'owner'=$2
             AND NULLIF(value_json->>'expires_at','')::timestamptz>clock_timestamp()`,
          [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY, owner, this.dispatchLockDurationMs],
        )
          .then((renewed) => {
            if (renewed.rowCount !== 1) lost = true;
          })
          .catch(() => {
            lost = true;
          })
          .finally(() => {
            inFlight = null;
            schedule();
          });
      }, intervalMs);
      timer.unref?.();
    };
    schedule();
    return {
      owner,
      assertOwned() {
        if (lost) throw new ContentEnrichDispatchLockLostError();
      },
      async stop() {
        stopped = true;
        if (timer) clearTimer(timer);
        if (inFlight) await inFlight;
        return { lost };
      },
    };
  }

  async #releaseDispatchLock(owner) {
    await this.query(
      `UPDATE crawler.settings
       SET value_json=jsonb_build_object('owner',NULL,'expires_at',NULL),
           updated_at=clock_timestamp()
       WHERE setting_key=$1 AND value_json->>'owner'=$2`,
      [CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY, owner],
    );
  }

  async withDispatchLock(action) {
    if (typeof action !== "function") throw new TypeError("dispatch lock action is required");
    const owner = requiredText(this.dispatchLockOwner(), "dispatch mutex owner");
    if (!await this.#acquireDispatchLock(owner)) return { acquired: false, result: null };
    const heartbeat = this.#startDispatchLockHeartbeat(owner);
    let result;
    let failure = null;
    try {
      result = await action(this, heartbeat);
      heartbeat.assertOwned();
    } catch (error) {
      failure = error;
    }
    const stopped = await heartbeat.stop();
    try {
      await this.#releaseDispatchLock(owner);
    } catch (error) {
      failure ??= error;
    }
    if (!failure && stopped.lost) failure = new ContentEnrichDispatchLockLostError();
    if (failure) throw failure;
    return { acquired: true, result };
  }

  async dispatchMode() {
    return loadContentEnrichMode({ query: this.query });
  }

  async #assertDispatchMutationOwned(client, dispatchOwner) {
    const owner = requiredText(dispatchOwner, "dispatch mutex owner");
    const locked = await client.query(
      `SELECT setting_key
       FROM crawler.settings
       WHERE setting_key='content_enrich_dispatch_mutex'
       FOR SHARE`,
    );
    if (locked.rowCount !== 1) throw new ContentEnrichDispatchLockLostError();
    const ownership = await client.query(
      `SELECT value_json->>'owner'=$1 AS owned,
              COALESCE(
                NULLIF(value_json->>'expires_at','')::timestamptz>clock_timestamp(),
                false
              ) AS live
       FROM crawler.settings
       WHERE setting_key='content_enrich_dispatch_mutex'`,
      [owner],
    );
    if (ownership.rows[0]?.owned !== true || ownership.rows[0]?.live !== true) {
      throw new ContentEnrichDispatchLockLostError();
    }
    const mode = await loadContentEnrichMode(client, { lock: true });
    if (mode !== "queue") throw new ContentEnrichDispatchLockLostError();
  }

  async listLeasedBatches({ limit = 10 } = {}) {
    const result = await this.query(
      `WITH owners AS (
         SELECT lease_owner,channel_id,min(priority) AS priority,min(created_at) AS created_at
         FROM crawler.content_enrich_tasks
         WHERE job_type='player-refresh'
           AND status='leased'
           AND lease_owner IS NOT NULL
         GROUP BY lease_owner,channel_id
         ORDER BY min(priority),min(created_at),channel_id,lease_owner
         LIMIT $1
       )
       SELECT task.*
       FROM owners
       JOIN crawler.content_enrich_tasks task
         ON task.lease_owner=owners.lease_owner
        AND task.channel_id=owners.channel_id
        AND task.job_type='player-refresh'
        AND task.status='leased'
       ORDER BY owners.priority,owners.created_at,owners.channel_id,
                task.priority,task.created_at,task.task_id`,
      [positiveInteger(limit, 10, 10_000)],
    );
    const batches = new Map();
    for (const row of result.rows) {
      const key = `${row.lease_owner}\n${row.channel_id}`;
      const batch = batches.get(key) ?? {
        job_id: row.lease_owner,
        channel_id: row.channel_id,
        tasks: [],
      };
      batch.tasks.push(row);
      batches.set(key, batch);
    }
    return [...batches.values()];
  }

  refreshLease({ jobId, taskIds, leaseDurationMs, dispatchOwner }) {
    const durationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    return this.withTransaction(async (client) => {
      await this.#assertDispatchMutationOwned(client, dispatchOwner);
      return client.query(
        `UPDATE crawler.content_enrich_tasks
         SET lease_expires_at=clock_timestamp()+($3::bigint*interval '1 millisecond'),
             updated_at=now()
         WHERE task_id=ANY($2::text[])
           AND job_type='player-refresh'
           AND status='leased'
           AND lease_owner=$1`,
        [jobId, taskIds, durationMs],
      );
    });
  }

  releaseLease({ jobId, taskIds }) {
    return this.query(
      `UPDATE crawler.content_enrich_tasks
       SET status='queued',lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
       WHERE task_id=ANY($2::text[])
         AND job_type='player-refresh'
         AND status='leased'
         AND lease_owner=$1`,
      [jobId, taskIds],
    );
  }

  leaseFairBatches({
    maxJobs,
    batchSize,
    leaseDurationMs,
    dispatchOwner,
    excludeChannelIds = [],
  }) {
    const jobLimit = positiveInteger(maxJobs, 1, 10_000);
    const taskLimit = positiveInteger(batchSize, 1, 100);
    const durationMs = positiveInteger(leaseDurationMs, 15 * 60_000, 24 * 60 * 60_000);
    const owner = requiredText(dispatchOwner, "dispatch mutex owner");
    const excludedChannels = [...new Set(
      (Array.isArray(excludeChannelIds) ? excludeChannelIds : [])
        .map((channelId) => String(channelId ?? "").trim())
        .filter(Boolean),
    )];
    return this.withTransaction(async (client) => {
      const singleton = await client.query(
        "SELECT pg_try_advisory_xact_lock(hashtext($1)) AS acquired",
        [CONTENT_ENRICH_DISPATCH_LOCK_KEY],
      );
      if (singleton.rows[0]?.acquired !== true) return [];
      await this.#assertDispatchMutationOwned(client, owner);

      await client.query(
        `INSERT INTO crawler.settings (setting_key,value_json,updated_at)
         VALUES ('content_enrich_dispatch_cursor','{"channel_id":""}'::jsonb,now())
         ON CONFLICT (setting_key) DO NOTHING`,
      );
      const cursorResult = await client.query(
        `SELECT COALESCE(value_json->>'channel_id','') AS channel_id
         FROM crawler.settings
         WHERE setting_key='content_enrich_dispatch_cursor'
         FOR UPDATE`,
      );
      const channelCursor = String(cursorResult.rows[0]?.channel_id ?? "");

      await client.query(
        `UPDATE crawler.content_enrich_tasks
         SET status='queued',lease_owner=NULL,lease_expires_at=NULL,updated_at=now()
         WHERE job_type='player-refresh'
           AND status='running'
           AND lease_expires_at<=clock_timestamp()`,
      );
      const selected = await client.query(
        `WITH eligible_channels AS (
           SELECT task.channel_id,min(task.priority) AS priority,
                  min(CASE
                    WHEN registry.status='active' AND (
                      content.published_at IS NULL
                      OR content.published_at>=clock_timestamp()-($5::int*interval '1 day')
                    ) THEN 0
                    WHEN registry.status='active' THEN 1
                    ELSE 2
                  END) AS dispatch_tier,
                  CASE WHEN task.channel_id>$3::text THEN 0 ELSE 1 END AS cursor_partition
           FROM crawler.content_enrich_tasks task
           JOIN crawler.contents content ON content.content_key=task.content_key
           JOIN crawler.channels registry ON registry.channel_id=task.channel_id
           WHERE task.job_type='player-refresh'
             AND (
               task.status IN ('queued','failed')
               OR (task.status='terminal' AND task.next_retry_at IS NOT NULL)
             )
             AND task.lease_owner IS NULL
             AND COALESCE(task.next_retry_at,clock_timestamp())<=clock_timestamp()
             AND NOT (task.channel_id=ANY($4::text[]))
           GROUP BY task.channel_id
           ORDER BY min(CASE
                      WHEN registry.status='active' AND (
                        content.published_at IS NULL
                        OR content.published_at>=clock_timestamp()-($5::int*interval '1 day')
                      ) THEN 0
                      WHEN registry.status='active' THEN 1
                      ELSE 2
                    END),
                    min(task.priority),
                    CASE WHEN task.channel_id>$3::text THEN 0 ELSE 1 END,
                    task.channel_id
           LIMIT $1
         )
         SELECT chosen.*
         FROM eligible_channels channel
         JOIN LATERAL (
           SELECT task.*
           FROM crawler.content_enrich_tasks task
           JOIN crawler.contents content ON content.content_key=task.content_key
           JOIN crawler.channels registry ON registry.channel_id=task.channel_id
           WHERE task.channel_id=channel.channel_id
             AND task.job_type='player-refresh'
             AND (
               task.status IN ('queued','failed')
               OR (task.status='terminal' AND task.next_retry_at IS NOT NULL)
             )
             AND task.lease_owner IS NULL
             AND COALESCE(task.next_retry_at,clock_timestamp())<=clock_timestamp()
           ORDER BY CASE
                      WHEN registry.status='active' AND (
                        content.published_at IS NULL
                        OR content.published_at>=clock_timestamp()-($5::int*interval '1 day')
                      ) THEN 0
                      WHEN registry.status='active' THEN 1
                      ELSE 2
                    END,
                    task.priority,task.created_at,task.task_id
           LIMIT $2
           FOR UPDATE OF task SKIP LOCKED
         ) chosen ON true
         ORDER BY channel.dispatch_tier,channel.priority,
                  channel.cursor_partition,channel.channel_id,
                  chosen.priority,chosen.created_at,chosen.task_id`,
        [jobLimit, taskLimit, channelCursor, excludedChannels, DORMANT_WINDOW_DAYS],
      );
      const grouped = new Map();
      for (const row of selected.rows) {
        const rows = grouped.get(row.channel_id) ?? [];
        rows.push({ ...row, dispatch_generation: Number(row.dispatch_generation ?? 0) + 1 });
        grouped.set(row.channel_id, rows);
      }
      const batches = [];
      for (const [channelId, tasks] of grouped) {
        const jobId = contentEnrichJobId({ channelId, tasks });
        const references = tasks.map((task) => ({
          task_id: task.task_id,
          dispatch_generation: task.dispatch_generation,
        }));
        const updated = await client.query(
          `WITH input AS (
             SELECT * FROM jsonb_to_recordset($1::jsonb)
               AS item(task_id text,dispatch_generation bigint)
           )
           UPDATE crawler.content_enrich_tasks task
           SET status='leased',dispatch_generation=input.dispatch_generation,
               attempts=CASE WHEN task.status='terminal' THEN 0 ELSE task.attempts END,
               next_retry_at=CASE WHEN task.status='terminal' THEN NULL ELSE task.next_retry_at END,
               lease_owner=$2,
               lease_expires_at=clock_timestamp()+($3::bigint*interval '1 millisecond'),
               updated_at=now()
           FROM input
           WHERE task.task_id=input.task_id
             AND task.job_type='player-refresh'
             AND (
               task.status IN ('queued','failed')
               OR (task.status='terminal' AND task.next_retry_at IS NOT NULL)
             )
             AND task.lease_owner IS NULL
             AND COALESCE(task.next_retry_at,clock_timestamp())<=clock_timestamp()
           RETURNING task.*`,
          [JSON.stringify(references), jobId, durationMs],
        );
        if (updated.rowCount !== references.length) {
          throw new Error(`failed to lease the complete Enrich batch ${jobId}`);
        }
        const updatedById = new Map(updated.rows.map((row) => [row.task_id, row]));
        batches.push({
          job_id: jobId,
          channel_id: channelId,
          tasks: references.map((reference) => updatedById.get(reference.task_id) ?? reference),
        });
      }
      if (batches.length > 0) {
        await client.query(
          `UPDATE crawler.settings
           SET value_json=value_json || jsonb_build_object('channel_id',$1::text),
               updated_at=now()
           WHERE setting_key='content_enrich_dispatch_cursor'`,
          [batches.at(-1).channel_id],
        );
      }
      return batches;
    });
  }
}
