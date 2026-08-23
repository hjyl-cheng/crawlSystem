import assert from "node:assert/strict";
import test from "node:test";
import {
  ContentEnrichExecutor,
  PostgresContentEnrichExecutionRepository,
  contentEnrichRetryDelayMs,
} from "../src/contentEnrichExecution.js";

const NOW = "2026-08-23T00:00:00.000Z";

function task(taskId, generation, overrides = {}) {
  return {
    task_id: taskId,
    content_key: `UC-enrich:video:${taskId}`,
    channel_id: "UC-enrich",
    source_content_id: taskId,
    content_type: "video",
    job_type: "player-refresh",
    status: "leased",
    attempts: 0,
    dispatch_generation: generation,
    lease_owner: "content_enrich__UC-enrich__batch",
    lease_expires_at: "2026-08-23T00:05:00.000Z",
    access_status: "unknown",
    last_enriched_at: null,
    ...overrides,
  };
}

function job(refs) {
  return {
    id: "content_enrich__UC-enrich__batch",
    queueName: "youtube-content-enrich",
    data: {
      channel_id: "UC-enrich",
      tasks: refs.map(([taskId, generation]) => ({
        task_id: taskId,
        dispatch_generation: generation,
      })),
      task_ids: refs.map(([taskId]) => taskId),
    },
  };
}

function publicDetail(videoId, overrides = {}) {
  return {
    id: videoId,
    title: `Enriched ${videoId}`,
    published_at: "2026-08-01T12:00:00.000Z",
    published_at_precision: "second",
    view_count: 123,
    view_count_text: "123",
    duration_seconds: 90,
    access_status: "public",
    content_type_signals: {
      source: "youtubei_player",
      canonical_url: `https://www.youtube.com/watch?v=${videoId}`,
      is_shorts_eligible: false,
      is_live_content: false,
      is_live: false,
      is_upcoming: false,
      is_live_now: false,
    },
    ...overrides,
  };
}

class InMemoryExecutionRepository {
  constructor(rows) {
    this.rows = new Map(rows.map((row) => [row.task_id, structuredClone(row)]));
    this.hashBatches = [];
    this.publications = [];
    this.renewCalls = 0;
  }

  async claimBatch({ jobId, channelId, tasks, now, leaseExpiresAt }) {
    const claimed = [];
    for (const ref of tasks) {
      const row = this.rows.get(ref.task_id);
      if (!row
          || row.channel_id !== channelId
          || row.job_type !== "player-refresh"
          || row.status !== "leased"
          || row.lease_owner !== jobId
          || row.dispatch_generation !== ref.dispatch_generation
          || Date.parse(row.lease_expires_at) <= now.getTime()) continue;
      row.status = "running";
      row.last_attempt_at = now.toISOString();
      row.lease_expires_at = leaseExpiresAt.toISOString();
      claimed.push(structuredClone(row));
    }
    return claimed;
  }

  async renewBatch({ jobId, channelId, tasks, now, leaseExpiresAt }) {
    this.renewCalls += 1;
    let renewed = 0;
    for (const ref of tasks) {
      const row = this.rows.get(ref.task_id);
      if (!row
          || row.channel_id !== channelId
          || row.status !== "running"
          || row.lease_owner !== jobId
          || row.dispatch_generation !== ref.dispatch_generation
          || Date.parse(row.lease_expires_at) <= now.getTime()) continue;
      row.lease_expires_at = leaseExpiresAt.toISOString();
      renewed += 1;
    }
    return renewed;
  }

  async settleBatch({
    jobId,
    channelId,
    outcomes,
    unattemptedTasks,
    observedAt,
    settledAt = observedAt,
  }) {
    const changedKeys = [];
    let done = 0;
    let terminal = 0;
    let retryable = 0;
    let deadLetter = 0;
    let skipped = 0;
    for (const outcome of outcomes) {
      const row = this.rows.get(outcome.task_id);
      if (!row
          || row.channel_id !== channelId
          || row.status !== "running"
          || row.lease_owner !== jobId
          || row.dispatch_generation !== outcome.dispatch_generation
          || Date.parse(row.lease_expires_at) <= settledAt.getTime()) {
        skipped += 1;
        continue;
      }
      if (outcome.detail) {
        row.title = outcome.detail.title ?? row.title;
        row.access_status = outcome.detail.access_status ?? row.access_status;
        row.access_status_source = outcome.detail.access_status_source ?? row.access_status_source;
        row.last_enriched_at = observedAt.toISOString();
        changedKeys.push(row.content_key);
      }
      row.lease_owner = null;
      row.lease_expires_at = null;
      row.error_message = outcome.error_message ?? null;
      if (outcome.kind === "done") {
        row.status = "done";
        row.last_success_at = observedAt.toISOString();
        done += 1;
      } else if (outcome.kind === "terminal") {
        row.status = "terminal";
        row.last_success_at = outcome.detail ? observedAt.toISOString() : row.last_success_at;
        row.next_retry_at = outcome.next_retry_at ?? null;
        terminal += 1;
      } else if (outcome.kind === "retryable") {
        row.status = "failed";
        row.attempts += 1;
        row.next_retry_at = outcome.next_retry_at;
        retryable += 1;
      } else if (outcome.kind === "dead_letter") {
        row.status = "dead_letter";
        row.attempts += 1;
        row.next_retry_at = null;
        deadLetter += 1;
      }
    }
    for (const ref of unattemptedTasks) {
      const row = this.rows.get(ref.task_id);
      if (row?.status === "running"
          && row.lease_owner === jobId
          && row.dispatch_generation === ref.dispatch_generation) row.status = "leased";
    }
    if (changedKeys.length > 0) {
      this.hashBatches.push([...new Set(changedKeys)].sort());
      this.publications.push(channelId);
    }
    return { done, terminal, retryable, dead_letter: deadLetter, skipped };
  }
}

function executor(repository, fetchDetail, overrides = {}) {
  return new ContentEnrichExecutor({
    repository,
    fetchDetail,
    now: () => new Date(NOW),
    leaseDurationMs: 60_000,
    retryBaseMs: 1_000,
    retryMaxMs: 8_000,
    ...overrides,
  });
}

function manualHeartbeatTimer() {
  let callback = null;
  return {
    set(next) {
      callback = next;
      return { unref() {} };
    },
    clear() {
      callback = null;
    },
    async fire() {
      assert.equal(typeof callback, "function", "a heartbeat must be scheduled");
      const next = callback;
      callback = null;
      next();
      for (let index = 0; index < 8; index += 1) await Promise.resolve();
    },
  };
}

test("Enrich Worker writes detail, closes the Task, refreshes Item Hash, and reconciles Publication", async () => {
  const repository = new InMemoryExecutionRepository([task("video-a", 1)]);

  const result = await executor(repository, async (videoId) => publicDetail(videoId))
    .execute(job([["video-a", 1]]));

  const row = repository.rows.get("video-a");
  assert.equal(result.done, 1);
  assert.equal(row.status, "done");
  assert.equal(row.title, "Enriched video-a");
  assert.equal(row.last_enriched_at, NOW);
  assert.deepEqual(repository.hashBatches, [[row.content_key]]);
  assert.deepEqual(repository.publications, ["UC-enrich"]);
});

test("an incomplete public detail stays retryable instead of recording false completion", async () => {
  const repository = new InMemoryExecutionRepository([task("incomplete-public", 2)]);

  const result = await executor(repository, async (videoId) => publicDetail(videoId, {
    duration_seconds: null,
    view_count: null,
    view_count_text: null,
  })).execute(job([["incomplete-public", 2]]));

  const row = repository.rows.get("incomplete-public");
  assert.equal(result.done, 0);
  assert.equal(result.retryable, 1);
  assert.equal(row.status, "failed");
  assert.equal(row.attempts, 1);
  assert.equal(row.last_enriched_at, null);
  assert.deepEqual(repository.hashBatches, []);
  assert.deepEqual(repository.publications, []);
});

test("authoritative private and unavailable evidence updates access state and closes the Task", async () => {
  const repository = new InMemoryExecutionRepository([
    task("private-video", 2),
    task("gone-video", 3),
  ]);

  const result = await executor(repository, async (videoId) => publicDetail(videoId, {
    access_status: videoId === "private-video" ? "private" : "unavailable",
  })).execute(job([["private-video", 2], ["gone-video", 3]]));

  assert.equal(result.terminal, 2);
  assert.equal(repository.rows.get("private-video").status, "terminal");
  assert.equal(repository.rows.get("private-video").access_status, "private");
  assert.equal(repository.rows.get("private-video").next_retry_at, "2026-08-30T00:00:00.000Z");
  assert.equal(repository.rows.get("gone-video").status, "terminal");
  assert.equal(repository.rows.get("gone-video").access_status, "unavailable");
  assert.equal(repository.rows.get("gone-video").next_retry_at, "2026-08-30T00:00:00.000Z");
});

test("an authoritative terminal error preserves its access evidence source", async () => {
  const repository = new InMemoryExecutionRepository([task("private-error", 4)]);
  const privateError = Object.assign(new Error("This is a private video"), {
    youtube_failure_evidence: { source: "yt_dlp_detail" },
  });

  const result = await executor(repository, async () => {
    throw privateError;
  }).execute(job([["private-error", 4]]));

  const row = repository.rows.get("private-error");
  assert.equal(result.terminal, 1);
  assert.equal(row.status, "terminal");
  assert.equal(row.access_status, "private");
  assert.equal(row.access_status_source, "yt_dlp_detail");
});

test("retryable failures increment attempts and use bounded exponential backoff", async () => {
  const repository = new InMemoryExecutionRepository([task("retry-video", 4, { attempts: 3 })]);

  const result = await executor(repository, async () => {
    throw new Error("temporary upstream timeout");
  }).execute(job([["retry-video", 4]]));

  const row = repository.rows.get("retry-video");
  assert.equal(result.retryable, 1);
  assert.equal(row.status, "failed");
  assert.equal(row.attempts, 4);
  assert.equal(row.next_retry_at, "2026-08-23T00:00:08.000Z");
  assert.equal(contentEnrichRetryDelayMs(20, { baseMs: 1_000, maxMs: 8_000 }), 8_000);
  assert.deepEqual(repository.hashBatches, []);
  assert.deepEqual(repository.publications, []);
});

test("the cumulative retry budget dead-letters a Task and makes duplicate execution a skip", async () => {
  const repository = new InMemoryExecutionRepository([
    task("retry-exhausted", 5, { attempts: 3 }),
  ]);
  const worker = executor(repository, async () => {
    throw new Error("temporary upstream timeout");
  }, { maxAttempts: 4 });

  const exhausted = await worker.execute(job([["retry-exhausted", 5]]));
  const duplicate = await worker.execute(job([["retry-exhausted", 5]]));

  const row = repository.rows.get("retry-exhausted");
  assert.equal(exhausted.retryable, 0);
  assert.equal(exhausted.dead_letter, 1);
  assert.equal(row.status, "dead_letter");
  assert.equal(row.attempts, 4);
  assert.equal(row.next_retry_at, null);
  assert.equal(duplicate.skipped, 1);
});

test("duplicate, completed, and stale-generation Jobs are idempotent skips", async () => {
  const repository = new InMemoryExecutionRepository([task("stable-video", 8)]);
  let fetches = 0;
  const worker = executor(repository, async (videoId) => {
    fetches += 1;
    return publicDetail(videoId);
  });

  await worker.execute(job([["stable-video", 8]]));
  const duplicate = await worker.execute(job([["stable-video", 8]]));
  const stale = await worker.execute(job([["stable-video", 7]]));

  assert.equal(fetches, 1);
  assert.equal(duplicate.skipped, 1);
  assert.equal(stale.skipped, 1);
  assert.equal(repository.rows.get("stable-video").status, "done");
});

test("a result that finishes after its Worker lease expires is fenced before Content is written", async () => {
  const repository = new InMemoryExecutionRepository([task("slow-video", 9)]);
  let currentTime = new Date(NOW);
  const worker = executor(repository, async (videoId) => {
    currentTime = new Date("2026-08-23T00:01:01.000Z");
    return publicDetail(videoId);
  }, {
    now: () => new Date(currentTime),
    leaseDurationMs: 60_000,
  });

  const result = await worker.execute(job([["slow-video", 9]]));

  const row = repository.rows.get("slow-video");
  assert.equal(result.done, 0);
  assert.equal(result.skipped, 1);
  assert.equal(row.status, "running");
  assert.equal(row.title, undefined);
  assert.equal(row.last_enriched_at, null);
  assert.deepEqual(repository.hashBatches, []);
  assert.deepEqual(repository.publications, []);
});

test("a live Worker renews its lease while a slow detail fetch is in flight", async () => {
  let currentTime = new Date(NOW);
  let finishFetch;
  const timer = manualHeartbeatTimer();
  const repository = new InMemoryExecutionRepository([task("slow-live-video", 10, {
    lease_expires_at: "2026-08-23T00:01:00.000Z",
  })]);
  const worker = executor(repository, (videoId) => new Promise((resolve) => {
    finishFetch = () => resolve(publicDetail(videoId));
  }), {
    now: () => new Date(currentTime),
    leaseDurationMs: 60_000,
    heartbeatIntervalMs: 10_000,
    setHeartbeatTimeout: timer.set,
    clearHeartbeatTimeout: timer.clear,
  });

  const execution = worker.execute(job([["slow-live-video", 10]]));
  for (let index = 0; index < 8; index += 1) await Promise.resolve();
  currentTime = new Date("2026-08-23T00:00:20.000Z");
  await timer.fire();
  currentTime = new Date("2026-08-23T00:00:40.000Z");
  await timer.fire();
  currentTime = new Date("2026-08-23T00:01:10.000Z");
  finishFetch();
  const result = await execution;

  assert.equal(result.done, 1);
  assert.ok(repository.renewCalls >= 2);
  assert.ok(result.heartbeat.renewals >= 2);
  assert.equal(result.heartbeat.failures, 0);
  assert.equal(result.heartbeat.lease_lost, false);
  assert.equal(repository.rows.get("slow-live-video").status, "done");
});

test("heartbeat ownership loss aborts the current fetch and stops the rest of the batch", async () => {
  const timer = manualHeartbeatTimer();
  const repository = new InMemoryExecutionRepository([
    task("lease-lost-first", 11),
    task("lease-lost-second", 12),
  ]);
  repository.renewBatch = async () => {
    repository.renewCalls += 1;
    return 0;
  };
  let finishFirstFetch;
  let activeSignal = null;
  const fetches = [];
  const worker = executor(repository, (videoId, { signal } = {}) => {
    fetches.push(videoId);
    activeSignal = signal ?? null;
    if (videoId === "lease-lost-first") {
      return new Promise((resolve) => {
        finishFirstFetch = () => resolve(publicDetail(videoId));
      });
    }
    return Promise.resolve(publicDetail(videoId));
  }, {
    heartbeatIntervalMs: 10_000,
    setHeartbeatTimeout: timer.set,
    clearHeartbeatTimeout: timer.clear,
  });

  const execution = worker.execute(job([
    ["lease-lost-first", 11],
    ["lease-lost-second", 12],
  ]));
  for (let index = 0; index < 8; index += 1) await Promise.resolve();
  await timer.fire();

  assert.equal(activeSignal?.aborted, true);
  finishFirstFetch();
  const result = await execution;

  assert.deepEqual(fetches, ["lease-lost-first"]);
  assert.equal(result.done, 0);
  assert.equal(result.heartbeat.lease_lost, true);
  assert.equal(repository.rows.get("lease-lost-first").status, "leased");
  assert.equal(repository.rows.get("lease-lost-second").status, "leased");
  assert.equal(repository.rows.get("lease-lost-first").title, undefined);
  assert.deepEqual(repository.hashBatches, []);
  assert.deepEqual(repository.publications, []);
});

test("a Route failure durably retries the attempted Task and releases untouched Tasks for the new Route", async () => {
  const repository = new InMemoryExecutionRepository([
    task("rate-limited", 9),
    task("after-rotation", 10),
  ]);
  const routeError = Object.assign(new Error("HTTP 429 rate limited"), {
    youtube_failure_decision: { kind: "youtube_rate_limited", retry_mode: "new_identity" },
  });
  const worker = executor(repository, async (videoId) => {
    if (videoId === "rate-limited") throw routeError;
    return publicDetail(videoId);
  });

  await assert.rejects(
    worker.execute(job([["rate-limited", 9], ["after-rotation", 10]])),
    (error) => error === routeError && error.content_enrich_checkpoint_persisted === true,
  );

  assert.equal(repository.rows.get("rate-limited").status, "failed");
  assert.equal(repository.rows.get("rate-limited").attempts, 1);
  assert.equal(repository.rows.get("after-rotation").status, "leased");

  const resumed = await worker.execute(job([["rate-limited", 9], ["after-rotation", 10]]));
  assert.equal(resumed.done, 1);
  assert.equal(repository.rows.get("after-rotation").status, "done");
});

test("a Route failure does not claim a durable checkpoint after its lease expires", async () => {
  const repository = new InMemoryExecutionRepository([
    task("expired-rate-limit", 11),
  ]);
  const routeError = Object.assign(new Error("HTTP 429 after lease expiry"), {
    youtube_failure_decision: { kind: "youtube_rate_limited", retry_mode: "new_identity" },
  });
  let clockReads = 0;
  const worker = executor(repository, async () => {
    throw routeError;
  }, {
    now: () => new Date(clockReads++ === 0 ? NOW : "2026-08-23T00:02:00.000Z"),
  });

  await assert.rejects(
    worker.execute(job([["expired-rate-limit", 11]])),
    (error) => error === routeError && error.content_enrich_checkpoint_persisted !== true,
  );

  const row = repository.rows.get("expired-rate-limit");
  assert.equal(row.status, "running");
  assert.equal(row.attempts, 0);
});

test("PostgreSQL settlement fences the Task and runs Content, Hash, and Publication in one transaction", async () => {
  const row = task("postgres-chain", 11);
  const calls = [];
  const client = {
    async query(sql, params = []) {
      if (sql.includes("SELECT requested.ordinal")) {
        return {
          rowCount: 1,
          rows: [{ ordinal: 0, task: structuredClone(row), content: structuredClone(row) }],
        };
      }
      if (sql.includes("SET status='running'")) {
        assert.match(sql, /lease_expires_at=clock_timestamp\(\)/);
        row.status = "running";
        row.last_attempt_at = NOW;
        row.lease_expires_at = new Date(Date.parse(NOW) + Number(params[3])).toISOString();
        return { rowCount: 1, rows: [{ task_id: row.task_id }] };
      }
      if (sql.includes("to_jsonb(task) AS task")) {
        return {
          rowCount: 1,
          rows: [{
            disposition: "outcome",
            task: structuredClone(row),
            content: structuredClone(row),
          }],
        };
      }
      if (sql.includes("SELECT task.task_id")) {
        return { rowCount: 1, rows: [{ task_id: row.task_id }] };
      }
      if (sql.includes("SET status=$5")) {
        row.status = params[4];
        row.lease_owner = null;
        row.lease_expires_at = null;
        calls.push("task-closed");
        return { rowCount: 1, rows: [] };
      }
      throw new Error(`unexpected SQL in PostgreSQL Enrich fixture: ${sql}`);
    },
  };
  const repository = new PostgresContentEnrichExecutionRepository({
    withTransaction: async (action) => action(client),
    applyDetail: async (_client, input) => {
      calls.push("content");
      row.title = input.detail.title;
      row.last_enriched_at = input.observedAt;
    },
    refreshHashes: async (_client, keys) => {
      calls.push(`hash:${keys.join(",")}`);
    },
    reconcilePublication: async (_client, input) => {
      calls.push(`publication:${input.channelId}`);
    },
  });

  const result = await executor(repository, async (videoId) => publicDetail(videoId))
    .execute(job([["postgres-chain", 11]]));

  assert.equal(result.done, 1);
  assert.equal(row.status, "done");
  assert.equal(row.title, "Enriched postgres-chain");
  assert.deepEqual(calls, [
    "content",
    "task-closed",
    `hash:${row.content_key}`,
    "publication:UC-enrich",
  ]);
});
