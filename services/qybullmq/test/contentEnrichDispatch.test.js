import assert from "node:assert/strict";
import test from "node:test";
import {
  ContentEnrichDispatcher,
  PostgresContentEnrichDispatchRepository,
  contentEnrichJobId,
} from "../src/contentEnrichDispatch.js";

function task(taskId, channelId, overrides = {}) {
  return {
    task_id: taskId,
    channel_id: channelId,
    job_type: "player-refresh",
    status: "queued",
    priority: 10,
    attempts: 0,
    dispatch_generation: 0,
    created_at: "2026-08-01T00:00:00.000Z",
    next_retry_at: "2026-08-01T00:00:00.000Z",
    lease_owner: null,
    lease_expires_at: null,
    ...overrides,
  };
}

class InMemoryDispatchRepository {
  constructor(rows = []) {
    this.rows = new Map(rows.map((row) => [row.task_id, structuredClone(row)]));
    this.leaseCalls = 0;
  }

  async listLeasedBatches() {
    const grouped = new Map();
    for (const row of this.rows.values()) {
      if (!row.lease_owner || row.status !== "leased") continue;
      const key = `${row.lease_owner}:${row.channel_id}`;
      const batch = grouped.get(key) ?? {
        job_id: row.lease_owner,
        channel_id: row.channel_id,
        tasks: [],
      };
      batch.tasks.push(structuredClone(row));
      grouped.set(key, batch);
    }
    return [...grouped.values()];
  }

  async refreshLease({ jobId, taskIds, leaseExpiresAt }) {
    for (const taskId of taskIds) {
      const row = this.rows.get(taskId);
      if (row?.lease_owner === jobId) row.lease_expires_at = leaseExpiresAt.toISOString();
    }
  }

  async releaseLease({ jobId, taskIds }) {
    for (const taskId of taskIds) {
      const row = this.rows.get(taskId);
      if (row?.lease_owner !== jobId) continue;
      row.lease_owner = null;
      row.lease_expires_at = null;
      if (row.status === "leased") row.status = "queued";
    }
  }

  async leaseFairBatches({
    maxJobs,
    batchSize,
    leaseExpiresAt,
    now,
    excludeChannelIds = [],
  }) {
    this.leaseCalls += 1;
    const excludedChannels = new Set(excludeChannelIds);
    for (const row of this.rows.values()) {
      if (row.status !== "running" || Date.parse(row.lease_expires_at) > now.getTime()) continue;
      row.status = "queued";
      row.lease_owner = null;
      row.lease_expires_at = null;
    }
    const eligible = [...this.rows.values()]
      .filter((row) => row.job_type === "player-refresh"
        && (["queued", "failed"].includes(row.status)
          || (row.status === "terminal" && row.next_retry_at != null))
        && (row.next_retry_at == null || Date.parse(row.next_retry_at) <= now.getTime())
        && !row.lease_owner
        && !excludedChannels.has(row.channel_id))
      .sort((left, right) => left.priority - right.priority
        || left.created_at.localeCompare(right.created_at)
        || left.task_id.localeCompare(right.task_id));
    const channels = [...new Set(eligible.map((row) => row.channel_id))].slice(0, maxJobs);
    return channels.map((channelId) => {
      const rows = eligible.filter((row) => row.channel_id === channelId).slice(0, batchSize);
      const leased = rows.map((row) => ({ ...row, dispatch_generation: row.dispatch_generation + 1 }));
      const jobId = contentEnrichJobId({ channelId, tasks: leased });
      for (const leasedTask of leased) {
        const row = this.rows.get(leasedTask.task_id);
        row.dispatch_generation = leasedTask.dispatch_generation;
        if (row.status === "terminal") {
          row.attempts = 0;
          row.next_retry_at = null;
        }
        row.status = "leased";
        row.lease_owner = jobId;
        row.lease_expires_at = leaseExpiresAt.toISOString();
      }
      return { job_id: jobId, channel_id: channelId, tasks: leased };
    });
  }
}

function queueFixture({ initialJobs = [], failAdds = 0 } = {}) {
  const jobs = new Map(initialJobs.map((job) => [job.id, job]));
  let remainingFailures = failAdds;
  return {
    jobs,
    async getJobCounts() {
      const counts = { waiting: 0, active: 0, delayed: 0, paused: 0, prioritized: 0 };
      for (const job of jobs.values()) counts[job.state] = (counts[job.state] ?? 0) + 1;
      return counts;
    },
    async getJob(jobId) {
      return jobs.get(jobId) ?? null;
    },
    async add(name, data, options) {
      if (remainingFailures > 0) {
        remainingFailures -= 1;
        throw new Error("simulated BullMQ outage after the database lease");
      }
      const existing = jobs.get(options.jobId);
      if (existing) return existing;
      const job = {
        id: options.jobId,
        name,
        data,
        state: "waiting",
        async getState() { return this.state; },
      };
      jobs.set(job.id, job);
      return job;
    },
  };
}

function dispatcher(repository, queue, overrides = {}) {
  return new ContentEnrichDispatcher({
    repository,
    queue,
    enabled: true,
    highWater: 10,
    refill: 10,
    batchSize: 2,
    leaseDurationMs: 60_000,
    now: () => new Date("2026-08-23T00:00:00.000Z"),
    ...overrides,
  });
}

test("Controller leases at most one small batch per channel in each fair refill", async () => {
  const repository = new InMemoryDispatchRepository([
    ...Array.from({ length: 6 }, (_, index) => task(`large-${index}`, "UC-large")),
    task("small-a", "UC-small-a"),
    task("small-b", "UC-small-b"),
  ]);
  const queue = queueFixture();

  const result = await dispatcher(repository, queue).dispatchAvailable();

  assert.equal(result.enqueued, 3);
  assert.deepEqual(
    [...queue.jobs.values()].map((job) => [job.data.channel_id, job.data.task_ids.length]).sort(),
    [["UC-large", 2], ["UC-small-a", 1], ["UC-small-b", 1]],
  );
});

test("Controller does not lease more work at or above the queue High Water", async () => {
  const repository = new InMemoryDispatchRepository([task("pending", "UC-pending")]);
  const queue = queueFixture({
    initialJobs: Array.from({ length: 4 }, (_, index) => ({
      id: `existing-${index}`,
      state: index === 0 ? "active" : "waiting",
      async getState() { return this.state; },
    })),
  });

  const result = await dispatcher(repository, queue, { highWater: 4 }).dispatchAvailable();

  assert.equal(result.reason, "high_water");
  assert.equal(repository.leaseCalls, 0);
  assert.equal(queue.jobs.size, 4);
});

test("Controller opens only a due terminal access recheck as one deterministic new generation", async () => {
  const repository = new InMemoryDispatchRepository([
    task("terminal-future", "UC-terminal-future", {
      status: "terminal",
      attempts: 4,
      dispatch_generation: 7,
      next_retry_at: "2026-08-24T00:00:00.000Z",
    }),
    task("terminal-due", "UC-terminal-due", {
      status: "terminal",
      attempts: 6,
      dispatch_generation: 11,
      next_retry_at: "2026-08-22T00:00:00.000Z",
    }),
  ]);
  const queue = queueFixture();
  const controller = dispatcher(repository, queue, { batchSize: 1 });

  const first = await controller.dispatchAvailable();
  const due = repository.rows.get("terminal-due");
  const future = repository.rows.get("terminal-future");
  const expectedJobId = contentEnrichJobId({
    channelId: due.channel_id,
    tasks: [{ task_id: due.task_id, dispatch_generation: 12 }],
  });

  assert.equal(first.enqueued, 1);
  assert.equal(due.status, "leased");
  assert.equal(due.attempts, 0);
  assert.equal(due.next_retry_at, null);
  assert.equal(due.dispatch_generation, 12);
  assert.equal(due.lease_owner, expectedJobId);
  assert.equal(future.status, "terminal");
  assert.equal(future.dispatch_generation, 7);

  const duplicate = await controller.dispatchAvailable();
  assert.equal(duplicate.enqueued, 0);
  assert.equal(duplicate.existing, 1);
  assert.equal(queue.jobs.size, 1);
});

test("Controller refreshes leases for already queued Jobs even at High Water", async () => {
  const jobId = "content_enrich__UC-waiting__persisted";
  const repository = new InMemoryDispatchRepository([task("waiting", "UC-waiting", {
    status: "leased",
    dispatch_generation: 3,
    lease_owner: jobId,
    lease_expires_at: "2026-08-22T00:00:00.000Z",
  })]);
  const queue = queueFixture({
    initialJobs: [
      {
        id: jobId,
        state: "waiting",
        async getState() { return this.state; },
      },
      ...Array.from({ length: 3 }, (_, index) => ({
        id: `other-${index}`,
        state: "waiting",
        async getState() { return this.state; },
      })),
    ],
  });

  const result = await dispatcher(repository, queue, { highWater: 4 }).dispatchAvailable();

  assert.equal(result.reason, "high_water");
  assert.equal(result.existing, 1);
  assert.equal(repository.rows.get("waiting").lease_expires_at, "2026-08-23T00:01:00.000Z");
  assert.equal(repository.leaseCalls, 0);
});

test("an undelivered lease consumes refill budget when its BullMQ replay fails", async () => {
  const repository = new InMemoryDispatchRepository([
    task("undelivered", "UC-undelivered", {
      status: "leased",
      dispatch_generation: 2,
      lease_owner: "content_enrich__UC-undelivered__persisted",
      lease_expires_at: "2026-08-22T00:00:00.000Z",
    }),
    task("must-wait", "UC-must-wait"),
  ]);
  const queue = queueFixture({ failAdds: 1 });

  const result = await dispatcher(repository, queue, {
    highWater: 1,
    refill: 1,
    batchSize: 1,
  }).dispatchAvailable();

  assert.equal(result.failed, 1);
  assert.equal(result.enqueued, 0);
  assert.equal(queue.jobs.size, 0);
  assert.equal(repository.rows.get("must-wait").status, "queued");
  assert.equal(repository.leaseCalls, 0);
});

test("concurrent Controllers serialize the complete High Water dispatch cycle", async () => {
  const repository = new InMemoryDispatchRepository([
    task("pending-a", "UC-pending-a"),
    task("pending-b", "UC-pending-b"),
    task("pending-c", "UC-pending-c"),
    task("pending-d", "UC-pending-d"),
  ]);
  const queue = queueFixture();
  const originalGetJobCounts = queue.getJobCounts.bind(queue);
  let releaseCountReaders;
  const countReadersReady = new Promise((resolve) => { releaseCountReaders = resolve; });
  let countReaders = 0;
  queue.getJobCounts = async (...states) => {
    const counts = await originalGetJobCounts(...states);
    countReaders += 1;
    if (countReaders >= 2) releaseCountReaders();
    await countReadersReady;
    return counts;
  };

  let dispatchLocked = false;
  repository.withDispatchLock = async (action) => {
    if (dispatchLocked) {
      releaseCountReaders();
      return { acquired: false, result: null };
    }
    dispatchLocked = true;
    try {
      return { acquired: true, result: await action(repository) };
    } finally {
      dispatchLocked = false;
    }
  };

  const first = dispatcher(repository, queue, { highWater: 2, refill: 2 });
  const second = dispatcher(repository, queue, { highWater: 2, refill: 2 });
  const results = await Promise.all([
    first.dispatchAvailable(),
    second.dispatchAvailable(),
  ]);

  assert.equal(queue.jobs.size, 2);
  assert.equal(results.reduce((total, result) => total + result.enqueued, 0), 2);
  assert.equal(results.filter((result) => result.reason === "dispatch_locked").length, 1);
});

test("dispatch ownership is a committed expiring mutex for transaction-mode PgBouncer", async () => {
  const statements = [];
  let transactionOpen = false;
  let acquisitionCommitted = false;
  const client = {
    async query(sql, params = []) {
      statements.push({ sql, params });
      if (sql === "BEGIN") {
        transactionOpen = true;
        return { rows: [], rowCount: 0 };
      }
      if (sql === "COMMIT") {
        transactionOpen = false;
        acquisitionCommitted = true;
        return { rows: [], rowCount: 0 };
      }
      if (sql.includes("jsonb_build_object") && sql.includes("RETURNING value_json")) {
        return { rows: [{ value_json: { owner: "controller-a" } }], rowCount: 1 };
      }
      return { rows: [], rowCount: 0 };
    },
  };
  const withTransaction = async (action) => {
    await client.query("BEGIN");
    try {
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction,
    dispatchLockOwner: () => "controller-a",
  });

  const locked = await repository.withDispatchLock(async (_lockedRepository, lock) => {
    assert.equal(acquisitionCommitted, true);
    assert.equal(transactionOpen, false);
    assert.equal(lock.owner, "controller-a");
    return "owned";
  });

  assert.deepEqual(locked, { acquired: true, result: "owned" });
  assert.equal(statements[0].sql, "BEGIN");
  assert.match(statements[2].sql, /clock_timestamp\(\).*interval '1 millisecond'/s);
  assert.equal(statements[3].sql, "COMMIT");
  assert.match(statements.at(-1).sql, /value_json->>'owner'=\$2/);
  assert.equal(
    statements.some(({ sql }) => /pg_try_advisory_lock\(/.test(sql)),
    false,
    "session advisory locks are unsafe through transaction-mode PgBouncer",
  );
  assert.equal(
    statements.some(({ sql }) => /pg_advisory_unlock/.test(sql)),
    false,
  );
});

test("a Controller that lost its committed mutex cannot lease another batch", async () => {
  let selected = false;
  const client = {
    async query(sql) {
      if (sql.includes("pg_try_advisory_xact_lock")) {
        return { rows: [{ acquired: true }], rowCount: 1 };
      }
      if (sql.includes("content_enrich_dispatch_mutex") && sql.includes("FOR SHARE")) {
        return { rows: [], rowCount: 0 };
      }
      if (sql.includes("SELECT chosen.*")) {
        selected = true;
        return { rows: [], rowCount: 0 };
      }
      return { rows: [], rowCount: 1 };
    },
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction: async (action) => action(client),
  });

  await assert.rejects(
    repository.leaseFairBatches({
      maxJobs: 1,
      batchSize: 1,
      leaseDurationMs: 60_000,
      dispatchOwner: "controller-expired",
    }),
    /dispatch mutex ownership was lost/,
  );
  assert.equal(selected, false);
});

test("a Controller that lost its committed mutex cannot refresh an old lease", async () => {
  let leaseRefreshed = false;
  const client = {
    async query(sql) {
      if (sql.includes("content_enrich_dispatch_mutex") && sql.includes("FOR SHARE")) {
        return { rows: [], rowCount: 0 };
      }
      if (sql.includes("UPDATE crawler.content_enrich_tasks")) leaseRefreshed = true;
      return { rows: [], rowCount: 1 };
    },
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction: async (action) => action(client),
  });

  await assert.rejects(
    repository.refreshLease({
      jobId: "content_enrich__UC-stale__old",
      taskIds: ["stale-task"],
      leaseDurationMs: 60_000,
      dispatchOwner: "controller-expired",
    }),
    /dispatch mutex ownership was lost/,
  );
  assert.equal(leaseRefreshed, false);
});

test("BullMQ delivery cannot race ahead of the committed database lease", async () => {
  const selectedTask = task("commit-before-delivery", "UC-commit", {
    dispatch_generation: 0,
  });
  let transactionOpen = false;
  let stagedLease = false;
  let committedLease = false;
  let leaseVisibleWhenDelivered = null;
  const client = {
    async query(sql, params = []) {
      if (sql === "BEGIN") {
        assert.equal(transactionOpen, false, "transactions must not be nested");
        transactionOpen = true;
        return { rows: [], rowCount: 0 };
      }
      if (sql === "COMMIT") {
        assert.equal(transactionOpen, true);
        transactionOpen = false;
        if (stagedLease) committedLease = true;
        return { rows: [], rowCount: 0 };
      }
      if (sql === "ROLLBACK") {
        transactionOpen = false;
        stagedLease = false;
        return { rows: [], rowCount: 0 };
      }
      if (sql.includes("pg_try_advisory_xact_lock")) {
        return { rows: [{ acquired: true }], rowCount: 1 };
      }
      if (sql.includes("AS owned") && sql.includes("content_enrich_dispatch_mutex")) {
        return { rows: [{ owned: true, live: true }], rowCount: 1 };
      }
      if (sql.includes("setting_key='content_enrich_dispatch'") && sql.includes("SELECT")) {
        return { rows: [{ mode: "queue" }], rowCount: 1 };
      }
      if (sql.includes("FROM owners")) return { rows: [], rowCount: 0 };
      if (sql.includes("setting_key='content_enrich_dispatch_cursor'") && sql.includes("FOR UPDATE")) {
        return { rows: [{ channel_id: "" }], rowCount: 1 };
      }
      if (sql.includes("SELECT chosen.*")) {
        return { rows: [structuredClone(selectedTask)], rowCount: 1 };
      }
      if (sql.includes("UPDATE crawler.content_enrich_tasks task")) {
        const [reference] = JSON.parse(params[0]);
        stagedLease = true;
        return {
          rows: [{
            ...structuredClone(selectedTask),
            ...reference,
            status: "leased",
            lease_owner: params[1],
            lease_expires_at: params[2],
          }],
          rowCount: 1,
        };
      }
      return { rows: [], rowCount: 1 };
    },
  };
  const withTransaction = async (action) => {
    await client.query("BEGIN");
    try {
      const result = await action(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    }
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction,
  });
  const queue = {
    async getJobCounts() { return {}; },
    async getJob() { return null; },
    async add() {
      leaseVisibleWhenDelivered = committedLease;
      return { id: "delivered" };
    },
  };

  const result = await dispatcher(repository, queue, { highWater: 1, refill: 1 }).dispatchAvailable();

  assert.equal(result.enqueued, 1);
  assert.equal(leaseVisibleWhenDelivered, true);
  assert.equal(transactionOpen, false);
});

test("deterministic Job IDs deduplicate replay and change only for a new dispatch generation", () => {
  const first = contentEnrichJobId({
    channelId: "UC-stable",
    tasks: [
      task("task-b", "UC-stable", { dispatch_generation: 4 }),
      task("task-a", "UC-stable", { dispatch_generation: 2 }),
    ],
  });
  const replay = contentEnrichJobId({
    channelId: "UC-stable",
    tasks: [
      task("task-a", "UC-stable", { dispatch_generation: 2 }),
      task("task-b", "UC-stable", { dispatch_generation: 4 }),
    ],
  });
  const next = contentEnrichJobId({
    channelId: "UC-stable",
    tasks: [
      task("task-a", "UC-stable", { dispatch_generation: 3 }),
      task("task-b", "UC-stable", { dispatch_generation: 5 }),
    ],
  });

  assert.equal(first, replay);
  assert.notEqual(first, next);
  assert.match(first, /^content_enrich__/);
});

test("an expired database lease with no BullMQ Job is replayed with the persisted Job ID", async () => {
  const leasedTask = task("leased", "UC-expired", {
    status: "leased",
    dispatch_generation: 7,
    lease_owner: "content_enrich__UC-expired__persisted",
    lease_expires_at: "2026-08-22T00:00:00.000Z",
  });
  const repository = new InMemoryDispatchRepository([leasedTask]);
  const queue = queueFixture();

  const result = await dispatcher(repository, queue).dispatchAvailable();

  assert.equal(result.recovered, 1);
  assert.equal(queue.jobs.has(leasedTask.lease_owner), true);
  assert.equal(repository.rows.get("leased").dispatch_generation, 7);
  assert.equal(repository.rows.get("leased").lease_expires_at, "2026-08-23T00:01:00.000Z");
});

test("a crash between leasing and BullMQ delivery recovers without creating a second Job", async () => {
  const repository = new InMemoryDispatchRepository([task("crash-window", "UC-crash")]);
  const queue = queueFixture({ failAdds: 1 });
  const controller = dispatcher(repository, queue);

  const failed = await controller.dispatchAvailable();
  const persistedJobId = repository.rows.get("crash-window").lease_owner;
  assert.equal(failed.failed, 1);
  assert.equal(queue.jobs.size, 0);

  const recovered = await controller.dispatchAvailable();
  const replayed = await controller.dispatchAvailable();

  assert.equal(recovered.recovered, 1);
  assert.equal(replayed.existing, 1);
  assert.equal(queue.jobs.size, 1);
  assert.equal(queue.jobs.has(persistedJobId), true);
  assert.equal(repository.rows.get("crash-window").dispatch_generation, 1);
  assert.equal(repository.rows.get("crash-window").status, "leased");
});

test("a recovered delivery consumes that channel's only fair slot for the refill", async () => {
  const recoveredJobId = "content_enrich__UC-a__persisted";
  const repository = new InMemoryDispatchRepository([
    task("a-recovery", "UC-a", {
      status: "leased",
      dispatch_generation: 3,
      lease_owner: recoveredJobId,
      lease_expires_at: "2026-08-22T00:00:00.000Z",
    }),
    task("a-next", "UC-a"),
    task("b-next", "UC-b"),
  ]);
  const queue = queueFixture();

  const result = await dispatcher(repository, queue, {
    highWater: 2,
    refill: 2,
    batchSize: 1,
  }).dispatchAvailable();

  assert.equal(result.recovered, 1);
  assert.equal(result.enqueued, 1);
  assert.deepEqual(
    [...queue.jobs.values()].map((queuedJob) => queuedJob.data.channel_id).sort(),
    ["UC-a", "UC-b"],
  );
  assert.equal(repository.rows.get("a-next").status, "queued");
  assert.equal(repository.rows.get("b-next").status, "leased");
});

test("an expired running attempt is fenced by a new generation instead of replaying its old Job", async () => {
  const oldJobId = "content_enrich__UC-running__old";
  const repository = new InMemoryDispatchRepository([task("stale-running", "UC-running", {
    status: "running",
    dispatch_generation: 12,
    lease_owner: oldJobId,
    lease_expires_at: "2026-08-22T00:00:00.000Z",
  })]);
  const queue = queueFixture();

  const result = await dispatcher(repository, queue).dispatchAvailable();

  const row = repository.rows.get("stale-running");
  assert.equal(result.enqueued, 1);
  assert.equal(row.dispatch_generation, 13);
  assert.notEqual(row.lease_owner, oldJobId);
  assert.equal(queue.jobs.has(oldJobId), false);
  assert.equal(queue.jobs.has(row.lease_owner), true);
});

test("PostgreSQL fair claim uses one locked player-refresh selection with SKIP LOCKED", async () => {
  const statements = [];
  const client = {
    async query(sql, params = []) {
      statements.push({ sql, params });
      if (sql.includes("pg_try_advisory_xact_lock")) return { rows: [{ acquired: true }], rowCount: 1 };
      if (sql.includes("AS owned") && sql.includes("content_enrich_dispatch_mutex")) {
        return { rows: [{ owned: true, live: true }], rowCount: 1 };
      }
      if (sql.includes("content_enrich_dispatch_mutex") && sql.includes("FOR SHARE")) {
        return { rows: [{ setting_key: "content_enrich_dispatch_mutex" }], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.settings")) return { rows: [{ mode: "queue" }], rowCount: 1 };
      if (sql.includes("SELECT chosen.*")) return { rows: [], rowCount: 0 };
      return { rows: [], rowCount: 0 };
    },
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction: async (action) => action(client),
  });

  await repository.leaseFairBatches({
    maxJobs: 5,
    batchSize: 3,
    now: new Date("2026-08-23T00:00:00.000Z"),
    leaseExpiresAt: new Date("2026-08-23T00:05:00.000Z"),
    leaseDurationMs: 60_000,
    dispatchOwner: "controller-a",
  });

  const claim = statements.find(({ sql }) => sql.includes("SELECT chosen.*"))?.sql ?? "";
  const expiredRunning = statements.find(({ sql }) => (
    sql.includes("status='running'") && sql.includes("lease_expires_at<=clock_timestamp()")
  ))?.sql ?? "";
  assert.match(expiredRunning, /SET status='queued',lease_owner=NULL,lease_expires_at=NULL/);
  assert.match(claim, /job_type='player-refresh'/);
  assert.match(claim, /JOIN LATERAL/);
  assert.match(claim, /LIMIT \$2[\s\S]*FOR UPDATE OF task SKIP LOCKED/);
  assert.match(claim, /JOIN crawler\.contents content/);
  assert.match(claim, /JOIN crawler\.channels registry/);
  assert.match(claim, /GROUP BY task\.channel_id/);
  assert.match(claim, /AS dispatch_tier/);
  assert.match(claim, /COALESCE\(task\.next_retry_at,clock_timestamp\(\)\)<=clock_timestamp\(\)/);
  assert.match(claim, /NOT \(task\.channel_id=ANY\(\$4::text\[\]\)\)/);
});

test("PostgreSQL fair claim advances a persistent channel cursor between refills", async () => {
  const statements = [];
  const selectedRows = [
    task("next-a", "UC-next", { dispatch_generation: 4 }),
    task("wrap-a", "UC-wrap", { dispatch_generation: 8 }),
  ];
  const client = {
    async query(sql, params = []) {
      statements.push({ sql, params });
      if (sql.includes("setting_key='content_enrich_dispatch'") && sql.includes("FOR SHARE")) {
        return { rows: [{ mode: "queue" }], rowCount: 1 };
      }
      if (sql.includes("pg_try_advisory_xact_lock")) {
        return { rows: [{ acquired: true }], rowCount: 1 };
      }
      if (sql.includes("AS owned") && sql.includes("content_enrich_dispatch_mutex")) {
        return { rows: [{ owned: true, live: true }], rowCount: 1 };
      }
      if (sql.includes("content_enrich_dispatch_mutex") && sql.includes("FOR SHARE")) {
        return { rows: [{ setting_key: "content_enrich_dispatch_mutex" }], rowCount: 1 };
      }
      if (sql.includes("setting_key='content_enrich_dispatch_cursor'") && sql.includes("FOR UPDATE")) {
        return { rows: [{ channel_id: "UC-middle" }], rowCount: 1 };
      }
      if (sql.includes("SELECT chosen.*")) {
        return { rows: selectedRows.map((row) => structuredClone(row)), rowCount: 2 };
      }
      if (sql.includes("UPDATE crawler.content_enrich_tasks task")) {
        const references = JSON.parse(params[0]);
        return {
          rows: references.map((reference) => ({
            ...selectedRows.find((row) => row.task_id === reference.task_id),
            ...reference,
            status: "leased",
            lease_owner: params[1],
          })),
          rowCount: references.length,
        };
      }
      return { rows: [], rowCount: 1 };
    },
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction: async (action) => action(client),
  });

  await repository.leaseFairBatches({
    maxJobs: 2,
    batchSize: 1,
    now: new Date("2026-08-23T00:00:00.000Z"),
    leaseExpiresAt: new Date("2026-08-23T00:05:00.000Z"),
    leaseDurationMs: 60_000,
    dispatchOwner: "controller-a",
  });

  const selection = statements.find(({ sql }) => sql.includes("SELECT chosen.*"));
  assert.equal(selection.params[2], "UC-middle");
  assert.equal(selection.params[4], 90);
  assert.match(
    selection.sql,
    /ORDER BY channel\.dispatch_tier,channel\.priority,[\s\S]*channel\.cursor_partition,channel\.channel_id/,
  );
  const cursorUpdate = statements.find(({ sql }) => (
    sql.includes("UPDATE crawler.settings")
    && sql.includes("setting_key='content_enrich_dispatch_cursor'")
  ));
  assert.equal(cursorUpdate.params[0], "UC-wrap");
});

test("PostgreSQL dispatch mode rejects unsupported ownership state", async () => {
  const client = {
    async query() {
      return { rows: [{ mode: "split-brain" }], rowCount: 1 };
    },
  };
  const repository = new PostgresContentEnrichDispatchRepository({
    queryFn: client.query.bind(client),
    withTransaction: async (action) => action(client),
  });

  await assert.rejects(repository.dispatchMode(), /unsupported Content Enrich consumer mode/);
});
