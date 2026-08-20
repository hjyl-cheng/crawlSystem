import assert from "node:assert/strict";
import test from "node:test";
import {
  InMemoryManagedJobDispatchRepository,
  ManagedJobOutboxDispatcher,
} from "../src/managedJobDispatchOutbox.js";

function dispatch(overrides = {}) {
  return {
    dispatch_id: "dispatch-1",
    aggregate_kind: "discover_page",
    aggregate_id: "page-1",
    intent_hash: "sha256:intent",
    queue_registry_key: "youtube-discover-page",
    deterministic_job_id: "discover-page__page-1",
    payload_json: { page_id: "page-1", intent_schema_version: 1 },
    status: "pending",
    attempts: 0,
    ...overrides,
  };
}

function deduplicatingQueue() {
  const jobs = new Map();
  return {
    jobs,
    async add(name, data, options) {
      const existing = jobs.get(options.jobId);
      if (existing) return existing;
      const job = { id: options.jobId, name, data };
      jobs.set(job.id, job);
      return job;
    },
  };
}

test("Outbox dispatch marks the persistent aggregate enqueued only after BullMQ accepts it", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
  });

  const result = await dispatcher.dispatchAvailable({ limit: 10 });

  assert.deepEqual(result, { claimed: 1, sent: 1, failed: 0, dead: 0 });
  assert.equal(queue.jobs.size, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "sent");
  assert.deepEqual(repository.aggregates.get("discover_page:page-1"), {
    dispatch_status: "enqueued",
    dispatched_job_id: "discover-page__page-1",
  });
});

test("replaying the queue-add/mark-sent crash window cannot create a second BullMQ job", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const queue = deduplicatingQueue();
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: { "youtube-discover-page": queue },
  });
  repository.failNextMarkSent = true;

  await assert.rejects(dispatcher.dispatchAvailable({ limit: 1 }), /simulated mark-sent crash/);
  assert.equal(queue.jobs.size, 1);
  repository.recoverSending("dispatch-1");
  const replay = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.equal(replay.sent, 1);
  assert.equal(queue.jobs.size, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "sent");
});

test("a repeatedly rejected dispatch becomes dead and its aggregate becomes terminal", async () => {
  const repository = new InMemoryManagedJobDispatchRepository({ rows: [dispatch()] });
  const dispatcher = new ManagedJobOutboxDispatcher({
    repository,
    queues: {
      "youtube-discover-page": {
        async add() { throw new Error("redis unavailable"); },
      },
    },
    maxAttempts: 2,
    retryDelayMs: () => 0,
  });

  const first = await dispatcher.dispatchAvailable({ limit: 1 });
  const second = await dispatcher.dispatchAvailable({ limit: 1 });

  assert.equal(first.failed, 1);
  assert.equal(second.dead, 1);
  assert.equal(repository.rows.get("dispatch-1").status, "dead");
  assert.equal(repository.aggregates.get("discover_page:page-1").dispatch_status, "terminal");
});
