import assert from "node:assert/strict";
import test from "node:test";
import { validateIncrementalJob } from "../src/incrementalPlan.js";
import { channelCandidateFailureDisposition } from "../src/managedWorkerJob.js";
import { ProxyBusinessRunPreparer } from "../src/proxyBusinessRun.js";
import { queuesByRole } from "../src/queues.js";

const resolvedPolicy = {
  policy: {
    id: "qy-br-channel-anonymous-v1",
    version: 1,
    hash: "sha256:policy",
  },
};

function bindingStore(result = {}) {
  const calls = [];
  return {
    calls,
    async resolve(input) {
      calls.push(input);
      return {
        created: result.created ?? true,
        terminal: false,
        binding: {
          business_run_key: input.businessRunKey,
          business_run_id: input.explicitBusinessRunId || "run:reserved",
          status: input.requestedStatus,
        },
      };
    },
  };
}

test("a standalone Detail recovery reuses its existing Channel Business Run", async () => {
  const store = bindingStore();
  const job = {
    id: "content-detail:run:existing",
    name: "content-detail-batch",
    queueName: queuesByRole.contentDetail,
    attemptsMade: 1,
    data: { channel_id: "UC1", run_id: "run:existing" },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM (SELECT")) return { rows: [{ channel_status: "active" }] };
      if (sql.includes("FROM crawler.channel_runs")) {
        return {
          rows: [{
            run_id: "run:existing",
            business_run_key: "full-candidate:42",
            business_run_id: "run:existing",
            binding_status: "materialized",
            identity_policy_id: resolvedPolicy.policy.id,
            identity_policy_version: resolvedPolicy.policy.version,
            identity_policy_hash: resolvedPolicy.policy.hash,
          }],
        };
      }
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:existing");
  assert.equal(prepared.businessRunKey, "full-candidate:42");
  assert.equal(prepared.workloadKind, "channel_full");
  assert.equal(prepared.initialResumeMode, "bullmq_redelivery_resume");
  assert.equal(store.calls.length, 0);
});

test("a candidate Full job resolves and caches one reserved Business Run", async () => {
  const store = bindingStore();
  const job = {
    id: "job-1",
    name: "channel-snapshot",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: { channel_id: "UC1", candidate_id: 42, crawl_mode: "full" },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{}] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:reserved");
  assert.equal(prepared.workloadKind, "channel_full");
  assert.equal(prepared.businessRunKey, "full-candidate:42");
  assert.equal(job.data.run_id, "run:reserved");
  assert.equal(store.calls[0].requestedStatus, "reserved");
  assert.equal(
    Object.prototype.hasOwnProperty.call(
      store.calls[0].intent,
      "checkpoint_target_run_id",
    ),
    false,
  );
  for (const field of [
    "publication_gap_domains",
    "publication_gap_root_run_id",
    "publication_gap_scope",
  ]) {
    assert.equal(
      store.calls[0].intent[field],
      null,
      `ordinary Full intent must freeze ${field} as null`,
    );
  }
});

test("a redelivered candidate Full job reuses its reserved Business Run identity", async () => {
  const store = bindingStore({ created: false });
  const job = {
    id: "job-redelivered",
    name: "channel-snapshot",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: { channel_id: "UC1", candidate_id: 42, crawl_mode: "full" },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM (SELECT")) return { rows: [{ candidate_status: "failed" }] };
      if (sql.includes("FROM crawler.channel_runs")) return { rows: [] };
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const first = await preparer.prepareChannel(job);
  job.attemptsMade = 1;
  const second = await preparer.prepareChannel(job);

  assert.equal(first.businessRunKey, "full-candidate:42");
  assert.equal(second.businessRunKey, "full-candidate:42");
  assert.equal(second.businessRunId, first.businessRunId);
  assert.deepEqual(store.calls.map((call) => call.businessRunKey), [
    "full-candidate:42",
    "full-candidate:42",
  ]);
});

test("a controlled Recovery Job resolves a new Candidate Business Run boundary", async () => {
  const store = bindingStore();
  const retryIntentId = "11111111-1111-4111-8111-111111111111";
  const job = {
    id: `channel-recovery__42__${retryIntentId}__g5`,
    name: "channel-snapshot-recovery",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: {
      candidate_id: 42,
      retry_intent_id: retryIntentId,
      recovery_business_run_id: "run:new",
      dispatch_generation: 5,
      channel_id: "UC1",
      channel_url: "https://www.youtube.com/channel/UC1",
      crawl_mode: "full",
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM crawler.migration_retry_intents")) {
        return {
          rows: [{
            retry_intent_id: retryIntentId,
            candidate_id: "42",
            new_business_run_id: "run:new",
            new_business_run_key: `full-candidate:42:recovery:${retryIntentId}`,
            new_job_id: job.id,
            dispatch_generation: "5",
            status: "running",
          }],
        };
      }
      if (sql.includes("FROM (SELECT")) return { rows: [{ candidate_status: "queued" }] };
      if (sql.includes("FROM crawler.channel_runs")) return { rows: [] };
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:new");
  assert.equal(
    prepared.businessRunKey,
    `full-candidate:42:recovery:${retryIntentId}`,
  );
  assert.equal(store.calls[0].explicitBusinessRunId, "run:new");
  assert.equal(store.calls[0].fullIntentId, retryIntentId);
  assert.equal(job.data.business_run_key, prepared.businessRunKey);
  assert.equal(job.data.run_id, "run:new");
});

test("a Recovery Intent identity mismatch is a retryable system failure", async () => {
  const retryIntentId = "22222222-2222-4222-8222-222222222222";
  const job = {
    id: `channel-recovery__482__${retryIntentId}__g2`,
    name: "channel-snapshot-recovery",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 2,
    opts: { attempts: 3 },
    data: {
      candidate_id: 482,
      retry_intent_id: retryIntentId,
      recovery_business_run_id: "run:recovery:482",
      dispatch_generation: 2,
      channel_id: "UC0NoarYHkSxek05QDqhtoYw",
      crawl_mode: "full",
    },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM crawler.migration_retry_intents")) {
        return {
          rows: [{
            retry_intent_id: retryIntentId,
            candidate_id: "999",
            new_business_run_id: job.data.recovery_business_run_id,
            new_business_run_key: `full-candidate:482:recovery:${retryIntentId}`,
            new_job_id: job.id,
            dispatch_generation: "2",
            status: "running",
          }],
        };
      }
      throw new Error("identity mismatch must fail before Candidate lifecycle lookup");
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });

  const error = await preparer.prepareChannel(job).then(
    () => null,
    (caught) => caught,
  );

  assert.deepEqual({
    name: error?.name,
    code: error?.code,
    message: error?.message,
    disposition: channelCandidateFailureDisposition({
      error,
      attemptsMade: 3,
      maxAttempts: 3,
      permanentFailure: true,
    }),
  }, {
    name: "MigrationRetryIntentConflictError",
    code: "MIGRATION_RETRY_INTENT_CONFLICT",
    message: `Recovery Intent identity mismatch: ${retryIntentId}`,
    disposition: "retryable_system_failure",
  });
});

test("an automatic Full Repair redelivery keeps its original Business Run identity", async () => {
  const store = bindingStore({ created: false });
  const job = {
    id: "job-auto-repair",
    name: "channel-crawl-repair",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: {
      channel_id: "UC1",
      channel_url: "https://www.youtube.com/channel/UC1",
      crawl_mode: "full",
      repair_parent_run_id: "run:parent",
      repair_round: 2,
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{}] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const first = await preparer.prepareChannel(job);
  job.attemptsMade = 1;
  const second = await preparer.prepareChannel(job);

  const expectedKey = "full-repair:auto:run:parent:2:UC1";
  assert.equal(first.businessRunKey, expectedKey);
  assert.equal(second.businessRunKey, expectedKey);
  assert.equal(second.businessRunId, first.businessRunId);
  assert.deepEqual(store.calls.map((call) => call.businessRunKey), [expectedKey, expectedKey]);
});

test("a checkpoint repair gets a new Rota budget while targeting the exhausted Run", async () => {
  const store = bindingStore();
  const transactionCalls = [];
  const job = {
    id: "job-checkpoint-repair",
    name: "channel-checkpoint-repair",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: {
      channel_id: "UC1",
      repair_parent_run_id: "run:parent",
      repair_round: 1,
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{}] }),
    withTransaction: async (operation) => operation({
      async query(sql, params) {
        transactionCalls.push({ sql, params });
        if (sql.includes("FROM crawler.channel_runs") && sql.includes("FOR SHARE")) {
          return {
            rowCount: 1,
            rows: [{
              run_id: "run:parent",
              channel_id: "UC1",
              crawl_mode: "full",
              content_limit: 30,
            }],
          };
        }
        if (sql.includes("FOR UPDATE")) {
          return {
            rowCount: 1,
            rows: [{
              run_id: "run:reserved",
              channel_id: "UC1",
              result_json: { checkpoint_repair: { target_run_id: "run:parent", repair_round: 1 } },
            }],
          };
        }
        return { rowCount: 1, rows: [{ run_id: "run:reserved" }] };
      },
    }),
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:reserved");
  assert.equal(prepared.checkpointTargetRunId, "run:parent");
  assert.equal(job.data.run_id, "run:reserved");
  assert.equal(job.data.checkpoint_target_run_id, "run:parent");
  assert.equal(store.calls[0].intent.checkpoint_target_run_id, "run:parent");
  assert.equal(transactionCalls.some(({ sql }) => /INSERT INTO crawler\.channel_runs/.test(sql)), true);
});

test("an incomplete Promotion Run repair reattaches to the Candidate Business Run", async () => {
  const job = {
    id: "job-promotion-repair",
    name: "channel-crawl-repair",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 1,
    data: {
      channel_id: "UC1",
      channel_url: "https://www.youtube.com/channel/UC1",
      crawl_mode: "full",
      repair_parent_run_id: "run:promotion",
      repair_round: 2,
      run_id: "run:abandoned-repair",
      business_run_key: "full-repair:auto:run:promotion:2:UC1",
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM (SELECT")) {
        return {
          rows: [{
            registry_promotion_run_id: "run:promotion",
            registry_promotion_candidate_id: 42,
          }],
        };
      }
      if (sql.includes("FROM crawler.channel_runs")) {
        return {
          rows: [{
            run_id: "run:promotion",
            business_run_key: "full-candidate:42",
            business_run_id: "run:promotion",
            binding_status: "materialized",
            identity_policy_id: resolvedPolicy.policy.id,
            identity_policy_version: resolvedPolicy.policy.version,
            identity_policy_hash: resolvedPolicy.policy.hash,
          }],
        };
      }
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: {
      async resolve() {
        assert.fail("a Promotion repair must reuse its Candidate Business Run");
      },
    },
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:promotion");
  assert.equal(prepared.businessRunKey, "full-candidate:42");
  assert.equal(job.data.run_id, "run:promotion");
  assert.equal(job.data.business_run_key, "full-candidate:42");
  assert.equal(job.data.candidate_id, 42);
});

test("a frozen Publication Gap creates a child Business Run instead of reopening Promotion", async () => {
  const store = bindingStore();
  const job = {
    id: "job-publication-gap-child",
    name: "channel-crawl-repair",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: {
      channel_id: "UC1",
      channel_url: "https://www.youtube.com/channel/UC1",
      crawl_mode: "full",
      repair_parent_run_id: "run:promotion",
      repair_round: 1,
      publication_gap_domains: ["channel", "video"],
      publication_gap_root_run_id: "run:promotion",
      require_complete_about_metrics: true,
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM (SELECT")) {
        return {
          rows: [{
            registry_promotion_run_id: "run:promotion",
            registry_promotion_candidate_id: 42,
          }],
        };
      }
      if (sql.includes("FROM crawler.channel_runs")) {
        assert.fail("a frozen Publication Gap must not attach to the Promotion Run");
      }
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:reserved");
  assert.equal(prepared.businessRunKey, "full-repair:auto:run:promotion:1:UC1");
  assert.equal(store.calls.length, 1);
  assert.equal(store.calls[0].explicitBusinessRunId, null);
  assert.deepEqual(store.calls[0].intent.publication_gap_domains, ["channel", "video"]);
  assert.equal(store.calls[0].intent.publication_gap_root_run_id, "run:promotion");
  assert.equal(store.calls[0].intent.publication_gap_scope, null);
  assert.equal(job.data.candidate_id, undefined);
});

test("a Detail Repair attaches to the existing Business Run binding", async () => {
  const job = {
    id: "job-detail-repair",
    name: "channel-detail-repair",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: {
      channel_id: "UC1",
      run_id: "run:existing",
      repair_batch_id: "repair:details",
    },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      if (sql.includes("FROM (SELECT")) return { rows: [{}] };
      if (sql.includes("FROM crawler.channel_runs")) {
        return {
          rows: [{
            run_id: "run:existing",
            business_run_key: "full-candidate:42",
            business_run_id: "run:existing",
            binding_status: "materialized",
            identity_policy_id: resolvedPolicy.policy.id,
            identity_policy_version: resolvedPolicy.policy.version,
            identity_policy_hash: resolvedPolicy.policy.hash,
          }],
        };
      }
      return { rows: [] };
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: {
      async resolve() {
        assert.fail("an existing Business Run must not be rebound under a new key");
      },
    },
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "run:existing");
  assert.equal(prepared.businessRunKey, "full-candidate:42");
  assert.equal(job.data.business_run_key, "full-candidate:42");
});

test("a removed Candidate reaches the channel pipeline so its batch state can converge", async () => {
  const store = bindingStore();
  const job = {
    id: "job-removed-candidate",
    name: "channel-snapshot",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: { channel_id: "UCremoved", candidate_id: 77, crawl_mode: "full" },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({
      rows: [{ channel_status: "removed", candidate_status: "queued" }],
    }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.kind, "ready");
  assert.equal(prepared.businessRunKey, "full-candidate:77");
  assert.equal(store.calls.length, 1);
});

test("a removed non-Candidate channel job still skips before acquiring a Route", async () => {
  const store = bindingStore();
  const job = {
    id: "job-removed-refresh",
    name: "channel-snapshot",
    queueName: queuesByRole.channelCrawl,
    attemptsMade: 0,
    data: { channel_id: "UCremoved", full_intent_id: "refresh-1", crawl_mode: "full" },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{ channel_status: "removed" }] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.deepEqual(prepared, {
    kind: "skip",
    reason: "channel_removed",
    result: { channel_id: "UCremoved", skipped: true },
  });
  assert.equal(store.calls.length, 0);
});

test("an Incremental Plan reuses the existing Run and freezes its Policy", async () => {
  const store = bindingStore();
  const updates = [];
  const job = {
    id: "job-inc",
    name: "channel-incremental",
    queueName: queuesByRole.channelIncremental,
    attemptsMade: 1,
    data: { plan_id: "plan-1", task_mask: { about: true }, plan_mode: "standard" },
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async (sql) => {
      updates.push(sql);
      return { rows: [{ run_id: "incremental:plan-1" }], rowCount: 1 };
    },
    withTransaction: async () => {},
    incrementalRunStore: {
      async claim() {
        return {
          created: false,
          resumed: true,
          terminal: false,
          run: { run_id: "incremental:plan-1", channel_id: "UC1", result_json: {} },
        };
      },
    },
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.workloadKind, "channel_incremental");
  assert.equal(prepared.initialResumeMode, "bullmq_redelivery_resume");
  assert.equal(store.calls[0].businessRunKey, "incremental-plan:plan-1");
  assert.match(updates[0], /identity_policy_id/);
});

test("managed Incremental preparation preserves the frozen Plan contract", async () => {
  const store = bindingStore();
  const plan = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCtest__20260814__agent_v5_canary__905c961593d7",
    plan_id: "905c9615-93d7-41dc-9c99-3ca33abd0aa6",
    plan_mode: "standard",
    plan_day: "2026-08-14",
    scheduled_at: "2026-08-14T13:19:13.887Z",
    channel_id: "UCtest",
    task_mask: { about: false, video: false, agent: true },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-v1" },
    clock_version: 1,
    policy_version: "v16-rule-7",
    planner_config_version: "agent-v5-canary",
  };
  const job = {
    id: plan.job_id,
    name: "channel.incremental.plan",
    queueName: queuesByRole.channelIncremental,
    attemptsMade: 0,
    data: structuredClone(plan),
    async updateData(data) { this.data = data; },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{ run_id: `incremental:${plan.plan_id}` }], rowCount: 1 }),
    withTransaction: async () => {},
    incrementalRunStore: {
      async claim() {
        return {
          created: false,
          resumed: false,
          terminal: false,
          run: {
            run_id: `incremental:${plan.plan_id}`,
            channel_id: plan.channel_id,
            result_json: {},
          },
        };
      },
    },
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, `incremental:${plan.plan_id}`);
  assert.deepEqual(job.data, plan);
  assert.equal(validateIncrementalJob(job).plan_id, plan.plan_id);
});

test("managed Content Enrich uses a stable Business Run identity and a real task kind", async () => {
  const job = {
    id: "content_enrich__UC1__stable",
    name: "content-enrich",
    queueName: queuesByRole.contentEnrich,
    attemptsMade: 1,
    data: {
      channel_id: "UC1",
      tasks: [{ task_id: "task-1", dispatch_generation: 4 }],
      task_ids: ["task-1"],
    },
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => {
      throw new Error("Content Enrich preparation must not materialize a Full/Incremental Run");
    },
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, "content-enrich:content_enrich__UC1__stable");
  assert.equal(prepared.workloadKind, "content_enrich");
  assert.equal(prepared.initialResumeMode, "bullmq_redelivery_resume");
});

test("managed Incremental preparation repairs only matching legacy runtime metadata", async () => {
  const store = bindingStore();
  const plan = {
    schema_version: 5,
    dispatch_generation: 1,
    job_id: "incremental__UCtest__20260814__clock_16__905c961593d7",
    plan_id: "905c9615-93d7-41dc-9c99-3ca33abd0aa6",
    plan_mode: "standard",
    plan_day: "2026-08-14",
    scheduled_at: "2026-08-14T13:19:13.887Z",
    channel_id: "UCtest",
    task_mask: { about: true, video: false, agent: false },
    capacity: { factor: 1, player_cap: 20, next_cap: 8, version: "capacity-v1" },
    clock_version: 16,
    policy_version: "v16-rule-7",
    planner_config_version: "video-plan-1",
  };
  const legacyData = {
    ...plan,
    run_id: `incremental:${plan.plan_id}`,
    business_run_key: `incremental-plan:${plan.plan_id}`,
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
  };
  const updates = [];
  const job = {
    id: plan.job_id,
    name: "channel.incremental.plan",
    queueName: queuesByRole.channelIncremental,
    attemptsMade: 1,
    data: structuredClone(legacyData),
    async updateData(data) {
      updates.push(structuredClone(data));
      this.data = data;
    },
  };
  const claims = [];
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [{ run_id: `incremental:${plan.plan_id}` }], rowCount: 1 }),
    withTransaction: async () => {},
    incrementalRunStore: {
      async claim(payload) {
        claims.push(structuredClone(payload));
        return {
          created: false,
          resumed: true,
          terminal: false,
          run: {
            run_id: `incremental:${plan.plan_id}`,
            channel_id: plan.channel_id,
            result_json: {},
          },
        };
      },
    },
    resolvedPolicy,
    bindingStore: store,
  });

  const prepared = await preparer.prepareChannel(job);

  assert.equal(prepared.businessRunId, `incremental:${plan.plan_id}`);
  assert.deepEqual(claims, [plan]);
  assert.deepEqual(updates, [plan]);
  assert.deepEqual(job.data, plan);
  assert.equal(validateIncrementalJob(job).plan_id, plan.plan_id);
});

test("Discover refuses a transient BullMQ identity and uses only its persisted Page Intent", async () => {
  const page = {
    page_id: "page-1",
    page_intent_hash: "sha256:page",
    managed_fetch_status: "pending",
    status: "queued",
    dispatch_status: "enqueued",
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [page] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });
  await assert.rejects(
    preparer.prepareDiscover({ data: { query_id: 1, page_no: 1 } }),
    /page_id/,
  );
  const prepared = await preparer.prepareDiscover({ data: { page_id: "page-1" }, attemptsMade: 0 });
  assert.equal(prepared.businessRunId, "discover-page:page-1");
});

test("Discover redelivery skips Rota when the persisted managed fetch is already complete", async () => {
  const page = {
    page_id: "page-done",
    page_intent_hash: "sha256:page-done",
    managed_fetch_status: "done",
    qualification_status: "pending",
    status: "running",
    dispatch_status: "terminal",
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [page] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });

  const prepared = await preparer.prepareDiscover({
    data: { page_id: "page-done" },
    attemptsMade: 1,
  });

  assert.deepEqual(prepared, {
    kind: "skip",
    reason: "discover_managed_fetch_complete",
    result: { ok: true, page_id: "page-done", managed_fetch_complete: true },
  });
});

test("Query Quality rejects a BullMQ member list that differs from the frozen Chunk", async () => {
  const chunk = {
    quality_chunk_id: "chunk-1",
    quality_batch_id: "batch-1",
    chunk_intent_hash: "sha256:chunk",
    status: "queued",
    quality_task_ids: [1, 2],
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [chunk] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });
  await assert.rejects(preparer.prepareQueryQuality({
    data: { quality_chunk_id: "chunk-1", quality_task_ids: [2, 1] },
  }), /members conflict/);
});

test("Query Quality reconstructs frozen members when BullMQ carries only quality_chunk_id", async () => {
  const chunk = {
    quality_chunk_id: "chunk-db-only",
    quality_batch_id: "batch-1",
    chunk_intent_hash: "sha256:chunk-db-only",
    status: "queued",
    quality_task_ids: [4, 7, 9],
    identity_policy_id: resolvedPolicy.policy.id,
    identity_policy_version: resolvedPolicy.policy.version,
    identity_policy_hash: resolvedPolicy.policy.hash,
  };
  const preparer = new ProxyBusinessRunPreparer({
    queryFn: async () => ({ rows: [chunk] }),
    withTransaction: async () => {},
    resolvedPolicy,
    bindingStore: bindingStore(),
  });

  const prepared = await preparer.prepareQueryQuality({
    data: { quality_chunk_id: "chunk-db-only" },
    attemptsMade: 0,
  });

  assert.deepEqual(prepared.chunk.quality_task_ids, [4, 7, 9]);
  assert.equal(prepared.businessRunId, "query-quality:batch-1:chunk-db-only");
});
