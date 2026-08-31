import assert from "node:assert/strict";
import test from "node:test";
import {
  incrementalAgentEventPayload,
  incrementalAgentFailureDisposition,
  IncrementalAgentResultStore,
  markAgentChannelsRunning,
  persistAgentChannelFailure,
  persistAgentChannelSuccess,
} from "../src/incrementalAgentResultStore.js";


const metrics = {
  audience_profile_agent: {
    channel_categories: {
      value: { level_1: "Technology", level_2: ["Artificial Intelligence"] },
      evidence: ["shared evidence"],
    },
    channel_tags: {
      value: { tags: ["AI", "Software", "AI"] },
      evidence: ["shared evidence", "tag evidence"],
    },
    active_subscriber_ratio: {
      value: 35,
      evidence: [{ source: "recent videos", strength: "medium" }],
    },
  },
};

const version = {
  provider: "rules",
  model: "agent-test",
  agentConfigId: 2,
  promptTemplateId: 3,
  promptHash: `sha256:${"a".repeat(64)}`,
  promptVariant: "country_resolved",
  taxonomyVersion: "taxonomy-v1",
  tools: [{ type: "web_search" }],
};


test("Agent event emits bounded identity signals instead of raw evidence", () => {
  const payload = incrementalAgentEventPayload(metrics, version);

  assert.deepEqual(payload.topic_tokens, [
    "l1:technology",
    "l2:artificial intelligence",
    "tag:ai",
    "tag:software",
  ]);
  assert.equal(payload.evidence_count, 4);
  assert.equal(payload.evidence_fingerprints.length, 3);
  assert.ok(payload.evidence_fingerprints.every((value) => /^sha256:[0-9a-f]{64}$/.test(value)));
  assert.match(payload.agent_version_hash, /^sha256:[0-9a-f]{64}$/);
  assert.equal(JSON.stringify(payload).includes("shared evidence"), false);
});


test("Agent version hash is deterministic and changes with prompt metadata", () => {
  const first = incrementalAgentEventPayload(metrics, version);
  const repeated = incrementalAgentEventPayload(metrics, { ...version });
  const changed = incrementalAgentEventPayload(metrics, {
    ...version,
    promptHash: `sha256:${"b".repeat(64)}`,
  });

  assert.equal(first.agent_version_hash, repeated.agent_version_hash);
  assert.notEqual(first.agent_version_hash, changed.agent_version_hash);
});

test("Agent failures remain internal retries until permanent failure or max attempts", () => {
  assert.equal(incrementalAgentFailureDisposition({
    attempts: 1,
    maxAttempts: 3,
    error: new Error("request timed out"),
  }).terminal, false);
  assert.equal(incrementalAgentFailureDisposition({
    attempts: 3,
    maxAttempts: 3,
    error: new Error("request timed out"),
  }).terminal, true);
  assert.equal(incrementalAgentFailureDisposition({
    attempts: 1,
    maxAttempts: 3,
    error: new Error("agent response incomplete: missing audience_region"),
  }).terminal, false);
  assert.equal(incrementalAgentFailureDisposition({
    attempts: 1,
    maxAttempts: 3,
    error: new Error("agent HTTP 401: invalid key"),
  }).terminal, true);
  assert.equal(incrementalAgentFailureDisposition({
    attempts: 1,
    maxAttempts: 3,
    error: new Error("local profile snapshot is stale: expected latest_run_id=run-1, actual=run-2"),
  }).terminal, false);
});

test("Agent running state is written through a Publication writer transaction", async () => {
  let transactionEntered = false;
  let statement = null;
  let parameters = null;
  const result = await markAgentChannelsRunning({
    withTransaction: async (action) => {
      transactionEntered = true;
      return action({
        async query(sql, params) {
          statement = String(sql);
          parameters = params;
          return { rows: [], rowCount: 2 };
        },
      });
    },
    channelIds: ["UCone", "UCone", "UCtwo"],
    forceRefresh: true,
  });

  assert.equal(transactionEntered, true);
  assert.match(statement, /UPDATE crawler\.channels/);
  assert.deepEqual(parameters, [["UCone", "UCtwo"], true]);
  assert.equal(result.rowCount, 2);
});

test("full Agent success persists Profile and Channel in one writer transaction", async () => {
  let transactions = 0;
  const statements = [];
  await persistAgentChannelSuccess({
    withTransaction: async (action) => {
      transactions += 1;
      return action({
        query: async (sql, params) => {
          statements.push({ sql: String(sql), params });
          return { rowCount: 1, rows: [] };
        },
      });
    },
    channelId: "UCsuccess",
    inputUrl: "https://www.youtube.com/channel/UCsuccess",
    metrics,
    publicationRun: {
      agent_model: "local-model",
      agent_config_id: 9,
      prompt_template_id: null,
      prompt_hash: null,
      prompt_variant: "local_offline",
      input_content_ids: ["video-1"],
      input_content_hash: `sha256:${"b".repeat(64)}`,
      taxonomy_version: "taxonomy-v1",
      agent_version_hash: `sha256:${"c".repeat(64)}`,
    },
  });

  assert.equal(transactions, 1);
  assert.equal(statements.length, 2);
  assert.match(statements[0].sql, /INSERT INTO crawler\.agent_profiles/);
  assert.match(statements[1].sql, /UPDATE crawler\.channels/);
  assert.deepEqual(statements[1].params, ["UCsuccess"]);
});

test("full Agent failure persists Profile and Channel in one writer transaction", async () => {
  let transactions = 0;
  const statements = [];
  await persistAgentChannelFailure({
    withTransaction: async (action) => {
      transactions += 1;
      return action({
        query: async (sql, params) => {
          statements.push({ sql: String(sql), params });
          return { rowCount: 1, rows: [] };
        },
      });
    },
    channelId: "UCfailure",
    inputUrl: "https://www.youtube.com/channel/UCfailure",
    agentModel: "local-model",
    agentConfigId: 9,
    promptTemplateId: null,
    promptHash: null,
    promptVariant: "local_offline",
    errorMessage: "local runtime failed",
  });

  assert.equal(transactions, 1);
  assert.equal(statements.length, 2);
  assert.match(statements[0].sql, /INSERT INTO crawler\.agent_profiles/);
  assert.match(statements[1].sql, /UPDATE crawler\.channels/);
  assert.deepEqual(statements[1].params, ["UCfailure", "local runtime failed"]);
});

test("full Agent writes stop before mutation when an execution Fence is stale", async () => {
  const writes = [];
  const withTransaction = async (action) => action({
    async query(sql, params) {
      writes.push({ sql: String(sql), params });
      return { rowCount: 1, rows: [] };
    },
  });
  let guardCalls = 0;
  const transactionGuard = async () => {
    guardCalls += 1;
    return false;
  };

  const running = await markAgentChannelsRunning({
    withTransaction,
    channelIds: ["UCstale"],
    transactionGuard,
  });
  const success = await persistAgentChannelSuccess({
    withTransaction,
    channelId: "UCstale",
    inputUrl: "https://www.youtube.com/channel/UCstale",
    metrics,
    publicationRun: {
      agent_model: "local-model",
      agent_config_id: 9,
      prompt_template_id: null,
      prompt_hash: null,
      prompt_variant: "local_offline",
      input_content_ids: [],
      input_content_hash: `sha256:${"b".repeat(64)}`,
      taxonomy_version: "taxonomy-v1",
      agent_version_hash: `sha256:${"c".repeat(64)}`,
    },
    transactionGuard,
  });
  const failure = await persistAgentChannelFailure({
    withTransaction,
    channelId: "UCstale",
    inputUrl: "https://www.youtube.com/channel/UCstale",
    agentModel: "local-model",
    agentConfigId: 9,
    promptTemplateId: null,
    promptHash: null,
    promptVariant: "local_offline",
    errorMessage: "stale execution",
    transactionGuard,
  });

  assert.equal(guardCalls, 3);
  assert.equal(running.fenceRejected, true);
  assert.equal(success.fenceRejected, true);
  assert.equal(failure.fenceRejected, true);
  assert.deepEqual(writes, []);
});

test("retryable Agent failure does not emit a terminal Observation", async () => {
  const sql = [];
  const client = {
    async query(statement) {
      sql.push(statement);
      if (statement.includes("SELECT plan_id,run_id FROM crawler.agent_refresh_requests")) {
        return { rows: [{ plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d", run_id: "incremental:plan" }] };
      }
      return { rows: [], rowCount: 1 };
    },
  };
  const store = new IncrementalAgentResultStore({
    withTransaction: async (action) => action(client),
    maxAttempts: 3,
  });

  const result = await store.fail({
    batchId: "incremental-agent:test",
    request: {
      channel_id: "UCretry",
      plan_ids: ["4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d"],
      run_ids: ["incremental:plan"],
      attempts: 1,
    },
    row: { input_url: "https://www.youtube.com/channel/UCretry", country_required: false },
    agentConfig: { model: "agent-test" },
    error: new Error("request timed out"),
  });

  assert.equal(result.outcome, "retrying");
  assert.equal(result.terminal, false);
  assert.equal(sql.some((statement) => statement.includes("crawler.crawl_observations")), false);
  assert.equal(sql.some((statement) => statement.includes("crawler.crawler_outbox")), false);
});

test("retryable local failure does not fabricate Prompt metadata", async () => {
  let profileParameters = null;
  const client = {
    async query(statement, params = []) {
      if (String(statement).includes("SELECT plan_id,run_id FROM crawler.agent_refresh_requests")) {
        return { rows: [{ plan_id: "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d", run_id: "incremental:local" }] };
      }
      if (String(statement).includes("INSERT INTO crawler.agent_profiles")) profileParameters = params;
      return { rows: [], rowCount: 1 };
    },
  };
  const store = new IncrementalAgentResultStore({
    withTransaction: async (action) => action(client),
    maxAttempts: 3,
  });

  await store.fail({
    batchId: "incremental-agent:local-retry",
    request: {
      channel_id: "UClocalretry",
      plan_ids: ["4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d"],
      run_ids: ["incremental:local"],
      attempts: 1,
    },
    row: { input_url: "https://www.youtube.com/channel/UClocalretry", country_required: false },
    agentConfig: {
      config_id: 8,
      provider: "local-offline",
      model: "qy-channel-profile",
      prompt_template_id: null,
      prompt_hash: null,
    },
    error: new Error("local profile runtime timed out"),
  });

  assert.ok(profileParameters);
  assert.equal(profileParameters[4], null);
  assert.equal(profileParameters[5], null);
  assert.equal(profileParameters[6], "local_offline");
});

test("successful Agent persistence reconciles after its Observation is durable", async () => {
  const sql = [];
  const planId = "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d";
  const client = {
    async query(statement, params = []) {
      const text = String(statement);
      sql.push(text);
      if (text.includes("INSERT INTO crawler.crawl_observation_keys")) {
        return { rowCount: 1, rows: [{ observation_id: params[1] }] };
      }
      if (text.includes("SELECT * FROM crawler.channel_domain_cursors")) {
        return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
      }
      if (text.includes("SELECT plan_id,run_id FROM crawler.agent_refresh_requests")) {
        return { rows: [{ plan_id: planId, run_id: "incremental:plan" }] };
      }
      return { rowCount: 1, rows: [] };
    },
  };
  const store = new IncrementalAgentResultStore({
    withTransaction: async (action) => action(client),
    maxAttempts: 3,
  });

  const result = await store.complete({
    batchId: "incremental-agent:complete-test",
    request: {
      channel_id: "UCagentcomplete",
      plan_ids: [planId],
      run_ids: ["incremental:plan"],
      attempts: 1,
      plan_day: "2026-07-27",
      scheduled_at: "2026-07-27T00:00:00.000Z",
    },
    resolved: {
      agent_model: "agent-test",
      country_required: false,
      input_content_ids: [],
      metrics,
    },
    row: { input_url: "https://www.youtube.com/channel/UCagentcomplete" },
    agentConfig: {
      config_id: 2,
      provider: "openai-compatible",
      model: "agent-test",
      prompt_template_id: 3,
      prompt_hash: "a".repeat(64),
      tools_json: [],
    },
  });

  assert.equal(result.outcome, "complete");
  const crawlerOutbox = sql.findIndex((statement) => (
    statement.includes("INSERT INTO crawler.crawler_outbox")
  ));
  const publication = sql.findIndex((statement) => (
    statement.includes("publication-reconciler:find-owner")
  ));
  assert.ok(crawlerOutbox >= 0 && publication > crawlerOutbox);
  const recoveredRun = sql.find((statement) => (
    statement.includes("UPDATE crawler.channel_runs")
    && statement.includes("ARRAY['domains','agent']")
  ));
  assert.match(recoveredRun, /status='failed'/);
  assert.match(recoveredRun, /detail_status='done'/);
  assert.match(recoveredRun, /domains,agent,status/);
});

test("successful local persistence records runtime identity without Prompt metadata", async () => {
  const sql = [];
  let profileParameters = null;
  const planId = "4c6ff5ea-b6d9-53f9-b8cf-43cfe430298d";
  const runtimeModelId = "qy-channel-profile:processor-v1:bundle-v2:prior-v3:qy-taxonomy-v1";
  const processingContext = {
    executor: "local-offline",
    runtime_model_id: runtimeModelId,
    processor_version: "processor-v1",
    model_bundle_version: "bundle-v2",
    model_bundle_hash: `sha256:${"1".repeat(64)}`,
    prior_catalog_version: "prior-v3",
    prior_catalog_hash: `sha256:${"2".repeat(64)}`,
    taxonomy_version: "qy-taxonomy-v1",
  };
  const client = {
    async query(statement, params = []) {
      const sqlText = String(statement);
      sql.push(sqlText);
      if (sqlText.includes("INSERT INTO crawler.crawl_observation_keys")) {
        return { rowCount: 1, rows: [{ observation_id: params[1] }] };
      }
      if (sqlText.includes("SELECT * FROM crawler.channel_domain_cursors")) {
        return { rowCount: 1, rows: [{ latest_sequence: 0 }] };
      }
      if (sqlText.includes("SELECT plan_id,run_id FROM crawler.agent_refresh_requests")) {
        return { rows: [{ plan_id: planId, run_id: "incremental:local" }] };
      }
      if (sqlText.includes("INSERT INTO crawler.agent_profiles")) profileParameters = params;
      return { rowCount: 1, rows: [] };
    },
  };
  const store = new IncrementalAgentResultStore({
    withTransaction: async (action) => action(client),
    maxAttempts: 3,
  });

  await store.complete({
    batchId: "incremental-agent:local-complete-test",
    request: {
      channel_id: "UClocalcomplete",
      plan_ids: [planId],
      run_ids: ["incremental:local"],
      attempts: 1,
      plan_day: "2026-08-14",
      scheduled_at: "2026-08-14T00:00:00.000Z",
    },
    resolved: {
      agent_model: runtimeModelId,
      execution_variant: "local_offline",
      country_required: false,
      input_content_ids: ["video-local"],
      metrics: { ...metrics, profile_processing_context: processingContext },
    },
    row: { input_url: "https://www.youtube.com/channel/UClocalcomplete" },
    agentConfig: {
      config_id: 8,
      provider: "local-offline",
      model: "qy-channel-profile",
      prompt_template_id: null,
      prompt_hash: null,
      tools_json: [],
    },
  });

  assert.ok(profileParameters);
  assert.equal(profileParameters[3], runtimeModelId);
  assert.equal(profileParameters[5], null);
  assert.equal(profileParameters[6], null);
  assert.equal(profileParameters[7], "local_offline");
});
