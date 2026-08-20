import assert from "node:assert/strict";
import test from "node:test";
import {
  AGENT_FACT_KEYS,
  buildAgentPublicationCurrent,
  buildAgentPublicationRun,
} from "../src/agentPublicationCurrent.js";
import { observationFactsHash } from "../src/crawlObservationStore.js";
import { AGENT_TAXONOMY_VERSION } from "../src/publicationContract.js";

const CHANNEL_ID = "UCagent-publication";
const OBSERVATION_ID = "agent-observation";
const PROMPT = "Analyze every input URL and return the V1 Agent facts.";

function config(overrides = {}) {
  return {
    config_id: 7,
    provider: "openai-compatible",
    model: "agent-test",
    prompt_template_id: 10,
    template_text: PROMPT,
    tools_json: [{ type: "web_search" }],
    ...overrides,
  };
}

function percentageRows(key, labels, percentages) {
  return labels.map((label, index) => ({
    [key]: label,
    percentage: percentages[index],
  }));
}

function factValue(field) {
  const values = {
    country: "Brazil",
    creator_language: "Portuguese",
    creator_gender: "brand_team",
    creator_age_range: 35,
    audience_region: percentageRows(
      "region",
      ["Brazil", "Portugal", "United States", "Angola", "Mozambique", "Other"],
      [70, 10, 8, 5, 2, 5],
    ),
    audience_language: percentageRows(
      "language",
      ["Portuguese", "English", "Other"],
      [90, 7, 3],
    ),
    audience_age_gender: [
      { age_range: "18-24", male: 8, female: 8 },
      { age_range: "25-34", male: 14, female: 14 },
      { age_range: "35-44", male: 10, female: 10 },
      { age_range: "45-54", male: 7, female: 7 },
      { age_range: "55-64", male: 4, female: 4 },
      { age_range: "65+", male: 7, female: 7 },
    ],
    active_subscriber_ratio: 0,
    channel_categories: {
      level_1: "Software & Internet",
      level_2: ["Artificial Intelligence"],
    },
    channel_tags: {
      tags: [
        "Artificial Intelligence",
        "Software",
        "Programming",
        "Technology",
        "Tutorials",
        "Machine Learning",
        "Developer Tools",
        "Product Reviews",
        "Industry News",
        "Digital Culture",
      ],
      top_5_distribution: [
        { tag: "Artificial Intelligence", percentage: 25 },
        { tag: "Software", percentage: 22 },
        { tag: "Programming", percentage: 18 },
        { tag: "Technology", percentage: 14 },
        { tag: "Tutorials", percentage: 11 },
        { tag: "Other", percentage: 10 },
      ],
    },
  };
  return values[field];
}

function metrics(overrides = {}) {
  const facts = Object.fromEntries(AGENT_FACT_KEYS.map((field) => [field, {
    value: factValue(field),
    confidence: "high",
    evidence: [`Evidence for ${field}`],
    source_urls: [
      `https://www.youtube.com/channel/${CHANNEL_ID}?utm_source=test`,
      `https://www.youtube.com/channel/${CHANNEL_ID}`,
    ],
    reason: null,
    source: field === "country" ? "crawler" : "agent",
  }]));
  return {
    audience_profile_agent: facts,
    ...overrides,
  };
}

function profile(metricsJson = metrics(), overrides = {}) {
  const run = buildAgentPublicationRun({
    agentConfig: config(),
    agentModel: "agent-test",
    promptVariant: "country_resolved",
    inputContentIds: ["video-02", "video-01"],
  });
  return {
    channel_id: CHANNEL_ID,
    agent_mode: "basic",
    input_url: `https://www.youtube.com/channel/${CHANNEL_ID}`,
    status: "success",
    metrics_json: metricsJson,
    ...run,
    current_output_hash: observationFactsHash(metricsJson),
    last_observation_id: OBSERVATION_ID,
    last_observed_at: "2026-07-26T10:00:00.000Z",
    ...overrides,
  };
}

function source(row, overrides = {}) {
  const factsHash = "sha256:" + "a".repeat(64);
  const observedAt = overrides.observedAt ?? "2026-07-26T10:00:00.000Z";
  return {
    cursor: {
      channel_id: CHANNEL_ID,
      observation_kind: "agent",
      latest_sequence: "2",
      latest_observation_id: OBSERVATION_ID,
      latest_observed_at: observedAt,
      latest_complete_observation_id: OBSERVATION_ID,
      latest_complete_observed_at: observedAt,
      current_facts_hash: factsHash,
    },
    latest_observation: {
      observation_id: OBSERVATION_ID,
      observed_at: observedAt,
      outcome: "complete",
      outcome_reason_code: "agent_refresh_complete",
    },
    complete_observation: {
      observation_id: OBSERVATION_ID,
      observed_at: observedAt,
      channel_id: CHANNEL_ID,
      run_id: "agent-run",
      observation_kind: "agent",
      kind_sequence: "2",
      outcome: "complete",
      outcome_reason_code: "agent_refresh_complete",
      facts_hash: factsHash,
      crawler_version: "qy-v16",
      extractor_versions: { agent: "agent-test" },
      result_summary_json: {
        output_hash: row.current_output_hash,
        input_content_hash: row.input_content_hash,
        agent_version_hash: row.agent_version_hash,
        ...overrides.summary,
      },
    },
    run: {
      run_id: "agent-run",
      channel_id: CHANNEL_ID,
      status: "done",
      crawl_mode: "incremental",
      plan_id: "agent-plan",
      policy_version: "v16-rule-6",
      crawler_version: "qy-v16",
    },
  };
}

test("Agent publication run freezes exact config and canonical Content identity input", () => {
  const first = buildAgentPublicationRun({
    agentConfig: config(),
    promptVariant: "country_resolved",
    inputContentIds: ["video-02", "video-01", "video-02"],
  });
  const repeated = buildAgentPublicationRun({
    agentConfig: config(),
    promptVariant: "country_resolved",
    inputContentIds: ["video-01", "video-02"],
  });
  const changedPrompt = buildAgentPublicationRun({
    agentConfig: config({ template_text: `${PROMPT}\nChanged.` }),
    promptVariant: "country_resolved",
    inputContentIds: ["video-01", "video-02"],
  });

  assert.deepEqual(first.input_content_ids, ["video-01", "video-02"]);
  assert.equal(first.input_content_hash, observationFactsHash(["video-01", "video-02"]));
  assert.equal(first.agent_version_hash, repeated.agent_version_hash);
  assert.notEqual(first.agent_version_hash, changedPrompt.agent_version_hash);
  assert.match(first.prompt_hash, /^[0-9a-f]{64}$/);
});

test("local publication run has no Prompt dependency and binds the real runtime identity", () => {
  const runtimeIdentity = {
    executor: "local-offline",
    runtime_model_id: "qy-channel-profile:processor-v1:bundle-v2:prior-v3:taxonomy-v1",
    processor_version: "processor-v1",
    model_bundle_version: "bundle-v2",
    model_bundle_hash: `sha256:${"1".repeat(64)}`,
    prior_catalog_version: "prior-v3",
    prior_catalog_hash: `sha256:${"2".repeat(64)}`,
    taxonomy_version: "taxonomy-v1",
  };
  const localConfig = config({
    provider: "local-offline",
    model: "qy-channel-profile",
    prompt_template_id: null,
    template_text: null,
    tools_json: [],
  });
  const first = buildAgentPublicationRun({
    agentConfig: localConfig,
    agentModel: runtimeIdentity.runtime_model_id,
    executionVariant: "local_offline",
    runtimeIdentity,
    inputContentIds: ["video-02", "video-01"],
    taxonomyVersion: "taxonomy-v1",
  });
  const changedBundle = buildAgentPublicationRun({
    agentConfig: localConfig,
    agentModel: runtimeIdentity.runtime_model_id,
    executionVariant: "local_offline",
    runtimeIdentity: { ...runtimeIdentity, model_bundle_hash: `sha256:${"3".repeat(64)}` },
    inputContentIds: ["video-01", "video-02"],
    taxonomyVersion: "taxonomy-v1",
  });

  assert.equal(first.prompt_template_id, null);
  assert.equal(first.prompt_hash, null);
  assert.equal(first.prompt_variant, "local_offline");
  assert.equal(first.execution_variant, "local_offline");
  assert.match(first.agent_version_hash, /^sha256:[0-9a-f]{64}$/);
  assert.notEqual(first.agent_version_hash, changedBundle.agent_version_hash);
});

test("a complete Agent Current produces the full deterministic publication payload", () => {
  const row = profile();
  const first = buildAgentPublicationCurrent({ row, config: config(), source: source(row) });
  const repeatedRow = profile(metrics(), {
    input_content_ids: ["video-02", "video-01"],
    last_observed_at: "2026-07-26T11:00:00.000Z",
  });
  const repeated = buildAgentPublicationCurrent({
    row: repeatedRow,
    config: config(),
    source: source(repeatedRow, { observedAt: "2026-07-26T11:00:00.000Z" }),
  });

  assert.equal(first.ready, true);
  assert.equal(first.result_hash, repeated.result_hash);
  assert.match(first.result_hash, /^sha256:[0-9a-f]{64}$/);
  assert.equal(first.payload.active_subscriber_ratio, undefined);
  assert.equal(first.payload.facts.active_subscriber_ratio.value, 0);
  assert.deepEqual(first.payload.input_content_ids, ["video-01", "video-02"]);
  assert.deepEqual(
    first.payload.facts.country.source_urls,
    [`https://www.youtube.com/channel/${CHANNEL_ID}`],
  );
  assert.equal(first.facts.length, 10);
});

test("a complete local profile publishes without fabricated Prompt metadata", () => {
  const runtimeIdentity = {
    executor: "local-offline",
    runtime_model_id: "qy-channel-profile:processor-v1:bundle-v2:prior-v3:qy-taxonomy-v1",
    processor_version: "processor-v1",
    model_bundle_version: "bundle-v2",
    model_bundle_hash: `sha256:${"1".repeat(64)}`,
    prior_catalog_version: "prior-v3",
    prior_catalog_hash: `sha256:${"2".repeat(64)}`,
    taxonomy_version: AGENT_TAXONOMY_VERSION,
  };
  const localConfig = config({
    provider: "local-offline",
    model: "qy-channel-profile",
    prompt_template_id: null,
    template_text: null,
    tools_json: [],
  });
  const localMetrics = metrics({ profile_processing_context: runtimeIdentity });
  const run = buildAgentPublicationRun({
    agentConfig: localConfig,
    agentModel: runtimeIdentity.runtime_model_id,
    executionVariant: "local_offline",
    runtimeIdentity,
    inputContentIds: ["video-02", "video-01"],
  });
  const row = profile(localMetrics, {
    ...run,
    current_output_hash: observationFactsHash(localMetrics),
  });

  const current = buildAgentPublicationCurrent({
    row,
    config: localConfig,
    source: source(row),
  });

  assert.equal(current.ready, true);
  assert.equal(current.payload.prompt_template_id, null);
  assert.equal(current.payload.prompt_hash, null);
  assert.equal(current.payload.prompt_variant, "local_offline");
  assert.equal(current.payload.agent_model, runtimeIdentity.runtime_model_id);
});

test("Agent Current rejects malformed distributions and taxonomy values", () => {
  const invalidMetrics = metrics();
  invalidMetrics.audience_profile_agent.audience_region.value[0].percentage = 69;
  invalidMetrics.audience_profile_agent.channel_categories.value = {
    level_1: "Tech",
    level_2: ["Artificial Intelligence"],
  };
  const row = profile(invalidMetrics);
  const result = buildAgentPublicationCurrent({ row, config: config(), source: source(row) });

  assert.equal(result.ready, false);
  assert.deepEqual(
    result.facts.filter((fact) => !fact.complete).map((fact) => fact.field),
    ["audience_region", "channel_categories"],
  );
  assert.equal(result.result_hash, null);
});

test("Agent Current binds persisted input, output and version hashes", () => {
  const outputMismatch = profile(metrics(), {
    current_output_hash: "sha256:" + "b".repeat(64),
  });
  const inputMismatch = profile(metrics(), {
    input_content_hash: "sha256:" + "c".repeat(64),
  });
  const versionMismatch = profile(metrics(), {
    agent_version_hash: "sha256:" + "d".repeat(64),
  });

  const output = buildAgentPublicationCurrent({
    row: outputMismatch,
    config: config(),
    source: source(outputMismatch),
  });
  const input = buildAgentPublicationCurrent({
    row: inputMismatch,
    config: config(),
    source: source(inputMismatch),
  });
  const version = buildAgentPublicationCurrent({
    row: versionMismatch,
    config: config(),
    source: source(versionMismatch),
  });

  assert.equal(output.issues.some((issue) => issue.code === "agent_output_hash_mismatch"), true);
  assert.equal(input.issues.some((issue) => issue.code === "agent_input_content_hash_mismatch"), true);
  assert.equal(version.issues.some((issue) => issue.code === "agent_version_hash_mismatch"), true);
});

test("Agent Current requires exact config and source Observation summary provenance", () => {
  const row = profile();
  const result = buildAgentPublicationCurrent({
    row,
    config: config({ model: "different-model" }),
    source: source(row, {
      summary: {
        output_hash: null,
        input_content_hash: "sha256:" + "e".repeat(64),
      },
    }),
  });

  assert.equal(result.ready, false);
  assert.equal(
    result.issues.some((issue) => issue.code === "agent_configuration_mismatch" && issue.field === "agent_model"),
    true,
  );
  assert.equal(
    result.issues.some((issue) => issue.code === "agent_source_summary_missing" && issue.field === "output_hash"),
    true,
  );
  assert.equal(
    result.issues.some((issue) => issue.code === "agent_source_summary_mismatch" && issue.field === "input_content_hash"),
    true,
  );
});

test("an explicit empty Content input is valid and remains distinguishable from missing audit", () => {
  const emptyRun = buildAgentPublicationRun({
    agentConfig: config(),
    promptVariant: "country_resolved",
    inputContentIds: [],
  });
  const row = profile(metrics(), emptyRun);
  const ready = buildAgentPublicationCurrent({ row, config: config(), source: source(row) });
  const missingRow = profile(metrics(), {
    input_content_ids: null,
    input_content_hash: null,
  });
  const missing = buildAgentPublicationCurrent({
    row: missingRow,
    config: config(),
    source: source(missingRow),
  });

  assert.equal(ready.ready, true);
  assert.equal(ready.input_content.content_count, 0);
  assert.equal(ready.input_content.content_hash, observationFactsHash([]));
  assert.equal(missing.ready, false);
  assert.equal(missing.issues.some((issue) => issue.code === "agent_input_content_ids_missing"), true);
});
