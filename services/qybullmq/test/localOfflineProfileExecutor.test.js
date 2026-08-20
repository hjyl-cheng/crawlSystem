import assert from "node:assert/strict";
import test from "node:test";

import {
  LocalOfflineProfileExecutor,
  localProfileRuntimeModelId,
} from "../src/localOfflineProfileExecutor.js";

const CHANNEL_ID = "UC1234567890123456789012";

function rawValue(field) {
  const values = {
    country: "Brazil",
    creator_gender: "male",
    creator_age_range: 31,
    creator_language: "Portuguese",
    audience_region: [
      { region: "Brazil", percentage: 75 },
      { region: "Portugal", percentage: 8 },
      { region: "United States", percentage: 6 },
      { region: "Angola", percentage: 4 },
      { region: "Mozambique", percentage: 2 },
      { region: "Other", percentage: 5 },
    ],
    audience_age_gender: [
      { age_range: "18-24", male: 14, female: 8 },
      { age_range: "25-34", male: 24, female: 14 },
      { age_range: "35-44", male: 12, female: 8 },
      { age_range: "45-54", male: 6, female: 4 },
      { age_range: "55-64", male: 3, female: 2 },
      { age_range: "65+", male: 3, female: 2 },
    ],
    audience_language: [
      { language: "Portuguese", percentage: 92 },
      { language: "English", percentage: 5 },
      { language: "Other", percentage: 3 },
    ],
    active_subscriber_ratio: 24,
    channel_tags: {
      tags: [
        "Anime", "Commentary", "Otaku Culture", "Comedy", "Reactions",
        "Manga", "Pop Culture", "Reviews", "Storytelling", "Digital Culture",
      ],
      top_5_distribution: [
        { tag: "Anime", percentage: 30 },
        { tag: "Commentary", percentage: 22 },
        { tag: "Otaku Culture", percentage: 18 },
        { tag: "Comedy", percentage: 12 },
        { tag: "Reactions", percentage: 10 },
        { tag: "Other", percentage: 8 },
      ],
    },
    channel_categories: { level_1: "Entertainment", level_2: ["Anime & Animation"] },
  };
  return values[field];
}

function runtimeResult(overrides = {}) {
  const fields = [
    "country", "creator_gender", "creator_age_range", "creator_language",
    "audience_region", "audience_age_gender", "audience_language",
    "active_subscriber_ratio", "channel_tags", "channel_categories",
  ];
  const facts = Object.fromEntries(fields.map((field) => [field, {
    value: rawValue(field),
    source_type: field.startsWith("audience_") ? "public_prior_estimate" : "public_signal_model",
    truth_status: "estimated",
    evidence_strength: field === "country" ? "strong" : "medium",
    model_confidence: field === "country" ? 0.91 : 0.72,
    evidence_confidence: field === "country" ? 0.88 : 0.61,
    candidates: [],
    evidence_refs: field === "country" ? ["channel:country"] : [],
    abstained: false,
    model_version: `${field}-test-v1`,
    decision_policy_version: `${field}-policy-v1`,
    metadata: {},
  }]));
  return {
    channel_id: CHANNEL_ID,
    input_url: "https://www.youtube.com/@example",
    payload: Object.fromEntries([
      ["input_url", "https://www.youtube.com/@example"],
      ...fields.map((field) => [field, rawValue(field)]),
    ]),
    input_content_ids: ["video-2", "video-1"],
    source_latest_run_id: "run-current",
    analysis_result: {
      channel_id: CHANNEL_ID,
      input_url: "https://www.youtube.com/@example",
      analysis_status: "completed_with_estimates",
      snapshot: {
        as_of: "2026-08-14T09:00:00Z",
        snapshot_hash: `sha256:${"1".repeat(64)}`,
        input_content_hash: `sha256:${"2".repeat(64)}`,
      },
      facts,
      processor: {
        version: "agent-free-v0.7-identity-evidence",
        model_bundle_version: "agent-free-20260813-v0.5-field-routing-deployment",
        model_bundle_hash: `sha256:${"3".repeat(64)}`,
        taxonomy_version: "qy-taxonomy-v1",
        prior_catalog_version: "bootstrap-shadow-v2.3-grok-audience-shape",
        prior_catalog_hash: `sha256:${"4".repeat(64)}`,
      },
      diagnostics: [{ code: "UNVALIDATED_BOOTSTRAP_PRIOR", severity: "warning" }],
    },
    ...overrides,
  };
}

test("local executor sends only Channel IDs and maps evidence-rich local output", async () => {
  let runtimeRequest = null;
  const executor = new LocalOfflineProfileExecutor({
    invokeRuntime: async (request) => {
      runtimeRequest = request;
      return { results: [runtimeResult()], errors: [] };
    },
    loadLatestRunIds: async () => new Map([[CHANNEL_ID, "run-current"]]),
  });

  const output = await executor.execute({
    config: { provider: "local-offline", model: "qy-channel-profile" },
    channelIds: [CHANNEL_ID],
  });

  assert.deepEqual(runtimeRequest, { channel_ids: [CHANNEL_ID] });
  assert.equal(output.errors.length, 0);
  const resolved = output.results.get(CHANNEL_ID);
  assert.equal(resolved.execution_variant, "local_offline");
  assert.equal(resolved.country_required, false);
  assert.deepEqual(resolved.input_content_ids, ["video-2", "video-1"]);
  assert.equal(
    resolved.metrics.audience_profile_agent.country.source,
    "local_profile:public_signal_model",
  );
  assert.deepEqual(
    resolved.metrics.audience_profile_agent.country.evidence,
    ["channel:country"],
  );
  assert.match(resolved.agent_model, /^qy-channel-profile:/);
  assert.equal(
    resolved.agent_model,
    resolved.metrics.profile_processing_context.runtime_model_id,
  );
  assert.equal(
    resolved.metrics.profile_processing_context.prior_catalog_version,
    "bootstrap-shadow-v2.3-grok-audience-shape",
  );
});

test("local runtime identity is deterministic and includes every inference catalog", () => {
  const result = runtimeResult();
  assert.equal(localProfileRuntimeModelId(result), [
    "qy-channel-profile",
    "agent-free-v0.7-identity-evidence",
    "agent-free-20260813-v0.5-field-routing-deployment",
    "bootstrap-shadow-v2.3-grok-audience-shape",
    "qy-taxonomy-v1",
  ].join(":"));
});

test("local executor discards a result when latest Channel Run changed", async () => {
  const executor = new LocalOfflineProfileExecutor({
    invokeRuntime: async () => ({ results: [runtimeResult()], errors: [] }),
    loadLatestRunIds: async () => new Map([[CHANNEL_ID, "run-newer"]]),
  });

  const output = await executor.execute({
    config: { provider: "local-offline" },
    channelIds: [CHANNEL_ID],
  });

  assert.equal(output.results.size, 0);
  assert.deepEqual(output.errors, [{
    channel_id: CHANNEL_ID,
    input_url: "https://www.youtube.com/@example",
    error: "local profile snapshot is stale: expected latest_run_id=run-current, actual=run-newer",
    error_kind: "stale_snapshot",
    retryable: true,
  }]);
});

test("local executor keeps one channel failure isolated from successful channels", async () => {
  const secondId = "UCabcdefghijklmnopqrstuv";
  const executor = new LocalOfflineProfileExecutor({
    invokeRuntime: async () => ({
      results: [runtimeResult()],
      errors: [{ channel_id: secondId, error: "SnapshotNotFound: channel snapshot not found" }],
    }),
    loadLatestRunIds: async () => new Map([[CHANNEL_ID, "run-current"]]),
  });

  const output = await executor.execute({
    config: { provider: "local-offline" },
    channelIds: [CHANNEL_ID, secondId],
  });

  assert.equal(output.results.size, 1);
  assert.equal(output.errors.length, 1);
  assert.equal(output.errors[0].channel_id, secondId);
  assert.equal(output.errors[0].error_kind, "snapshot_not_found");
});
