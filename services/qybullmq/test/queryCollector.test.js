import assert from "node:assert/strict";
import test from "node:test";
import {
  collectQueryTerms,
  resolveMetadataQueryIdentityPolicy,
} from "../src/queryCollector.js";

const policy = Object.freeze({
  id: "qy-br-query-quality-anonymous-v1",
  version: 1,
  hash: "sha256:quality-br",
  role: "query_quality",
  youtube_language: "pt-BR",
  youtube_country: "BR",
});

test("metadata Query collection requires an explicit Query Quality Identity Policy", () => {
  assert.throws(
    () => resolveMetadataQueryIdentityPolicy({
      environment: {},
      catalog: { policies: new Map([[policy.id, policy]]) },
    }),
    /QUERY_METADATA_IDENTITY_POLICY_ID is required/,
  );
});

test("metadata Query locale is derived from the selected policy and rejects conflicts", () => {
  const catalog = { policies: new Map([[policy.id, policy]]) };
  const resolved = resolveMetadataQueryIdentityPolicy({
    environment: { QUERY_METADATA_IDENTITY_POLICY_ID: policy.id },
    catalog,
  });
  assert.equal(resolved.language, "pt-BR");
  assert.equal(resolved.country, "BR");
  assert.throws(
    () => resolveMetadataQueryIdentityPolicy({
      environment: {
        QUERY_METADATA_IDENTITY_POLICY_ID: policy.id,
        QUERY_METADATA_COUNTRY: "US",
      },
      catalog,
    }),
    /conflicts with Identity Policy/,
  );
});

test("resolved metadata Query Identity Policy is accepted by the Query term collector", async () => {
  const catalog = { policies: new Map([[policy.id, policy]]) };
  const resolved = resolveMetadataQueryIdentityPolicy({
    environment: { QUERY_METADATA_IDENTITY_POLICY_ID: policy.id },
    catalog,
  });
  const calls = [];
  const client = {
    query: async (sql, params) => {
      calls.push({ sql, params });
      return { rows: [{ query_id: "18", query_text: "children songs" }] };
    },
  };

  const result = await collectQueryTerms(client, {
    qualityBatchId: "metadata:resolved-policy",
    identityPolicy: resolved,
    sources: [{ kind: "channel_keyword", values: ["Children Songs"] }],
  });

  assert.equal(result.inserted_count, 1);
  assert.equal(calls[0].params[4], "pt-BR");
  assert.equal(calls[0].params[5], "BR");
});

test("metadata Query producer writes the policy locale into terms and Batch options", async () => {
  const calls = [];
  const client = {
    query: async (sql, params) => {
      calls.push({ sql, params });
      return { rows: [{ query_id: "17", query_text: "roblox" }] };
    },
  };
  const result = await collectQueryTerms(client, {
    qualityBatchId: "metadata:test",
    identityPolicy: policy,
    sources: [{ kind: "video_hashtag", values: ["#Roblox"] }],
  });

  assert.equal(result.inserted_count, 1);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].params[4], "pt-BR");
  assert.equal(calls[0].params[5], "BR");
  const options = JSON.parse(calls[0].params[3]);
  assert.equal(options.identity_policy_id, policy.id);
  assert.equal(options.language, "pt-BR");
  assert.equal(options.country, "BR");
});
