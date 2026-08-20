import assert from "node:assert/strict";
import test from "node:test";
import {
  BusinessRunBindingConflictError,
  BusinessRunBindingStore,
  businessRunIntentHash,
  materializeBusinessRunBinding,
} from "../src/businessRunBindingStore.js";

const policy = {
  id: "qy-br-channel-anonymous-v1",
  version: 1,
  hash: "sha256:policy",
};

function fixture() {
  const rows = new Map();
  const client = {
    async query(sql, params = []) {
      if (sql.includes("pg_advisory_xact_lock")) return { rows: [], rowCount: 1 };
      if (sql.startsWith("SELECT * FROM crawler.business_run_bindings")) {
        const row = rows.get(params[0]);
        return { rows: row ? [{ ...row }] : [], rowCount: row ? 1 : 0 };
      }
      if (sql.startsWith("INSERT INTO crawler.business_run_bindings")) {
        const row = {
          business_run_key: params[0],
          business_run_id: params[1],
          intent_schema_version: params[2],
          intent_hash: params[3],
          intent_json: JSON.parse(params[4]),
          identity_policy_id: params[5],
          identity_policy_version: params[6],
          identity_policy_hash: params[7],
          run_kind: params[8],
          channel_id: params[9],
          candidate_id: params[10],
          plan_id: params[11],
          full_intent_id: params[12],
          status: params[13],
          materialized_at: params[13] === "materialized" ? new Date().toISOString() : null,
          terminal_reason: null,
        };
        rows.set(row.business_run_key, row);
        return { rows: [{ ...row }], rowCount: 1 };
      }
      if (sql.startsWith("UPDATE crawler.business_run_bindings")) {
        const row = rows.get(params[0]);
        if (!row || row.business_run_id !== params[1]) return { rows: [], rowCount: 0 };
        row.status = "materialized";
        row.materialized_at ??= new Date().toISOString();
        return { rows: [{ ...row }], rowCount: 1 };
      }
      if (sql.startsWith("UPDATE crawler.channel_runs run")) {
        return { rows: [{ run_id: params[1] }], rowCount: 1 };
      }
      throw new Error(`unexpected query: ${sql}`);
    },
  };
  return {
    rows,
    client,
    store: new BusinessRunBindingStore({
      withTransaction: (callback) => callback(client),
      randomUUID: () => "stable-generated-id",
    }),
  };
}

function input(overrides = {}) {
  return {
    businessRunKey: "full-candidate:42",
    runKind: "full",
    channelId: "UC42",
    candidateId: 42,
    policy,
    intent: { crawl_mode: "full", source: "channel-snapshot" },
    ...overrides,
  };
}

test("canonical Business Run intent hashes ignore object key order", () => {
  assert.equal(businessRunIntentHash({ a: 1, b: { c: 2 } }), businessRunIntentHash({ b: { c: 2 }, a: 1 }));
});

test("a replay resolves the same persistent Business Run without BullMQ state", async () => {
  const { store } = fixture();
  const first = await store.resolve(input());
  const replay = await store.resolve(input());

  assert.equal(first.created, true);
  assert.equal(replay.created, false);
  assert.equal(replay.binding.business_run_id, "run:stable-generated-id");
  assert.equal(replay.binding.status, "reserved");
});

test("the same Business Run key cannot be rebound to a different immutable intent", async () => {
  const { store } = fixture();
  await store.resolve(input());
  await assert.rejects(
    store.resolve(input({ channelId: "UC-wrong" })),
    (error) => error instanceof BusinessRunBindingConflictError,
  );
});

test("a reserved Binding materializes with the same Run ID", async () => {
  const { store, client } = fixture();
  const resolved = await store.resolve(input());
  const materialized = await materializeBusinessRunBinding(client, {
    businessRunKey: resolved.binding.business_run_key,
    businessRunId: resolved.binding.business_run_id,
  });

  assert.equal(materialized.status, "materialized");
  assert.equal(materialized.business_run_id, resolved.binding.business_run_id);
});
