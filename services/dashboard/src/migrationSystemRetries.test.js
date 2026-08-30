import assert from "node:assert/strict";
import test from "node:test";

import {
  loadMigrationSystemRetries,
  loadMigrationSystemRetriesSafely,
  renderMigrationSystemRetries,
  requestMigrationSystemRetry,
} from "./migrationSystemRetries.js";

const item = {
  system_retry_id: 801,
  failure_type: "retryable_system_failure",
  failure_category: "lease",
  failure_code: "LEASE_CONFLICT",
  channel_id: "UC0NoarYHkSxek05QDqhtoYw",
  candidate_id: 482,
  failed_job_attempt: 3,
  failed_dispatch_generation: 1,
  retry_dispatch_generation: null,
  status: "pending",
};

test("Dashboard reads and normalizes the independent Migration system retry list from Crawler", async () => {
  let query = null;
  const result = await loadMigrationSystemRetries({
    read: async (sql, params) => {
      query = { sql: sql.replace(/\s+/g, " ").trim(), params };
      return {
        rows: [{
          ...item,
          system_retry_id: "801",
          migration_intent_id: "701",
          candidate_id: "482",
          failed_dispatch_generation: "1",
          failed_job_attempt: "3",
          snapshot_dispatch_generation: "1",
          snapshot_active_job_attempt: "2",
        }],
      };
    },
  });

  assert.match(query.sql, /FROM crawler\.migration_system_retry_items retry/);
  assert.match(query.sql, /JOIN crawler\.migration_channel_intents intent/);
  assert.match(query.sql, /JOIN crawler\.channel_candidates candidate/);
  assert.deepEqual(query.params, [["retrying", "pending", "dispatched"], 100]);
  assert.deepEqual(result, {
    count: 1,
    items: [{
      ...item,
      failure_type: "retryable_system_failure",
      migration_intent_id: 701,
      snapshot_dispatch_generation: 1,
      snapshot_active_job_attempt: 2,
    }],
  });
});

test("Dashboard isolates retry-list read failure from the Migration inventory", async () => {
  let reads = 0;
  const result = await loadMigrationSystemRetriesSafely({
    read: async () => {
      reads += 1;
      throw new Error("Crawler database unavailable");
    },
  });

  assert.equal(reads, 1);
  assert.deepEqual(result, {
    available: false,
    count: 0,
    items: [],
    error: "系统失败重试清单当前不可用",
  });
  const html = renderMigrationSystemRetries(result);
  assert.match(html, /系统失败重试清单当前不可用/);
  assert.doesNotMatch(html, /当前没有系统失败待重试项/);
});

test("Dashboard system retry table exposes controlled actions for pending and dispatched items", () => {
  const html = renderMigrationSystemRetries([
    item,
    { ...item, system_retry_id: 802, status: "dispatched", retry_dispatch_generation: 2 },
  ]);
  for (const evidence of [
    "retryable_system_failure",
    "LEASE_CONFLICT",
    "UC0NoarYHkSxek05QDqhtoYw",
    "482",
    "G1",
    "G2",
  ]) assert.match(html, new RegExp(evidence));
  assert.equal((html.match(/>受控重试</g) || []).length, 2);
  assert.match(html, /system-retries\/801\/retry/);
  assert.match(html, /system-retries\/802\/retry/);
});

test("Dashboard controlled retry posts only the retry item identity", async () => {
  let request = null;
  const result = await requestMigrationSystemRetry({
    crawlerApiUrl: "http://crawler-api:3000",
    systemRetryId: 801,
    fetchImpl: async (url, options) => {
      request = { url, options };
      return {
        ok: true,
        json: async () => ({ ok: true, system_retry_id: 801, dispatch_generation: 2 }),
      };
    },
  });
  assert.equal(request.url, "http://crawler-api:3000/api/migration/system-retries/801/retry");
  assert.equal(request.options.method, "POST");
  assert.equal(request.options.body, "{}");
  assert.equal(result.dispatch_generation, 2);
});
