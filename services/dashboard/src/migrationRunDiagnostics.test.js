import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";
import {
  loadMigrationRunDiagnostics,
  loadRotaBusinessRunBudget,
  renderMigrationRunDiagnostics,
} from "./migrationRunDiagnostics.js";

test("Rota budget diagnostics use the authenticated authoritative endpoint", async () => {
  const calls = [];
  const budget = await loadRotaBusinessRunBudget({
    controlUrl: "http://rota-core:8001/api/v1/proxy-control/",
    controlToken: "control-token-secret",
    businessRunId: "run:budget/1",
    fetchImpl: async (url, options) => {
      calls.push({ url, options });
      return {
        ok: true,
        async json() {
          return {
            ok: true,
            workload_scope: "qy-production",
            business_run_id: "run:budget/1",
            business_tasks_used: 9,
            business_tasks_limit: 9,
            current_execution_id: "exec:v1:budget",
            execution_tasks_used: 3,
            execution_tasks_limit: 3,
            budget_exhausted_at: "2026-08-26T06:50:33Z",
          };
        },
      };
    },
  });

  assert.equal(calls.length, 1);
  assert.equal(
    calls[0].url,
    "http://rota-core:8001/api/v1/proxy-control/business-runs/run%3Abudget%2F1/budget",
  );
  assert.equal(calls[0].options.headers.authorization, "Bearer control-token-secret");
  assert.deepEqual(budget, {
    available: true,
    workload_scope: "qy-production",
    business_run_id: "run:budget/1",
    business_tasks_used: 9,
    business_tasks_limit: 9,
    current_execution_id: "exec:v1:budget",
    execution_tasks_used: 3,
    execution_tasks_limit: 3,
    budget_exhausted_at: "2026-08-26T06:50:33Z",
  });
  assert.doesNotMatch(JSON.stringify(budget), /control-token-secret/);
});

test("Migration diagnostics expose the latest persisted Business Run attempt", async () => {
  const calls = [];
  const diagnostics = await loadMigrationRunDiagnostics({
    candidateId: 42,
    read: async (sql, params) => {
      calls.push({ sql, params });
      return {
        rows: [{
          candidate_id: "42",
          snapshot_dispatch_generation: "6",
          binding_status: "terminal",
          terminal_reason: "proxy_control_business_run_budget_exhausted",
          business_run_key: "full-candidate:42:recovery:retry-6",
          business_run_id: "run:recovery-6",
          latest_rota_attempt: "9",
          latest_bullmq_attempt_zero_based: "2",
          latest_job_id: "migration-retry:42:6",
          latest_attempt_status: "failed",
        }],
      };
    },
  });

  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0].params, [42]);
  assert.match(calls[0].sql, /crawler\.business_run_bindings/);
  assert.match(calls[0].sql, /crawler\.channel_execution_attempts/);
  assert.deepEqual(diagnostics, {
    candidate_id: 42,
    dispatch_generation: 6,
    binding_status: "terminal",
    terminal_reason: "proxy_control_business_run_budget_exhausted",
    business_run_key: "full-candidate:42:recovery:retry-6",
    business_run_id: "run:recovery-6",
    rota_attempt: 9,
    bullmq_attempt: 3,
    latest_job_id: "migration-retry:42:6",
    latest_attempt_status: "failed",
  });
});

test("Migration diagnostics render persisted execution identity on the operations page", () => {
  const html = renderMigrationRunDiagnostics({
    candidate_id: 42,
    dispatch_generation: 6,
    binding_status: "terminal",
    terminal_reason: "proxy_control_business_run_budget_exhausted",
    business_run_key: "full-candidate:42:recovery:retry-6",
    business_run_id: "run:recovery-6",
    rota_attempt: 9,
    bullmq_attempt: 3,
    latest_job_id: "migration-retry:<42>:6",
    latest_attempt_status: "failed",
    budget: {
      available: true,
      business_tasks_used: 9,
      business_tasks_limit: 9,
      current_execution_id: "exec:v1:budget",
      execution_tasks_used: 3,
      execution_tasks_limit: 3,
      budget_exhausted_at: "2026-08-26T06:50:33Z",
    },
  });

  assert.match(html, /Business Run Binding/);
  assert.match(html, /terminal/);
  assert.match(html, /proxy_control_business_run_budget_exhausted/);
  assert.match(html, /Dispatch generation/);
  assert.match(html, />6</);
  assert.match(html, /BullMQ attempt/);
  assert.match(html, />3</);
  assert.match(html, /Rota attempt/);
  assert.match(html, />9</);
  assert.match(html, /Business Run budget/);
  assert.match(html, />9 \/ 9</);
  assert.match(html, /Execution budget/);
  assert.match(html, />3 \/ 3</);
  assert.match(html, /budget_exhausted_at/);
  assert.match(html, /2026-08-26T06:50:33Z/);
  assert.match(html, /migration-retry:&lt;42&gt;:6/);
  assert.doesNotMatch(html, /migration-retry:<42>:6/);
});

test("Migration detail route loads and renders persisted run diagnostics", async () => {
  const server = await readFile(new URL("./server.js", import.meta.url), "utf8");
  const dataStart = server.indexOf("async function migrationChannelDetailData(channelId)");
  const dataEnd = server.indexOf("async function migrateChannel", dataStart);
  const pageStart = server.indexOf("function migrationChannelDetailPage(data)");
  const pageEnd = server.indexOf("function channelListPage", pageStart);
  assert.ok(dataStart >= 0 && dataEnd > dataStart);
  assert.ok(pageStart >= 0 && pageEnd > pageStart);

  assert.match(server.slice(dataStart, dataEnd), /loadMigrationRunDiagnostics/);
  assert.match(server.slice(dataStart, dataEnd), /loadRotaBusinessRunBudget/);
  assert.match(server.slice(dataStart, dataEnd), /rotaProxyControlToken/);
  assert.match(server.slice(dataStart, dataEnd), /target_candidate_id/);
  assert.match(server.slice(pageStart, pageEnd), /renderMigrationRunDiagnostics/);
});
