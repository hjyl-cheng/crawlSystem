import assert from "node:assert/strict";
import test from "node:test";
import { agentConcurrencyLimit, buildAgentDispatchPlan } from "../src/agentDispatchPolicy.js";

const configs = [
  { config_id: 1, name: "default", max_workers: 2, enabled: true },
  { config_id: 5, name: "grokapi-console", max_workers: 6, enabled: true },
  { config_id: 6, name: "aistudio-gemini-search", max_workers: 1, enabled: true },
];

function counts(plan) {
  return Object.fromEntries(plan.reduce((out, config) => {
    out.set(config.name, (out.get(config.name) ?? 0) + 1);
    return out;
  }, new Map()));
}

test("agent dispatch fills configured 6+2+1 capacity", () => {
  const plan = buildAgentDispatchPlan({ configs, workerCapacity: 9 });
  assert.equal(plan.length, 9);
  assert.deepEqual(counts(plan), {
    "grokapi-console": 6,
    default: 2,
    "aistudio-gemini-search": 1,
  });
});

test("agent dispatch shares smaller physical capacity proportionally", () => {
  const plan = buildAgentDispatchPlan({ configs, workerCapacity: 5 });
  assert.deepEqual(counts(plan), {
    "grokapi-console": 3,
    default: 1,
    "aistudio-gemini-search": 1,
  });
});

test("agent dispatch refills only the config with a free slot", () => {
  const plan = buildAgentDispatchPlan({
    configs,
    outstandingByConfig: new Map([[1, 1], [5, 6], [6, 1]]),
    outstandingTotal: 8,
    workerCapacity: 9,
  });
  assert.deepEqual(plan.map((config) => config.name), ["default"]);
});

test("agent dispatch ignores disabled configs and respects outstanding workers", () => {
  const plan = buildAgentDispatchPlan({
    configs: configs.map((config) => config.config_id === 5 ? { ...config, enabled: false } : config),
    outstandingByConfig: new Map([[1, 1]]),
    outstandingTotal: 3,
    workerCapacity: 9,
  });
  assert.deepEqual(counts(plan), { default: 1, "aistudio-gemini-search": 1 });
});

test("agent global concurrency follows both registered workers and config capacity", () => {
  assert.equal(agentConcurrencyLimit(configs, 9), 9);
  assert.equal(agentConcurrencyLimit(configs, 5), 5);
  assert.equal(agentConcurrencyLimit(configs.filter((config) => config.config_id !== 5), 9), 3);
  assert.equal(agentConcurrencyLimit([], 9), 0);
});
