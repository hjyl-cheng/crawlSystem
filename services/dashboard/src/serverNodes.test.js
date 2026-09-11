import assert from "node:assert/strict";
import test from "node:test";
import { normalizeServerNode, serverNodeDeletionEligibility } from "./serverNodes.js";
import { allowDashboardRequestDuringControlledMigration } from "./controlledWritePolicy.js";

const node = { name: "Test node", host: "192.0.2.10", port: 22, username: "ubuntu", kind: "execution", workers: [] };

test("node registration accepts addresses but never credentials, shell commands or runtime state", () => {
  assert.equal(normalizeServerNode({ ...node, host: "2001:db8::10" }).host, "2001:db8::10");
  assert.equal(normalizeServerNode({ ...node, host: "NODE.example.test" }).host, "node.example.test");
  for (const patch of [
    { host: "https://192.0.2.10" }, { host: "host; id" }, { host: "999.999.1.1" },
    { username: "ubuntu; id" }, { sshAlias: "-F /tmp/config" }, { port: 0 },
    { password: "should-not-be-stored" }, { privateKey: "should-not-be-stored" },
    { online: true }, { workers: [{ role: "fullcrawl", count: 1, running: true }] },
    { provisioning: { state: "not_started" } }, { runtime: { state: "ready" } },
    { workers: [{ role: "toString", count: 1 }] },
    { workers: [{ role: "fullcrawl", count: 1 }, { role: "fullcrawl", count: 2 }] },
    { workers: [{ role: "incremental", count: -1 }] },
  ]) assert.throws(() => normalizeServerNode({ ...node, ...patch }), { statusCode: 400 });
});

test("controlled migration allows node metadata changes and removal but no deployment or queue control", () => {
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/api/server-nodes"), true);
  assert.equal(allowDashboardRequestDuringControlledMigration("PUT", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604"), true);
  assert.equal(allowDashboardRequestDuringControlledMigration("DELETE", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604"), true);
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/initialize"), true);
  assert.equal(allowDashboardRequestDuringControlledMigration("POST", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/prepare-runtime"), true);
  for (const [method, path] of [
    ["POST", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/deploy"],
    ["DELETE", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/deploy"],
    ["DELETE", "/api/server-nodes"],
    ["DELETE", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/initialize"],
    ["POST", "/api/server-nodes/deploy"], ["POST", "/queues/pause"],
    ["DELETE", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/prepare-runtime"],
    ["POST", "/api/server-nodes/65e95c15-0311-4079-a90c-bdf887db6604/prepare-runtime/deploy"],
  ]) assert.equal(allowDashboardRequestDuringControlledMigration(method, path), false);
});

test("only known uninitialized execution registrations are eligible for metadata removal", () => {
  for (const patch of [{}, { provisioning: { state: "not_started" } },
    { sshAlias: "existing-key", workers: [{ role: "incremental", count: 4 }] }]) {
    assert.equal(serverNodeDeletionEligibility({ ...node, ...patch }).allowed, true);
  }
  for (const patch of [{ kind: "center" }, { kind: "unknown" }, { provisioning: null },
    { provisioning: { state: "started" } }, { provisioning: { state: "completed" } },
    { provisioning: { state: "not_started", operationId: "pending" } }, { deployment: {} }]) {
    assert.equal(serverNodeDeletionEligibility({ ...node, ...patch }).allowed, false);
  }
});

test("failed login is removable only when the stored operation proves no remote changes began", () => {
  const provisioning = { state: "failed", remoteChanges: false, steps: { ssh: "failed", key: "pending", monitoring: "pending", metrics: "pending" } };
  assert.equal(serverNodeDeletionEligibility({ ...node, provisioning }).allowed, true);
  for (const patch of [{ remoteChanges: true }, { deployment: "unknown" }, { state: "running" }]) {
    assert.equal(serverNodeDeletionEligibility({ ...node, provisioning: { ...provisioning, ...patch } }).allowed, false);
  }
});
