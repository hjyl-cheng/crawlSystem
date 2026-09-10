import assert from "node:assert/strict";
import test from "node:test";
import express from "express";
import pg from "pg";
import { createServerNodeStore } from "./serverNodes.js";
import { serverNodesRoutes } from "./serverNodesRoutes.js";
import { allowDashboardRequestDuringControlledMigration } from "./controlledWritePolicy.js";

const url = process.env.SERVER_NODES_TEST_DATABASE_URL;
test("node registration persists only configuration, survives reload, and rejects concurrent stale updates", { skip: !url }, async t => {
  const parsed = new URL(url);
  assert.equal(parsed.pathname, "/server_nodes_dashboard_test");
  assert.ok(["127.0.0.1", "localhost"].includes(parsed.hostname));
  const pool = new pg.Pool({ connectionString: url });
  t.after(() => pool.end());
  await pool.query(`CREATE SCHEMA IF NOT EXISTS crawler;
    CREATE TABLE IF NOT EXISTS crawler.settings(setting_key TEXT PRIMARY KEY,value_json JSONB NOT NULL,updated_at TIMESTAMPTZ DEFAULT now());
    DELETE FROM crawler.settings;
    INSERT INTO crawler.settings(setting_key,value_json) VALUES('query_scheduler','{"status":"running"}')`);
  const store = createServerNodeStore(pool.query.bind(pool));
  const app = express();
  app.use(express.json());
  app.use((req, res, next) => allowDashboardRequestDuringControlledMigration(req.method, req.path) ? next() : res.sendStatus(423));
  app.use(serverNodesRoutes({ store, layout: ({ body }) => body }));
  app.use((error, _req, res, _next) => res.status(500).json({ error: error.message }));
  const server = app.listen(0, "127.0.0.1");
  await new Promise(resolve => server.once("listening", resolve));
  t.after(() => new Promise(resolve => server.close(resolve)));
  const base = `http://127.0.0.1:${server.address().port}`;
  const node = { name: "Hand-entered server", host: "192.0.2.15", port: 22, username: "ubuntu", kind: "execution", workers: [] };
  const save = (node, version, id) => fetch(base + "/api/server-nodes" + (id ? "/" + id : ""), {
    method: id ? "PUT" : "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ node, version }),
  });
  assert.deepEqual(await (await fetch(base + "/api/server-nodes")).json(), { version: 0, nodes: [] });
  assert.equal((await pool.query("SELECT count(*)::int AS count FROM crawler.settings")).rows[0].count, 1, "reading an empty registry must not seed nodes");
  const concurrent = await Promise.all([save(node, 0), save({ ...node, host: "192.0.2.16" }, 0)]);
  assert.deepEqual(concurrent.map(response => response.status).sort(), [200, 409]);
  let registry = await concurrent.find(response => response.ok).json();
  const savedNode = registry.nodes[0];
  assert.equal((await save(savedNode, registry.version)).status, 400, "server-generated fields cannot be supplied as config");
  assert.equal((await save({ ...node, host: savedNode.host }, registry.version)).status, 409, "duplicate endpoints are rejected");
  assert.equal((await fetch(base + "/api/server-nodes", { method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body: "name=unsafe" })).status, 415);
  const planned = { ...node, host: savedNode.host, sshAlias: "existing-node", workers: [{ role: "incremental", count: 4 }] };
  assert.equal((await save(planned, registry.version, savedNode.id)).status, 409, "worker configuration requires verified readiness, even through the API");
  assert.equal((await save({ ...planned, host: "192.0.2.99" }, registry.version)).status, 409, "new nodes cannot bypass onboarding with embedded worker plans");
  // A registry saved by the previous page can already contain a worker plan.
  registry.nodes[0] = { ...registry.nodes[0], workers: planned.workers, sshAlias: planned.sshAlias };
  await pool.query("UPDATE crawler.settings SET value_json=$1::jsonb WHERE setting_key='dashboard_server_nodes_v1'", [JSON.stringify(registry)]);
  const response = await save({ ...planned, notes: "Edited description" }, registry.version, savedNode.id);
  assert.equal(response.status, 200);
  registry = await response.json();
  assert.deepEqual(registry.nodes[0].workers, [{ role: "incremental", count: 4 }]);
  assert.equal(registry.nodes[0].sshAlias, "existing-node");
  assert.equal((await save({ ...planned, workers: [] }, registry.version, savedNode.id)).status, 409, "editing cannot silently remove existing plans");
  assert.equal((await save(node, 1, savedNode.id)).status, 409, "stale editor cannot erase a newly saved worker plan");
  assert.deepEqual(await createServerNodeStore(pool.query.bind(pool)).load(), registry, "reopening the store retains configuration");
  assert.deepEqual((await pool.query("SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler'")).rows[0].value_json, { status: "running" });
  assert.equal((await fetch(base + "/api/server-nodes/" + savedNode.id + "/deploy", { method: "POST" })).status, 423);
  const remove = (id, version) => fetch(base + "/api/server-nodes/" + id, {
    method: "DELETE", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ version }),
  });
  const check = async id => (await fetch(base + "/api/server-nodes/" + id + "/deletion-check")).json();
  const persist = () => pool.query("UPDATE crawler.settings SET value_json=$1::jsonb WHERE setting_key='dashboard_server_nodes_v1'", [JSON.stringify(registry)]);
  assert.deepEqual(registry.nodes[0].provisioning, { state: "not_started" });
  assert.equal((await check(savedNode.id)).allowed, true, "saved plans and SSH aliases do not block metadata deletion");
  assert.equal((await fetch(base + "/api/server-nodes/" + savedNode.id, { method: "DELETE" })).status, 415);
  assert.equal((await remove(savedNode.id, 0)).status, 409);
  assert.equal((await remove(savedNode.id)).status, 400);
  for (const patch of [{ provisioning: { state: "started" } }, { kind: "center" }, { deployment: { status: "running" } }]) {
    const original = registry.nodes[0];
    registry.nodes[0] = { ...original, ...patch };
    await persist();
    assert.equal((await check(savedNode.id)).allowed, false);
    assert.equal((await remove(savedNode.id, registry.version)).status, 409);
    assert.deepEqual(await store.load(), registry, "rejected deletion preserves the complete registry");
    registry.nodes[0] = original;
  }
  // Editing must not discard evidence of initialization/deployment.
  registry.nodes[0].provisioning = { state: "started", operationId: "test-operation" };
  await persist();
  registry = await (await save({ ...planned, notes: "Preserve server-owned state" }, registry.version, savedNode.id)).json();
  assert.deepEqual(registry.nodes[0].provisioning, { state: "started", operationId: "test-operation" });
  assert.equal((await remove(savedNode.id, registry.version)).status, 409);
  delete registry.nodes[0].provisioning;
  await persist();
  assert.equal((await check(savedNode.id)).allowed, true, "legacy V1 metadata records remain deletable");
  const checkedVersion = registry.version;
  registry = await (await save({ ...node, host: "192.0.2.50" }, registry.version)).json();
  const otherNode = registry.nodes.find(item => item.id !== savedNode.id);
  assert.equal((await remove(savedNode.id, checkedVersion)).status, 409, "check does not authorize a stale deletion");
  // Change provisioning after the store read but before CAS, without a version
  // increment: the entire-document comparison must still reject the write.
  for (const operation of ["remove", "save"]) {
    const beforeRace = structuredClone(registry);
    const racingStore = createServerNodeStore(async (sql, params) => {
      if (!sql.startsWith("SELECT")) {
        registry.nodes[0].provisioning = { state: "started" };
        await persist();
      }
      return pool.query(sql, params);
    });
    await assert.rejects(() => racingStore[operation]({ id: savedNode.id, version: registry.version, node: planned }), { statusCode: 409 });
    assert.deepEqual(await store.load(), registry, "racing write cannot remove a started node or erase its marker");
    registry = beforeRace;
    await persist();
  }
  const deleted = await remove(savedNode.id, registry.version);
  assert.equal(deleted.status, 200);
  registry = await deleted.json();
  assert.deepEqual(registry.nodes, [otherNode], "deleting one registration preserves other nodes exactly");
  assert.deepEqual(await createServerNodeStore(pool.query.bind(pool)).load(), registry);
  assert.equal((await remove(savedNode.id, registry.version)).status, 404);
  assert.equal((await fetch(base + "/api/server-nodes/" + savedNode.id + "/deletion-check")).status, 404);
  assert.deepEqual((await pool.query("SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler'")).rows[0].value_json, { status: "running" });
  assert.equal((await fetch(base + "/server-nodes")).status, 200);
  assert.equal((await fetch(base + "/assets/server-nodes.js")).status, 200);
  assert.equal((await fetch(base + "/assets/server-nodes.css")).status, 200);
  await pool.query("DELETE FROM crawler.settings WHERE setting_key='dashboard_server_nodes_v1'");
});
