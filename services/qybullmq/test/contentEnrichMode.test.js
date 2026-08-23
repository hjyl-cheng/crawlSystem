import assert from "node:assert/strict";
import test from "node:test";
import {
  CONTENT_ENRICH_CLOCK_MODE,
  CONTENT_ENRICH_DISPATCH_LOCK_KEY,
  CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY,
  CONTENT_ENRICH_QUEUE_MODE,
  loadContentEnrichMode,
  switchContentEnrichMode,
} from "../src/contentEnrichMode.js";

test("Clock reads the database owner under a shared transaction lock", async () => {
  const statements = [];
  const client = {
    async query(sql) {
      statements.push(sql);
      return { rows: [{ mode: CONTENT_ENRICH_QUEUE_MODE }], rowCount: 1 };
    },
  };

  assert.equal(await loadContentEnrichMode(client, { lock: true }), CONTENT_ENRICH_QUEUE_MODE);
  assert.match(statements[0], /setting_key='content_enrich_dispatch'[\s\S]*FOR SHARE/);
});

test("missing ownership state fails closed to the legacy Clock consumer", async () => {
  const client = { async query() { return { rows: [], rowCount: 0 }; } };
  assert.equal(await loadContentEnrichMode(client), CONTENT_ENRICH_CLOCK_MODE);
});

test("cutover and rollback serialize with dispatch before locking the ownership row", async () => {
  const statements = [];
  const client = {
    async query(sql, params = []) {
      statements.push({ sql, params });
      if (sql.includes("SELECT value_json") && sql.includes("FOR UPDATE")) {
        return { rows: [{ value_json: { mode: "clock" } }], rowCount: 1 };
      }
      if (sql.includes("RETURNING value_json")) {
        return { rows: [{ value_json: { mode: params[0] } }], rowCount: 1 };
      }
      return { rows: [], rowCount: 1 };
    },
  };

  const switched = await switchContentEnrichMode(client, {
    mode: "queue",
    changedBy: "test-operator",
    reason: "drain BUG-6 backlog",
  });

  assert.equal(switched.previous_mode, "clock");
  assert.equal(switched.mode, "queue");
  const dispatchLock = statements.find(({ sql }) => sql.includes("pg_advisory_xact_lock"));
  const ownershipRowLock = statements.find(({ sql }) => (
    sql.includes("SELECT value_json") && sql.includes("setting_key='content_enrich_dispatch'")
  ));
  assert.deepEqual(dispatchLock?.params, [CONTENT_ENRICH_DISPATCH_LOCK_KEY]);
  assert.match(
    ownershipRowLock?.sql ?? "",
    /setting_key='content_enrich_dispatch'[\s\S]*FOR UPDATE/,
  );
  assert.ok(
    statements.indexOf(dispatchLock) < statements.indexOf(ownershipRowLock),
  );
});

test("cutover fails closed while a committed dispatch mutex is still live", async () => {
  let modeUpdated = false;
  const client = {
    async query(sql, params = []) {
      if (sql === "SELECT clock_timestamp() AS observed_at") {
        return { rows: [{ observed_at: "2026-08-23T00:00:00.000Z" }], rowCount: 1 };
      }
      if (sql.includes("FROM crawler.settings") && sql.includes("FOR UPDATE")
          && params[0] === CONTENT_ENRICH_DISPATCH_MUTEX_SETTING_KEY) {
        return {
          rows: [{
            value_json: {
              owner: "content-enrich-controller:active",
              expires_at: "2026-08-23T00:01:00.000Z",
            },
          }],
          rowCount: 1,
        };
      }
      if (sql.includes("UPDATE crawler.settings")
          && sql.includes("setting_key='content_enrich_dispatch'")) modeUpdated = true;
      return { rows: [], rowCount: 1 };
    },
  };

  await assert.rejects(
    switchContentEnrichMode(client, {
      mode: "queue",
      changedBy: "test-operator",
      reason: "must not overlap an active dispatch",
    }),
    /dispatch mutex is active/,
  );
  assert.equal(modeUpdated, false);
});
