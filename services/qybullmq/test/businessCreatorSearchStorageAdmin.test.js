import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  businessCreatorSearchStorageCommand,
  runBusinessCreatorSearchStorage,
} from "../scripts/manageBusinessCreatorSearchStorage.mjs";
import {
  BusinessCreatorSearchStorageAdministrator,
  businessCreatorSearchStorageConfig,
  businessCreatorSearchStorageConfirmation,
} from "../src/businessCreatorSearchStorageAdmin.js";

function environment(overrides = {}) {
  return {
    BUSINESS_ADMIN_DATABASE_URL: "postgres://business-admin@business/business_test",
    EXPECTED_BUSINESS_DATABASE: "business_test",
    EXPECTED_BUSINESS_CHANNEL_COUNT: "197",
    PUBLICATION_OPERATOR: "storage-test",
    PUBLICATION_ACTION_REASON: "enable incremental Creator Search storage",
    ...overrides,
  };
}

function state(overrides = {}) {
  return {
    database_name: "business_test",
    database_user: "business_admin",
    identity_kind: "business",
    identity_database: "business_test",
    channel_count: 197,
    write_mode: "shadow",
    read_mode: "legacy",
    active_watermark: "publication_projection_abc",
    live_count: 197,
    legacy_count: 197,
    parity_diffs: 0,
    in_flight_projection_count: 0,
    abnormal_ownership_count: 0,
    ...overrides,
  };
}

function fakePool(initialState, {
  activationResult = "incremental",
  rollbackTarget = null,
} = {}) {
  const calls = [];
  const client = {
    async query(sql, parameters = []) {
      calls.push({ sql, parameters });
      if (sql.includes("AS parity_diffs")) return { rows: [initialState] };
      if (sql.includes("WITH RECURSIVE release_chain")) {
        return { rows: [rollbackTarget ?? {
          rollback_target_exists: true,
          rollback_target_count: 197,
          rollback_target_expected_count: 197,
          rollback_target_parity_diffs: 0,
          rollback_target_reachable: true,
          rollback_chain_errors: 0,
        }] };
      }
      if (sql.includes("activate_creator_search_incremental_v1")) {
        return { rows: [{ storage_mode: activationResult }] };
      }
      if (sql.includes("rollback_creator_search_incremental_storage_v1")) {
        return { rows: [{ rollback_count: 3 }] };
      }
      return { rows: [] };
    },
    release() {
      calls.push({ sql: "RELEASE", parameters: [] });
    },
  };
  return {
    calls,
    pool: {
      async connect() {
        calls.push({ sql: "CONNECT", parameters: [] });
        return client;
      },
    },
  };
}

test("Creator Search storage command keeps plan, apply, and rollback explicit", () => {
  assert.deepEqual(businessCreatorSearchStorageCommand([]), {
    help: false,
    apply: false,
    rollback: false,
    output: null,
  });
  assert.deepEqual(businessCreatorSearchStorageCommand(["--apply"]), {
    help: false,
    apply: true,
    rollback: false,
    output: null,
  });
  assert.deepEqual(businessCreatorSearchStorageCommand(["--rollback"]), {
    help: false,
    apply: false,
    rollback: true,
    output: null,
  });
  assert.deepEqual(
    businessCreatorSearchStorageCommand(["--rollback", "--apply", "--output", "plan.json"]),
    { help: false, apply: true, rollback: true, output: "plan.json" },
  );
  assert.throws(
    () => businessCreatorSearchStorageCommand(["--apply", "--apply"]),
    /--apply may only be provided once/,
  );
  assert.throws(
    () => businessCreatorSearchStorageCommand(["--rollback", "--rollback"]),
    /--rollback may only be provided once/,
  );
  assert.throws(
    () => businessCreatorSearchStorageCommand(["--unknown"]),
    /unknown option/,
  );
});

test("Creator Search storage apply reserves its output before database access", async () => {
  const directory = await mkdtemp(join(tmpdir(), "creator-search-storage-output-"));
  const output = join(directory, "apply-result.json");
  await writeFile(output, "existing audit record\n");
  let poolCreated = false;
  try {
    await assert.rejects(
      runBusinessCreatorSearchStorage({
        argv: ["--apply", "--output", output],
        environment: environment({
          BUSINESS_DATABASE_URL: "",
        }),
        createPool() {
          poolCreated = true;
          throw new Error("database access must not begin");
        },
      }),
      (error) => error?.code === "EEXIST",
    );
    assert.equal(poolCreated, false);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Creator Search storage apply remains successful after a committed output failure", async () => {
  const inspected = state();
  const baseEnvironment = environment();
  const config = businessCreatorSearchStorageConfig(baseEnvironment);
  const fixture = fakePool(inspected);
  let stdout = "";
  let stderr = "";
  await runBusinessCreatorSearchStorage({
    argv: ["--apply", "--output", "apply-result.json"],
    environment: {
      ...baseEnvironment,
      CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE: businessCreatorSearchStorageConfirmation(
        config,
        inspected,
        "activate",
      ),
    },
    createPool: () => fixture.pool,
    openOutput: async () => ({
      async writeFile() { throw new Error("simulated audit disk failure"); },
      async sync() {},
      async close() {},
    }),
    stdout: { write(value) { stdout += value; } },
    stderr: { write(value) { stderr += value; } },
  });
  assert.ok(fixture.calls.some(({ sql }) => sql === "COMMIT"));
  assert.equal(JSON.parse(stdout).result.outcome, "applied");
  assert.match(stderr, /action committed.*output file could not be finalized/i);
});

test("Creator Search storage config requires an exact database and reviewable actor", () => {
  assert.deepEqual(businessCreatorSearchStorageConfig(environment()), {
    databaseUrl: "postgres://business-admin@business/business_test",
    expectedDatabase: "business_test",
    expectedBusinessChannelCount: 197,
    actor: "storage-test",
    reason: "enable incremental Creator Search storage",
    rollbackWatermark: null,
  });
  assert.throws(
    () => businessCreatorSearchStorageConfig(environment({ EXPECTED_BUSINESS_CHANNEL_COUNT: "197.0" })),
    /explicit non-negative integer/,
  );
  assert.throws(
    () => businessCreatorSearchStorageConfig(environment({ PUBLICATION_OPERATOR: "" })),
    /PUBLICATION_OPERATOR/,
  );
  assert.throws(
    () => businessCreatorSearchStorageConfig(environment({
      BUSINESS_ADMIN_DATABASE_URL: "",
      BUSINESS_DATABASE_URL: "postgres://runtime-writer@business/business_test",
    })),
    /BUSINESS_ADMIN_DATABASE_URL/,
  );
});

test("Creator Search storage confirmation binds the exact inspected state and action", () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const confirmation = businessCreatorSearchStorageConfirmation(config, state(), "activate");
  assert.match(
    confirmation,
    /^ACTIVATE_CREATOR_SEARCH_INCREMENTAL:business_test:publication_projection_abc:197:sha256:[0-9a-f]{64}$/,
  );
  assert.notEqual(
    confirmation,
    businessCreatorSearchStorageConfirmation(
      { ...config, reason: "a different reason" },
      state(),
      "activate",
    ),
  );
});

test("Creator Search storage plan is read-only and reports cutover blockers", async () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const fixture = fakePool(state({ in_flight_projection_count: 2 }));
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  const plan = await administrator.inspectReadOnly();
  assert.equal(plan.ready, false);
  assert.deepEqual(plan.blockers, ["2 Projection Outbox rows are in flight"]);
  assert.ok(fixture.calls.some(({ sql }) => /BEGIN.*READ ONLY/i.test(sql)));
  assert.ok(fixture.calls.some(({ sql }) => sql === "ROLLBACK"));
  assert.ok(!fixture.calls.some(({ sql }) => /SELECT public\.activate_creator_search_incremental_v1/.test(sql)));
});

test("Creator Search storage apply locks, rechecks, and calls the guarded database function", async () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const inspected = state();
  const fixture = fakePool(inspected);
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  const result = await administrator.apply({
    expectedWatermark: inspected.active_watermark,
    expectedLiveCount: inspected.live_count,
  });
  assert.equal(result.outcome, "applied");
  assert.equal(result.storage_mode, "incremental");
  assert.ok(fixture.calls.some(({ sql }) => /BEGIN.*SERIALIZABLE/i.test(sql)));
  assert.ok(fixture.calls.some(({ sql }) => /creator-search-publish/.test(sql)));
  assert.ok(fixture.calls.some(({ sql }) => /SELECT public\.activate_creator_search_incremental_v1/.test(sql)));
  assert.ok(fixture.calls.some(({ sql }) => sql === "COMMIT"));
});

test("Creator Search storage apply rejects stale or unsafe state before calling cutover", async () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const fixture = fakePool(state({ active_watermark: "publication_projection_new" }));
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  await assert.rejects(
    administrator.apply({
      expectedWatermark: "publication_projection_old",
      expectedLiveCount: 197,
    }),
    /active watermark changed/,
  );
  assert.ok(fixture.calls.some(({ sql }) => sql === "ROLLBACK"));
  assert.ok(!fixture.calls.some(({ sql }) => /SELECT public\.activate_creator_search_incremental_v1/.test(sql)));
});

test("Creator Search storage rollback plan verifies the retained change chain", async () => {
  const config = businessCreatorSearchStorageConfig(environment({
    BUSINESS_CREATOR_SEARCH_ROLLBACK_WATERMARK: "publication_projection_baseline",
  }));
  const fixture = fakePool(state({
    write_mode: "incremental",
    read_mode: "live",
  }));
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  const plan = await administrator.inspectRollbackReadOnly();
  assert.equal(plan.ready, true);
  assert.equal(plan.state.rollback_target_count, 197);
  assert.match(
    businessCreatorSearchStorageConfirmation(config, plan.state, "rollback"),
    /^ROLLBACK_CREATOR_SEARCH_STORAGE:business_test:publication_projection_baseline:197:sha256:/,
  );
  assert.ok(!fixture.calls.some(({ sql }) => (
    /SELECT public\.rollback_creator_search_incremental_storage_v1/.test(sql)
  )));
});

test("Creator Search storage rollback plan rejects an incomplete Legacy target snapshot", async () => {
  const config = businessCreatorSearchStorageConfig(environment({
    BUSINESS_CREATOR_SEARCH_ROLLBACK_WATERMARK: "publication_projection_incremental",
  }));
  const fixture = fakePool(state({
    write_mode: "incremental",
    read_mode: "live",
  }), {
    rollbackTarget: {
      rollback_target_exists: true,
      rollback_target_count: 2,
      rollback_target_expected_count: 197,
      rollback_target_parity_diffs: 195,
      rollback_target_reachable: true,
      rollback_chain_errors: 0,
    },
  });
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  const plan = await administrator.inspectRollbackReadOnly();
  assert.equal(plan.ready, false);
  assert.deepEqual(plan.blockers, [
    "rollback target Legacy snapshot has 2/197 rows and 195 parity differences",
  ]);
});

test("Creator Search storage rollback locks, rechecks, and calls the guarded function", async () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const inspected = state({
    write_mode: "incremental",
    read_mode: "live",
  });
  const fixture = fakePool(inspected);
  const administrator = new BusinessCreatorSearchStorageAdministrator({
    pool: fixture.pool,
    config,
  });
  const result = await administrator.rollback({
    expectedActiveWatermark: inspected.active_watermark,
    expectedCurrentLiveCount: inspected.live_count,
    targetWatermark: "publication_projection_baseline",
    expectedTarget: {
      rollback_target_exists: true,
      rollback_target_reachable: true,
      rollback_target_count: 197,
      rollback_target_expected_count: 197,
      rollback_target_parity_diffs: 0,
      rollback_chain_errors: 0,
    },
  });
  assert.equal(result.outcome, "rolled_back");
  assert.equal(result.rollback_count, 3);
  const call = fixture.calls.find(({ sql }) => (
    /SELECT public\.rollback_creator_search_incremental_storage_v1/.test(sql)
  ));
  assert.deepEqual(call.parameters, [
    "publication_projection_baseline",
    197,
    "storage-test",
    "enable incremental Creator Search storage",
  ]);
});

test("Creator Search storage rollback rejects target drift after taking the publish lock", async () => {
  const config = businessCreatorSearchStorageConfig(environment());
  const inspected = state({
    write_mode: "incremental",
    read_mode: "live",
  });
  const approvedTarget = {
    rollback_target_exists: true,
    rollback_target_reachable: true,
    rollback_target_count: 197,
    rollback_target_expected_count: 197,
    rollback_target_parity_diffs: 0,
    rollback_chain_errors: 0,
  };
  for (const [field, driftedValue] of [
    ["rollback_target_exists", false],
    ["rollback_target_reachable", false],
    ["rollback_target_count", 196],
    ["rollback_target_expected_count", 196],
    ["rollback_target_parity_diffs", 1],
    ["rollback_chain_errors", 1],
  ]) {
    const fixture = fakePool(inspected, {
      rollbackTarget: { ...approvedTarget, [field]: driftedValue },
    });
    const administrator = new BusinessCreatorSearchStorageAdministrator({
      pool: fixture.pool,
      config,
    });
    await assert.rejects(
      administrator.rollback({
        expectedActiveWatermark: inspected.active_watermark,
        expectedCurrentLiveCount: inspected.live_count,
        targetWatermark: "publication_projection_baseline",
        expectedTarget: approvedTarget,
      }),
      new RegExp(`rollback target changed after the approved plan: ${field}`),
    );
    const lockIndex = fixture.calls.findIndex(({ sql }) => /creator-search-publish/.test(sql));
    const targetIndex = fixture.calls.findIndex(({ sql }) => /WITH RECURSIVE release_chain/.test(sql));
    assert.ok(lockIndex >= 0 && targetIndex > lockIndex);
    assert.ok(fixture.calls.some(({ sql }) => sql === "ROLLBACK"));
    assert.ok(!fixture.calls.some(({ sql }) => (
      /SELECT public\.rollback_creator_search_incremental_storage_v1/.test(sql)
    )));
  }
});
