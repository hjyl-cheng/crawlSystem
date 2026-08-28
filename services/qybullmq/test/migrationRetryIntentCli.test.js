import assert from "node:assert/strict";
import test from "node:test";

import {
  migrationRetryIntentPlan,
  parseMigrationRetryIntentCommand,
  runMigrationRetryIntentCommand,
} from "../scripts/createMigrationRetryIntent.mjs";
import {
  InMemoryMigrationRetryIntentRepository,
  MigrationRetryIntentStore,
} from "../src/migrationRetryIntent.js";

const environment = {
  EXPECTED_CRAWLER_DATABASE: "crawler_test",
};

const argumentsFor = (overrides = {}) => [
  "--candidate-id", String(overrides.candidateId ?? 42),
  "--previous-business-run-id", overrides.previousBusinessRunId ?? "run:old",
  "--request-key", overrides.requestKey ?? "operator-ticket-20260826-candidate-42",
  "--reason", overrides.reason ?? "controlled retry after proxy fixes",
  "--min-subscriber-count", String(overrides.minSubscriberCount ?? 1000),
];

test("Migration Recovery CLI defaults to a parameter-bound read-only plan", () => {
  const command = parseMigrationRetryIntentCommand(argumentsFor(), environment);
  const plan = migrationRetryIntentPlan(command);

  assert.equal(command.execute, false);
  assert.deepEqual(plan, {
    ok: true,
    mode: "plan",
    committed_writes: false,
    expected_database: "crawler_test",
    candidate_id: 42,
    previous_business_run_id: "run:old",
    request_key: "operator-ticket-20260826-candidate-42",
    reason: "controlled retry after proxy fixes",
    min_subscriber_count: 1000,
    required_confirmation: command.requiredConfirmation,
  });
  assert.match(
    command.requiredConfirmation,
    /^confirm-migration-retry:42:sha256:[a-f0-9]{64}$/,
  );
  assert.notEqual(
    parseMigrationRetryIntentCommand(
      argumentsFor({ minSubscriberCount: 2000 }),
      environment,
    ).requiredConfirmation,
    command.requiredConfirmation,
  );
});

test("Migration Recovery CLI execute requires the plan's exact confirmation", () => {
  const planned = parseMigrationRetryIntentCommand(argumentsFor(), environment);
  assert.throws(
    () => parseMigrationRetryIntentCommand([...argumentsFor(), "--execute"], environment),
    /--confirm must exactly match/,
  );
  const command = parseMigrationRetryIntentCommand([
    ...argumentsFor(),
    "--execute",
    "--confirm", planned.requiredConfirmation,
  ], environment);
  assert.equal(command.execute, true);
});

test("Migration Recovery CLI plan verifies Crawler identity using only reads", async () => {
  const statements = [];
  const query = async (sql) => {
    statements.push(sql);
    if (sql.includes("FROM crawler.database_identity")) {
      return {
        rows: [{
          database_name: "crawler_test",
          database_user: "crawler_operator",
          transaction_read_only: "off",
          identity_kind: "crawler",
          identity_database: "crawler_test",
          schema_ready: true,
        }],
      };
    }
    if (sql.includes("FROM crawler.channel_candidates")) {
      return {
        rows: [{
          candidate_id: "42",
          channel_id: "UC1234567890123456789012",
          candidate_status: "failed",
          snapshot_dispatch_generation: "4",
          previous_business_run_id: "run:old",
          binding_status: "terminal",
          binding_terminal_reason: "proxy_control_business_run_budget_exhausted",
          retry_intent_id: null,
          retry_intent_candidate_id: null,
          retry_intent_previous_business_run_id: null,
          retry_intent_reason: null,
          retry_intent_payload: null,
          retry_intent_status: null,
          outbox_dispatch_id: null,
          active_retry_intent_id: null,
        }],
      };
    }
    throw new Error(`unexpected query: ${sql}`);
  };
  const command = parseMigrationRetryIntentCommand(argumentsFor(), environment);

  const result = await runMigrationRetryIntentCommand(command, { query, environment });

  assert.equal(result.mode, "plan");
  assert.equal(result.committed_writes, false);
  assert.deepEqual(result.database, {
    database: "crawler_test",
    user: "crawler_operator",
  });
  assert.equal(result.target.eligible, true);
  assert.deepEqual(result.target.blockers, []);
  assert.equal(statements.length, 2);
  assert.equal(statements.every((sql) => /^\s*SELECT\b/.test(sql)), true);
});

test("Migration Recovery CLI execute creates only the controlled Intent dispatch boundary", async () => {
  const repository = new InMemoryMigrationRetryIntentRepository({
    candidates: [{
      candidate_id: 42,
      channel_id: "UC1234567890123456789012",
      channel_url: "https://www.youtube.com/channel/UC1234567890123456789012",
      dispatch_batch_id: "legacy-results-manual-v1",
      pipeline_cycle_id: "legacy-results-manual-v1",
      status: "failed",
      snapshot_dispatch_generation: 4,
      source_json: { source: "legacy_results_db" },
    }],
    bindings: [{
      business_run_id: "run:old",
      candidate_id: 42,
      channel_id: "UC1234567890123456789012",
      status: "terminal",
      terminal_reason: "proxy_control_business_run_budget_exhausted",
    }],
  });
  const ids = [
    "77777777-7777-4777-8777-777777777777",
    "88888888-8888-4888-8888-888888888888",
  ];
  const store = new MigrationRetryIntentStore({
    repository,
    randomUUID: () => ids.shift(),
    now: () => new Date("2026-08-26T14:00:00.000Z"),
  });
  const query = async (sql) => {
    if (sql.includes("FROM crawler.database_identity")) {
      return {
        rows: [{
          database_name: "crawler_test",
          database_user: "crawler_operator",
          transaction_read_only: "off",
          identity_kind: "crawler",
          identity_database: "crawler_test",
          schema_ready: true,
        }],
      };
    }
    return {
      rows: [{
        candidate_id: "42",
        channel_id: "UC1234567890123456789012",
        candidate_status: "failed",
        snapshot_dispatch_generation: "4",
        previous_business_run_id: "run:old",
        binding_status: "terminal",
        binding_terminal_reason: "proxy_control_business_run_budget_exhausted",
        retry_intent_id: null,
        retry_intent_candidate_id: null,
        retry_intent_previous_business_run_id: null,
        retry_intent_reason: null,
        retry_intent_payload: null,
        retry_intent_status: null,
        outbox_dispatch_id: null,
        active_retry_intent_id: null,
      }],
    };
  };
  const planned = parseMigrationRetryIntentCommand(argumentsFor(), environment);
  const command = parseMigrationRetryIntentCommand([
    ...argumentsFor(),
    "--execute",
    "--confirm", planned.requiredConfirmation,
  ], environment);

  const result = await runMigrationRetryIntentCommand(command, {
    query,
    environment,
    store,
  });

  assert.equal(result.mode, "execute");
  assert.equal(result.created, true);
  assert.equal(repository.intents.size, 1);
  assert.equal(repository.outbox.size, 1);
  assert.equal(repository.candidates.get(42).status, "queued");
  assert.equal(repository.bindings.get("run:old").status, "terminal");
});
