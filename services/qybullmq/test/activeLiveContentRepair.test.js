import assert from "node:assert/strict";
import test from "node:test";
import {
  ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT,
  ACTIVE_LIVE_CONTENT_REPAIR_TARGETS,
  activeLiveContentRepairConfirmation,
  activeLiveContentRepairEvidenceHash,
} from "../src/activeLiveContentRepair.js";
import { activeLiveContentRepairCommand } from
  "../scripts/repairActiveLiveContents.mjs";

function environment(overrides = {}) {
  return {
    CRAWLER_DATABASE_URL: "postgres://crawler.test/active_live_repair_test",
    EXPECTED_CRAWLER_CHANNEL_COUNT: "200",
    EXPECTED_CRAWLER_CONTENT_COUNT: "4000",
    ACTIVE_LIVE_CONTENT_REPAIR_OPERATOR: "integration-test",
    ACTIVE_LIVE_CONTENT_REPAIR_REASON: "remove historically stored active broadcasts",
    ...overrides,
  };
}

test("active-Live repair manifest is fixed to nine Videos in four Channels", () => {
  assert.equal(ACTIVE_LIVE_CONTENT_REPAIR_TARGETS.length, 9);
  assert.equal(new Set(
    ACTIVE_LIVE_CONTENT_REPAIR_TARGETS.map((target) => target.channel_id),
  ).size, 4);
  assert.equal(new Set(
    ACTIVE_LIVE_CONTENT_REPAIR_TARGETS.map((target) => target.content_id),
  ).size, 9);
  assert.equal(ACTIVE_LIVE_CONTENT_REPAIR_NEVER_RETRY_AT, "9999-12-31T23:59:59.999Z");
});

test("active-Live repair evidence hash and confirmation bind the exact target", () => {
  const evidence = {
    database_name: "active_live_repair_test",
    target_count: 9,
    target_channel_count: 4,
    targets: [{ channel_id: "UC2" }, { channel_id: "UC1" }],
  };
  const reordered = {
    targets: evidence.targets,
    target_channel_count: evidence.target_channel_count,
    target_count: evidence.target_count,
    database_name: evidence.database_name,
  };
  const evidenceHash = activeLiveContentRepairEvidenceHash(evidence);
  assert.equal(activeLiveContentRepairEvidenceHash(reordered), evidenceHash);
  assert.equal(
    activeLiveContentRepairConfirmation({ ...evidence, evidence_hash: evidenceHash }),
    `repair-active-live-content:active_live_repair_test:9:4:${evidenceHash}`,
  );
});

test("active-Live repair defaults to a rolled-back plan", () => {
  assert.deepEqual(activeLiveContentRepairCommand([], environment()), {
    apply: false,
    databaseConfig: {
      connectionString: "postgres://crawler.test/active_live_repair_test",
    },
    expectedChannelCount: 200,
    expectedContentCount: 4000,
    operator: "integration-test",
    reason: "remove historically stored active broadcasts",
    expectedTargetCount: null,
    expectedEvidenceHash: null,
    confirmation: null,
  });
});

test("active-Live repair apply requires exact count, hash, and confirmation", () => {
  assert.throws(
    () => activeLiveContentRepairCommand(["--apply"], environment()),
    /EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_COUNT/,
  );
  const command = activeLiveContentRepairCommand(["--apply"], environment({
    EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_COUNT: "9",
    EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_EVIDENCE_HASH: `sha256:${"a".repeat(64)}`,
    CONFIRM_ACTIVE_LIVE_CONTENT_REPAIR: "explicit-confirmation",
  }));
  assert.equal(command.expectedTargetCount, 9);
  assert.equal(command.expectedEvidenceHash, `sha256:${"a".repeat(64)}`);
  assert.equal(command.confirmation, "explicit-confirmation");
});

test("active-Live repair uses the standard Worker PostgreSQL settings when no URL exists", () => {
  const command = activeLiveContentRepairCommand([], environment({
    CRAWLER_DATABASE_URL: "",
    POSTGRES_HOST: "crawler-pgbouncer",
    POSTGRES_PORT: "6432",
    POSTGRES_USER: "bullmq",
    POSTGRES_PASSWORD: "test-password",
    POSTGRES_DB: "active_live_repair_test",
  }));
  assert.deepEqual(command.databaseConfig, {
    host: "crawler-pgbouncer",
    port: 6432,
    user: "bullmq",
    password: "test-password",
    database: "active_live_repair_test",
  });
});
