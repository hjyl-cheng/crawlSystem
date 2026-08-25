#!/usr/bin/env node

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  ACTIVE_LIVE_CONTENT_REPAIR_TARGETS,
  activeLiveContentRepairConfirmation,
  applyActiveLiveContentRepair,
  inspectActiveLiveContentRepair,
} from "../src/activeLiveContentRepair.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Client } = pg;

function explicitCount(environment, name) {
  const raw = String(environment[name] ?? "").trim();
  const parsed = Number(raw);
  assert.ok(
    /^(0|[1-9][0-9]*)$/.test(raw) && Number.isSafeInteger(parsed),
    `${name} must be an explicit non-negative integer`,
  );
  return parsed;
}

function requiredText(environment, name) {
  const value = String(environment[name] ?? "").trim();
  assert.ok(value, `${name} is required`);
  return value;
}

function crawlerDatabaseConfig(environment) {
  const connectionString = String(environment.CRAWLER_DATABASE_URL ?? "").trim();
  if (connectionString) return { connectionString };
  return {
    host: requiredText(environment, "POSTGRES_HOST"),
    port: explicitCount(environment, "POSTGRES_PORT"),
    user: requiredText(environment, "POSTGRES_USER"),
    password: requiredText(environment, "POSTGRES_PASSWORD"),
    database: requiredText(environment, "POSTGRES_DB"),
  };
}

export function activeLiveContentRepairCommand(
  argv = process.argv.slice(2),
  environment = process.env,
) {
  const unknown = argv.filter((value) => value !== "--apply");
  if (unknown.length > 0) throw new TypeError(`unknown option: ${unknown[0]}`);
  if (argv.filter((value) => value === "--apply").length > 1) {
    throw new TypeError("--apply may only be provided once");
  }
  const apply = argv.includes("--apply");
  const command = {
    apply,
    databaseConfig: crawlerDatabaseConfig(environment),
    expectedChannelCount: explicitCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedContentCount: explicitCount(environment, "EXPECTED_CRAWLER_CONTENT_COUNT"),
    operator: requiredText(environment, "ACTIVE_LIVE_CONTENT_REPAIR_OPERATOR"),
    reason: requiredText(environment, "ACTIVE_LIVE_CONTENT_REPAIR_REASON"),
    expectedTargetCount: null,
    expectedEvidenceHash: null,
    confirmation: null,
  };
  if (apply) {
    command.expectedTargetCount = explicitCount(
      environment,
      "EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_COUNT",
    );
    command.expectedEvidenceHash = String(
      environment.EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_EVIDENCE_HASH ?? "",
    ).trim();
    assert.match(
      command.expectedEvidenceHash,
      /^sha256:[0-9a-f]{64}$/,
      "EXPECTED_ACTIVE_LIVE_CONTENT_REPAIR_EVIDENCE_HASH must be a sha256 hash",
    );
    command.confirmation = String(
      environment.CONFIRM_ACTIVE_LIVE_CONTENT_REPAIR ?? "",
    ).trim();
    assert.ok(command.confirmation, "CONFIRM_ACTIVE_LIVE_CONTENT_REPAIR is required");
  }
  return command;
}

function assertExpectedState(evidence, command) {
  assert.equal(
    evidence.channel_count,
    command.expectedChannelCount,
    "unexpected Crawler Channel count",
  );
  assert.equal(
    evidence.content_count,
    command.expectedContentCount,
    "unexpected Crawler Content count",
  );
  assert.equal(
    evidence.target_count,
    ACTIVE_LIVE_CONTENT_REPAIR_TARGETS.length,
    "unexpected repair manifest size",
  );
}

async function main({
  argv = process.argv.slice(2),
  environment = process.env,
} = {}) {
  const command = activeLiveContentRepairCommand(argv, environment);
  const client = new Client({
    ...command.databaseConfig,
    application_name: "active-live-content-repair-v1",
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  let began = false;
  try {
    await client.connect();
    await verifyCrawlerWriterDatabase(client.query.bind(client), environment);
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
    began = true;
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='300s'");
    const evidence = await inspectActiveLiveContentRepair(client, {
      operator: command.operator,
      reason: command.reason,
    });
    assertExpectedState(evidence, command);

    if (!command.apply) {
      let preview = null;
      if (evidence.blocker_count === 0) {
        preview = await applyActiveLiveContentRepair(client, {
          expectedEvidenceHash: evidence.evidence_hash,
          expectedTargetCount: evidence.target_count,
          operator: command.operator,
          reason: command.reason,
        });
      }
      await client.query("ROLLBACK");
      began = false;
      process.stdout.write(`${JSON.stringify({
        ok: evidence.blocker_count === 0,
        mode: "plan",
        committed_writes: false,
        required_confirmation_environment: "CONFIRM_ACTIVE_LIVE_CONTENT_REPAIR",
        required_confirmation: activeLiveContentRepairConfirmation(evidence),
        evidence,
        preview,
      }, null, 2)}\n`);
      return;
    }

    assert.equal(
      command.confirmation,
      activeLiveContentRepairConfirmation(evidence),
      "CONFIRM_ACTIVE_LIVE_CONTENT_REPAIR does not match the current plan",
    );
    const result = await applyActiveLiveContentRepair(client, {
      expectedEvidenceHash: command.expectedEvidenceHash,
      expectedTargetCount: command.expectedTargetCount,
      operator: command.operator,
      reason: command.reason,
    });
    await client.query("COMMIT");
    began = false;
    process.stdout.write(`${JSON.stringify({
      ok: true,
      mode: "apply",
      committed_writes: true,
      result,
    }, null, 2)}\n`);
  } catch (error) {
    if (began) await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
