#!/usr/bin/env node

import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  applyDisabledCommentCountRepair,
  disabledCommentCountRepairConfirmation,
  inspectDisabledCommentCountRepair,
} from "../src/disabledCommentCountRepair.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { environmentValue } from "../src/runtimeEnvironment.js";

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

export function disabledCommentCountRepairCommand(
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
    databaseUrl: environmentValue("CRAWLER_DATABASE_URL", { environment }),
    expectedChannelCount: explicitCount(environment, "EXPECTED_CRAWLER_CHANNEL_COUNT"),
    expectedContentCount: explicitCount(environment, "EXPECTED_CRAWLER_CONTENT_COUNT"),
    operator: requiredText(environment, "DISABLED_COMMENT_COUNT_REPAIR_OPERATOR"),
    reason: requiredText(environment, "DISABLED_COMMENT_COUNT_REPAIR_REASON"),
    expectedTargetCount: null,
    expectedEvidenceHash: null,
    confirmation: null,
  };
  if (apply) {
    command.expectedTargetCount = explicitCount(
      environment,
      "EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_COUNT",
    );
    command.expectedEvidenceHash = String(
      environment.EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_EVIDENCE_HASH ?? "",
    ).trim();
    assert.match(
      command.expectedEvidenceHash,
      /^sha256:[0-9a-f]{64}$/,
      "EXPECTED_DISABLED_COMMENT_COUNT_REPAIR_EVIDENCE_HASH must be a sha256 hash",
    );
    command.confirmation = String(
      environment.CONFIRM_DISABLED_COMMENT_COUNT_REPAIR ?? "",
    ).trim();
    assert.ok(command.confirmation, "CONFIRM_DISABLED_COMMENT_COUNT_REPAIR is required");
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
}

async function main({
  argv = process.argv.slice(2),
  environment = process.env,
} = {}) {
  const command = disabledCommentCountRepairCommand(argv, environment);
  const client = new Client({
    connectionString: command.databaseUrl,
    application_name: "disabled-comment-count-repair-v1",
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  let began = false;
  try {
    await client.connect();
    await verifyCrawlerWriterDatabase(client.query.bind(client), environment);
    await client.query(command.apply
      ? "BEGIN ISOLATION LEVEL REPEATABLE READ"
      : "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    began = true;
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='300s'");
    const evidence = await inspectDisabledCommentCountRepair(client, {
      operator: command.operator,
      reason: command.reason,
    });
    assertExpectedState(evidence, command);

    if (!command.apply) {
      await client.query("ROLLBACK");
      began = false;
      process.stdout.write(`${JSON.stringify({
        ok: true,
        mode: "plan",
        committed_writes: false,
        required_confirmation_environment: "CONFIRM_DISABLED_COMMENT_COUNT_REPAIR",
        required_confirmation: disabledCommentCountRepairConfirmation(evidence),
        evidence,
      }, null, 2)}\n`);
      return;
    }

    assert.equal(
      command.confirmation,
      disabledCommentCountRepairConfirmation(evidence),
      "CONFIRM_DISABLED_COMMENT_COUNT_REPAIR does not match the current plan",
    );
    const result = await applyDisabledCommentCountRepair(client, {
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
