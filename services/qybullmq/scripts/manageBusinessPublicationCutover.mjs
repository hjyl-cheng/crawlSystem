#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  BusinessPublicationCutoverAdministrator,
  buildBusinessPublicationCutoverTarget,
  businessPublicationCutoverConfig,
  businessPublicationCutoverConfirmation,
  businessPublicationCutoverRollbackConfirmation,
  readBusinessPublicationCutoverEvidence,
} from "../src/businessPublicationCutoverAdmin.js";
import { readPublicationChannelSet } from "../src/publicationCohortAdmin.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  node scripts/manageBusinessPublicationCutover.mjs [--apply] [--rollback] [--output <path>]

Without --apply, the default mode runs the real Projection and Search release
path in a repeatable-read transaction, rolls it back, and emits immutable
evidence. --rollback without --apply emits a read-only rollback plan. An output
file is created exclusively and is never overwritten.

Required environment:
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_BUSINESS_CHANNEL_COUNT
  PUBLICATION_STREAM_ID
  PUBLICATION_CHANNEL_IDS_FILE
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Optional environment:
  BUSINESS_PUBLICATION_CUTOVER_DRY_RUN_BATCH_SIZE (default: 25)

Apply-only environment:
  BUSINESS_PUBLICATION_CUTOVER_EVIDENCE_FILE
  CONFIRM_BUSINESS_PUBLICATION_CUTOVER

Rollback environment:
  BUSINESS_PUBLICATION_CUTOVER_EVIDENCE_FILE
  CONFIRM_BUSINESS_PUBLICATION_CUTOVER_ROLLBACK (apply only)
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

export function businessPublicationCutoverCommand(argv = process.argv.slice(2)) {
  const command = { help: false, apply: false, rollback: false, output: null };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--help" || value === "-h") command.help = true;
    else if (value === "--apply") {
      if (command.apply) throw new TypeError("--apply may only be provided once");
      command.apply = true;
    } else if (value === "--rollback") {
      if (command.rollback) throw new TypeError("--rollback may only be provided once");
      command.rollback = true;
    } else if (value === "--output") {
      if (command.output) throw new TypeError("--output may only be provided once");
      command.output = requiredValue(argv, index++, value);
    } else throw new TypeError(`unknown option: ${value}`);
  }
  return command;
}

async function emit(value, output) {
  const rendered = `${JSON.stringify(value, null, 2)}\n`;
  if (output) await writeFile(output, rendered, { encoding: "utf8", flag: "wx" });
  else process.stdout.write(rendered);
}

async function main({ environment = process.env, argv = process.argv.slice(2) } = {}) {
  const command = businessPublicationCutoverCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = businessPublicationCutoverConfig(environment, {
    apply: command.apply,
    rollback: command.rollback,
  });
  const channelIds = await readPublicationChannelSet(config.channelIdsFile);
  const target = buildBusinessPublicationCutoverTarget(config, channelIds);
  const evidence = command.apply || command.rollback
    ? await readBusinessPublicationCutoverEvidence(config.evidenceFile, config, target)
    : null;
  const confirmation = evidence && (command.rollback
    ? businessPublicationCutoverRollbackConfirmation(config, target, evidence)
    : businessPublicationCutoverConfirmation(config, target, evidence));
  if (command.apply) {
    const confirmationName = command.rollback
      ? "CONFIRM_BUSINESS_PUBLICATION_CUTOVER_ROLLBACK"
      : "CONFIRM_BUSINESS_PUBLICATION_CUTOVER";
    if (String(environment[confirmationName] ?? "").trim() !== confirmation) {
      throw new Error(`${confirmationName} must exactly equal the value emitted by the plan command`);
    }
  }
  const pool = new Pool({
    connectionString: config.databaseUrl,
    application_name: "business-publication-cutover-admin",
    max: 4,
    options: "-c timezone=UTC",
  });
  try {
    const administrator = new BusinessPublicationCutoverAdministrator({
      pool,
      config,
      target,
      evidence,
    });
    if (command.rollback) {
      if (!command.apply) {
        const plan = await administrator.inspectRollbackReadOnly();
        await emit({
          ok: true,
          mode: "rollback_plan",
          writes_performed: false,
          required_confirmation_environment:
            "CONFIRM_BUSINESS_PUBLICATION_CUTOVER_ROLLBACK",
          required_confirmation: confirmation,
          state: plan.state,
          state_hash: plan.state_hash,
        }, command.output);
        return;
      }
      const result = await administrator.rollback();
      await emit({
        ok: true,
        mode: "rollback_apply",
        writes_performed: result.outcome === "rolled_back",
        evidence_hash: evidence.evidence_hash,
        result,
      }, command.output);
      return;
    }
    if (!command.apply) {
      const plan = await administrator.inspectReadOnly();
      const requiredConfirmation = businessPublicationCutoverConfirmation(
        config,
        target,
        plan.evidence,
      );
      await emit({
        ok: true,
        mode: "plan",
        writes_performed: false,
        dry_run_transaction: "rolled_back",
        required_confirmation_environment: "CONFIRM_BUSINESS_PUBLICATION_CUTOVER",
        required_confirmation: requiredConfirmation,
        state: plan.summary,
        evidence: plan.evidence,
      }, command.output);
      return;
    }
    const result = await administrator.apply();
    await emit({
      ok: true,
      mode: "apply",
      writes_performed: result.outcome === "applied",
      evidence_hash: evidence.evidence_hash,
      result,
    }, command.output);
  } finally {
    await pool.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
