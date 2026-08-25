#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  BusinessCreatorSearchStorageAdministrator,
  businessCreatorSearchStorageConfig,
  businessCreatorSearchStorageConfirmation,
} from "../src/businessCreatorSearchStorageAdmin.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  node scripts/manageBusinessCreatorSearchStorage.mjs [--apply] [--rollback] [--output <path>]

The default command emits a read-only incremental cutover plan. --rollback emits
a read-only rollback plan. No database writes occur unless --apply is supplied
and the exact confirmation emitted by the corresponding plan is provided.

Required environment:
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_BUSINESS_CHANNEL_COUNT
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Rollback environment:
  BUSINESS_CREATOR_SEARCH_ROLLBACK_WATERMARK

Apply environment:
  CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

export function businessCreatorSearchStorageCommand(argv = process.argv.slice(2)) {
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
  const command = businessCreatorSearchStorageCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = businessCreatorSearchStorageConfig(environment);
  const pool = new Pool({
    connectionString: config.databaseUrl,
    application_name: "business-creator-search-storage-admin",
    max: 1,
    options: "-c timezone=UTC",
  });
  try {
    const administrator = new BusinessCreatorSearchStorageAdministrator({ pool, config });
    const plan = command.rollback
      ? await administrator.inspectRollbackReadOnly()
      : await administrator.inspectReadOnly();
    const action = command.rollback ? "rollback" : "activate";
    const confirmation = businessCreatorSearchStorageConfirmation(config, plan.state, action);
    if (!command.apply) {
      await emit({
        ok: true,
        mode: command.rollback ? "rollback_plan" : "plan",
        writes_performed: false,
        ready: plan.ready,
        blockers: plan.blockers,
        required_confirmation_environment: "CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE",
        required_confirmation: confirmation,
        state: plan.state,
      }, command.output);
      return;
    }
    if (!plan.ready) {
      throw new Error(`Creator Search storage action is blocked: ${plan.blockers.join("; ")}`);
    }
    if (String(environment.CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE ?? "").trim()
        !== confirmation) {
      throw new Error(
        "CONFIRM_BUSINESS_CREATOR_SEARCH_STORAGE must exactly equal the value emitted by the plan command",
      );
    }
    const result = command.rollback
      ? await administrator.rollback({
        expectedActiveWatermark: plan.state.active_watermark,
        expectedCurrentLiveCount: plan.state.live_count,
        targetWatermark: plan.state.rollback_target_watermark,
        expectedLiveCount: plan.state.rollback_target_count,
      })
      : await administrator.apply({
        expectedWatermark: plan.state.active_watermark,
        expectedLiveCount: plan.state.live_count,
      });
    await emit({
      ok: true,
      mode: command.rollback ? "rollback_apply" : "apply",
      writes_performed: true,
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
