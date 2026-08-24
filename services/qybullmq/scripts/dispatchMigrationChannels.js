#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const APPROVED_SELECTIONS = new Set(["100", "200", "500", "1000", "2000"]);
const SAFE_BATCH_ID = /^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/;

export function controlledMigrationUsage() {
  return `Usage: npm run dispatch:migration-channels -- --selection <100|200|500|1000|2000> [options]

Default mode is a read-only local plan. Execute reads the legacy Migration
Source in a separate read-only transaction, then writes idempotent intents to
the fresh Crawler database.

Options:
  --selection <100|200|500|1000|2000>  Approved canary size
  --batch-id <id>              Optional fresh Crawler dispatch batch ID
  --execute                    Materialize Target intents and Candidates
  --confirm <token>            Exact confirmation emitted by plan mode
  --help                       Show this help
`;
}

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${name} is required`);
  return normalized;
}

export function parseControlledMigrationCommand(argv = [], environment = process.env) {
  const { values } = parseArgs({
    args: argv,
    options: {
      selection: { type: "string" },
      "batch-id": { type: "string" },
      execute: { type: "boolean", default: false },
      confirm: { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) return { help: true, execute: false };

  const sourceId = requiredText(environment.MIGRATION_SOURCE_ID, "MIGRATION_SOURCE_ID");
  const selection = requiredText(values.selection, "--selection");
  if (!APPROVED_SELECTIONS.has(selection)) {
    throw new TypeError("--selection must be one of: 100, 200, 500, 1000, 2000");
  }
  const batchId = String(values["batch-id"] ?? "").trim() || null;
  if (batchId && !SAFE_BATCH_ID.test(batchId)) {
    throw new TypeError("--batch-id must use 1-128 letters, numbers, dots, underscores, or hyphens");
  }
  const requiredConfirmation = `confirm-controlled-migration:${sourceId}:${selection}`;
  const confirmation = String(values.confirm ?? "").trim() || null;
  if (values.execute && confirmation !== requiredConfirmation) {
    throw new TypeError(`--confirm must exactly match ${requiredConfirmation}`);
  }
  return {
    help: false,
    execute: values.execute,
    sourceId,
    selection,
    batchId,
    requiredConfirmation,
  };
}

export function controlledMigrationPlan(command) {
  return {
    ok: true,
    mode: "plan",
    writes_performed: false,
    source_id: command.sourceId,
    selection: command.selection,
    required_confirmation: command.requiredConfirmation,
  };
}

async function main() {
  const command = parseControlledMigrationCommand(process.argv.slice(2));
  if (command.help) {
    process.stdout.write(controlledMigrationUsage());
    return;
  }
  if (!command.execute) {
    process.stdout.write(`${JSON.stringify(controlledMigrationPlan(command))}\n`);
    return;
  }

  const [{ dispatchManualMigrationBatch }, { closeDb }, { closeMigrationSourcePool }] =
    await Promise.all([
      import("../src/manualMigrationDispatch.js"),
      import("../src/db.js"),
      import("../src/migrationSource.js"),
    ]);
  try {
    const result = await dispatchManualMigrationBatch({
      selection: command.selection,
      sourceId: command.sourceId,
      ...(command.batchId ? { batchId: command.batchId } : {}),
    });
    process.stdout.write(`${JSON.stringify({
      ...result,
      mode: "execute",
      writes_performed: result.target_count > 0,
    })}\n`);
  } finally {
    await Promise.allSettled([closeDb(), closeMigrationSourcePool()]);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack || String(error)}\n`);
    process.exitCode = 1;
  });
}
