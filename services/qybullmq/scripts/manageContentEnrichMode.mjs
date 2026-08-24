#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import { closeDb, query, withTransaction } from "../src/db.js";
import {
  CONTENT_ENRICH_CLOCK_MODE,
  loadContentEnrichMode,
  switchContentEnrichMode,
} from "../src/contentEnrichMode.js";

function usage() {
  return `Usage:
  node scripts/manageContentEnrichMode.mjs status
  node scripts/manageContentEnrichMode.mjs queue [--apply]
  node scripts/manageContentEnrichMode.mjs clock [--apply]

Without --apply, queue and clock only print the current state and requested transition.
Applying a transition requires CONTENT_ENRICH_MODE_CONFIRM to equal the target mode.
CONTENT_ENRICH_MODE_OPERATOR and CONTENT_ENRICH_MODE_REASON are also required.
`;
}

export function contentEnrichModeCommand(argv = process.argv.slice(2)) {
  if (argv.includes("--help") || argv.includes("-h")) return { help: true };
  const command = String(argv[0] ?? "").trim().toLowerCase();
  if (!new Set(["status", "queue", "clock"]).has(command)) {
    throw new TypeError("the command must be status, queue, or clock");
  }
  const options = argv.slice(1);
  const unknown = options.filter((value) => value !== "--apply");
  if (unknown.length > 0) throw new TypeError(`unknown option: ${unknown[0]}`);
  if (options.filter((value) => value === "--apply").length > 1) {
    throw new TypeError("--apply may only be provided once");
  }
  if (command === "status" && options.includes("--apply")) {
    throw new TypeError("status does not accept --apply");
  }
  return { command, apply: options.includes("--apply"), help: false };
}

async function inspect() {
  const [mode, counts] = await Promise.all([
    loadContentEnrichMode({ query }),
    query(
      `SELECT status,count(*)::bigint AS count,
              min(created_at) AS oldest_created_at,
              min(next_retry_at) FILTER (WHERE status='failed') AS next_retry_at
       FROM crawler.content_enrich_tasks
       WHERE job_type='player-refresh'
       GROUP BY status
       ORDER BY status`,
    ),
  ]);
  return {
    mode,
    player_refresh: counts.rows.map((row) => ({
      ...row,
      count: Number(row.count),
    })),
  };
}

async function main({ argv = process.argv.slice(2), environment = process.env } = {}) {
  const command = contentEnrichModeCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const before = await inspect();
  if (command.command === "status" || !command.apply) {
    process.stdout.write(`${JSON.stringify({
      ok: true,
      apply: false,
      current: before,
      requested_mode: command.command === "status" ? before.mode : command.command,
      writes_performed: false,
    }, null, 2)}\n`);
    return;
  }
  if (String(environment.CONTENT_ENRICH_MODE_CONFIRM ?? "").trim() !== command.command) {
    throw new Error(`CONTENT_ENRICH_MODE_CONFIRM must equal ${command.command}`);
  }
  const changedBy = String(environment.CONTENT_ENRICH_MODE_OPERATOR ?? "").trim();
  const reason = String(environment.CONTENT_ENRICH_MODE_REASON ?? "").trim();
  if (!changedBy) throw new Error("CONTENT_ENRICH_MODE_OPERATOR is required");
  if (!reason) throw new Error("CONTENT_ENRICH_MODE_REASON is required");
  const transition = await withTransaction((client) => switchContentEnrichMode(client, {
    mode: command.command,
    changedBy,
    reason,
  }));
  process.stdout.write(`${JSON.stringify({
    ok: true,
    apply: true,
    before,
    transition,
    rollback_mode: command.command === CONTENT_ENRICH_CLOCK_MODE ? "queue" : "clock",
    writes_performed: true,
  }, null, 2)}\n`);
}

const invokedPath = process.argv[1] ? fileURLToPath(import.meta.url) === process.argv[1] : false;
if (invokedPath) {
  main()
    .catch((error) => {
      process.stderr.write(`${error?.stack || error}\n`);
      process.exitCode = 1;
    })
    .finally(() => closeDb());
}
