#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  BusinessStorageBaselineReporter,
  businessStorageBaselineConfig,
} from "../src/businessStorageBaseline.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  node scripts/reportBusinessStorageBaseline.mjs [--output <path>]

Captures a repeatable, read-only Business database capacity baseline. Output
files are created exclusively and are never overwritten. There is no write mode.

Required environment:
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_BUSINESS_CHANNEL_COUNT
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

export function businessStorageBaselineCommand(argv = process.argv.slice(2)) {
  const command = { help: false, output: null };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--help" || value === "-h") command.help = true;
    else if (value === "--output") {
      if (command.output) throw new TypeError("--output may only be provided once");
      command.output = requiredValue(argv, index++, value);
    } else throw new TypeError(`unknown option: ${value}`);
  }
  return command;
}

async function main({ argv = process.argv.slice(2), environment = process.env } = {}) {
  const command = businessStorageBaselineCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = businessStorageBaselineConfig(environment);
  const pool = new Pool({
    connectionString: config.databaseUrl,
    application_name: "business-storage-baseline",
    max: 1,
    options: "-c timezone=UTC",
  });
  try {
    const reporter = new BusinessStorageBaselineReporter({ pool, config });
    const report = await reporter.capture();
    const rendered = `${JSON.stringify(report, null, 2)}\n`;
    if (command.output) {
      await writeFile(command.output, rendered, { encoding: "utf8", flag: "wx" });
    } else {
      process.stdout.write(rendered);
    }
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
