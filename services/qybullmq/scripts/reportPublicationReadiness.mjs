#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import pg from "pg";
import { generatePublicationReadinessReport } from "../src/publicationReadinessReport.js";

const { Client } = pg;

function usage() {
  return `Usage: npm run publication:readiness -- [options]

Options:
  --as-of <timestamp>       UTC report boundary (default: current time)
  --channel-id <id>         Restrict to a Channel; may be repeated
  --format <json|jsonl>     Output format (default: json)
  --output <path>           Write to a new file instead of stdout; never overwrite
  --fail-on-not-ready       Exit with code 2 when any Channel is not ready
  --help                    Show this help

Environment:
  CRAWLER_DATABASE_URL      Read-only Crawler PostgreSQL connection URL
  BUSINESS_DATABASE_URL     Read-only Business PostgreSQL connection URL
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

function parseArgs(argv) {
  const options = {
    asOf: new Date().toISOString(),
    channelIds: [],
    format: "json",
    output: null,
    failOnNotReady: false,
    help: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (arg === "--help") options.help = true;
    else if (arg === "--fail-on-not-ready") options.failOnNotReady = true;
    else if (arg === "--as-of") options.asOf = requiredValue(argv, index++, arg);
    else if (arg === "--channel-id") options.channelIds.push(requiredValue(argv, index++, arg));
    else if (arg === "--format") options.format = requiredValue(argv, index++, arg);
    else if (arg === "--output") options.output = requiredValue(argv, index++, arg);
    else throw new TypeError(`unknown option: ${arg}`);
  }
  if (!timestamp(options.asOf)) throw new TypeError("--as-of must be a valid timestamp");
  if (!["json", "jsonl"].includes(options.format)) throw new TypeError("--format must be json or jsonl");
  options.channelIds = [...new Set(options.channelIds.map((value) => value.trim()).filter(Boolean))];
  return options;
}

function timestamp(value) {
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? null : parsed.toISOString();
}

function connectionUrl(name, fallback = null) {
  const value = String(process.env[name] ?? fallback ?? "").trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

function renderJsonl(report) {
  const { channels, ...metadata } = report;
  return [
    JSON.stringify({ record_type: "report", ...metadata }),
    ...channels.map((channel) => JSON.stringify({ record_type: "channel", ...channel })),
  ].join("\n") + "\n";
}

async function rollback(client, begun) {
  if (begun) await client.query("ROLLBACK").catch(() => {});
}

async function run(options) {
  const crawler = new Client({
    connectionString: connectionUrl("CRAWLER_DATABASE_URL", process.env.DATABASE_URL),
    application_name: "publication-readiness-crawler",
    options: "-c timezone=UTC",
  });
  const business = new Client({
    connectionString: connectionUrl("BUSINESS_DATABASE_URL"),
    application_name: "publication-readiness-business",
    options: "-c timezone=UTC",
  });
  let crawlerBegun = false;
  let businessBegun = false;
  try {
    await Promise.all([crawler.connect(), business.connect()]);
    await Promise.all([
      crawler.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").then(() => { crawlerBegun = true; }),
      business.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY").then(() => { businessBegun = true; }),
    ]);
    const report = await generatePublicationReadinessReport({
      crawlerQuery: crawler.query.bind(crawler),
      businessQuery: business.query.bind(business),
      asOf: timestamp(options.asOf),
      channelIds: options.channelIds.length > 0 ? options.channelIds : null,
    });
    await Promise.all([crawler.query("COMMIT"), business.query("COMMIT")]);
    crawlerBegun = false;
    businessBegun = false;
    const output = options.format === "jsonl"
      ? renderJsonl(report)
      : JSON.stringify(report, null, 2) + "\n";
    if (options.output) await writeFile(options.output, output, { encoding: "utf8", flag: "wx" });
    else process.stdout.write(output);
    if (options.failOnNotReady && report.summary.not_ready > 0) process.exitCode = 2;
  } catch (error) {
    await Promise.all([rollback(crawler, crawlerBegun), rollback(business, businessBegun)]);
    throw error;
  } finally {
    await Promise.all([crawler.end().catch(() => {}), business.end().catch(() => {})]);
  }
}

try {
  const options = parseArgs(process.argv.slice(2));
  if (options.help) process.stdout.write(usage());
  else await run(options);
} catch (error) {
  process.stderr.write(`${error?.stack ?? error}\n`);
  process.exitCode = 1;
}
