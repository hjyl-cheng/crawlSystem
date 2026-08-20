#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";
import pg from "pg";
import { PostgresBusinessPublicationActivator } from "../src/businessPublicationActivator.js";
import {
  PublicationDeadLetterRecoveryAdministrator,
  assertPublicationDeadLetterRecoveryEvidence,
  planPublicationDeadLetterRecovery,
  publicationDeadLetterRecoveryConfirmation,
} from "../src/publicationDeadLetterRecovery.js";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  npm run publication:repair-dead-letters -- --revision-id <uuid> [--revision-id <uuid> ...] [--output <path>]
  npm run publication:repair-dead-letters -- --apply [--output <path>]

Without --apply, this command performs a read-only two-database audit and creates a
new recovery Stream plan for the explicitly listed dead-letter Revisions. The output
file is created exclusively and never overwritten.

Required environment:
  CRAWLER_DATABASE_URL (DATABASE_URL is accepted for the QY runtime container)
  BUSINESS_DATABASE_URL
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_BUSINESS_DATABASE
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Optional environment:
  PUBLICATION_DESTINATION (default: business)
  PUBLICATION_RECOVERY_DELIVERY_TIMEOUT_MS (default: 120000)
  PUBLICATION_RECOVERY_PROJECTION_TIMEOUT_MS (default: 120000)
  PUBLICATION_RECOVERY_POLL_MS (default: 1000)

Apply-only environment:
  PUBLICATION_DEAD_LETTER_RECOVERY_EVIDENCE_FILE
  CONFIRM_PUBLICATION_DEAD_LETTER_RECOVERY
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

function parseArgs(argv) {
  const options = { help: false, apply: false, output: null, revisionIds: [] };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--help" || value === "-h") options.help = true;
    else if (value === "--apply") options.apply = true;
    else if (value === "--revision-id") {
      options.revisionIds.push(requiredValue(argv, index++, value));
    }
    else if (value === "--output") {
      if (options.output) throw new TypeError("--output may only be supplied once");
      options.output = requiredValue(argv, index++, value);
    } else throw new TypeError(`unknown option: ${value}`);
  }
  return options;
}

function positiveInteger(environment, name, fallback, minimum) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new TypeError(`${name} must be an integer >= ${minimum}`);
  }
  return value;
}

function config(environment, { apply }) {
  const crawlerDatabaseUrl = environmentValue("CRAWLER_DATABASE_URL", {
    environment,
    required: false,
  }) || environmentValue("DATABASE_URL", { environment });
  const output = {
    crawlerDatabaseUrl,
    businessDatabaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    expectedCrawlerDatabase: environmentValue("EXPECTED_CRAWLER_DATABASE", { environment }),
    expectedBusinessDatabase: environmentValue("EXPECTED_BUSINESS_DATABASE", { environment }),
    destination: String(environment.PUBLICATION_DESTINATION ?? "business").trim(),
    actor: environmentValue("PUBLICATION_OPERATOR", { environment }),
    reason: environmentValue("PUBLICATION_ACTION_REASON", { environment }),
    deliveryTimeoutMs: positiveInteger(
      environment,
      "PUBLICATION_RECOVERY_DELIVERY_TIMEOUT_MS",
      120000,
      1000,
    ),
    projectionTimeoutMs: positiveInteger(
      environment,
      "PUBLICATION_RECOVERY_PROJECTION_TIMEOUT_MS",
      120000,
      1000,
    ),
    pollMs: positiveInteger(environment, "PUBLICATION_RECOVERY_POLL_MS", 1000, 50),
    evidenceFile: null,
  };
  if (!output.destination) throw new TypeError("PUBLICATION_DESTINATION cannot be empty");
  if (output.expectedCrawlerDatabase === output.expectedBusinessDatabase) {
    throw new TypeError("Crawler and Business database names must be different");
  }
  if (apply) {
    output.evidenceFile = environmentValue(
      "PUBLICATION_DEAD_LETTER_RECOVERY_EVIDENCE_FILE",
      { environment },
    );
  }
  return output;
}

async function emit(value, output) {
  const body = `${JSON.stringify(value, null, 2)}\n`;
  if (output) await writeFile(output, body, { encoding: "utf8", flag: "wx" });
  else process.stdout.write(body);
}

async function loadEvidence(path) {
  const parsed = JSON.parse(await readFile(path, "utf8"));
  return assertPublicationDeadLetterRecoveryEvidence(parsed.evidence ?? parsed);
}

async function assertDatabaseIdentity(pool, expected, label) {
  const result = await pool.query("SELECT current_database() AS database_name");
  const actual = String(result.rows[0].database_name);
  if (actual !== expected) throw new Error(`${label} database mismatch: expected ${expected}, got ${actual}`);
  return actual;
}

async function main(environment = process.env, argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    process.stdout.write(usage());
    return;
  }
  if (!options.apply && options.revisionIds.length === 0) {
    throw new TypeError("at least one --revision-id is required when creating a recovery plan");
  }
  if (options.apply && options.revisionIds.length > 0) {
    throw new TypeError("--revision-id cannot be combined with --apply; apply uses the evidence file");
  }
  const settings = config(environment, options);
  const crawlerPool = new Pool({
    connectionString: settings.crawlerDatabaseUrl,
    application_name: "publication-dead-letter-recovery-source-admin",
    max: 4,
    options: "-c timezone=UTC",
  });
  const businessPool = new Pool({
    connectionString: settings.businessDatabaseUrl,
    application_name: "publication-dead-letter-recovery-business-admin",
    max: 4,
    options: "-c timezone=UTC",
  });
  try {
    const [crawlerDatabase, businessDatabase] = await Promise.all([
      assertDatabaseIdentity(crawlerPool, settings.expectedCrawlerDatabase, "Crawler"),
      assertDatabaseIdentity(businessPool, settings.expectedBusinessDatabase, "Business"),
    ]);
    if (!options.apply) {
      const evidence = await planPublicationDeadLetterRecovery({
        crawlerPool,
        businessPool,
        destination: settings.destination,
        revisionIds: options.revisionIds,
      });
      if (evidence.databases.crawler !== crawlerDatabase
          || evidence.databases.business !== businessDatabase) {
        throw new Error("Recovery evidence database identity changed during inspection");
      }
      await emit({
        ok: true,
        mode: "plan",
        committed_writes: false,
        required_confirmation_environment: "CONFIRM_PUBLICATION_DEAD_LETTER_RECOVERY",
        required_confirmation: publicationDeadLetterRecoveryConfirmation(evidence),
        summary: evidence.summary,
        evidence,
      }, options.output);
      return;
    }
    const evidence = await loadEvidence(settings.evidenceFile);
    if (evidence.databases.crawler !== crawlerDatabase
        || evidence.databases.business !== businessDatabase) {
      throw new Error("Recovery evidence targets different databases");
    }
    const expectedConfirmation = publicationDeadLetterRecoveryConfirmation(evidence);
    if (String(environment.CONFIRM_PUBLICATION_DEAD_LETTER_RECOVERY ?? "").trim()
        !== expectedConfirmation) {
      throw new Error(
        "CONFIRM_PUBLICATION_DEAD_LETTER_RECOVERY must exactly match the plan confirmation",
      );
    }
    const activator = new PostgresBusinessPublicationActivator(businessPool, {
      actor: settings.actor,
      reason: settings.reason,
    });
    const administrator = new PublicationDeadLetterRecoveryAdministrator({
      crawlerPool,
      businessPool,
      activator,
      evidence,
      actor: settings.actor,
      reason: settings.reason,
      deliveryTimeoutMs: settings.deliveryTimeoutMs,
      projectionTimeoutMs: settings.projectionTimeoutMs,
      pollMs: settings.pollMs,
    });
    const result = await administrator.apply();
    await emit({
      ok: true,
      mode: "apply",
      committed_writes: true,
      evidence_hash: evidence.evidence_hash,
      result,
    }, options.output);
  } finally {
    await Promise.all([
      crawlerPool.end().catch(() => {}),
      businessPool.end().catch(() => {}),
    ]);
  }
}

main().catch((error) => {
  process.stderr.write(`${error?.stack ?? error}\n`);
  process.exitCode = 1;
});
