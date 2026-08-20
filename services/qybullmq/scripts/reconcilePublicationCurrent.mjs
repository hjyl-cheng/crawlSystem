#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { readPublicationChannelSet } from "../src/publicationCohortAdmin.js";
import {
  PublicationCurrentReconciliationAdministrator,
  PublicationCurrentReconciliationPartialFailure,
  buildPublicationCurrentReconciliationTarget,
  publicationCurrentReconciliationConfig,
  publicationCurrentReconciliationConfirmation,
  publicationCurrentReconciliationSummary,
  readPublicationCurrentReconciliationEvidence,
} from "../src/publicationCurrentReconciliation.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  node scripts/reconcilePublicationCurrent.mjs [--apply] [--output <path>]

Without --apply, the command runs the real Reconciler inside rolled-back transactions.
It never fetches YouTube and commits no database changes. The output file is created
exclusively and is never overwritten.

Required environment:
  CRAWLER_DATABASE_URL or CRAWLER_DATABASE_URL_FILE
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_CRAWLER_CHANNEL_COUNT
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_BUSINESS_CHANNEL_COUNT
  PUBLICATION_STREAM_ID
  PUBLICATION_DESTINATION
  PUBLICATION_CURRENT_RECONCILE_KEY
  PUBLICATION_CURRENT_RECONCILE_AS_OF
  PUBLICATION_CHANNEL_IDS_FILE
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Optional environment:
  PUBLICATION_RECONCILE_DOMAINS (default: channel)
  PUBLICATION_CURRENT_RECONCILE_CONCURRENCY (default: 4, maximum: 8)

Apply-only environment:
  PUBLICATION_CURRENT_RECONCILE_EVIDENCE_FILE
  CONFIRM_PUBLICATION_CURRENT_RECONCILE
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

export function publicationCurrentReconciliationCommand(argv = process.argv.slice(2)) {
  const command = { help: false, apply: false, output: null };
  for (let index = 0; index < argv.length; index += 1) {
    const value = argv[index];
    if (value === "--help" || value === "-h") command.help = true;
    else if (value === "--apply") {
      if (command.apply) throw new TypeError("--apply may only be provided once");
      command.apply = true;
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
  const command = publicationCurrentReconciliationCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = publicationCurrentReconciliationConfig(environment, { apply: command.apply });
  const channelIds = await readPublicationChannelSet(config.channelIdsFile);
  const target = buildPublicationCurrentReconciliationTarget(config, channelIds);
  const evidence = command.apply
    ? await readPublicationCurrentReconciliationEvidence(config.evidenceFile, config, target)
    : null;
  const confirmation = evidence
    ? publicationCurrentReconciliationConfirmation(config, target, evidence)
    : null;
  if (command.apply
      && String(environment.CONFIRM_PUBLICATION_CURRENT_RECONCILE ?? "").trim() !== confirmation) {
    throw new Error(
      "CONFIRM_PUBLICATION_CURRENT_RECONCILE must exactly equal the value emitted by the plan command",
    );
  }

  const crawlerPool = new Pool({
    connectionString: config.crawlerDatabaseUrl,
    application_name: "publication-current-reconciliation-admin",
    max: config.concurrency + 1,
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const businessPool = new Pool({
    connectionString: config.businessDatabaseUrl,
    application_name: "publication-current-reconciliation-business-reader",
    max: 2,
    options: "-c timezone=UTC",
  });
  try {
    const administrator = new PublicationCurrentReconciliationAdministrator({
      crawlerPool,
      businessPool,
      config,
      target,
      evidence,
    });
    if (!command.apply) {
      const plannedEvidence = await administrator.inspectRollbackPreview();
      await emit({
        ok: true,
        mode: "plan",
        committed_writes: false,
        rollback_preview: true,
        crawler_refetch_performed: false,
        required_confirmation_environment: "CONFIRM_PUBLICATION_CURRENT_RECONCILE",
        required_confirmation: publicationCurrentReconciliationConfirmation(
          config,
          target,
          plannedEvidence,
        ),
        summary: publicationCurrentReconciliationSummary(plannedEvidence),
        evidence: plannedEvidence,
      }, command.output);
      return;
    }
    const result = await administrator.apply();
    await emit({
      ok: true,
      mode: "apply",
      committed_writes: true,
      crawler_refetch_performed: false,
      evidence_hash: evidence.evidence_hash,
      result,
    }, command.output);
  } catch (error) {
    if (error instanceof PublicationCurrentReconciliationPartialFailure) {
      await emit({
        ok: false,
        mode: "apply",
        committed_writes: "partial",
        crawler_refetch_performed: false,
        error: error.message,
        result: error.details,
      }, command.output);
      process.exitCode = 2;
      return;
    }
    throw error;
  } finally {
    await Promise.all([
      crawlerPool.end().catch(() => {}),
      businessPool.end().catch(() => {}),
    ]);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
