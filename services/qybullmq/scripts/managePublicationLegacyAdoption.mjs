#!/usr/bin/env node

import { writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import pg from "pg";
import { readPublicationChannelSet } from "../src/publicationCohortAdmin.js";
import {
  PublicationLegacyAdoptionAdministrator,
  PublicationLegacyAdoptionPartialFailure,
  buildPublicationLegacyAdoptionTarget,
  publicationLegacyAdoptionConfig,
  publicationLegacyAdoptionConfirmation,
  readPublicationLegacyAdoptionEvidence,
} from "../src/publicationLegacyAdoption.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";

const { Pool } = pg;

function usage() {
  return `Usage:
  node scripts/managePublicationLegacyAdoption.mjs [--apply] [--output <path>]

Without --apply this command performs a read-only dual-database preflight and
emits immutable adoption evidence. It never fetches YouTube or runs a Crawl.
The output file is created exclusively and is never overwritten.

Required environment:
  CRAWLER_DATABASE_URL or CRAWLER_DATABASE_URL_FILE
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_CRAWLER_CHANNEL_COUNT
  EXPECTED_BUSINESS_CHANNEL_COUNT
  PUBLICATION_STREAM_ID
  PUBLICATION_DESTINATION
  PUBLICATION_LEGACY_ADOPTION_KEY
  PUBLICATION_LEGACY_ADOPTION_AS_OF
  PUBLICATION_CHANNEL_IDS_FILE
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Optional environment:
  PUBLICATION_LEGACY_ADOPTION_CONCURRENCY (default: 6, maximum: 16)

Apply-only environment:
  PUBLICATION_LEGACY_ADOPTION_EVIDENCE_FILE
  CONFIRM_PUBLICATION_LEGACY_ADOPTION
`;
}

function requiredValue(argv, index, option) {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) throw new TypeError(`${option} requires a value`);
  return value;
}

export function publicationLegacyAdoptionCommand(argv = process.argv.slice(2)) {
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
  const command = publicationLegacyAdoptionCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = publicationLegacyAdoptionConfig(environment, { apply: command.apply });
  const channelIds = await readPublicationChannelSet(config.channelIdsFile);
  const target = buildPublicationLegacyAdoptionTarget(config, channelIds);
  const evidence = command.apply
    ? await readPublicationLegacyAdoptionEvidence(config.evidenceFile, config, target)
    : null;
  const confirmation = evidence
    ? publicationLegacyAdoptionConfirmation(config, target, evidence)
    : null;
  if (command.apply
      && String(environment.CONFIRM_PUBLICATION_LEGACY_ADOPTION ?? "").trim() !== confirmation) {
    throw new Error(
      "CONFIRM_PUBLICATION_LEGACY_ADOPTION must exactly equal the value emitted by the plan command",
    );
  }

  const crawlerPool = new Pool({
    connectionString: config.crawlerDatabaseUrl,
    application_name: "publication-legacy-adoption-crawler",
    max: config.concurrency + 2,
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  const businessPool = new Pool({
    connectionString: config.businessDatabaseUrl,
    application_name: "publication-legacy-adoption-business",
    max: 3,
    options: "-c timezone=UTC",
  });
  try {
    const administrator = new PublicationLegacyAdoptionAdministrator({
      crawlerPool,
      businessPool,
      config,
      target,
      evidence,
    });
    if (!command.apply) {
      const plan = await administrator.inspectReadOnly();
      const requiredConfirmation = publicationLegacyAdoptionConfirmation(
        config,
        target,
        plan.evidence,
      );
      await emit({
        ok: true,
        mode: "plan",
        writes_performed: false,
        crawler_refetch_performed: false,
        required_confirmation_environment: "CONFIRM_PUBLICATION_LEGACY_ADOPTION",
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
      writes_performed: true,
      crawler_refetch_performed: false,
      evidence_hash: evidence.evidence_hash,
      result,
    }, command.output);
  } catch (error) {
    if (error instanceof PublicationLegacyAdoptionPartialFailure) {
      await emit({
        ok: false,
        mode: "apply",
        writes_performed: "partial",
        crawler_refetch_performed: false,
        error: error.message,
        result: error.details,
      }, command.output);
      process.exitCode = 2;
      return;
    }
    throw error;
  } finally {
    await Promise.all([crawlerPool.end().catch(() => {}), businessPool.end().catch(() => {})]);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
