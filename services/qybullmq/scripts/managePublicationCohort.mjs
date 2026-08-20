#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  PublicationCohortAdministrator,
  assertPublicationDeliveryReleaseState,
  assertPublicationCohortState,
  buildPublicationCohort,
  publicationCohortConfirmation,
  publicationCohortRuntimeConfig,
  publicationCohortSummary,
  publicationDeliveryReleaseSummary,
  readPublicationChannelSet,
  readPublicationReadinessEvidence,
} from "../src/publicationCohortAdmin.js";

const { Client } = pg;

function usage() {
  return `Usage:
  node scripts/managePublicationCohort.mjs init [--apply]
  node scripts/managePublicationCohort.mjs enable-capture [--apply]
  node scripts/managePublicationCohort.mjs release-delivery [--apply]

Without --apply the command performs both database preflights and prints the exact
confirmation string. No rows are changed.

Common environment:
  CRAWLER_DATABASE_URL or CRAWLER_DATABASE_URL_FILE
  BUSINESS_DATABASE_URL or BUSINESS_DATABASE_URL_FILE
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_CRAWLER_CHANNEL_COUNT
  EXPECTED_BUSINESS_CHANNEL_COUNT
  PUBLICATION_STREAM_ID
  PUBLICATION_SOURCE_DEPLOYMENT_KEY
  PUBLICATION_SOURCE_IDENTITY_JSON or PUBLICATION_SOURCE_IDENTITY_JSON_FILE
  PUBLICATION_COHORT_KEY
  PUBLICATION_CHANNEL_IDS_FILE
  PUBLICATION_DESTINATION
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON

Apply confirmation:
  CONFIRM_PUBLICATION_COHORT_INIT       Required by init --apply
  CONFIRM_PUBLICATION_CAPTURE_ENABLE    Required by enable-capture --apply
  CONFIRM_PUBLICATION_DELIVERY_RELEASE  Required by release-delivery --apply

Capture activation also requires:
  PUBLICATION_WRITER_DEPLOYMENT_REF     Immutable sha256 image digest

Delivery release also requires:
  PUBLICATION_RUNTIME_DEPLOYMENT_REF    Immutable sha256 image digest
  PUBLICATION_READINESS_REPORT_FILE     Exact passing Readiness JSON evidence
`;
}

export function publicationCohortCommand(argv = process.argv.slice(2)) {
  if (argv.includes("--help") || argv.includes("-h")) return { help: true };
  const command = argv[0];
  if (!new Set(["init", "enable-capture", "release-delivery"]).has(command)) {
    throw new TypeError("the command must be init, enable-capture, or release-delivery");
  }
  const options = argv.slice(1);
  const unknown = options.filter((value) => value !== "--apply");
  if (unknown.length > 0) throw new TypeError(`unknown option: ${unknown[0]}`);
  if (options.filter((value) => value === "--apply").length > 1) {
    throw new TypeError("--apply may only be provided once");
  }
  return { command, apply: options.includes("--apply"), help: false };
}

function captureAlreadyEnabled(state, streamId) {
  return state.source.streams.some(
    (row) => String(row.publication_stream_id).toLowerCase() === streamId
      && row.capture_enabled_at != null,
  );
}

async function main({ environment = process.env, argv = process.argv.slice(2) } = {}) {
  const command = publicationCohortCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = publicationCohortRuntimeConfig(environment, { command: command.command });
  const channelIds = await readPublicationChannelSet(config.channelIdsFile);
  const cohort = buildPublicationCohort(config, channelIds);
  const evidence = command.command === "release-delivery"
    ? await readPublicationReadinessEvidence(config.readinessReportFile, cohort)
    : null;
  const confirmation = publicationCohortConfirmation(command.command, config, cohort, evidence);
  const confirmationNames = {
    init: "CONFIRM_PUBLICATION_COHORT_INIT",
    "enable-capture": "CONFIRM_PUBLICATION_CAPTURE_ENABLE",
    "release-delivery": "CONFIRM_PUBLICATION_DELIVERY_RELEASE",
  };
  const confirmationName = confirmationNames[command.command];
  if (command.apply && String(environment[confirmationName] ?? "").trim() !== confirmation) {
    throw new Error(`${confirmationName} must exactly equal the value emitted by the plan command`);
  }

  const crawler = new Client({
    connectionString: config.crawlerDatabaseUrl,
    application_name: `publication-cohort-admin-${command.command}-crawler`,
    options: "-c timezone=UTC",
  });
  const business = new Client({
    connectionString: config.businessDatabaseUrl,
    application_name: `publication-cohort-admin-${command.command}-business`,
    options: "-c timezone=UTC",
  });
  try {
    await Promise.all([crawler.connect(), business.connect()]);
    const administrator = new PublicationCohortAdministrator({
      crawlerClient: crawler,
      businessClient: business,
      config,
      cohort,
    });
    const before = await administrator.inspectReadOnly();
    if (command.command === "release-delivery") {
      assertPublicationDeliveryReleaseState(before, config, cohort, evidence);
    } else {
      assertPublicationCohortState(before, config, cohort, command.command === "init"
        ? { complete: false, capture: "disabled", requireEmpty: true }
        : {
            complete: true,
            capture: "enableable",
            requireEmpty: !captureAlreadyEnabled(before, config.streamId),
          });
    }

    if (!command.apply) {
      process.stdout.write(`${JSON.stringify({
        ok: true,
        mode: "plan",
        command: command.command,
        writes_performed: false,
        required_confirmation_environment: confirmationName,
        required_confirmation: confirmation,
        state: command.command === "release-delivery"
          ? publicationDeliveryReleaseSummary(before, config, cohort, evidence)
          : publicationCohortSummary(before, config, cohort),
      }, null, 2)}\n`);
      return;
    }

    let result;
    if (command.command === "init") result = await administrator.initialize();
    else if (command.command === "enable-capture") result = await administrator.enableCapture();
    else result = await administrator.releaseDelivery(evidence);
    let writesPerformed;
    if (command.command === "init") {
      writesPerformed = { crawler: result.source, business: result.business };
    } else if (command.command === "enable-capture") {
      writesPerformed = { crawler_stream_updates: result.updated };
    } else {
      writesPerformed = {
        crawler_delivery_updates: result.delivery,
        crawler_outbox_updates: result.outbox,
      };
    }
    process.stdout.write(`${JSON.stringify({
      ok: true,
      mode: "apply",
      command: command.command,
      writes_performed: writesPerformed,
      state: command.command === "release-delivery"
        ? publicationDeliveryReleaseSummary(result.state, config, cohort, evidence)
        : publicationCohortSummary(result.state, config, cohort),
    }, null, 2)}\n`);
  } finally {
    await Promise.all([crawler.end().catch(() => {}), business.end().catch(() => {})]);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${error?.stack ?? error}\n`);
    process.exitCode = 1;
  });
}
