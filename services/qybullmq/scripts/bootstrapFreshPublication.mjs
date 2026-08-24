#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  FreshPublicationBootstrapAdministrator,
  freshPublicationBootstrapConfirmation,
  freshPublicationBootstrapConfig,
  freshPublicationBootstrapSummary,
  validateFreshPublicationBootstrapState,
} from "../src/freshPublicationBootstrap.js";

const { Client } = pg;

function usage() {
  return `Usage:
  node scripts/bootstrapFreshPublication.mjs
  node scripts/bootstrapFreshPublication.mjs --apply

The default command opens read-only preflight transactions against the fresh Crawler
and Business databases and prints the exact confirmation string. It performs no writes.

Required environment:
  CRAWLER_ADMIN_DATABASE_URL or CRAWLER_ADMIN_DATABASE_URL_FILE
  BUSINESS_ADMIN_DATABASE_URL or BUSINESS_ADMIN_DATABASE_URL_FILE
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_CRAWLER_CHANNEL_COUNT=0
  EXPECTED_BUSINESS_CHANNEL_COUNT=0
  PUBLICATION_STREAM_ID
  PUBLICATION_SOURCE_DEPLOYMENT_KEY
  PUBLICATION_SOURCE_IDENTITY_JSON or PUBLICATION_SOURCE_IDENTITY_JSON_FILE
  PUBLICATION_DESTINATION
  PUBLICATION_BOOTSTRAP_PROJECTION_MODE
  PUBLICATION_OPERATOR
  PUBLICATION_ACTION_REASON
  PUBLICATION_WRITER_DEPLOYMENT_REF
  PUBLICATION_RUNTIME_DEPLOYMENT_REF

Apply also requires CONFIRM_FRESH_PUBLICATION_BOOTSTRAP to exactly match the plan.
Business is committed first and Crawler second in separate idempotent transactions.
`;
}

export function freshPublicationBootstrapCommand(argv = process.argv.slice(2)) {
  if (argv.includes("--help") || argv.includes("-h")) return { apply: false, help: true };
  const unknown = argv.filter((value) => value !== "--apply");
  if (unknown.length > 0) throw new TypeError(`unknown option: ${unknown[0]}`);
  if (argv.filter((value) => value === "--apply").length > 1) {
    throw new TypeError("--apply may only be provided once");
  }
  return { apply: argv.includes("--apply"), help: false };
}

async function main({ environment = process.env, argv = process.argv.slice(2) } = {}) {
  const command = freshPublicationBootstrapCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = freshPublicationBootstrapConfig(environment);
  const confirmation = freshPublicationBootstrapConfirmation(config);
  if (
    command.apply
    && String(environment.CONFIRM_FRESH_PUBLICATION_BOOTSTRAP ?? "").trim() !== confirmation
  ) {
    throw new Error(
      "CONFIRM_FRESH_PUBLICATION_BOOTSTRAP must exactly equal the value emitted by the plan command",
    );
  }

  const crawler = new Client({
    connectionString: config.crawlerAdminDatabaseUrl,
    application_name: "fresh-publication-bootstrap-crawler-admin",
    options: "-c timezone=UTC",
  });
  const business = new Client({
    connectionString: config.businessAdminDatabaseUrl,
    application_name: "fresh-publication-bootstrap-business-admin",
    options: "-c timezone=UTC",
  });
  try {
    await Promise.all([crawler.connect(), business.connect()]);
    const administrator = new FreshPublicationBootstrapAdministrator({
      crawlerClient: crawler,
      businessClient: business,
      config,
    });
    if (!command.apply) {
      const state = await administrator.inspectReadOnly();
      const phase = validateFreshPublicationBootstrapState(state, config);
      process.stdout.write(`${JSON.stringify({
        ok: true,
        mode: "plan",
        writes_performed: false,
        phase,
        required_confirmation_environment: "CONFIRM_FRESH_PUBLICATION_BOOTSTRAP",
        required_confirmation: confirmation,
        state: freshPublicationBootstrapSummary(state, config),
      }, null, 2)}\n`);
      return;
    }
    const result = await administrator.initialize();
    process.stdout.write(`${JSON.stringify({
      ok: true,
      mode: "apply",
      writes_performed: {
        business_stream_inserts: result.business,
        crawler_stream_inserts: result.source,
      },
      state: freshPublicationBootstrapSummary(result.state, config),
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
