#!/usr/bin/env node

import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  PublicationRuntimeRoleAdministrator,
  publicationRuntimeRoleConfig,
  publicationRuntimeRoleConfirmation,
  publicationRuntimeRoleSummary,
} from "../src/publicationRuntimeRoleAdmin.js";

const { Client } = pg;

function usage() {
  return `Usage:
  node scripts/managePublicationRuntimeRoles.mjs [--apply]

Without --apply this command performs read-only dual-database preflights and
prints the exact confirmation string. It never prints database URLs or passwords.

Required environment:
  CRAWLER_ADMIN_DATABASE_URL or CRAWLER_ADMIN_DATABASE_URL_FILE
  BUSINESS_ADMIN_DATABASE_URL or BUSINESS_ADMIN_DATABASE_URL_FILE
  PUBLICATION_CRAWLER_PUBLISHER_DATABASE_URL or its _FILE form
  PUBLICATION_BUSINESS_INGRESS_DATABASE_URL or its _FILE form
  PUBLICATION_BUSINESS_RECONCILER_DATABASE_URL or its _FILE form
  EXPECTED_CRAWLER_DATABASE
  EXPECTED_BUSINESS_DATABASE
  EXPECTED_CRAWLER_CHANNEL_COUNT
  EXPECTED_BUSINESS_CHANNEL_COUNT

Apply confirmation:
  CONFIRM_PUBLICATION_RUNTIME_ROLES
`;
}

export function publicationRuntimeRoleCommand(argv = process.argv.slice(2)) {
  if (argv.includes("--help") || argv.includes("-h")) return { help: true, apply: false };
  const unknown = argv.filter((value) => value !== "--apply");
  if (unknown.length > 0) throw new TypeError(`unknown option: ${unknown[0]}`);
  if (argv.filter((value) => value === "--apply").length > 1) {
    throw new TypeError("--apply may only be provided once");
  }
  return { help: false, apply: argv.includes("--apply") };
}

async function main({ environment = process.env, argv = process.argv.slice(2) } = {}) {
  const command = publicationRuntimeRoleCommand(argv);
  if (command.help) {
    process.stdout.write(usage());
    return;
  }
  const config = publicationRuntimeRoleConfig(environment);
  const confirmation = publicationRuntimeRoleConfirmation(config);
  if (
    command.apply
    && String(environment.CONFIRM_PUBLICATION_RUNTIME_ROLES ?? "").trim() !== confirmation
  ) {
    throw new Error(
      "CONFIRM_PUBLICATION_RUNTIME_ROLES must exactly equal the value emitted by the plan command",
    );
  }
  const crawler = new Client({
    connectionString: config.crawlerAdminDatabaseUrl,
    application_name: "publication-runtime-role-admin-crawler",
    options: "-c timezone=UTC",
  });
  const business = new Client({
    connectionString: config.businessAdminDatabaseUrl,
    application_name: "publication-runtime-role-admin-business",
    options: "-c timezone=UTC",
  });
  try {
    await Promise.all([crawler.connect(), business.connect()]);
    const administrator = new PublicationRuntimeRoleAdministrator({
      crawlerClient: crawler,
      businessClient: business,
      config,
    });
    if (!command.apply) {
      const state = await administrator.inspectReadOnly();
      process.stdout.write(`${JSON.stringify({
        ok: true,
        mode: "plan",
        writes_performed: false,
        required_confirmation_environment: "CONFIRM_PUBLICATION_RUNTIME_ROLES",
        required_confirmation: confirmation,
        state: publicationRuntimeRoleSummary(state),
      }, null, 2)}\n`);
      return;
    }
    const state = await administrator.apply();
    process.stdout.write(`${JSON.stringify({
      ok: true,
      mode: "apply",
      writes_performed: true,
      state: publicationRuntimeRoleSummary(state),
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
