#!/usr/bin/env node

import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import pg from "pg";
import { databaseUrl } from "../src/databaseConnection.js";
import { verifyCrawlerWriterDatabase } from "../src/databaseIdentity.js";
import {
  MigrationRetryIntentConflictError,
  MigrationRetryIntentStore,
  PostgresMigrationRetryIntentRepository,
} from "../src/migrationRetryIntent.js";
import { PUBLICATION_WRITER_VERSION } from "../src/publicationWriterVersion.js";
import { environmentValue } from "../src/runtimeEnvironment.js";

const { Client } = pg;

export function migrationRetryIntentUsage() {
  return `Usage: npm run recover:migration-retry-intent -- [options]

Default mode verifies the Crawler database and prints a read-only recovery plan.
Execute creates the Recovery Intent and PostgreSQL Outbox boundary only; it never
retries an old BullMQ Job directly.

Required options:
  --candidate-id <id>                Failed Candidate ID
  --previous-business-run-id <id>    Terminal Business Run ID
  --request-key <key>                Stable operator request key
  --reason <text>                    Auditable recovery reason

Options:
  --min-subscriber-count <count>     Validation threshold (default: 1000)
  --execute                          Commit the controlled Recovery Intent
  --confirm <token>                  Exact confirmation emitted by plan mode
  --help                             Show this help
`;
}

function requiredText(value, name) {
  const normalized = String(value ?? "").trim();
  if (!normalized) throw new TypeError(`${name} is required`);
  return normalized;
}

function positiveInteger(value, name) {
  const parsed = Number(value);
  if (!Number.isSafeInteger(parsed) || parsed <= 0) {
    throw new TypeError(`${name} must be a positive integer`);
  }
  return parsed;
}

function confirmationHash(value) {
  return `sha256:${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

export function parseMigrationRetryIntentCommand(
  argv = process.argv.slice(2),
  environment = process.env,
) {
  const { values } = parseArgs({
    args: argv,
    options: {
      "candidate-id": { type: "string" },
      "previous-business-run-id": { type: "string" },
      "request-key": { type: "string" },
      reason: { type: "string" },
      "min-subscriber-count": { type: "string", default: "1000" },
      execute: { type: "boolean", default: false },
      confirm: { type: "string" },
      help: { type: "boolean", short: "h", default: false },
    },
    strict: true,
  });
  if (values.help) return { help: true, execute: false };

  const command = {
    help: false,
    execute: values.execute,
    expectedDatabase: requiredText(
      environment.EXPECTED_CRAWLER_DATABASE,
      "EXPECTED_CRAWLER_DATABASE",
    ),
    candidateId: positiveInteger(values["candidate-id"], "--candidate-id"),
    previousBusinessRunId: requiredText(
      values["previous-business-run-id"],
      "--previous-business-run-id",
    ),
    requestKey: requiredText(values["request-key"], "--request-key"),
    reason: requiredText(values.reason, "--reason"),
    minSubscriberCount: positiveInteger(
      values["min-subscriber-count"],
      "--min-subscriber-count",
    ),
    confirmation: String(values.confirm ?? "").trim() || null,
  };
  command.requiredConfirmation = `confirm-migration-retry:${command.candidateId}:${confirmationHash({
    version: "migration-retry-intent-cli-v1",
    expected_database: command.expectedDatabase,
    candidate_id: command.candidateId,
    previous_business_run_id: command.previousBusinessRunId,
    request_key: command.requestKey,
    reason: command.reason,
    min_subscriber_count: command.minSubscriberCount,
  })}`;
  if (command.execute && command.confirmation !== command.requiredConfirmation) {
    throw new TypeError(`--confirm must exactly match ${command.requiredConfirmation}`);
  }
  return command;
}

export function migrationRetryIntentPlan(command) {
  return {
    ok: true,
    mode: "plan",
    committed_writes: false,
    expected_database: command.expectedDatabase,
    candidate_id: command.candidateId,
    previous_business_run_id: command.previousBusinessRunId,
    request_key: command.requestKey,
    reason: command.reason,
    min_subscriber_count: command.minSubscriberCount,
    required_confirmation: command.requiredConfirmation,
  };
}

function existingIntentMatches(row, command) {
  return Number(row.retry_intent_candidate_id) === command.candidateId
    && row.retry_intent_previous_business_run_id === command.previousBusinessRunId
    && row.retry_intent_reason === command.reason
    && Number(row.retry_intent_payload?.min_subscriber_count) === command.minSubscriberCount;
}

export async function inspectMigrationRetryIntentTarget(query, command) {
  if (typeof query !== "function") throw new TypeError("query is required");
  const result = await query(
    `SELECT candidate.candidate_id,candidate.channel_id,
            candidate.status AS candidate_status,
            candidate.snapshot_dispatch_generation,
            binding.business_run_id AS previous_business_run_id,
            binding.status AS binding_status,
            binding.terminal_reason AS binding_terminal_reason,
            intent.retry_intent_id,
            intent.candidate_id AS retry_intent_candidate_id,
            intent.previous_business_run_id AS retry_intent_previous_business_run_id,
            intent.reason AS retry_intent_reason,
            intent.job_payload_json AS retry_intent_payload,
            intent.status AS retry_intent_status,
            outbox.dispatch_id AS outbox_dispatch_id,
            active.retry_intent_id AS active_retry_intent_id
     FROM crawler.channel_candidates candidate
     LEFT JOIN crawler.business_run_bindings binding
       ON binding.business_run_id=$2
      AND binding.candidate_id=candidate.candidate_id
      AND binding.channel_id=candidate.channel_id
     LEFT JOIN crawler.migration_retry_intents intent
       ON intent.request_key=$3
     LEFT JOIN crawler.proxy_job_dispatch_outbox outbox
       ON outbox.aggregate_kind='migration_retry'
      AND outbox.aggregate_id=intent.retry_intent_id
      AND outbox.intent_hash=intent.intent_hash
     LEFT JOIN LATERAL (
       SELECT active_intent.retry_intent_id
       FROM crawler.migration_retry_intents active_intent
       WHERE active_intent.candidate_id=candidate.candidate_id
         AND active_intent.status IN ('requested','dispatched','running')
       ORDER BY active_intent.requested_at,active_intent.retry_intent_id
       LIMIT 1
     ) active ON true
     WHERE candidate.candidate_id=$1`,
    [command.candidateId, command.previousBusinessRunId, command.requestKey],
  );
  const row = result.rows[0];
  if (!row) {
    return {
      candidate_id: command.candidateId,
      eligible: false,
      blockers: ["candidate_not_found"],
    };
  }

  const blockers = [];
  const hasExisting = Boolean(row.retry_intent_id);
  if (hasExisting) {
    if (!existingIntentMatches(row, command)) blockers.push("request_key_conflict");
    if (!row.outbox_dispatch_id) blockers.push("recovery_intent_outbox_missing");
    if (row.active_retry_intent_id
        && row.active_retry_intent_id !== row.retry_intent_id) {
      blockers.push("candidate_has_active_recovery_intent");
    }
  } else {
    if (row.candidate_status !== "failed") blockers.push("candidate_not_failed");
    if (row.binding_status !== "terminal") blockers.push("previous_binding_not_terminal");
    if (row.active_retry_intent_id) blockers.push("candidate_has_active_recovery_intent");
  }

  return {
    candidate_id: Number(row.candidate_id),
    channel_id: row.channel_id,
    candidate_status: row.candidate_status,
    snapshot_dispatch_generation: Number(row.snapshot_dispatch_generation ?? 0),
    previous_business_run_id: row.previous_business_run_id,
    binding_status: row.binding_status,
    binding_terminal_reason: row.binding_terminal_reason,
    existing_retry_intent_id: row.retry_intent_id ?? null,
    existing_retry_intent_status: row.retry_intent_status ?? null,
    outbox_present: Boolean(row.outbox_dispatch_id),
    active_retry_intent_id: row.active_retry_intent_id ?? null,
    eligible: blockers.length === 0,
    blockers,
  };
}

export async function runMigrationRetryIntentCommand(command, {
  query,
  environment = process.env,
  store = null,
} = {}) {
  const database = await verifyCrawlerWriterDatabase(query, environment);
  const target = await inspectMigrationRetryIntentTarget(query, command);
  const plan = {
    ...migrationRetryIntentPlan(command),
    ok: target.eligible,
    database,
    target,
  };
  if (!command.execute) return plan;
  if (!target.eligible) {
    throw new MigrationRetryIntentConflictError(
      command.requestKey,
      `Recovery target is blocked (${target.blockers.join(", ")})`,
    );
  }
  if (!store || typeof store.prepare !== "function") {
    throw new TypeError("Recovery Intent store is required for execute mode");
  }
  const prepared = await store.prepare({
    requestKey: command.requestKey,
    candidateId: command.candidateId,
    previousBusinessRunId: command.previousBusinessRunId,
    reason: command.reason,
    minSubscriberCount: command.minSubscriberCount,
  });
  return {
    ...plan,
    mode: "execute",
    committed_writes: prepared.created,
    created: prepared.created,
    retry_intent_id: prepared.intent.retry_intent_id,
    new_business_run_id: prepared.intent.new_business_run_id,
    new_business_run_key: prepared.intent.new_business_run_key,
    new_job_id: prepared.intent.new_job_id,
    dispatch_generation: Number(prepared.intent.dispatch_generation),
    outbox_dispatch_id: prepared.outbox.dispatch_id,
    outbox_status: prepared.outbox.status,
  };
}

function crawlerConnectionString(environment) {
  return environmentValue("CRAWLER_DATABASE_URL", {
    environment,
    required: false,
  }) || databaseUrl(environment);
}

async function withVerifiedTransaction(client, environment, action) {
  await client.query("BEGIN");
  try {
    await verifyCrawlerWriterDatabase(client.query.bind(client), environment);
    await client.query(
      "SELECT set_config('publication.writer_version',$1,true)",
      [PUBLICATION_WRITER_VERSION],
    );
    await client.query("SET LOCAL lock_timeout='10s'");
    await client.query("SET LOCAL statement_timeout='30s'");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

export async function main({
  argv = process.argv.slice(2),
  environment = process.env,
} = {}) {
  const command = parseMigrationRetryIntentCommand(argv, environment);
  if (command.help) {
    process.stdout.write(migrationRetryIntentUsage());
    return;
  }

  const client = new Client({
    connectionString: crawlerConnectionString(environment),
    application_name: "migration-retry-intent-cli-v1",
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
  });
  try {
    await client.connect();
    const query = client.query.bind(client);
    const store = command.execute
      ? new MigrationRetryIntentStore({
          repository: new PostgresMigrationRetryIntentRepository({
            withTransaction: (action) => withVerifiedTransaction(
              client,
              environment,
              action,
            ),
          }),
        })
      : null;
    const result = await runMigrationRetryIntentCommand(command, {
      query,
      environment,
      store,
    });
    process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } finally {
    await client.end().catch(() => {});
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    process.stderr.write(`${JSON.stringify({
      event: "migration_retry_intent_cli_failed",
      code: error?.code ?? null,
      error: error?.message ?? String(error),
    })}\n`);
    process.exitCode = 1;
  });
}
