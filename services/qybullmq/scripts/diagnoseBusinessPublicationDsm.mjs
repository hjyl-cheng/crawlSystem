#!/usr/bin/env node

import { execFile } from "node:child_process";
import { randomInt, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import pg from "pg";
import { inspectBusinessPublication } from "../src/businessPublicationAuditQueries.js";

const { Client, Pool } = pg;
const execFileAsync = promisify(execFile);
const PROBE_TABLES = ["inc009_probe_left", "inc009_probe_right"];
const PROBE_SQL = `
  SELECT count(*)::int AS count
  FROM inc009_probe_left AS probe_left
  JOIN inc009_probe_right AS probe_right USING (id)
  WHERE length(probe_left.payload)>0
    AND length(probe_right.payload)>0`;

export const DSM_DIAGNOSTIC_HELP = `Business Publication DSM diagnostic

Modes:
  audit-plans  read-only EXPLAIN of all 11 Business Publication audit queries.
  reproduce    controlled Parallel Hash concurrency matrix in an isolated test database.

Usage:
  INC009_DSM_MODE=audit-plans \
  INC009_DSM_DATABASE_URL=<database-url> \
    node scripts/diagnoseBusinessPublicationDsm.mjs

  INC009_DSM_MODE=reproduce \
  INC009_DSM_DATABASE_URL=<test-database-url> \
  INC009_DSM_CONTAINER=<postgres-container> \
  INC009_DSM_EXPECTED_SHM_MIB=64 \
  INC009_DSM_PREPARE=1 \
    node scripts/diagnoseBusinessPublicationDsm.mjs

The calibrated expectation is: one parallel probe succeeds at 64 MiB, while
concurrency 2 and 4 produce at least one DSM failure; concurrency 4 with
transaction-local parallelism disabled succeeds. At 256 MiB all four parallel
probes succeed, showing that a larger /dev/shm moves the failure threshold.

reproduce refuses non-test databases. INC009_DSM_PREPARE=1 creates two unlogged
probe tables only when neither exists; it never truncates or replaces data.
`;

function textEnvironment(environment, name, fallback = "") {
  const value = String(environment[name] ?? fallback).trim();
  if (!value) throw new TypeError(`${name} is required`);
  return value;
}

function integerEnvironment(environment, name, fallback, { minimum, maximum }) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

function booleanEnvironment(environment, name, fallback = false) {
  const raw = String(environment[name] ?? "").trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes"].includes(raw)) return true;
  if (["0", "false", "no"].includes(raw)) return false;
  throw new TypeError(`${name} must be true or false`);
}

function enumEnvironment(environment, name, fallback, allowed) {
  const value = String(environment[name] ?? fallback).trim();
  if (!allowed.includes(value)) {
    throw new TypeError(`${name} must be one of: ${allowed.join(", ")}`);
  }
  return value;
}

function errorText(error) {
  return String(error?.message || error).slice(0, 2000);
}

function isDsmExhaustion(error) {
  const message = errorText(error);
  return /shared memory segment/i.test(message) && /No space left on device/i.test(message);
}

function visitPlan(plan, visitor) {
  visitor(plan);
  for (const child of plan.Plans ?? []) visitPlan(child, visitor);
}

function planSummary(explained) {
  const root = explained.rows[0]["QUERY PLAN"][0].Plan;
  const nodeTypes = [];
  const parallelNodes = [];
  let workersPlanned = 0;
  visitPlan(root, (node) => {
    const nodeType = node["Node Type"];
    nodeTypes.push(nodeType);
    if (nodeType === "Gather" || nodeType === "Gather Merge") {
      parallelNodes.push(nodeType);
    } else if (node["Parallel Aware"] === true) {
      parallelNodes.push(nodeType.startsWith("Parallel ") ? nodeType : `Parallel ${nodeType}`);
    }
    workersPlanned += Number(node["Workers Planned"] ?? 0);
  });
  return {
    node_types: nodeTypes,
    parallel_nodes: parallelNodes,
    workers_planned: workersPlanned,
    total_cost: root["Total Cost"],
  };
}

async function captureAuditStatements() {
  const statements = [];
  await inspectBusinessPublication({
    async query(sql, params) {
      const statement = String(sql);
      const key = /business-publication-auditor:([a-z_]+)/.exec(statement)?.[1];
      statements.push({ key, sql: statement, params });
      return { rows: [{ count: 0, oldest_at: null, samples: [] }] };
    },
  });
  return statements;
}

async function databaseRuntime(client) {
  const result = await client.query(
    `SELECT current_database() AS database_name,
            current_setting('server_version') AS server_version,
            current_setting('dynamic_shared_memory_type') AS dsm_type,
            current_setting('max_parallel_workers_per_gather')::int
              AS parallel_workers_per_gather,
            current_setting('max_parallel_workers')::int AS max_parallel_workers,
            current_setting('max_worker_processes')::int AS max_worker_processes,
            current_setting('work_mem') AS work_mem,
            current_setting('hash_mem_multiplier') AS hash_mem_multiplier,
            current_setting('debug_parallel_query') AS debug_parallel_query`,
  );
  return result.rows[0];
}

async function reportAuditPlans(environment) {
  const databaseUrl = textEnvironment(environment, "INC009_DSM_DATABASE_URL");
  const expectedDatabase = String(environment.INC009_DSM_EXPECTED_DATABASE ?? "").trim();
  const parallelWorkers = integerEnvironment(
    environment,
    "INC009_DSM_PLAN_PARALLEL_WORKERS",
    2,
    { minimum: 0, maximum: 16 },
  );
  const pool = new Pool({
    connectionString: databaseUrl,
    max: 1,
    application_name: "inc009-audit-plan-diagnostic",
  });
  const client = await pool.connect();
  try {
    await client.query("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY");
    await client.query(`SET LOCAL max_parallel_workers_per_gather = ${parallelWorkers}`);
    await client.query("SET LOCAL statement_timeout = '2min'");
    const runtime = await databaseRuntime(client);
    if (expectedDatabase && runtime.database_name !== expectedDatabase) {
      throw new Error(
        `refusing unexpected database ${runtime.database_name}; expected ${expectedDatabase}`,
      );
    }
    const statements = await captureAuditStatements();
    const queries = [];
    for (const statement of statements) {
      const explained = await client.query(
        `EXPLAIN (FORMAT JSON) ${statement.sql}`,
        statement.params,
      );
      queries.push({ query: statement.key, ...planSummary(explained) });
    }
    await client.query("ROLLBACK");
    const triggering = queries.filter((query) => query.parallel_nodes.length > 0);
    console.log(JSON.stringify({
      event: "inc009_business_publication_audit_plans",
      database: runtime.database_name,
      server_version: runtime.server_version,
      dynamic_shared_memory_type: runtime.dsm_type,
      parallel_workers_per_gather: parallelWorkers,
      query_count: queries.length,
      queries_triggering_parallel_query: triggering.map((query) => query.query),
      queries,
    }));
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
    await pool.end();
  }
}

async function inspectContainer(container) {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(container)) {
    throw new TypeError("INC009_DSM_CONTAINER contains invalid characters");
  }
  const format = "{{.State.Running}}|{{.HostConfig.ShmSize}}|{{.Config.Image}}";
  const { stdout } = await execFileAsync("docker", ["inspect", "--format", format, container]);
  const [running, shmBytes, image] = stdout.trim().split("|");
  if (running !== "true") throw new Error(`PostgreSQL container ${container} is not running`);
  return { container, image, shm_bytes: Number(shmBytes) };
}

async function ensureProbeFixture(client, { prepare, rows }) {
  const existing = await client.query(
    `SELECT to_regclass('public.inc009_probe_left') IS NOT NULL AS probe_left,
            to_regclass('public.inc009_probe_right') IS NOT NULL AS probe_right`,
  );
  const state = existing.rows[0];
  if (state.probe_left !== state.probe_right) {
    throw new Error("DSM probe fixture is incomplete; use a fresh dedicated test database");
  }
  if (!state.probe_left) {
    if (!prepare) {
      throw new Error("DSM probe fixture is missing; set INC009_DSM_PREPARE=1 to create it");
    }
    await client.query("BEGIN");
    try {
      for (const table of PROBE_TABLES) {
        await client.query(`CREATE UNLOGGED TABLE ${table} (id integer NOT NULL,payload text NOT NULL)`);
        await client.query(
          `INSERT INTO ${table} (id,payload)
           SELECT value,repeat(md5(value::text),4)
           FROM generate_series(1,$1::int) AS value`,
          [rows],
        );
        await client.query(`ANALYZE ${table}`);
      }
      await client.query("COMMIT");
    } catch (error) {
      await client.query("ROLLBACK").catch(() => {});
      throw error;
    }
  }

  const fixture = {};
  for (const table of PROBE_TABLES) {
    const result = await client.query(
      `SELECT count(*)::int AS rows,min(length(payload))::int AS minimum_payload_bytes,
              max(length(payload))::int AS maximum_payload_bytes
       FROM ${table}`,
    );
    fixture[table] = result.rows[0];
    if (
      result.rows[0].rows !== rows
      || result.rows[0].minimum_payload_bytes !== 128
      || result.rows[0].maximum_payload_bytes !== 128
    ) {
      throw new Error(`${table} does not match the calibrated ${rows}-row fixture`);
    }
  }
  return fixture;
}

async function explainProbe(client) {
  await client.query("BEGIN READ ONLY");
  try {
    await client.query("SET LOCAL max_parallel_workers_per_gather = 2");
    const explained = await client.query(`EXPLAIN (FORMAT JSON) ${PROBE_SQL}`);
    await client.query("ROLLBACK");
    const summary = planSummary(explained);
    if (!summary.parallel_nodes.includes("Gather") || !summary.parallel_nodes.includes("Parallel Hash")) {
      throw new Error(`probe plan is not Parallel Hash: ${summary.parallel_nodes.join(" -> ")}`);
    }
    return summary;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function runProbe({ databaseUrl, applicationName, lockKey, parallelWorkers }) {
  const client = new Client({ connectionString: databaseUrl, application_name: applicationName });
  const startedAt = performance.now();
  let connected = false;
  try {
    await client.connect();
    connected = true;
    await client.query("BEGIN TRANSACTION READ ONLY");
    await client.query(`SET LOCAL max_parallel_workers_per_gather = ${parallelWorkers}`);
    await client.query("SELECT pg_advisory_xact_lock_shared($1::bigint)", [lockKey]);
    const result = await client.query(PROBE_SQL);
    await client.query("COMMIT");
    return {
      status: "succeeded",
      row_count: result.rows[0].count,
      duration_ms: Math.round(performance.now() - startedAt),
      error_code: null,
      error: null,
    };
  } catch (error) {
    if (connected) await client.query("ROLLBACK").catch(() => {});
    return {
      status: "failed",
      row_count: null,
      duration_ms: Math.round(performance.now() - startedAt),
      error_code: error?.code ?? null,
      error: isDsmExhaustion(error) ? "dynamic_shared_memory_exhausted" : errorText(error),
    };
  } finally {
    await client.end().catch(() => {});
  }
}

async function waitForBlockedProbes(client, applicationName, concurrency) {
  const deadline = performance.now() + 15_000;
  while (performance.now() < deadline) {
    const result = await client.query(
      `SELECT count(*)::int AS count
       FROM pg_stat_activity
       WHERE application_name=$1 AND wait_event_type='Lock' AND wait_event='advisory'`,
      [applicationName],
    );
    if (result.rows[0].count === concurrency) return;
    await delay(25);
  }
  throw new Error(`only part of the ${concurrency}-query probe batch reached its start barrier`);
}

async function runProbeBatch({ databaseUrl, concurrency, parallelWorkers }) {
  const applicationName = `inc009-dsm-probe-${randomUUID()}`;
  const lockKey = randomInt(1, 2_147_483_647);
  const coordinator = new Client({
    connectionString: databaseUrl,
    application_name: "inc009-dsm-probe-coordinator",
  });
  await coordinator.connect();
  await coordinator.query("SELECT pg_advisory_lock($1::bigint)", [lockKey]);
  const startedAt = performance.now();
  const tasks = Array.from({ length: concurrency }, () => runProbe({
    databaseUrl,
    applicationName,
    lockKey,
    parallelWorkers,
  }));
  let barrierError = null;
  try {
    await waitForBlockedProbes(coordinator, applicationName, concurrency);
  } catch (error) {
    barrierError = error;
  } finally {
    await coordinator.query("SELECT pg_advisory_unlock($1::bigint)", [lockKey]).catch(() => {});
    await coordinator.end().catch(() => {});
  }
  const probes = await Promise.all(tasks);
  if (barrierError) throw barrierError;
  return {
    concurrency,
    parallel_workers_per_gather: parallelWorkers,
    wall_ms: Math.round(performance.now() - startedAt),
    succeeded: probes.filter((probe) => probe.status === "succeeded").length,
    failed: probes.filter((probe) => probe.status === "failed").length,
    shared_memory_errors: probes.filter((probe) => (
      probe.error === "dynamic_shared_memory_exhausted"
    )).length,
    probes,
  };
}

function assertConstrainedMatrix(matrix) {
  if (matrix.parallel_1.succeeded !== 1) {
    throw new Error("64 MiB baseline invalid: the single parallel probe must succeed");
  }
  for (const key of ["parallel_2", "parallel_4"]) {
    if (matrix[key].shared_memory_errors < 1) {
      throw new Error(`64 MiB baseline invalid: ${key} did not reproduce DSM exhaustion`);
    }
  }
  if (matrix.serial_4.succeeded !== 4 || matrix.serial_4.failed !== 0) {
    throw new Error("transaction-local serial plan did not protect all four probes");
  }
}

function assertExpandedMatrix(matrix) {
  for (const key of ["parallel_1", "parallel_2", "parallel_4", "serial_4"]) {
    if (matrix[key].failed !== 0) {
      throw new Error(`256 MiB capacity baseline invalid: ${key} did not fully succeed`);
    }
  }
}

async function reproduceDsm(environment) {
  const databaseUrl = textEnvironment(environment, "INC009_DSM_DATABASE_URL");
  const containerName = textEnvironment(environment, "INC009_DSM_CONTAINER");
  const expectedShmMib = integerEnvironment(
    environment,
    "INC009_DSM_EXPECTED_SHM_MIB",
    64,
    { minimum: 64, maximum: 256 },
  );
  if (![64, 256].includes(expectedShmMib)) {
    throw new TypeError("INC009_DSM_EXPECTED_SHM_MIB must be 64 or 256");
  }
  const expectation = enumEnvironment(
    environment,
    "INC009_DSM_EXPECTATION",
    expectedShmMib === 64 ? "constrained" : "expanded",
    ["constrained", "expanded"],
  );
  const expectedPostgresVersion = String(
    environment.INC009_DSM_EXPECTED_POSTGRES_VERSION ?? "18.4",
  ).trim();
  const rows = integerEnvironment(environment, "INC009_DSM_FIXTURE_ROWS", 1_000_000, {
    minimum: 100_000,
    maximum: 5_000_000,
  });
  const prepare = booleanEnvironment(environment, "INC009_DSM_PREPARE", false);
  const container = await inspectContainer(containerName);
  if (container.shm_bytes !== expectedShmMib * 1024 * 1024) {
    throw new Error(
      `${containerName} /dev/shm is ${container.shm_bytes} bytes; expected ${expectedShmMib} MiB`,
    );
  }

  const pool = new Pool({
    connectionString: databaseUrl,
    max: 1,
    application_name: "inc009-dsm-diagnostic",
  });
  const client = await pool.connect();
  let clientReleased = false;
  try {
    const runtime = await databaseRuntime(client);
    if (!/_test$/i.test(runtime.database_name)) {
      throw new Error(`reproduce refuses non-test database: ${runtime.database_name}`);
    }
    const expectedSettings = {
      server_version: expectedPostgresVersion,
      dsm_type: "posix",
      parallel_workers_per_gather: 2,
      max_parallel_workers: 8,
      max_worker_processes: 8,
      work_mem: "4MB",
      hash_mem_multiplier: "2",
      debug_parallel_query: "off",
    };
    for (const [field, expected] of Object.entries(expectedSettings)) {
      if (String(runtime[field]) !== String(expected)) {
        throw new Error(`PostgreSQL ${field}=${runtime[field]}; calibrated value is ${expected}`);
      }
    }
    const fixture = await ensureProbeFixture(client, { prepare, rows });
    const probePlan = await explainProbe(client);
    client.release();
    clientReleased = true;
    const matrix = {
      parallel_1: await runProbeBatch({ databaseUrl, concurrency: 1, parallelWorkers: 2 }),
      parallel_2: await runProbeBatch({ databaseUrl, concurrency: 2, parallelWorkers: 2 }),
      parallel_4: await runProbeBatch({ databaseUrl, concurrency: 4, parallelWorkers: 2 }),
      serial_4: await runProbeBatch({ databaseUrl, concurrency: 4, parallelWorkers: 0 }),
    };
    if (expectation === "constrained") assertConstrainedMatrix(matrix);
    else assertExpandedMatrix(matrix);
    console.log(JSON.stringify({
      event: "inc009_business_publication_dsm_reproduction",
      verdict: "passed",
      expectation,
      container,
      database: runtime.database_name,
      postgres: runtime,
      fixture,
      probe_plan: probePlan,
      matrix,
    }));
  } finally {
    if (!clientReleased) client.release();
    await pool.end();
  }
}

export async function main(environment = process.env) {
  if (process.argv.includes("--help") || process.argv.includes("-h")) {
    process.stdout.write(DSM_DIAGNOSTIC_HELP);
    return;
  }
  const mode = enumEnvironment(environment, "INC009_DSM_MODE", "", [
    "audit-plans",
    "reproduce",
  ]);
  if (mode === "audit-plans") await reportAuditPlans(environment);
  else await reproduceDsm(environment);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(JSON.stringify({
      event: "inc009_business_publication_dsm_diagnostic_failed",
      error: errorText(error),
    }));
    process.exitCode = 1;
  });
}
