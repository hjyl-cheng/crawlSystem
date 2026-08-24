import { createServer as createHttpServer } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { fileURLToPath } from "node:url";
import pg from "pg";
import {
  PostgresBusinessPublicationStore,
  createBusinessPublicationIngressApp,
} from "./businessPublicationIngress.js";
import { verifyBusinessWriterDatabase } from "./databaseIdentity.js";
import { environmentValue } from "./runtimeEnvironment.js";

const { Pool } = pg;

function integerSetting(environment, name, fallback, { minimum, maximum }) {
  const raw = String(environment[name] ?? "").trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum) {
    throw new TypeError(`${name} must be an integer between ${minimum} and ${maximum}`);
  }
  return value;
}

export function businessPublicationIngressRuntimeConfig(environment = process.env) {
  const expectedDatabase = String(environment.EXPECTED_BUSINESS_DATABASE || "").trim();
  const host = String(environment.BUSINESS_PUBLICATION_INGRESS_HOST || "127.0.0.1").trim();
  if (!expectedDatabase) throw new TypeError("EXPECTED_BUSINESS_DATABASE is required");
  if (!host) throw new TypeError("BUSINESS_PUBLICATION_INGRESS_HOST is required");
  const tlsCertificate = environmentValue("BUSINESS_PUBLICATION_TLS_CERT", {
    environment,
    required: false,
  });
  const tlsPrivateKey = environmentValue("BUSINESS_PUBLICATION_TLS_KEY", {
    environment,
    required: false,
  });
  if (Boolean(tlsCertificate) !== Boolean(tlsPrivateKey)) {
    throw new TypeError("BUSINESS_PUBLICATION_TLS_CERT and BUSINESS_PUBLICATION_TLS_KEY must be set together");
  }
  return {
    databaseUrl: environmentValue("BUSINESS_DATABASE_URL", { environment }),
    token: environmentValue("BUSINESS_PUBLICATION_INGRESS_TOKEN", { environment }),
    expectedDatabase,
    tls: tlsCertificate ? { cert: tlsCertificate, key: tlsPrivateKey } : null,
    host,
    port: integerSetting(environment, "BUSINESS_PUBLICATION_INGRESS_PORT", 8081, {
      minimum: 1,
      maximum: 65535,
    }),
    poolMaximum: integerSetting(environment, "BUSINESS_INGRESS_POSTGRES_POOL_MAX", 10, {
      minimum: 1,
      maximum: 100,
    }),
    maximumBodyBytes: integerSetting(
      environment,
      "BUSINESS_PUBLICATION_MAX_BODY_BYTES",
      4 * 1024 * 1024,
      { minimum: 1024, maximum: 4 * 1024 * 1024 },
    ),
  };
}

function listen(server, host, port) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, host, () => {
      server.off("error", reject);
      resolve();
    });
  });
}

async function main() {
  const config = businessPublicationIngressRuntimeConfig();
  const pool = new Pool({
    connectionString: config.databaseUrl,
    max: config.poolMaximum,
  });
  let server = null;
  try {
    await verifyBusinessWriterDatabase(pool.query.bind(pool));
    const preflight = await pool.query(
      `SELECT current_database() AS database_name,
              to_regclass('publication.inbox') IS NOT NULL AS inbox_ready,
              to_regclass('publication.revision') IS NOT NULL AS revision_ready`,
    );
    const state = preflight.rows[0] ?? {};
    if (state.database_name !== config.expectedDatabase || !state.inbox_ready || !state.revision_ready) {
      throw new Error(`refusing to start on unexpected or unmigrated Business database: ${state.database_name}`);
    }
    const store = new PostgresBusinessPublicationStore(pool);
    const app = createBusinessPublicationIngressApp({
      store,
      token: config.token,
      maximumBodyBytes: config.maximumBodyBytes,
    });
    server = config.tls
      ? createHttpsServer(config.tls, app)
      : createHttpServer(app);
    await listen(server, config.host, config.port);
  } catch (error) {
    if (server?.listening) await new Promise((resolve) => server.close(resolve));
    await pool.end().catch(() => {});
    throw error;
  }
  console.log(JSON.stringify({
    event: "business_publication_ingress_ready",
    protocol: config.tls ? "https" : "http",
    host: config.host,
    port: config.port,
  }));

  let closing = false;
  const close = async () => {
    if (closing) return;
    closing = true;
    await new Promise((resolve) => server.close(resolve));
    await pool.end();
  };
  process.once("SIGINT", () => void close());
  process.once("SIGTERM", () => void close());
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(JSON.stringify({
      event: "business_publication_ingress_fatal",
      error: error?.stack || String(error),
    }));
    process.exitCode = 1;
  });
}
