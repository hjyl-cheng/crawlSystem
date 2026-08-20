import pg from "pg";
import { environmentValue } from "./runtimeEnvironment.js";

const { Pool } = pg;

const databaseUrl = environmentValue("FEATURE_DATABASE_URL");

export const pool = new Pool({
  connectionString: databaseUrl,
  options: "-c timezone=UTC",
  max: Math.max(1, Number(process.env.FEATURE_POSTGRES_POOL_MAX || 4)),
  min: 0,
  idleTimeoutMillis: Math.max(1000, Number(process.env.FEATURE_POSTGRES_IDLE_TIMEOUT_MS || 300000)),
  connectionTimeoutMillis: Math.max(1000, Number(process.env.FEATURE_POSTGRES_CONNECTION_TIMEOUT_MS || 10000)),
  keepAlive: true,
});

pool.on("error", (error) => {
  console.error(JSON.stringify({ event: "feature_postgres_pool_error", error: error?.message || String(error) }));
});

export async function query(text, params = []) {
  return pool.query(text, params);
}

export async function withTransaction(action) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    const result = await action(client);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  } finally {
    client.release();
  }
}

export async function closeDb() {
  await pool.end();
}
