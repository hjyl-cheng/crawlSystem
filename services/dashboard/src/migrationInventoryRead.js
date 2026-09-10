export async function readMigrationInventory(pool, sql, params = []) {
  const client = await pool.connect();
  let discard;
  try {
    await client.query("BEGIN READ ONLY");
    // Parallel hash joins over the full inventory can exhaust PostgreSQL's
    // container /dev/shm. Scope this setting to this read, including PgBouncer.
    await client.query("SET LOCAL max_parallel_workers_per_gather=0");
    await client.query("SET LOCAL statement_timeout='20s'");
    const result = await client.query(sql, params);
    await client.query("COMMIT");
    return result;
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (rollbackError) {
      discard = rollbackError;
    }
    throw error;
  } finally {
    client.release(discard);
  }
}
