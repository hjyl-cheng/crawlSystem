export const ISOLATED_DATABASE = 'remote_node_ingestion_test';

export async function assertIsolatedRemoteDatabase(pool) {
  const { rows } = await pool.query('SELECT current_database() AS name');
  if (rows[0]?.name !== ISOLATED_DATABASE) {
    throw new Error(`remote-node isolation requires database ${ISOLATED_DATABASE}`);
  }
}
