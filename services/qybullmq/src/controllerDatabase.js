import pg from 'pg';
import { databaseUrl } from './databaseConnection.js';
import { PUBLICATION_WRITER_VERSION } from './publicationWriterVersion.js';
import { ensureSchema } from './db.js';

export function createControllerDatabase(name, { statementTimeoutMs = 5000, max = 1 } = {}) {
  const pool = new pg.Pool({
    connectionString: databaseUrl(), max,
    application_name: `controller-${name}`,
    options: `-c timezone=UTC -c publication.writer_version=${PUBLICATION_WRITER_VERSION}`,
    connectionTimeoutMillis: 5000, idleTimeoutMillis: 30000,
  });
  pool.on('error', error => console.error(JSON.stringify({ event: 'controller_database_error', name, error: error.message })));
  async function withTransaction(action) {
    await ensureSchema();
    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      await client.query("SELECT set_config('statement_timeout',$1,true),set_config('lock_timeout','1000',true)", [String(statementTimeoutMs)]);
      const result = await action(client);
      await client.query('COMMIT');
      return result;
    } catch (error) {
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally { client.release(); }
  }
  return {
    query: (sql, values) => withTransaction(client => client.query(sql, values)),
    withTransaction,
    close: () => pool.end(),
  };
}
