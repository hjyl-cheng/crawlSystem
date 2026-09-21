// Development-only entry point: cannot target the production database or use
// DATABASE_URL defaults. No production schema or worker switch is changed.
import pg from 'pg';
import { readFile } from 'node:fs/promises';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';

const url = process.env.REMOTE_NODE_TEST_DATABASE_URL;
if (!url || process.argv.slice(2).some(arg => !['--apply','--with-business-fence'].includes(arg))) {
  throw new Error('Explicit REMOTE_NODE_TEST_DATABASE_URL and optional --apply / --with-business-fence required');
}
const pool = new pg.Pool({ connectionString: url, max: 1 });
try {
  await assertIsolatedRemoteDatabase(pool);
  const client = await pool.connect();
  try {
    if (process.argv.includes('--apply')) {
      await client.query('BEGIN');
      await client.query("SET LOCAL lock_timeout='3s'");
      await client.query("SET LOCAL statement_timeout='15s'");
      await client.query('SELECT pg_advisory_xact_lock(781137981)');
      for (const file of ['schema.sql','routeSchema.sql','workerConnectionSchema.sql','workerActivationSchema.sql','fullCrawlSchema.sql']) {
        await client.query(await readFile(new URL(`../src/remoteNodes/${file}`, import.meta.url), 'utf8'));
      }
      if (process.argv.includes('--with-business-fence')) {
        if (!(await client.query("SELECT to_regclass('crawler.channel_execution_attempts') AS name")).rows[0].name) {
          throw new Error('Install the crawler business schema in the isolated test database first');
        }
        await client.query(await readFile(new URL('../src/remoteNodes/fullCrawlBusinessSchema.sql',import.meta.url),'utf8'));
      }
      await client.query('COMMIT');
    }
    const { rows } = await client.query("SELECT tablename FROM pg_tables WHERE schemaname='remote_ingestion' AND tablename LIKE 'full_crawl_%' ORDER BY tablename");
    console.log(JSON.stringify({ database: 'remote_node_ingestion_test', applied: process.argv.includes('--apply'),
      businessFenceRequested:process.argv.includes('--with-business-fence'), tables: rows.map(row => row.tablename) }));
  } catch (error) {
    await client.query('ROLLBACK').catch(() => {});
    throw error;
  } finally { client.release(); }
} finally { await pool.end(); }
