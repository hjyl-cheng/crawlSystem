// Explicit release tool. Startup never applies migrations; rollback retains
// the additive schema and all receipts so an old incremental image can run.
import pg from 'pg';
import {applyFullCrawlReleaseSchema, assertFullCrawlReleaseSchema, fullCrawlRollbackReadiness} from '../src/remoteNodes/fullCrawlReleaseSchema.js';
const args = process.argv.slice(2);
if (args.length !== 1 || !['--check', '--apply', '--rollback-check'].includes(args[0])
  || !process.env.FULL_CRAWL_RELEASE_DATABASE_URL || !process.env.FULL_CRAWL_RELEASE_EXPECTED_DATABASE) {
  throw Error('Explicit release database URL, expected database and --check/--apply/--rollback-check required');
}
const pool = new pg.Pool({connectionString: process.env.FULL_CRAWL_RELEASE_DATABASE_URL,
  max: 1, connectionTimeoutMillis: 5000, application_name: 'full-crawl-release-schema',
  options: '-c statement_timeout=15000 -c lock_timeout=3000'});
try {
  const identity = (await pool.query('SELECT current_database() AS name')).rows[0];
  if (identity.name !== process.env.FULL_CRAWL_RELEASE_EXPECTED_DATABASE) throw Error('FULL_CRAWL_RELEASE_DATABASE_MISMATCH');
  if (args[0] === '--apply') console.log(JSON.stringify({applied: await applyFullCrawlReleaseSchema(pool)}));
  else {
    await assertFullCrawlReleaseSchema(pool.query.bind(pool));
    const result = args[0] === '--rollback-check' ? await fullCrawlRollbackReadiness(pool) : {schema_ready: true};
    console.log(JSON.stringify(result));
    if (result.ready === false) process.exitCode = 2;
  }
} finally { await pool.end(); }
