import assert from 'node:assert/strict';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { databaseUrl } from '../src/databaseConnection.js';
import { verifyCrawlerWriterDatabase } from '../src/databaseIdentity.js';

assert(process.argv.includes('--apply'), 'Pass --apply for the additive scan cursor schema');
const expected = process.env.EXPECTED_CRAWLER_DATABASE;
assert(expected && process.env.CONFIRM_BACKGROUND_RECONCILIATION_SCHEMA_APPLY === expected, 'Confirm expected crawler database');
const client = new pg.Client({ connectionString: databaseUrl(process.env), application_name: 'background-reconciliation-schema', connectionTimeoutMillis: 5000 });
try {
  await client.connect();
  await client.query('BEGIN');
  await client.query("SET LOCAL lock_timeout='1s'");
  await client.query("SET LOCAL statement_timeout='10s'");
  const identity = await verifyCrawlerWriterDatabase(client.query.bind(client), process.env);
  assert.equal(identity.database, expected);
  await client.query(await readFile(new URL('../src/backgroundReconciliationSchema.sql', import.meta.url), 'utf8'));
  await client.query('COMMIT');
  console.log(JSON.stringify({ ok: true, database: identity.database, table: 'crawler.background_reconciliation_scans' }));
} catch (error) {
  await client.query('ROLLBACK').catch(() => {});
  throw error;
} finally { await client.end(); }
