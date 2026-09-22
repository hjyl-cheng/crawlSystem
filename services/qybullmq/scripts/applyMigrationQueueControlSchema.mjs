import assert from 'node:assert/strict';
import pg from 'pg';
import { databaseUrl } from '../src/databaseConnection.js';
import { verifyCrawlerWriterDatabase } from '../src/databaseIdentity.js';

assert(process.argv.includes('--apply'), 'Pass --apply to add queue control fencing');
const expected = process.env.EXPECTED_CRAWLER_DATABASE;
assert(expected && process.env.CONFIRM_MIGRATION_QUEUE_SCHEMA_APPLY === expected, 'Confirm the expected crawler database');
const client = new pg.Client({ connectionString: databaseUrl(), application_name: 'migration-queue-control-schema' });
try {
  await client.connect();
  await client.query('BEGIN');
  await client.query("SET LOCAL lock_timeout='5s'");
  await client.query("SET LOCAL statement_timeout='10s'");
  const identity = await verifyCrawlerWriterDatabase(client.query.bind(client));
  assert.equal(identity.database, expected);
  await client.query('CREATE SEQUENCE IF NOT EXISTS crawler.migration_queue_control_revision');
  await client.query('COMMIT');
  console.log(JSON.stringify({ ok: true, database: identity.database, sequence: 'migration_queue_control_revision' }));
} catch (error) {
  await client.query('ROLLBACK').catch(() => {});
  throw error;
} finally { await client.end(); }
