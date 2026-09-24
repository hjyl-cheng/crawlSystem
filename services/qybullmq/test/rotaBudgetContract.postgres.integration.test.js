import test from 'node:test';
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import pg from 'pg';
import { assertIsolatedRemoteDatabase } from '../src/remoteNodes/isolation.js';

test('real Rota PostgreSQL budget admits nine attempts and rejects exhausted redelivery', {
  skip: !process.env.REMOTE_NODE_TEST_DATABASE_URL || !process.env.ROTA_BUDGET_TEST_GO,
  timeout: 240000,
}, async () => {
  const pool = new pg.Pool({connectionString:process.env.REMOTE_NODE_TEST_DATABASE_URL});
  try {await assertIsolatedRemoteDatabase(pool);} finally {await pool.end();}
  const {stdout} = await promisify(execFile)(process.env.ROTA_BUDGET_TEST_GO,
    ['test','./internal/proxycontrol','-count=1','-v','-run','TestBeginTask(StopsAtBusinessRunBudgetAfterThreeExecutions|PrioritizesExhaustedBusinessRunOverExecution)$'],
    {cwd:new URL('../../rota/core/',import.meta.url),env:{...process.env,ROTA_TEST_DATABASE_URL:process.env.REMOTE_NODE_TEST_DATABASE_URL},timeout:230000,maxBuffer:2*1024*1024});
  assert.doesNotMatch(stdout,/--- SKIP:/);
  assert.match(stdout,/--- PASS: TestBeginTaskStopsAtBusinessRunBudgetAfterThreeExecutions/);
  assert.match(stdout,/--- PASS: TestBeginTaskPrioritizesExhaustedBusinessRunOverExecution/);
  console.log(stdout);
});
