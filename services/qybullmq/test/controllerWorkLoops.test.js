import assert from 'node:assert/strict';
import test from 'node:test';
import { createControllerWorkLoops } from '../src/controllerWorkLoops.js';

test('a blocked recovery cycle cannot prevent repeated intake or API dispatch, and shutdown drains all work', async () => {
  let release;
  const blocked = new Promise(resolve => { release = resolve; });
  const events = [];
  const loops = createControllerWorkLoops({
    tasks: {
      maintenance: { run: async () => { events.push('maintenance'); await blocked; } },
      migration: { run: async () => { events.push('migration'); } },
      api: { run: async () => { events.push('api'); } },
    },
    closeResources: async () => events.push('closed'),
  });
  const slow = loops.run('maintenance');
  await Promise.all([loops.run('migration'), loops.run('api')]);
  await Promise.all([loops.run('migration'), loops.run('api')]);
  assert.equal(events.filter(x => x === 'migration').length, 2);
  assert.equal(events.filter(x => x === 'api').length, 2);
  assert.equal(loops.run('maintenance'), slow);
  const stopping = loops.shutdown();
  assert.equal(await loops.run('migration'), false);
  assert.ok(!events.includes('closed'));
  release();
  await stopping;
  assert.equal(events.at(-1), 'closed');
});
