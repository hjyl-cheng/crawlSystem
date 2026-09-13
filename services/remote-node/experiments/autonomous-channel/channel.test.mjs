import assert from 'node:assert/strict';
import test from 'node:test';
import { mkdtemp, rm, appendFile, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawn } from 'node:child_process';
import { collectChannel, ChannelInbox, resultChunks } from './channel.mjs';
import { Journal } from './journal.mjs';
import { inputFixture, youtubeFixture, detailFixture } from './fixture.mjs';

async function setup(t) {
  const directory = await mkdtemp(join(tmpdir(), 'channel-autonomy-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const nodePath = join(directory, 'node');
  return { directory, nodePath, journal: await new Journal(nodePath).init(), input: inputFixture() };
}

test('node discovers targets, uses full then metrics validation, and requires no center requests between videos', async t => {
  const { input, journal } = await setup(t);
  const events = [];
  const result = await collectChannel({ input, journal, youtube: youtubeFixture(events) });
  assert.deepEqual(events.filter(x => x[0] === 'detail'), [
    ['detail', 'new-a', 'full'], ['detail', 'new-b', 'full'], ['detail', 'stored', 'metrics'],
  ]);
  assert.deepEqual(events[1][1].anchors, input.anchors);
  assert.equal(result.state, 'collected');
  assert.equal(result.items.length, 3);
  assert.ok(result.items.every(item => item.detail.comment_count === 0));
  assert.equal([...journal.records.keys()].filter(key => key.startsWith('api:')).length, 0);
  const again = await collectChannel({ input, journal, youtube: { openChannel: () => assert.fail('must not recollect') } });
  assert.deepEqual(again, result);
});

test('real SIGKILL after an fsynced video resumes only unfinished videos in a new process owner', async t => {
  const { input, nodePath } = await setup(t);
  const child = spawn(process.execPath, [new URL('./crash-child.mjs', import.meta.url).pathname, nodePath], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = '';
  child.stderr.on('data', bytes => { stderr += bytes; });
  const status = await new Promise((resolve, reject) => {
    child.on('error', reject); child.on('exit', (code, signal) => resolve({ code, signal }));
  });
  assert.equal(status.signal, 'SIGKILL', stderr);
  const events = [];
  const result = await collectChannel({ input, journal: await new Journal(nodePath).init(), youtube: youtubeFixture(events) });
  assert.deepEqual(events.map(e => e[1]), ['new-b', 'stored']);
  assert.equal(result.items.length, 3);
});

test('parser exhaustion uses shared API policy, saves partial evidence and returns without waiting or crossing the first-seen barrier', async t => {
  const { input, journal, nodePath } = await setup(t);
  const events = [];
  const youtube = youtubeFixture(events, { fetchDetail: async id => {
    if (id === 'new-b') throw Object.assign(new Error('required player surface incomplete'), {
      name: 'YoutubeJsRequiredSurfaceError', partial_detail: { id, like_count: 12, comments_disabled: true },
    });
    return detailFixture(id);
  } });
  const result = await collectChannel({ input, journal, youtube });
  assert.equal(events.filter(e => e[1] === 'new-b').length, 3);
  assert.equal(result.state, 'api_pending');
  assert.deepEqual(result.items.map(i => i.status), ['captured', 'api_pending', 'pending']);
  assert.equal(result.items[1].request.partialDetail.like_count, 12);
  assert.equal(result.items[1].request.requireComments, false);
  assert.deepEqual(await collectChannel({ input, journal: await new Journal(nodePath).init(), youtube: {} }), result);
});

test('unexhausted network error remains retryable; it is neither dormant nor an API request', async t => {
  const { input, journal } = await setup(t);
  await assert.rejects(collectChannel({ input, journal, getBudget: async () => ({ business_tasks_used: 1, business_tasks_limit: 3 }),
    youtube: youtubeFixture([], { fetchDetail: async () => { throw Object.assign(new Error('connect ECONNRESET'), { code: 'ECONNRESET' }); } }),
  }), /ECONNRESET/);
  assert.equal(journal.get('result'), undefined);
  assert.equal([...journal.records.keys()].filter(key => key.startsWith('api:')).length, 0);
});

test('exhausted managed route budget hands off API work without extra local route attempts', async t => {
  const { input, journal } = await setup(t);
  const events = [];
  const result = await collectChannel({ input, journal,
    getBudget: async () => ({ business_tasks_used: 3, business_tasks_limit: 3 }),
    youtube: youtubeFixture(events, { fetchDetail: async () => { throw Object.assign(new Error('connect ECONNRESET'), { code: 'ECONNRESET' }); } }),
  });
  assert.equal(result.state, 'api_pending');
  assert.equal(events.filter(e => e[0] === 'detail').length, 1);
});

test('About-only Plan never scans or fetches a video', async t => {
  const { input, journal } = await setup(t);
  input.plan.task_mask.video = false;
  const events = [];
  const result = await collectChannel({ input, journal, youtube: youtubeFixture(events) });
  assert.deepEqual(events.map(e => e[0]), ['about']);
  assert.deepEqual(result.items, []);
});

test('confirmed dormant uploads skip all recent work while retaining the original evidence', async t => {
  const { input, journal } = await setup(t);
  const result = await collectChannel({ input, journal, youtube: youtubeFixture([], {
    entries: [], scanPatch: { empty_uploads: { outcome: 'dormant', reason: 'no_country_reserve', country: 'BR' } },
  }) });
  assert.equal(result.scan.empty_uploads.country, 'BR');
  assert.equal(result.items.length, 0);
});

test('future deferred videos and live/upcoming entries do not become first-seen detail requests', async t => {
  const { input, journal } = await setup(t);
  input.snapshot.dispositions = [['new-a', { disposition: 'deferred', next_attempt_at: '2026-10-01' }]];
  const result = await collectChannel({ input, journal, youtube: youtubeFixture([], {
    entries: [{ id: 'new-a' }, { id: 'live', is_live: true }, { id: 'upcoming', is_upcoming: true }, { id: 'stored' }],
  }) });
  assert.deepEqual(result.batch.pendingDeferredVideoIds, ['new-a']);
  assert.deepEqual(result.items.map(i => i.video_id), ['stored']);
});

test('incomplete scan cannot silently collect recent videos or invent a complete scan', async t => {
  const { input, journal } = await setup(t);
  const result = await collectChannel({ input, journal, youtube: youtubeFixture([], {
    scanPatch: { complete: false, stop_reason: 'page_limit', terminal_reason: 'page_limit' },
  }) });
  assert.equal(result.scan.complete, false);
  assert.equal(result.items.length, 0);
});

test('lease abort stops further requests and does not seal a successful channel result', async t => {
  const { input, journal } = await setup(t);
  const abort = new AbortController();
  const events = [];
  await assert.rejects(collectChannel({ input, journal, signal: abort.signal, youtube: youtubeFixture(events),
    afterCheckpoint: async key => { if (key.startsWith('detail:')) abort.abort(new Error('LEASE_LOST')); },
  }), /LEASE_LOST/);
  assert.equal(events.filter(e => e[0] === 'detail').length, 1);
  assert.equal(journal.get('result'), undefined);
});

test('same local directory rejects a different generation or input snapshot', async t => {
  const { input, journal, nodePath } = await setup(t);
  await collectChannel({ input, journal, youtube: youtubeFixture() });
  await assert.rejects(collectChannel({ input: { ...input, generation: 2 }, journal: await new Journal(nodePath).init(), youtube: {} }), /IDENTITY_CONFLICT/);
});

test('whole-channel result larger than NATS limit is chunked, durable ACK survives restart and lost ACK replay', async t => {
  const { input, journal, directory } = await setup(t);
  const result = await collectChannel({ input, journal, youtube: youtubeFixture([], {
    fetchDetail: async id => ({ ...detailFixture(id), description: '多字节'.repeat(90000) }),
  }) });
  const { manifest, chunks } = resultChunks(result);
  assert.ok(manifest.bytes > 2 * 1024 * 1024);
  assert.ok(chunks.every(chunk => Buffer.byteLength(JSON.stringify(chunk)) < 1024 * 1024));
  let generation = 1;
  const assertOwner = m => { assert.equal(m.generation, generation, 'STALE_GENERATION'); };
  const centerPath = join(directory, 'center');
  let inbox = new ChannelInbox({ journal: await new Journal(centerPath).init(), assertOwner });
  // Out-of-order delivery and duplicate chunk before completion.
  assert.equal((await inbox.receive(manifest, chunks.at(-1))).durable, false);
  assert.equal((await inbox.receive(manifest, chunks.at(-1))).durable, false);
  inbox = new ChannelInbox({ journal: await new Journal(centerPath).init(), assertOwner });
  let receipt;
  for (const chunk of chunks) receipt = await inbox.receive(manifest, chunk);
  assert.equal(receipt.durable, true);
  assert.equal(receipt.state, 'received');
  generation = 2;
  inbox = new ChannelInbox({ journal: await new Journal(centerPath).init(), assertOwner });
  assert.deepEqual(await inbox.receive(manifest, chunks[0]), receipt);
  await assert.rejects(inbox.receive({ ...manifest, sha256: 'different' }, chunks[0]), /RESULT_CONFLICT/);
  const empty = new ChannelInbox({ journal: await new Journal(join(directory, 'fresh-center')).init(), assertOwner });
  await assert.rejects(empty.receive(manifest, chunks[0]), /STALE_GENERATION/);
});

test('partial transfer does not permit generation handoff to seal an old result', async t => {
  const { directory } = await setup(t);
  let generation = 1;
  const inbox = new ChannelInbox({ journal: await new Journal(join(directory, 'center')).init(),
    assertOwner: m => { if (m.generation !== generation) throw new Error('STALE_GENERATION'); } });
  const { manifest, chunks } = resultChunks({ plan_id: 'p', generation: 1, input_sha256: 'input', data: 'x'.repeat(100) }, 50);
  await inbox.receive(manifest, chunks[0]);
  generation = 2;
  await assert.rejects(inbox.receive(manifest, chunks[1]), /STALE_GENERATION/);
  assert.equal(inbox.journal.get('receipt'), undefined);
});

test('corrupt chunks cannot produce a receipt', async t => {
  const { directory } = await setup(t);
  const inbox = new ChannelInbox({ journal: await new Journal(join(directory, 'center')).init(), assertOwner: () => {} });
  const { manifest, chunks } = resultChunks({ plan_id: 'p', generation: 1, input_sha256: 'i' });
  await assert.rejects(inbox.receive(manifest, { ...chunks[0], data: 'corrupt' }), /CHUNK_CORRUPT/);
  assert.equal(inbox.journal.get('receipt'), undefined);
});

test('journal recovers an interrupted last append but fails closed on corruption and disk budget exhaustion', async t => {
  const { journal, nodePath } = await setup(t);
  await journal.put('first', { data: 'saved' });
  await appendFile(journal.path, '{unfinished');
  const recovered = await new Journal(nodePath).init();
  assert.deepEqual(recovered.get('first'), { data: 'saved' });
  await recovered.put('second', { data: 'next' });
  const small = await new Journal(nodePath, recovered.bytes + 1).init();
  await assert.rejects(small.put('third', 'large'.repeat(100)), /JOURNAL_FULL/);
  assert.equal((await new Journal(nodePath).init()).get('third'), undefined);
  const bytes = await readFile(journal.path, 'utf8');
  await writeFile(journal.path, bytes.replace('saved', 'wrong'));
  await assert.rejects(new Journal(nodePath).init(), /JOURNAL_CORRUPT/);
});
