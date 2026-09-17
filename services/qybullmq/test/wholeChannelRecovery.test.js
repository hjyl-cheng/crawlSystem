import assert from 'node:assert/strict';
import { loginRequiredDetail } from '../src/youtubeLoginRequired.js';
import test from 'node:test';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm, writeFile, readdir } from 'node:fs/promises';
import { spawn } from 'node:child_process';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { Journal } from '../src/remoteNodes/wholeChannelJournal.js';
import { recoverWholeChannel } from '../src/remoteNodes/wholeChannelNode.js';
import { collectWholeChannel, interruptWholeChannel } from '../src/remoteNodes/wholeChannelCollector.js';
import { wholeChannelParts, decodeWholeChannelParts } from '../src/remoteNodes/wholeChannelProtocol.js';
import { decodeResult, RemoteProtocolError } from '../src/remoteNodes/protocol.js';
import { detailFixture } from '../../remote-node/experiments/autonomous-channel/fixture.mjs';

async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), 'whole-recovery-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const input = { version: 1, generation: 1, collectAbout: false, channelOptions: { includeAbout: false },
    plan: { plan_id: randomUUID(), channel_id: 'UC-recovery-fixture', task_mask: { about: false, video: true } },
    apiPolicy: { enabled: false, available: false, dailyRequestLimit: 0 },
    video: { runId: 'fixture-run', cycleKey: 'base', resumeBatch: {
      scan_json: { complete: true, entries: [], pages: 1, stop_reason: 'list_end' },
      items: ['first', 'second'].map((video_id, ordinal) => ({ phase: 'first_seen', ordinal, video_id, target_json: { id: video_id }, status: 'pending' })),
    } },
  };
  const command = { command_id: randomUUID(), input: { input_sha256: wholeChannelParts(input).manifest.sha256 } };
  const lease = { task_id: randomUUID(), generation: 1, input: { plan: input.plan } };
  const spool = new RemoteResultSpool({ directory }); await spool.init();
  return { directory, input, command, lease, spool, detail: detailFixture('first') };
}

test('remote collection journals login exclusions, continues, and replays without API or network', async t => {
  const f = await fixture(t);
  f.input.apiPolicy = { enabled: true, available: true, dailyRequestLimit: 100 };
  f.input.networkBudget = { business_tasks_used: 9, business_tasks_limit: 9 };
  const journal = await new Journal(join(f.directory, 'whole', f.command.command_id)).init();
  const fetched = [];
  const youtube = { openChannel: () => assert.fail('frozen scan is reused'), fetchDetail: async id => {
    fetched.push(id);
    return id === 'first' ? loginRequiredDetail(id, []) : { ...f.detail, id };
  } };
  const result = await collectWholeChannel({ input: f.input, journal,
    signal: new AbortController().signal, youtube });
  assert.equal(result.failure, null);
  assert.deepEqual(fetched, ['first', 'second']);
  assert.deepEqual(result.items.map(item => item.status), ['captured', 'captured']);
  assert.equal(result.items[0].detail.collection_exclusion.reason_code, 'login_required');
  assert.equal(journal.get('api:detail:first_seen:first'), undefined);
  await collectWholeChannel({ input: f.input, journal, signal: new AbortController().signal,
    youtube: { openChannel: () => assert.fail('no scan'), fetchDetail: () => assert.fail('no retry') } });
});

async function crash(f, crashAt) {
  const path = join(f.directory, 'fixture.json');
  await writeFile(path, JSON.stringify({ input: f.input, lease: f.lease, command: f.command, detail: f.detail }));
  const child = spawn(process.execPath, [new URL('./fixtures/wholeChannelCrash.mjs', import.meta.url).pathname,
    f.directory, path, crashAt], { stdio: ['ignore', 'pipe', 'pipe'] });
  let stderr = ''; child.stderr.on('data', bytes => { stderr += bytes; });
  const status = await new Promise((resolve, reject) => {
    child.once('error', reject); child.once('exit', (code, signal) => resolve({ code, signal }));
  });
  assert.equal(status.signal, 'SIGKILL', stderr);
}

function receiver() {
  const parts = new Map(); let manifest; let calls = 0;
  const client = { transport: 'nats', uploadWholeChannel: async (pending, bytes) => {
    calls++;
    const { value } = await decodeResult(bytes);
    assert.equal(value.command_id, pending.command_id);
    const data = value.data; manifest = data.manifest;
    const prior = parts.get(data.chunk.part);
    if (prior) assert.deepEqual(prior, data.chunk);
    parts.set(data.chunk.part, data.chunk);
    return { durable: true, command_id: pending.command_id, part: data.chunk.part,
      sha256: data.chunk.sha256, result_sha256: manifest.sha256, complete: parts.size === manifest.parts };
  } };
  return { client, get calls() { return calls; }, result: () => decodeWholeChannelParts(manifest, [...parts.values()]) };
}

for (const crashAt of ['before_pointer', 'during_collection']) test(`real SIGKILL ${crashAt}: restart returns durable progress without using the lost browser identity`, async t => {
  const f = await fixture(t); await crash(f, crashAt);
  assert.equal(await f.spool.read('whole-pending.json'), null);
  const r = receiver();
  await recoverWholeChannel({ client: r.client, spool: new RemoteResultSpool({ directory: f.directory }) });
  const result = r.result();
  assert.equal(result.items[0].status, 'captured');
  assert.equal(result.items[1].status, crashAt === 'during_collection' ? 'retryable' : 'captured');
  assert.equal(result.failure?.error.code ?? null, crashAt === 'during_collection' ? 'REMOTE_CHANNEL_INTERRUPTED' : null);
  assert.deepEqual(await readdir(join(f.directory, 'whole')), []);
  assert.equal(await f.spool.read('whole-pending.json'), null);
  const calls = r.calls;
  await recoverWholeChannel({ client: r.client, spool: f.spool });
  assert.equal(r.calls, calls);
});

test('lost final ACK replays exact fsynced bytes, and only a durable receipt removes the journal', async t => {
  const f = await fixture(t); await crash(f, 'before_pointer');
  const r = receiver();
  await assert.rejects(recoverWholeChannel({ spool: f.spool, client: { ...r.client,
    uploadWholeChannel: async (...args) => { await r.client.uploadWholeChannel(...args); throw new Error('ACK lost'); } } }), /ACK lost/);
  assert.equal((await readdir(join(f.directory, 'whole'))).length, 1);
  const before = r.result();
  await recoverWholeChannel({ client: r.client, spool: new RemoteResultSpool({ directory: f.directory }) });
  assert.deepEqual(r.result(), before);
  assert.equal(await f.spool.read('whole-pending.json'), null);
});

for (const code of ['STALE_LEASE', 'INCREMENTAL_BUSINESS_FENCE_STALE']) test(`${code} archives evidence once and no longer prevents intake`, async t => {
  const f = await fixture(t); await crash(f, 'before_pointer');
  let calls = 0;
  const client = { transport: 'nats', uploadWholeChannel: async () => { calls++; throw new RemoteProtocolError(code, 409); } };
  await recoverWholeChannel({ client, spool: f.spool });
  await recoverWholeChannel({ client, spool: f.spool });
  assert.equal(calls, 1);
  assert.equal(await f.spool.read('whole-pending.json'), null);
  assert.equal(await f.spool.writable(), true);
  const archived = await new Journal(join(f.directory, 'whole-archive', f.command.command_id)).init();
  assert.equal(archived.get('stale').code, code);
  assert.ok(archived.get('result'), 'retain the rejected result for inspection');
});

test('a rejected business execution releases its old claim and reaches new intake', async t => {
  const { RemoteChannelPlanExecutor } = await import('../src/remoteNodes/channelPlanExecutor.js');
  const f = await fixture(t); await crash(f, 'before_pointer');
  await f.spool.save('claim.json', Buffer.from(JSON.stringify({ claim_id: randomUUID(), lease: f.lease })));
  let uploads = 0; let claims = 0;
  const worker = new RemoteChannelPlanExecutor({ spool: f.spool,
    client: { transport: 'nats',
      uploadWholeChannel: async () => { uploads++; throw new RemoteProtocolError('INCREMENTAL_BUSINESS_FENCE_STALE', 409); },
      pollCommands: async () => { throw new RemoteProtocolError('STALE_LEASE', 409); },
      claim: async () => { claims++; return null; },
    },
    youtube: { openChannel: () => assert.fail('no replayed network'), fetchDetail: () => assert.fail('no replayed network') },
    withSession: () => assert.fail('no replayed network'),
  });
  assert.equal(await worker.runOnce(), 'expired');
  assert.equal(await f.spool.read('claim.json'), null);
  assert.equal(await worker.runOnce(), 'idle');
  assert.equal(uploads, 1);
  assert.equal(claims, 1);
});

for (const [code, status] of [['WHOLE_CHANNEL_RESULT_CONFLICT', 409], ['INCREMENTAL_BUSINESS_FENCE_STALE', 503]]) {
  test(`${code}/${status} does not discard unacknowledged results`, async t => {
    const f = await fixture(t); await crash(f, 'before_pointer');
    await assert.rejects(recoverWholeChannel({ spool: f.spool, client: { transport: 'nats',
      uploadWholeChannel: async () => { throw new RemoteProtocolError(code, status); },
    } }), { code, status });
    assert.equal((await f.spool.read('whole-pending.json')).task_id, f.lease.task_id);
    assert.ok((await new Journal(join(f.directory, 'whole', f.command.command_id)).init()).get('result'));
  });
}

test('restart between API outbox fsync and item checkpoint preserves the original API request', async t => {
  const f = await fixture(t);
  const journal = await new Journal(join(f.directory, 'whole', f.command.command_id)).init();
  await journal.put('input', f.input);
  await journal.put('scan', f.input.video.resumeBatch.scan_json);
  const request = { requestId: 'original-request', videoId: 'first', partialDetail: { like_count: 12 } };
  await journal.put('api:detail:first_seen:first', request);
  const result = await interruptWholeChannel(journal);
  assert.equal(result.failure, null);
  assert.deepEqual(result.items.map(item => item.status), ['api_pending', 'pending']);
  assert.deepEqual(result.items[0].api_request, request);
});

test('restart after receipt fsync cleans remaining pointer without uploading again', async t => {
  const f = await fixture(t); await crash(f, 'before_pointer');
  const journal = await new Journal(join(f.directory, 'whole', f.command.command_id)).init();
  await journal.put('receipt', { durable: true });
  await f.spool.save('whole-pending.json', Buffer.from(JSON.stringify(journal.get('delivery'))));
  await recoverWholeChannel({ client: { uploadWholeChannel: () => assert.fail('already received') }, spool: f.spool });
  assert.equal(await f.spool.read('whole-pending.json'), null);
  assert.deepEqual(await readdir(join(f.directory, 'whole')), []);
});

test('received evidence is reused only if it still satisfies the new frozen target requirements', async t => {
  const f=await fixture(t);
  f.input.previousEvidence={scan:f.input.video.resumeBatch.scan_json,items:[
    {phase:'first_seen',video_id:'first',status:'captured',detail:{...f.detail,view_count:null,view_count_text:null}},
    {phase:'first_seen',video_id:'second',status:'captured',detail:{...f.detail,id:'second'}},
  ]};
  const journal=await new Journal(join(f.directory,'whole',f.command.command_id)).init();
  const fetched=[];
  const result=await collectWholeChannel({input:f.input,journal,signal:new AbortController().signal,
    youtube:{openChannel:()=>assert.fail('frozen scan is reused'),fetchDetail:async id=>{fetched.push(id);return {...f.detail,id};}}});
  assert.deepEqual(fetched,['first']);
  assert.deepEqual(result.items.map(item=>item.status),['captured','captured']);
});

test('idle execution archives terminal evidence without replaying it on each pass', async t => {
  const { RemoteChannelPlanExecutor } = await import('../src/remoteNodes/channelPlanExecutor.js');
  const f = await fixture(t); await crash(f, 'before_pointer');
  const path = join(f.directory, 'whole', f.command.command_id);
  await (await new Journal(path).init()).put('stale', { code: 'STALE_LEASE' });
  let opens = 0;
  const init = Journal.prototype.init;
  t.mock.method(Journal.prototype, 'init', async function () { opens++; return init.call(this); });
  const worker = new RemoteChannelPlanExecutor({ spool: f.spool,
    client: { transport: 'nats', claim: async () => null, uploadWholeChannel: () => assert.fail('terminal result') },
    youtube: { openChannel: () => assert.fail('idle'), fetchDetail: () => assert.fail('idle') },
    withSession: () => assert.fail('idle'),
  });
  assert.equal(await worker.runOnce(), 'idle');
  const initial = opens;
  for (let n = 0; n < 3; n++) assert.equal(await worker.runOnce(), 'idle');
  assert.equal(opens, initial, 'idle passes must not reopen terminal journals');
  assert.deepEqual(await readdir(join(f.directory, 'whole')), []);
  const archived = await new Journal(join(f.directory, 'whole-archive', f.command.command_id)).init();
  assert.equal(archived.get('stale').code, 'STALE_LEASE');
  assert.ok(archived.get('result'), 'preserve historical evidence');
});
