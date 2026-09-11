import test from 'node:test';
import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRemoteYoutubeRuntime } from '../src/remoteNodes/youtubeRuntime.js';
import { RemoteResultSpool } from '../src/remoteNodes/spool.js';
import { sessionRequest } from '../src/remoteNodes/youtubeSessionContract.js';
import { hash } from '../src/remoteNodes/protocol.js';
import { canonicalIncrementalJson } from '../src/incrementalPlan.js';
import { currentChannelExecution } from '../src/channelExecutionContext.js';
import { fetchWithFingerprint } from '../src/fingerprintFetch.js';
import { currentProxyIdentity } from '../src/proxyIdentity.js';
import { youtubeJsState } from '../src/youtubeJs.js';

export function runtimeFixture() {
  const identity = { network_identity_key: 'fixture-network', profile_epoch: 0, identity_policy_id: 'fixture-policy',
    identity_policy_version: 1, egress_country: 'BR' };
  const profile = { profile_id: 'fixture-browser', engine: 'youtubejs_chrome', impersonate_target: 'chrome136',
    user_agent: 'Fixture Chrome', visitor_data: 'CgtmaXh0dXJldmlzaXQ=', language: 'en', country: 'BR', timezone: 'America/Sao_Paulo',
    fingerprint_json: { max_connections: 1 }, cookie_state: { cookies: [] } };
  const planId = randomUUID(); const now = new Date().toISOString();
  const plan = { schema_version: 5, dispatch_generation: 1, job_id: `remote_${planId}`, plan_id: planId,
    plan_mode: 'standard', plan_day: now.slice(0,10), scheduled_at: now, channel_id: `UC${'f'.repeat(22)}`,
    task_mask: { about: false, video: true, agent: false }, capacity: { factor: 1, player_cap: 20, next_cap: 8, version: 'capacity-1' },
    clock_version: 7, policy_version: 'v16-rule-1', planner_config_version: 'video-plan-1' };
  const lease = { task_id: randomUUID(), generation: 1, worker_slot: 'worker-1', capability: 'youtube.incremental.plan.v1', input: { plan } };
  const route = { lease, slot: 'worker-1', bootId: 'a'.repeat(48), epoch: 1, route_id: randomUUID(),
    identity_id: hash(canonicalIncrementalJson(identity)), proxyUrl: 'http://worker-1:fixture-token@127.0.0.1:8000/', signal: new AbortController().signal };
  const request = sessionRequest(route, lease, route.slot, route.bootId);
  const bundle = { version: 1, ...request, identity, attempt_id: randomUUID(),
    proxy: { ...identity, slot_name: 'central-slot', lease_id: randomUUID(), route_generation: 1 },
    profile_group: { ...identity, profile_group_id: randomUUID(), profile_revision: 1, clients: { youtubejs_chrome: profile } } };
  return { route, bundle, profile, plan };
}

async function fixture(t, { realYoutube = false } = {}) {
  const f = runtimeFixture(); const events = []; const checkpoints = [];
  const directory = await mkdtemp(join(tmpdir(), 'remote-youtube-runtime-'));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const spool = new RemoteResultSpool({ directory }); await spool.init();
  const client = { youtubeSession: async request => { assert.deepEqual(request, sessionRequest(f.route, f.route.lease, f.route.slot, f.route.bootId)); return f.bundle; },
    youtubeCheckpoint: async value => { checkpoints.push(value); return { durable: true, route_id: value.request.route_id,
      sha256: hash(canonicalIncrementalJson(value.checkpoint)) }; } };
  const gateway = { prepare: async value => { events.push('prepare'); assert.equal(value.proxyUrl, f.route.proxyUrl); },
    fetch: async (_profile, input) => { events.push('fetch'); return new Response(JSON.stringify({
      metadata: { channelMetadataRenderer: { title: 'Remote fixture', externalId: f.plan.channel_id, channelUrl: `https://www.youtube.com/channel/${f.plan.channel_id}` } },
      contents: { twoColumnBrowseResultsRenderer: { tabs: [{ tabRenderer: { title: 'Home', selected: true,
        content: { sectionListRenderer: { contents: [] } } } }] } },
    }), { headers: { 'content-type': 'application/json' } }); },
    snapshot: async () => { events.push('snapshot'); return { cookies: [{ name: 'VISITOR_INFO1_LIVE', value: 'updated', domain: '.youtube.com', path: '/' }] }; },
    close: async () => { events.push('close-gateway'); } };
  const youtube = { acquire: async (_channelId, options) => { events.push('acquire'); assert.deepEqual(options.profile, f.profile); return { enabled: true }; },
    release: async () => { events.push('release'); }, close: async () => { events.push('close-youtube'); },
    openChannel: () => {}, fetchDetail: () => {} };
  const runtime = createRemoteYoutubeRuntime({ client, spool, gateway, ...(!realYoutube ? { youtube } : {}) });
  return { ...f, runtime, client, gateway, youtube, events, checkpoints, spool };
}

test('managed runtime uses the authorized profile, drains requests and durably checkpoints before return', async t => {
  const f = await fixture(t);
  assert.equal(await f.runtime.withRuntime(f.route, async () => {
    assert.equal(currentChannelExecution().attempt_id, f.bundle.attempt_id);
    assert.equal(currentProxyIdentity().proxy_url, f.route.proxyUrl);
    await fetchWithFingerprint('youtubejs_chrome', 'https://www.youtube.com/', {}, () => assert.fail('no direct fallback'));
    return 'channel-finished';
  }), 'channel-finished');
  assert.deepEqual(f.events, ['prepare','acquire','fetch','release','snapshot','close-youtube','close-gateway']);
  assert.equal(f.checkpoints[0].checkpoint.status, 'success');
  assert.equal(f.checkpoints[0].checkpoint.cookies.cookies[0].value, 'updated');
  assert.equal(await f.spool.read('youtube-session.json'), null);
  assert.equal(currentChannelExecution(), null);
});

test('actual YouTubeJS session and channel parser use the managed transport fixture', async t => {
  const mode = process.env.YOUTUBEJS_EXTRACTOR_MODE; process.env.YOUTUBEJS_EXTRACTOR_MODE = 'full';
  t.after(() => { if (mode === undefined) delete process.env.YOUTUBEJS_EXTRACTOR_MODE; else process.env.YOUTUBEJS_EXTRACTOR_MODE = mode; });
  const f = await fixture(t, { realYoutube: true });
  const result = await f.runtime.withRuntime(f.route, () => f.runtime.youtube.openChannel(f.plan.channel_id, { includeAbout: false }));
  assert.equal(result.metadata.title, 'Remote fixture');
  assert.equal(result.metadata.channel_id, f.plan.channel_id);
  assert.equal(f.checkpoints[0].checkpoint.metrics.request_count, 1);
  assert.equal(youtubeJsState().ready, false);
  assert.equal(youtubeJsState().active_channel, null);
});

test('invalid identity or nonlocal proxy cannot start collection', async t => {
  const f = await fixture(t);
  await assert.rejects(f.runtime.withRuntime({ ...f.route, proxyUrl: 'http://worker-1:token@8.8.8.8:80' }, () => assert.fail()), /LOCAL_ROTA_PROXY_REQUIRED/);
  f.bundle.profile_group.network_identity_key = 'different';
  await assert.rejects(f.runtime.withRuntime(f.route, () => assert.fail()), { code: 'INVALID_YOUTUBE_SESSION' });
  assert.deepEqual(f.events, []);
});

test('quiescence waits for admitted requests and prevents another channel from sharing the process', async t => {
  const f = await fixture(t); let started; let finish;
  const admitted = new Promise(resolve => { started = resolve; });
  const response = new Promise(resolve => { finish = resolve; });
  f.gateway.fetch = async () => { started(); await response; return new Response('{}'); };
  let request;
  const operation = f.runtime.withRuntime(f.route, () => {
    request = fetchWithFingerprint('youtubejs_chrome', 'https://www.youtube.com/', {}, () => assert.fail('no direct path'));
    return 'done';
  });
  await admitted;
  try {
    assert.equal(f.events.includes('release'), false);
    assert.equal(f.events.includes('snapshot'), false);
    await assert.rejects(f.runtime.withRuntime(f.route, () => assert.fail()), /REMOTE_YOUTUBE_RUNTIME_BUSY/);
  } finally { finish(); }
  assert.equal(await operation, 'done'); await request;
  assert.equal(f.checkpoints[0].checkpoint.active_managed_requests, 0);
});

test('cancelled and failed acquisitions discard cookies and close both runtimes', async t => {
  const f = await fixture(t); f.youtube.acquire = async () => ({ enabled: false });
  await assert.rejects(f.runtime.withRuntime(f.route, () => assert.fail()), /REMOTE_YOUTUBE_ACQUIRE_FAILED/);
  assert.equal(f.checkpoints[0].checkpoint.status, 'failed');
  assert.equal(f.checkpoints[0].checkpoint.cookies, null);
  assert.ok(f.events.includes('close-youtube')); assert.ok(f.events.includes('close-gateway'));
  f.youtube.acquire = async () => ({ enabled: true });
  const abort = new AbortController();
  await assert.rejects(f.runtime.withRuntime({ ...f.route, signal: abort.signal }, () => { abort.abort(new Error('lease lost')); }), /lease lost/);
  assert.equal(f.checkpoints[1].checkpoint.status, 'aborted');
  assert.equal(f.checkpoints[1].checkpoint.cookies, null);
});

test('a lost checkpoint receipt is replayed without recollection or changed cookies', async t => {
  const f = await fixture(t); const original = f.client.youtubeCheckpoint; let lose = true;
  f.client.youtubeCheckpoint = async value => { const ack = await original(value); if (lose) { lose = false; throw new Error('response lost'); } return ack; };
  let captures = 0;
  await assert.rejects(f.runtime.withRuntime(f.route, () => { captures++; }), /response lost/);
  assert.equal((await f.spool.read('youtube-session.json')).checkpoint.status, 'success');
  await f.runtime.withRuntime.recover();
  assert.deepEqual(f.checkpoints[0], f.checkpoints[1]); assert.equal(captures, 1);
  assert.equal(await f.spool.read('youtube-session.json'), null);
});

test('restart reports an interrupted session instead of silently reacquiring its browser identity', async t => {
  const f = await fixture(t);
  await f.spool.save('youtube-session.json', Buffer.from(JSON.stringify({ request: sessionRequest(f.route, f.route.lease, f.route.slot, f.route.bootId) })));
  await f.runtime.withRuntime.recover();
  assert.equal(f.checkpoints[0].checkpoint.status, 'aborted');
  assert.equal(f.checkpoints[0].checkpoint.error_code, 'REMOTE_YOUTUBE_PROCESS_INTERRUPTED');
  assert.deepEqual(f.events, []);
});
