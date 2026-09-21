import test from 'node:test';
import assert from 'node:assert/strict';
import {spawnSync} from 'node:child_process';
import {fullCrawlReleaseConfig} from '../src/remoteNodes/fullCrawlReleaseConfig.js';
import {createCenterFullCrawlProcessor} from '../src/remoteNodes/centerFullCrawlProcessor.js';
import {DelayedError} from 'bullmq';

const secrets = {controlToken: 'fixture-control-only', profileSecret: 'fixture-profile-only'};
const config = () => ({REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED: 'true', SKIP_SCHEMA_MIGRATION: 'true',
  YOUTUBEJS_EXTRACTOR_MODE: 'full', PROXY_SLOT_ROLE: 'channel', WORKER_QUEUES: 'youtube-channel-crawl',
  REMOTE_NODE_FULL_CRAWL_NODE_IDS: 'e6928a55-d46b-4234-b0b4-fc15b4d90f82', REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS: '3',
  REMOTE_NODE_QUEUE_PREFIX: 'fixture-full', BULLMQ_PREFIX: 'fixture-full',
  REMOTE_NODE_REDIS_URL: 'redis://:fixture-redis@localhost:6379/0', REDIS_HOST: 'localhost', REDIS_PASSWORD: 'fixture-redis',
  DATABASE_URL: 'postgres://fixture@localhost/fixture', REMOTE_NODE_DATABASE_URL: 'postgres://fixture@localhost/fixture',
  EXPECTED_CRAWLER_DATABASE: 'fixture', FORBIDDEN_CRAWLER_DATABASE: 'business',
  REMOTE_NODE_NATS_URL: 'tls://fixture:4222', REMOTE_NODE_FULL_CRAWL_IMAGE: 'fixture/full@sha256:' + 'a'.repeat(64),
  ROTA_PROXY_BASE_URL: 'http://fixture:8000', PROXY_WORKER_ID: 'fixture-full-compatibility',
  ROTA_PROXY_CONTROL_TOKEN: secrets.controlToken, BROWSER_PROFILE_ENCRYPTION_KEY: secrets.profileSecret,
  ROTA_BULLMQ_PROXY_PASSWORD: 'fixture-proxy-only'});

test('release explicitly reserves compatibility capacity and refuses cross-database/queue/identity configuration', () => {
  assert.equal(fullCrawlReleaseConfig(config(), secrets).maxSlots, 2);
  for (const override of [
    {REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS: '1'}, {REMOTE_NODE_FULL_CRAWL_TOTAL_SLOTS: '2.5'},
    {REMOTE_NODE_FULL_CRAWL_NODE_IDS: ''}, {REMOTE_NODE_FULL_CRAWL_NODE_IDS: 'invalid'},
    {SKIP_SCHEMA_MIGRATION: 'false'}, {WORKER_QUEUES: 'youtube-channel-incremental'},
    {DATABASE_URL: 'postgres://fixture@localhost/other'}, {BULLMQ_PREFIX: 'other'},
    {REDIS_PASSWORD: 'wrong'}, {REMOTE_NODE_REDIS_URL: 'rediss://:fixture-redis@localhost:6379/0'},
    {REMOTE_NODE_REDIS_URL: 'redis://user:fixture-redis@localhost:6379/0'},
    {BROWSER_PROFILE_ENCRYPTION_KEY: 'other'}, {ROTA_PROXY_CONTROL_TOKEN: 'other'},
    {REMOTE_NODE_FULL_CRAWL_DEPLOYMENT_ENABLED: 'false'},
  ]) assert.throws(() => fullCrawlReleaseConfig({...config(), ...override}, secrets), /FULL_CRAWL_/);
});

test('importing original worker starts no consumer, schema migration or signal handler', () => {
  const child = spawnSync(process.execPath, ['--input-type=module', '-e', `
    const before = process.listenerCount('SIGTERM');
    const {createWorkerRuntime} = await import('./src/worker.js');
    if (typeof createWorkerRuntime !== 'function' || before !== process.listenerCount('SIGTERM')) process.exit(2);
  `], {cwd: new URL('..', import.meta.url), timeout: 10000, encoding: 'utf8',
    env: {...process.env, DATABASE_URL: 'postgres://invalid:invalid@127.0.0.1:1/invalid', REDIS_PORT: '1'}});
  assert.equal(child.status, 0, child.stderr || child.error?.message);
  assert.equal(child.stdout, '');
});

test('paused admission also defers local repair and migration controls without executing them', async () => {
  let localCalls = 0, delayed = 0;
  const processJob = createCenterFullCrawlProcessor({store: {pool: {query: async () => assert.fail('no business writes')}},
    runtime: {}, rota: {}, ready: async () => false,
    compatibility: {execute: async () => localCalls++, replay: async () => localCalls++},
    handoff: {candidateSettled() {}, fetchCompleted() {}}});
  for (const name of ['channel-detail-repair', 'channel-checkpoint-repair']) {
    const job = {queueName: 'youtube-channel-crawl', name, data: {},
      updateData: async () => {}, updateProgress: async () => {},
      moveToDelayed: async (_, token) => {assert.equal(token, 'owned-token'); delayed++;}};
    await assert.rejects(processJob(job, 'owned-token'), DelayedError);
  }
  assert.equal(localCalls, 0);
  assert.equal(delayed, 2);
});
