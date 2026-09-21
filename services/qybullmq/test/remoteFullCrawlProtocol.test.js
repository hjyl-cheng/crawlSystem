import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import test from 'node:test';
import { YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT,
  YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT, LEGACY_FULL_CRAWL_FETCH_CONTRACT } from '../src/fullCrawlFetchContract.js';
import { FULL_CRAWL_WORKLOAD, collectingWorkload } from '../src/remoteNodes/collectingWorkload.js';
import { parseWorkerConfig } from '../src/remoteNodes/workerConfig.js';
import { FULL_CRAWL_LIMITS, fullCrawlInputHash, validateFullCrawlExecution,
  validateFullCrawlStage, validateFullCrawlBatch } from '../src/remoteNodes/fullCrawlProtocol.js';

function execution(patch = {}) {
  return { version: 1, queue_name: 'youtube-channel-crawl', job_name: 'channel-snapshot',
    job_id: 'snapshot:fixture', job_attempt: 1, candidate_id: 7, channel_id: 'UCfixture',
    run_id: 'run:fixture', business_run_id: 'run:fixture', business_run_key: 'candidate:fixture',
    intent_hash: `sha256:${'a'.repeat(64)}`, dispatch_generation: 2,
    execution_attempt_id: 'channel-attempt:fixture', fetch_contract: YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT, ...patch };
}

function stage(patch = {}) {
  const input = { targets: [{ video_id: 'fixture1234', ordinal: 1, reservation_id: randomUUID() }] };
  return { version: 1, task_id: randomUUID(), generation: 3, stage_id: randomUUID(),
    stage: 'details', sequence: 2, execution_hash: fullCrawlInputHash(execution()),
    input_hash: fullCrawlInputHash(input), target_hash: 'b'.repeat(64), input, ...patch };
}

test('full-crawl config requires an explicit mode and cannot masquerade as incremental', () => {
  const config = { version: 1, role: 'fullcrawl', mode: 'full_crawl_collect', slot: 'full-crawl-1',
    node_id: randomUUID(), deployment_id: randomUUID(), gateway_url: 'https://center.example/remote' };
  const parse = (patch = {}, mode = config.mode) => parseWorkerConfig(Buffer.from(JSON.stringify({ ...config, ...patch })), { mode });
  assert.equal(parse().role, 'fullcrawl');
  assert.equal(collectingWorkload(config.mode), FULL_CRAWL_WORKLOAD);
  assert.equal(FULL_CRAWL_WORKLOAD.queue, 'youtube-channel-crawl');
  assert.equal(collectingWorkload('unknown'), null);
  assert.throws(() => parseWorkerConfig(Buffer.from(JSON.stringify(config))), /NODE_CONFIG_INVALID/);
  for (const patch of [{ role: 'incremental' }, { slot: 'incremental-1' }, { slot: 'full-crawl-0' },
    { slot: 'full-crawl-01' }, { mode: 'incremental_collect' }, { gateway_url: 'https://user:password@center.example' }]) {
    assert.throws(() => parse(patch));
  }
  assert.throws(() => parse({}, 'incremental_collect'));
  assert.throws(() => parse({ role: 'incremental', mode: 'incremental_collect' }, 'incremental_collect'));
});

test('ordinary full snapshots retain their frozen v1/v2/v3 contracts and reject legacy, repair and incremental identities', () => {
  for (const fetch_contract of [YOUTUBEJS_FULL_CRAWL_V1_FETCH_CONTRACT, YOUTUBEJS_FULL_CRAWL_FETCH_CONTRACT, YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT]) {
    const value = execution({ fetch_contract });
    assert.equal(validateFullCrawlExecution(value), value);
  }
  for (const patch of [{ queue_name: 'youtube-channel-incremental' }, { job_name: 'channel-detail-repair' },
    { version: 2 }, { candidate_id: 0 }, { job_attempt: '1' }, { dispatch_generation: Number.MAX_SAFE_INTEGER + 1 },
    { fetch_contract: null }, { fetch_contract: 'youtubejs_full_v3' }, { fetch_contract: LEGACY_FULL_CRAWL_FETCH_CONTRACT },
    { fetch_contract: { ...YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT, contract_hash: 'sha256:wrong' } },
    { fetch_contract: { ...YOUTUBEJS_API_FULL_CRAWL_FETCH_CONTRACT, executor_version: '3' } },
    { plan_id: randomUUID() }, { intent_hash: 'unhashed' }, { job_id: '' }]) {
    assert.throws(() => validateFullCrawlExecution(execution(patch)));
  }
});

test('input hashes are stable across key ordering and reject ambiguous or oversized JSON', () => {
  assert.equal(fullCrawlInputHash({ b: [1, { c: true, a: null }], a: 'text' }),
    fullCrawlInputHash({ a: 'text', b: [1, { a: null, c: true }] }));
  for (const value of [{ x: undefined }, { x: NaN }, { x: Infinity }, new Date(), { x: 1n }, Array(2)]) {
    assert.throws(() => fullCrawlInputHash(value));
  }
  assert.throws(() => fullCrawlInputHash({ text: 'x'.repeat(FULL_CRAWL_LIMITS.inputBytes) }), { code: 'FULL_CRAWL_INPUT_TOO_LARGE' });
  let value = {}; for (let i = 0; i < 34; i++) value = { next: value };
  assert.throws(() => fullCrawlInputHash(value), { code: 'FULL_CRAWL_JSON_DEPTH' });
});

test('stages bind immutable input and require unique bounded detail reservations', () => {
  const value = stage();
  assert.equal(validateFullCrawlStage(value), value);
  for (const name of ['admission','uploads','close_fetch']) {
    const input = {};
    validateFullCrawlStage(stage({ stage: name, input, input_hash: fullCrawlInputHash(input), target_hash: name === 'close_fetch' ? 'a'.repeat(64) : null }));
  }
  for (const patch of [{ generation: 0 }, { stage: 'incremental' }, { target_hash: null },
    { input_hash: 'a'.repeat(64) }, { stage_id: value.stage_id.toUpperCase() }, { extra: true }]) {
    assert.throws(() => validateFullCrawlStage({ ...value, ...patch }));
  }
  for (const targets of [[], [value.input.targets[0], value.input.targets[0]],
    [value.input.targets[0], { ...value.input.targets[0], ordinal: 2, video_id: 'other' }],
    [{ ...value.input.targets[0], ordinal: 2 }, { ...value.input.targets[0], video_id: 'other', reservation_id: randomUUID() }],
    Array.from({ length: 21 }, (_, i) => ({ video_id: `video${i}`, ordinal: i + 1, reservation_id: randomUUID() }))]) {
    const input = { targets };
    assert.throws(() => validateFullCrawlStage({ ...value, input, input_hash: fullCrawlInputHash(input) }));
  }
});

test('batch manifest binds task, generation, stage and input hashes with exact bounded parts', () => {
  const command = stage();
  const batch = { version: 1, task_id: command.task_id, generation: command.generation,
    stage_id: command.stage_id, batch_id: randomUUID(), sequence: 1, input_hash: command.input_hash,
    target_hash: command.target_hash, payload_hash: 'c'.repeat(64), payload_bytes: 524289, part_count: 2 };
  assert.equal(validateFullCrawlBatch(batch, command), batch);
  for (const patch of [{ task_id: randomUUID() }, { generation: 4 }, { stage_id: randomUUID() },
    { input_hash: 'e'.repeat(64) }, { target_hash: null }, { payload_hash: 'unhashed' },
    { payload_bytes: 0 }, { payload_bytes: FULL_CRAWL_LIMITS.batchBytes + 1 }, { part_count: 1 }]) {
    assert.throws(() => validateFullCrawlBatch({ ...batch, ...patch }, command));
  }
});
