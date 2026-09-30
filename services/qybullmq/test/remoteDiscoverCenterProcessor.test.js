import assert from 'node:assert/strict';
import test from 'node:test';
import { DelayedError } from 'bullmq';

import { resolveWorkerIdentityPolicy } from '../src/identityPolicyCatalog.js';
import { annotateYoutubeFailure } from '../src/youtubeFailurePolicy.js';
import { createCenterDiscoverProcessor } from '../src/remoteNodes/centerDiscoverProcessor.js';

const resolvedPolicy = resolveWorkerIdentityPolicy({ role: 'discover', policyId: 'qy-br-discover-anonymous-v1',
  expectedWorkloadScope: 'qy-production' });
const policy = resolvedPolicy.policy;

function searchHtml(viewCountText = '1,234 views') {
  const data = { contents: { items: [{ videoRenderer: { videoId: 'v1', title: { runs: [{ text: 'video' }] },
    ownerText: { runs: [{ text: 'owner', navigationEndpoint: { browseEndpoint: { browseId: 'UCnewchannel00000000000a', canonicalBaseUrl: '/@owner' } } }] },
    viewCountText: { simpleText: viewCountText } } }] } };
  return `<html><script>var ytInitialData = ${JSON.stringify(data)};</script>`
    + '<script>ytcfg.set({"INNERTUBE_API_KEY":"k","INNERTUBE_CONTEXT":{"client":{"clientName":"WEB"}}});</script></html>';
}

function harness({ requestPage, ready = true, attemptsMade = 0 } = {}) {
  const calls = [];
  const events = [];
  const pageRow = { page_id: 'page-1', page_intent_hash: 'h1', managed_fetch_status: 'pending', dispatch_status: 'dispatched',
    status: 'queued', identity_policy_id: policy.id, identity_policy_version: policy.version, identity_policy_hash: policy.hash,
    query_text: 'receitas', query_id: 42, page_no: 1, dispatch_batch_id: 'batch-1', request_language: 'pt-BR',
    request_country: 'BR', priority: 100, result_json: { managed_intent: { pipeline_cycle_id: 'cycle-1' } } };
  const query = async (sql, params = []) => {
    const text = sql.replace(/\s+/g, ' ').trim();
    calls.push({ text, params });
    // Task events carry seven parameters; network events from the page fetch carry five.
    if (text.includes('INSERT INTO crawler.task_events') && params.length === 7) {
      events.push({ status: params[4], payload: JSON.parse(params[5]), error: params[6] });
    }
    if (text.startsWith('SELECT * FROM crawler.query_pages WHERE page_id=$1')) return { rows: [pageRow] };
    if (text.startsWith("UPDATE crawler.query_pages SET status='running'")) return { rowCount: 1, rows: [] };
    if (text.startsWith('UPDATE crawler.query_pages SET managed_fetch_status=\'failed\'')) return { rowCount: 1, rows: [{ page_id: 'page-1' }] };
    if (text.includes('INSERT INTO crawler.channel_candidates')) {
      return { rows: JSON.parse(params[0]).map((spec, index) => ({ candidate_id: String(100 + index), channel_id: spec.channel_id,
        channel_url: spec.channel_url, status: spec.candidate_status, priority: spec.priority })) };
    }
    if (text.startsWith("UPDATE crawler.channel_candidates SET status='queued'")) {
      return { rows: params[0].map(candidate_id => ({ candidate_id, snapshot_dispatch_generation: 1 })) };
    }
    return { rowCount: 0, rows: [] };
  };
  const attempts = [];
  const rota = {
    // The real RotaSlotAdapter contract: prepare, then one managed attempt.
    executeJob: async (job, { prepare, executeAttempt }) => {
      const prepared = await prepare();
      const result = await executeAttempt(prepared, { resumeMode: 'initial', egressCountry: null });
      attempts.push(result);
      return result.result ?? result;
    },
  };
  const requests = [];
  const runtime = { nodeId: 'node-1', slot: 'discover-1', requestPage: async input => { requests.push(input); return requestPage(input); } };
  const delayed = [];
  const job = { id: 'job-1', name: 'discover-page', queueName: 'youtube-discover-page', data: { page_id: 'page-1', query_id: 42 },
    attemptsMade, attemptsStarted: attemptsMade + 1, opts: { attempts: 3 },
    updateProgress: async () => {}, moveToDelayed: async (at) => { delayed.push(at); }, discard() { this.discarded = true; } };
  const processor = createCenterDiscoverProcessor({ query, withTransaction: async action => action({ query }), runtime, rota,
    resolvedPolicy, ready: async () => ready,
    queues: { 'youtube-channel-crawl': { addBulk: async () => {}, add: async () => {} } },
    putRawObject: async () => {}, getCrawlSettings: async () => ({ minSubscriberCount: 1000, discoverStopMinQualifiedRatio: 0.2 }),
    getQueryScheduler: async () => ({ status: 'running' }), signalReadyPages: async () => [], language: 'pt-BR', country: 'BR',
    demoChannelId: () => 'demo' });
  const find = prefix => calls.filter(({ text }) => text.startsWith(prefix) || text.includes(prefix));
  return { processor, calls, events, attempts, requests, job, delayed, find };
}

const okPage = body => async input => ({ kind: input.kind, url: 'https://www.youtube.com/results?search_query=receitas',
  status: 200, rawText: body, rawContentType: 'text/html; charset=utf-8' });

test('a remote page runs the local managed attempt and records remote task events', async () => {
  const h = harness({ requestPage: okPage(searchHtml()) });
  const result = await h.processor(h.job, 'token');
  assert.equal(result.candidate_count, 1);
  assert.equal(result.qualification_pending, true);
  assert.deepEqual(h.requests, [{ kind: 'initial', queryText: 'receitas', language: 'pt', country: 'BR', ytConfig: null, continuation: null }]);
  assert.deepEqual(h.events.map(event => event.status), ['started', 'completed']);
  assert.equal(h.events[0].payload.remote_node_id, 'node-1');
  assert.equal(h.events[1].payload.remote_slot, 'discover-1');
  assert.equal(h.attempts[0].kind, 'managed_work_complete');
  assert.equal(h.attempts[0].businessState, 'waiting_downstream');
});

test('a retryable remote failure persists the local route-switch checkpoint', async () => {
  const challenge = annotateYoutubeFailure(Object.assign(new Error('YouTube bot challenge HTTP 200'), { status: 200 }),
    { status: 200, body: "Sign in to confirm you're not a bot", source: 'youtube_text' });
  const h = harness({ requestPage: async () => { throw challenge; } });
  await h.processor(h.job, 'token');
  assert.equal(h.attempts[0].kind, 'retryable_network_failure');
  assert.equal(h.attempts[0].observation, 'youtube_challenge');
  assert.equal(h.attempts[0].failedStage, 'discover_page');
  const checkpoint = h.find("UPDATE crawler.query_pages SET managed_fetch_status='failed'")[0];
  assert.deepEqual(checkpoint.params, ['page-1', 'youtube_challenge', 'YouTube bot challenge HTTP 200']);
  assert.equal(h.find("UPDATE crawler.query_pages SET status = 'failed'").length, 1, 'the fetch failure is recorded as locally');
});

test('parser drift in a remote page is persisted as a terminal parser failure', async () => {
  const h = harness({ requestPage: okPage(searchHtml('not-a-count views')) });
  const error = await h.processor(h.job, 'token').then(() => null, value => value);
  assert.ok(error, 'parser drift fails the job');
  assert.equal(error.name, 'ParserContractError');
  const parser = h.find("dispatch_reason='parser_contract_error'");
  assert.equal(parser.length, 1);
  assert.equal(parser[0].params[0], 'page-1');
  assert.equal(error.youtube_failure_decision.retry_mode, 'none');
});

test('a slot that is not ready defers the job without contacting Rota', async () => {
  const h = harness({ requestPage: okPage(searchHtml()), ready: false });
  await assert.rejects(h.processor(h.job, 'token'), error => error instanceof DelayedError);
  assert.equal(h.delayed.length, 1);
  assert.equal(h.requests.length, 0);
  assert.equal(h.attempts.length, 0);
});

test('BullMQ failure marks the page terminal only on its last attempt', async () => {
  const failure = Object.assign(new Error('timeout 20000ms'), { name: 'TimeoutError' });
  const early = harness({ requestPage: okPage(searchHtml()), attemptsMade: 1 });
  await early.processor.recordFailed(early.job, failure);
  assert.deepEqual(early.events.map(event => event.status), ['failed']);
  assert.equal(early.find('dispatch_status=\'terminal\',dispatch_reason=$4').length, 0);

  const last = harness({ requestPage: okPage(searchHtml()), attemptsMade: 3 });
  await last.processor.recordFailed(last.job, failure);
  const terminal = last.find("dispatch_status='terminal',dispatch_reason=$4");
  assert.equal(terminal.length, 1);
  assert.equal(terminal[0].params[0], 'page-1');
});
