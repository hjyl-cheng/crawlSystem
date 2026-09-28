import { createRequire } from 'node:module';
import { normalizeQueryScheduler } from './queryScheduler.js';
import { ProxyControlClient } from './proxyControlClient.js';
import { normalizeRotaCapacity } from './rotaCapacity.js';
import { operatorQueueNames, operatorDiscoveryState } from './operatorQueuePolicy.js';

const require = createRequire(import.meta.url);
// Reuse the installed BullMQ script, including its priority/delay markers.
const { pause } = require('bullmq/dist/cjs/scripts/pause-7.js');
export const MIGRATION_QUEUE_WAKE = 'crawler:migration-queue-control:v1';
export const migrationQueueNames = new Set([
  'youtube-discover-page', 'youtube-channel-crawl', 'youtube-content-detail',
  'youtube-data-api-batch', 'youtube-agent-batch', 'youtube-finalize',
]);
async function bounded(operation) {
  let timer;
  try {
    return await Promise.race([operation, new Promise((_, reject) => {
      timer = setTimeout(() => reject(new Error('QUEUE_CONTROL_REDIS_TIMEOUT')), 2000);
    })]);
  } finally { clearTimeout(timer); }
}
const guardedPause = `
local previous = redis.call('HGET', KEYS[3], 'migrationControlRevision')
local revision = ARGV[2]
if previous and (#previous > #revision or (#previous == #revision and previous > revision)) then return -1 end
redis.call('HSET', KEYS[3], 'migrationControlRevision', revision, 'migrationControlReason', ARGV[3])
local paused = redis.call('HEXISTS', KEYS[3], 'paused') == 1
if paused == (ARGV[1] == 'paused') then return 0 end
${pause.content}
return 1
`;

export async function applyFencedQueueState(queue, { paused, revision, reason }) {
  if (!/^\d+$/.test(String(revision))) throw new Error('INVALID_QUEUE_CONTROL_REVISION');
  const client = await bounded(queue.client);
  const args = queue.scripts.pauseArgs(paused);
  return bounded(client.eval(guardedPause, pause.keys, ...args, String(revision), reason));
}

export function migrationQueuePolicy({ scheduler, batch, pressure, recoveryQueues = [], apiEnabled = true,
  apiDemand = false, inlineDetails = true, detailHigh = 100, detailLow = 40, channelPaused = false,
  discoveryEnabled = false, discoveryPaused = true, env = {} }) {
  const active = ['preparing', 'running', 'pausing', 'stopping'].includes(batch.status)
    && !['paused', 'stopped'].includes(scheduler.status);
  // Completion stops producers; consumers remain open and wait for work.
  // Explicit operator stops and pauses are still authoritative.
  const completedIdle = batch.status === 'completed' && scheduler.status === 'stopped'
    && scheduler.stop_reason === 'pipeline_complete';
  const consumersEnabled = active || completedIdle;
  const result = {};
  const set = (name, paused, reason) => { result[name] = { paused, reason }; };
  result['youtube-discover-page'] = operatorDiscoveryState({ enabled: discoveryEnabled, completedIdle,
    pressure, paused: discoveryPaused, env });
  const crawl = 'youtube-channel-crawl';
  if (!consumersEnabled) set(crawl, true, `migration_${batch.status}`);
  else if (!Number.isFinite(pressure.channelReady)) set(crawl, true, 'proxy_capacity_unavailable');
  else if (pressure.channelReady === 0) set(crawl, true, 'proxy_capacity_low');
  else if (!inlineDetails && (pressure.detailBacklog >= detailHigh
    || (channelPaused && pressure.detailBacklog > detailLow))) set(crawl, true, 'content_detail_backlog_high');
  else set(crawl, false, completedIdle ? 'migration_idle' : 'migration_active');
  set('youtube-content-detail', (inlineDetails && !(completedIdle && recoveryQueues.includes('youtube-content-detail'))) || !consumersEnabled
    || !Number.isFinite(pressure.detailReady) || pressure.detailReady === 0,
  inlineDetails ? 'details_run_inside_channel_queue' : 'migration_detail_policy');
  set('youtube-agent-batch', !consumersEnabled, completedIdle ? 'migration_idle' : active ? 'migration_active' : 'migration_inactive');
  const apiWanted = apiEnabled && (active || completedIdle) && (completedIdle || apiDemand || recoveryQueues.includes('youtube-data-api-batch'));
  set('youtube-data-api-batch', !apiWanted, apiWanted ? (completedIdle ? 'migration_idle' : 'migration_api_demand') : 'migration_api_not_required');
  set('youtube-finalize', false, 'database_finalize_recovery');
  return result;
}

export function createMigrationPressureReader({ catalog, queues, env = process.env }) {
  const rota = new ProxyControlClient({ timeoutMs: 1500, maxAttempts: 1 });
  const backlog = name => bounded(queues[name].getJobCounts('waiting', 'active', 'delayed', 'prioritized', 'paused'))
    .then(counts => Object.values(counts).reduce((a, b) => a + b, 0));
  return async () => {
    const at = Date.now();
    const inline = env.YOUTUBE_CHANNEL_INLINE_DETAILS !== 'false';
    const [capacity, detail, channel, agent, dataApi] = await Promise.all([
      rota.capacity().then(value => normalizeRotaCapacity(value, { catalog })).catch(() => null),
      inline ? 0 : backlog('youtube-content-detail'),
      backlog('youtube-channel-crawl'), backlog('youtube-agent-batch'), backlog('youtube-data-api-batch'),
    ]);
    return { at, channelReady: capacity?.roles?.channel?.ready ?? null,
      detailReady: capacity?.roles?.detail?.ready ?? null, detailBacklog: detail,
      discoverReady: capacity?.roles?.discover?.ready ?? null,
      channelBacklog: channel, agentBacklog: agent, dataApiBacklog: dataApi };
  };
}

export function createMigrationQueueControl({ withTransaction, queues, loadPressure,
  recoveryQueues = () => [], env = process.env, report = () => {} }) {
  const snapshot = async client => {
    await client.query("SET LOCAL lock_timeout='1000ms'");
    await client.query("SET LOCAL statement_timeout='3000ms'");
    await client.query("SET LOCAL idle_in_transaction_session_timeout='5000ms'");
    const row = (await client.query("SELECT value_json FROM crawler.settings WHERE setting_key='query_scheduler' FOR UPDATE")).rows[0];
    if (!row) throw new Error('QUERY_SCHEDULER_MISSING');
    const scheduler = normalizeQueryScheduler(row.value_json);
    const batch = (await client.query('SELECT batch_id,status,version FROM crawler.migration_control_batches WHERE batch_id=$1',
      [scheduler.pipeline_cycle_id])).rows[0];
    return { scheduler, batch };
  };
  const revision = async client => (await client.query("SELECT nextval('crawler.migration_queue_control_revision')::text AS revision")).rows[0].revision;
  const operatorPauses = async client => (await client.query(
    "SELECT value_json FROM crawler.settings WHERE setting_key='migration_operator_queue_pauses'",
  )).rows[0]?.value_json ?? {};
  return {
    async setOperatorPaused(queueName, paused) {
      if (!operatorQueueNames.includes(queueName) || typeof paused !== 'boolean') throw new Error('INVALID_OPERATOR_QUEUE_CONTROL');
      // Share the reconciler's scheduler lock and Redis revision fence, so a
      // previously calculated automatic resume cannot overwrite this intent.
      const managed = await withTransaction(async client => {
        const { scheduler, batch } = await snapshot(client);
        if (queueName === 'youtube-discover-page' && !paused && batch
          && !(batch.status === 'completed' && scheduler.status === 'stopped' && scheduler.stop_reason === 'pipeline_complete')) {
          throw Object.assign(new Error('当前迁移或调度状态不允许恢复发现队列，请先完成迁移；恢复队列不会启动 Query 调度。'), { statusCode: 409 });
        }
        const pauses = await operatorPauses(client);
        if (paused || queueName === 'youtube-discover-page') pauses[queueName] = paused;
        else delete pauses[queueName];
        await client.query(`INSERT INTO crawler.settings(setting_key,value_json)
          VALUES('migration_operator_queue_pauses',$1::jsonb) ON CONFLICT(setting_key)
          DO UPDATE SET value_json=EXCLUDED.value_json,updated_at=now()`, [JSON.stringify(pauses)]);
        if (paused || !batch) {
          const changed = await applyFencedQueueState(queues[queueName], {
            paused, revision: await revision(client), reason: paused ? 'operator_paused' : 'operator_resumed',
          });
          if (changed < 0 || await bounded(queues[queueName].isPaused()) !== paused) throw new Error('QUEUE_CONTROL_NOT_APPLIED');
        }
        return Boolean(batch);
      });
      await this.request();
      // Only the controller has capacity credentials. It applies a released hold
      // on the next wake/tick, retaining all migration and capacity constraints.
      return { paused: await bounded(queues[queueName].isPaused()), ...(!paused && managed ? { pending: true } : {}) };
    },
    async reconcile() {
      const pressure = await loadPressure();
      return withTransaction(async client => {
        const { scheduler, batch } = await snapshot(client);
        if (!batch) return { managed: false };
        if (Date.now() - pressure.at > 5000) throw new Error('QUEUE_PRESSURE_SAMPLE_EXPIRED');
        const api = (await client.query("SELECT value_json FROM crawler.settings WHERE setting_key='youtube_api'")).rows[0]?.value_json;
        const apiEnabled = (api?.fallback_mode ?? env.YOUTUBE_DATA_API_FALLBACK_MODE) !== 'disabled';
        const queuedApi = await bounded(queues['youtube-data-api-batch'].getJobCounts('waiting', 'active', 'paused', 'prioritized', 'delayed'));
        const apiDemand = apiEnabled && (Object.values(queuedApi).some(count => count > 0) || (await client.query("SELECT EXISTS(SELECT 1 FROM crawler.youtube_api_detail_requests WHERE status='pending') AS pending")).rows[0].pending);
        const pauses = await operatorPauses(client);
        const plan = migrationQueuePolicy({ scheduler, batch, pressure, recoveryQueues: recoveryQueues(), apiEnabled, apiDemand,
          discoveryEnabled: pauses['youtube-discover-page'] === false, env,
          discoveryPaused: await bounded(queues['youtube-discover-page'].isPaused()),
          inlineDetails: env.YOUTUBE_CHANNEL_INLINE_DETAILS !== 'false', detailHigh: Number(env.CHANNEL_PAUSE_DETAIL_BACKLOG || 100),
          detailLow: Number(env.CHANNEL_RESUME_DETAIL_BACKLOG || 40), channelPaused: await bounded(queues['youtube-channel-crawl'].isPaused()) });
        for (const name of Object.keys(plan)) {
          if (pauses[name] === true) plan[name] = { paused: true, reason: 'operator_paused' };
        }
        const version = await revision(client);
        const actions = [];
        for (const [name, state] of Object.entries(plan)) {
          const changed = await applyFencedQueueState(queues[name], { ...state, revision: version });
          if (changed < 0) throw new Error('QUEUE_CONTROL_SUPERSEDED');
          if (await bounded(queues[name].isPaused()) !== state.paused) throw new Error('QUEUE_CONTROL_NOT_APPLIED');
          if (changed) actions.push({ queue: name, ...state });
        }
        const result = { managed: true, batch_id: batch.batch_id, batch_version: batch.version,
          batch_status: batch.status, revision: version, applied_at: new Date().toISOString(), queues: plan };
        await client.query(`INSERT INTO crawler.settings(setting_key,value_json) VALUES('migration_queue_control',$1::jsonb)
          ON CONFLICT(setting_key) DO UPDATE SET value_json=EXCLUDED.value_json,updated_at=now()`, [JSON.stringify(result)]);
        if (actions.length) report({ event: 'migration_queue_control_applied', ...result, actions });
        return result;
      });
    },
    async legacy({ queueName, paused, reason, actions, expectedScheduler }) {
      if (!migrationQueueNames.has(queueName)) return false;
      await withTransaction(async client => {
        const { scheduler, batch } = await snapshot(client);
        // Managed queues are exclusively reconciled from fresh durable state.
        // Neither a slow main tick nor recovery callbacks may bypass that owner.
        if (batch) return;
        if (!expectedScheduler || JSON.stringify(scheduler) !== JSON.stringify(normalizeQueryScheduler(expectedScheduler))) {
          report({ event: 'queue_control_stale_decision', queue: queueName, reason });
          return;
        }
        if ((await operatorPauses(client))[queueName] === true) {
          paused = true;
          reason = 'operator_paused';
        }
        const changed = await applyFencedQueueState(queues[queueName], { paused, reason, revision: await revision(client) });
        if (changed > 0) actions.push({ action: paused ? 'pause' : 'resume', queue: queueName, reason });
      });
      return true;
    },
    async request() {
      // Batch state is already committed. Failure must not look like a failed
      // batch creation: the independent reconciler retries the same intent.
      try {
        const client = await bounded(queues['youtube-channel-crawl'].client);
        await bounded(client.publish(MIGRATION_QUEUE_WAKE, 'changed'));
        return { pending: true };
      } catch (error) {
        report({ event: 'migration_queue_control_pending', error: error.message });
        return { pending: true, reason: error.message };
      }
    },
  };
}
