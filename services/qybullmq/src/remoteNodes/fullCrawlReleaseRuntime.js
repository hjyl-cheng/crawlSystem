import {Queue} from 'bullmq';
import {createFullCrawlCenter} from './fullCrawlCenter.js';
import {fullCrawlReleaseConfig} from './fullCrawlReleaseConfig.js';
import {createFullCrawlHandoff} from '../fullCrawlHandoff.js';
import {defaultJobOptions, queuesByRole} from '../queues.js';
import {createVideoDetailApiFallback} from '../videoDetailApiFallback.js';
import {createYoutubeApiSettingsLoader} from '../youtubeApiSettings.js';

export async function createFullCrawlReleaseRuntime({env = process.env, controlToken, profileSecret,
  store, guardPool, resolvedPolicy, rotaClient, transportOptions, report = () => {}}) {
  const config = fullCrawlReleaseConfig(env, {controlToken, profileSecret});
  const {createWorkerRuntime} = await import('../worker.js');
  const compatibility = createWorkerRuntime({embedded: true});
  const queues = new Map();
  const outbound = name => {
    if (!queues.has(name)) queues.set(name, new Queue(name, {
      connection: config.connection, prefix: config.prefix, defaultJobOptions,
    }));
    return queues.get(name);
  };
  const query = store.pool.query.bind(store.pool);
  const handoff = createFullCrawlHandoff({query, withTransaction: action => store.transaction(action),
    discoveryQueue: () => outbound(queuesByRole.discoverPage), finalizeQueue: () => outbound(queuesByRole.finalize)});
  const loadSettings = createYoutubeApiSettingsLoader({query});
  const createApiFallback = env.YOUTUBEJS_VIDEO_API_BATCH_FALLBACK === 'true'
    ? args => createVideoDetailApiFallback({...args, loadSettings}) : null;
  let center, budgetGuard, closing;
  const close = () => closing ??= (async () => {
    try {
      await center?.supervisor.stop();
      await compatibility.shutdown('full_crawl_center_stop');
      await Promise.all([...queues.values()].map(queue => queue.close()));
    } finally {
      if (budgetGuard) {
        await budgetGuard.query('SELECT pg_advisory_unlock(781138015,1)').catch(() => {});
        budgetGuard.release(); budgetGuard = null;
      }
    }
  })();
  try {
    center = createFullCrawlCenter({store, guardPool, ...config, resolvedPolicy, profileSecret,
      rotaClient, proxyBaseUrl: 'http://unused-center.invalid:8000', proxyPassword: 'remote-transport-only',
      transportOptions, handoff, compatibility, createApiFallback, report});
    return {...center, image: env.REMOTE_NODE_FULL_CRAWL_IMAGE, execution: center.supervisor,
      async start() {
        // The explicit total is shared by release processes, including the
        // local compatibility slot. A rival must not reserve a second budget.
        budgetGuard = await guardPool.connect();
        budgetGuard.on('error', () => {
          report({event: 'remote_full_crawl_budget_guard_lost'});
          void close().catch(() => report({event: 'remote_full_crawl_shutdown_failed'}));
        });
        if (!(await budgetGuard.query('SELECT pg_try_advisory_lock(781138015,1) AS owned')).rows[0].owned) {
          budgetGuard.release(); budgetGuard = null;
          throw new Error('FULL_CRAWL_RELEASE_ALREADY_RUNNING');
        }
        await compatibility.startCompatibility();
        center.supervisor.start();
        report({event: 'remote_full_crawl_execution_started', total_slots: config.totalSlots,
          remote_slots: config.maxSlots, compatibility_slots: config.compatibilitySlots});
      }, close};
  } catch (error) {
    await close();
    throw error;
  }
}
