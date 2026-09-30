import { Queue } from 'bullmq';
import { nanoid } from 'nanoid';
import { createCrawlSettingsLoader } from '../crawlSettings.js';
import { publishReadyDiscoveryPages } from '../discoveryPageWakeup.js';
import { getQueryScheduler } from '../queryScheduler.js';
import { queuesByRole } from '../queues.js';
import { RemoteCenterExecutionSupervisor } from './centerExecutionSupervisor.js';
import { createCenterDiscoverProcessor } from './centerDiscoverProcessor.js';
import { RemoteChannelRouteStore } from './channelRouteStore.js';
import { DISCOVER_WORKLOAD } from './collectingWorkload.js';
import { discoverBusinessRunId, discoverTaskFrom } from './discoverPageContract.js';
import { RemoteDiscoverPageStore } from './discoverPageStore.js';
import { recoverRemoteDiscoverSlot, remoteDiscoverSlotUnsettled } from './discoverRecovery.js';
import { RemoteManagedDiscoverRuntime } from './managedDiscoverRuntime.js';

// discoverSchema.sql must be applied before a center accepts Discover nodes.
export async function assertDiscoverSchema(query) {
  const rows = (await query(`SELECT conname,pg_get_constraintdef(oid) AS definition FROM pg_constraint
    WHERE conname IN ('worker_connections_workload_check','channel_commands_operation_check')`)).rows;
  const definition = name => rows.find(row => row.conname === name)?.definition ?? '';
  if (!definition('worker_connections_workload_check').includes(DISCOVER_WORKLOAD.mode)
    || !definition('channel_commands_operation_check').includes('collect_search_page')) {
    throw new Error('DISCOVER_SCHEMA_REQUIRED');
  }
}

// Assembles the center side of remote Discover: its task store, routes and
// one supervisor for the Discover slots. Queue consumers start with start().
export function createDiscoverCenterRuntime({ store, guardPool, connection, prefix, dashboardManaged = false,
  allowedNodeIds = [], activation, rotaClient, resolvedPolicy, privateKey, secretKey, readRotaRoute, putRawObject,
  language, country, report = () => {}, QueueClass = Queue, WorkerClass = undefined }) {
  const pages = new RemoteDiscoverPageStore({ store });
  const routes = new RemoteChannelRouteStore({ channelStore: pages,
    assertBusinessFence: (client, task) => pages.assertBusinessFence(client, task),
    businessRunId: task => discoverBusinessRunId(discoverTaskFrom(task).page_id),
    readRotaRoute, privateKey, secretKey });
  const query = store.pool.query.bind(store.pool);
  const withTransaction = action => store.transaction(action);
  const queueOptions = { connection, prefix };
  const queues = { [queuesByRole.channelCrawl]: new QueueClass(queuesByRole.channelCrawl, queueOptions) };
  const discoverQueue = new QueueClass(queuesByRole.discoverPage, queueOptions);
  const getCrawlSettings = createCrawlSettingsLoader({ query });
  // Same wakeup as pipelineV2's signalReadyDiscoveryPageQualifications, with
  // this center's database pool and queue connection.
  const signalReadyPages = async ({ candidateId = null, pageId = null } = {}) => {
    try {
      return await publishReadyDiscoveryPages({ query, queue: discoverQueue, candidateId, pageId });
    } catch (error) {
      report({ event: 'discovery_page_wakeup_failed', candidate_id: candidateId, page_id: pageId, error: error?.message || String(error) });
      return [];
    }
  };
  const supervisor = new RemoteCenterExecutionSupervisor({ mode: DISCOVER_WORKLOAD.mode, store, channelStore: pages, routes,
    activation, guardPool, connection, prefix, allowedNodeIds, dashboardManaged, resolvedPolicy, rotaClient,
    proxyBaseUrl: 'http://unused-center.invalid:8000', proxyPassword: 'remote-transport-only', report,
    ...(WorkerClass ? { WorkerClass } : {}),
    createRuntime: ({ nodeId, slot, assertAdmission }) => new RemoteManagedDiscoverRuntime({ pages, routes, nodeId, slot, assertAdmission }),
    createProcessor: ({ runtime, rota, ready }) => createCenterDiscoverProcessor({ query, withTransaction, runtime, rota,
      resolvedPolicy, ready, queues, putRawObject, getCrawlSettings, getQueryScheduler: () => getQueryScheduler(query),
      signalReadyPages, language, country, demoChannelId: () => nanoid(8), report }),
    recoverSlot: recoverRemoteDiscoverSlot, slotUnsettled: remoteDiscoverSlotUnsettled, settleHandoffs: false });
  return {
    pages, routes, supervisor,
    // RemoteWorkerActivationStore's dedicated-workload execution hooks.
    execution: {
      verifyExecution: (client, row) => supervisor.verifyExecution(client, row),
      assertTask: (client, task) => pages.assertBusinessFence(client, task),
    },
    attachSignals(signals) { pages.transportSignals = signals; },
    start() { supervisor.start(); },
    async close() {
      await supervisor.stop();
      await Promise.all([...Object.values(queues), discoverQueue].map(queue => queue.close()));
    },
  };
}
