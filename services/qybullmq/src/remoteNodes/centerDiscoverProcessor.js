import { terminateExhaustedBusinessRun } from '../businessRunBudgetRecovery.js';
import { describeChannelCandidateWorkerFailure } from '../channelCandidateWorkerLifecycle.js';
import { deferJobForSlotPause } from '../channelJobDeferral.js';
import { youtubeErrorText } from '../detailPolicy.js';
import {
  createDiscoverPageProcessor,
  persistDiscoverParserContractFailure,
  persistDiscoverRetryCheckpoint,
  recordDiscoverPageTerminalFailure,
} from '../discoverPageProcessor.js';
import { isParserContractError, parserContractDetails } from '../localizedParsing.js';
import { executeManagedWorkerAttempt } from '../managedWorkerExecution.js';
import { isStaleExecutionFailure, processManagedWorkerJob, retryableSystemFailureDecision } from '../managedWorkerJob.js';
import { ProxyBusinessRunPreparer } from '../proxyBusinessRun.js';
import { applyFailureRetryDecision } from '../queues.js';
import { decideYoutubeFailure } from '../youtubeFailurePolicy.js';

const message = error => String(error?.message ?? error ?? 'unknown error');
const entityKey = job => {
  const value = job?.data?.channel_id ?? job?.data?.query_id ?? job?.data?.content_key ?? null;
  return value == null ? null : String(value);
};

// The same managed attempt, page processor and failure bookkeeping as a local
// Discover worker. Only the page's single YouTube request runs on the node.
export function createCenterDiscoverProcessor({ query, withTransaction, runtime, rota, resolvedPolicy, ready,
  queues, putRawObject, getCrawlSettings, getQueryScheduler, signalReadyPages, language, country,
  demoChannelId, delayMs = 5000, report = () => {} }) {
  const preparer = new ProxyBusinessRunPreparer({ queryFn: query, withTransaction, resolvedPolicy });
  const processDiscoverPage = createDiscoverPageProcessor({
    query, queues, putRawObject, getCrawlSettingsV2: getCrawlSettings, getQueryScheduler,
    signalReadyDiscoveryPageQualifications: signalReadyPages,
    requestPage: request => runtime.requestPage(request), language, country, demoChannelId,
  });
  const taskEvent = (job, status, payload, errorMessage = null) => query(`INSERT INTO crawler.task_events (
      queue_name, job_id, job_name, entity_key, status, payload_json, error_message
    ) VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)`,
  [job.queueName, job.id == null ? null : String(job.id), job.name == null ? null : String(job.name),
    entityKey(job), status, JSON.stringify(payload ?? {}), errorMessage]);
  const remote = { remote_node_id: runtime.nodeId, remote_slot: runtime.slot };

  async function execute(job, prepared) {
    const startedAt = Date.now();
    await job.updateProgress({ stage: 'started', started_at: new Date(startedAt).toISOString(), ...remote });
    await taskEvent(job, 'started', { ...(job.data ?? {}), ...remote });
    let result;
    try {
      result = await processDiscoverPage(job, prepared?.page ?? null);
    } catch (error) {
      const failureDecision = retryableSystemFailureDecision(error) ?? decideYoutubeFailure({ error });
      error.youtube_failure_decision = failureDecision;
      if (isParserContractError(error)) {
        try {
          await persistDiscoverParserContractFailure(query, job, {
            message: youtubeErrorText(error), details: parserContractDetails(error),
          });
        } catch (persistenceError) {
          report({ event: 'parser_contract_persistence_failed', job_id: job.id, error: message(persistenceError) });
        }
      }
      applyFailureRetryDecision(job, failureDecision);
      throw error;
    }
    const finishedAt = Date.now();
    await job.updateProgress({ stage: 'completed', finished_at: new Date(finishedAt).toISOString(), ...remote });
    await taskEvent(job, 'completed', { ...result, ...remote, duration_ms: finishedAt - startedAt });
    return result;
  }

  // BullMQ's failed event for this slot's consumer, as recordFailedJob does locally.
  async function recordFailed(job, error) {
    const failure = describeChannelCandidateWorkerFailure(error);
    try {
      await taskEvent(job, 'failed', {
        ...(job?.data ?? {}),
        ...(failure.parserDetails ? { parser_contract_error: failure.parserDetails } : {}),
        youtube_failure_decision: failure.failureDecision,
        ...remote,
      }, failure.message);
      await recordDiscoverPageTerminalFailure(query, job, {
        message: failure.message, parserFailure: failure.parserFailure, parserDetails: failure.parserDetails,
        failureDecision: failure.failureDecision, permanentFailure: failure.permanentFailure,
        attemptsMade: Number(job?.attemptsMade ?? 0), maxAttempts: Math.max(1, Number(job?.opts?.attempts ?? 1)),
      });
    } catch (eventError) {
      report({ event: 'task_event_failed', job_id: job?.id, error: message(eventError) });
    }
  }

  const process = async (job, token) => {
    if (!await ready()) return deferJobForSlotPause(job, token);
    try {
      return await processManagedWorkerJob({
        job,
        token,
        execute: () => rota.executeJob(job, {
          prepare: () => preparer.prepareDiscover(job),
          executeAttempt: (prepared, attempt) => executeManagedWorkerAttempt({
            job,
            prepared,
            attempt,
            execute: () => execute(job, prepared),
            persistRetryableCheckpoint: ({ error, failure }) => persistDiscoverRetryCheckpoint(query, {
              job, prepared, failure, message: message(error).slice(0, 2000),
            }),
          }),
        }),
        terminateBusinessRun: (currentJob, error) => terminateExhaustedBusinessRun(withTransaction, currentJob, error),
        deferForSlotPause: deferJobForSlotPause,
        defaultDelayMs: delayMs,
        onDeferred: event => report({ event: 'rota_job_deferred', ...event, ...remote }),
      });
    } catch (error) {
      if (error?.code === 'REMOTE_SUPERVISOR_NOT_READY') return deferJobForSlotPause(job, token);
      if (isStaleExecutionFailure(error)) job.discard();
      report({ event: 'remote_discover_failed', node_id: runtime.nodeId, slot: runtime.slot, job_id: job.id,
        code: error?.code ?? error?.name });
      throw error;
    }
  };
  process.recordFailed = recordFailed;
  return process;
}
