import { buildAboutObservation } from '../aboutObservation.js';
import { recordAboutObservation } from '../aboutObservationStore.js';
import { ABOUT_CAPABILITY } from './aboutExtractor.js';
import { RemoteProtocolError } from './protocol.js';

export function createRemoteAboutIngestion({ assertBusinessFence }) {
  // No permissive default: the transport lease does not replace a business run fence.
  if (typeof assertBusinessFence !== 'function') throw new TypeError('transactional business fence required');
  return {
    [ABOUT_CAPABILITY]: async (client, { task, data, receivedAt }) => {
      const snapshot = data?.snapshot;
      const observedAt = Date.parse(data?.observed_at);
      const context = task.context;
      if (snapshot?.about_requested !== true || typeof snapshot.about_observed !== 'boolean'
        || !snapshot.metadata || snapshot.metadata.channel_id !== task.input.channel_id
        || typeof context.run_id !== 'string' || !context.run_id
        || typeof context.execution_attempt_id !== 'string' || !context.execution_attempt_id
        || !Number.isFinite(observedAt) || observedAt < new Date(task.created_at).getTime() - 300000
        || observedAt > new Date(receivedAt).getTime() + 300000) {
        throw new RemoteProtocolError('INVALID_ABOUT_RESULT', 400);
      }
      await assertBusinessFence(client, task);
      const command = buildAboutObservation(snapshot, {
        locale: context.locale || 'en',
        executionAttemptId: `${context.execution_attempt_id}:${task.task_id}:${task.generation}`,
        channelId: task.input.channel_id,
        runId: context.run_id,
        observedAt: new Date(observedAt).toISOString(),
        startedAt: task.created_at,
        crawlerVersion: context.crawler_version,
        planId: context.plan_id,
        planDay: context.plan_day,
        triggerReason: context.trigger_reason,
        scheduledAt: context.scheduled_at,
      });
      return recordAboutObservation(client, command);
    },
  };
}
