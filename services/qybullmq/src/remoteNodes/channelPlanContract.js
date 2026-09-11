import { incrementalPlanHash, validateIncrementalJob, validateIncrementalPlan } from '../incrementalPlan.js';
import { RemoteProtocolError } from './protocol.js';

export const CHANNEL_PLAN_CAPABILITY = 'youtube.incremental.plan.v1';

export function remoteChannelPlan(job) {
  const plan = validateIncrementalJob(job);
  return {
    route: plan.task_mask.about || plan.task_mask.video ? 'remote' : 'central',
    plan,
    planHash: incrementalPlanHash(plan),
    workKey: `incremental-plan:${plan.plan_id}:${plan.dispatch_generation}`,
  };
}

export function planFromTask(task) {
  if (task.capability !== CHANNEL_PLAN_CAPABILITY) throw new RemoteProtocolError('NOT_A_CHANNEL_PLAN', 400);
  const plan = validateIncrementalPlan(task.input?.plan);
  if (task.context && incrementalPlanHash(plan) !== task.context.plan_hash) {
    throw new RemoteProtocolError('PLAN_HASH_CONFLICT');
  }
  return plan;
}

export function assertChannelOperation(plan, operation, input) {
  const allowed = operation === 'open_channel'
    ? input.channel_id === plan.channel_id && input.options?.includeAbout === plan.task_mask.about
    : operation === 'scan_uploads' || operation === 'video_detail' ? plan.task_mask.video : false;
  if (!allowed) throw new RemoteProtocolError('OPERATION_OUTSIDE_PLAN', 400);
}
