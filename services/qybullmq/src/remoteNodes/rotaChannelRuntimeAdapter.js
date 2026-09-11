import { withUploadsCountryExecution } from '../youtubeUploadsCountry.js';

// Fits the existing RotaSlotAdapter's runtime seam. That adapter continues to
// own claim/renew, BeginTask, observations, retry budgets and CompleteTask.
// The browser runs on the node. Optional session adapters freeze the original
// central profile and consume its checkpoint after actual network retirement.
export function createRemoteRotaChannelRuntime({ routes, nodeId, lease, slot, stopTimeoutMs = 45000,
  youtubeSessions = null, youtubeSession = null, youtubeCheckpointConsumer = null }) {
  if (Boolean(youtubeSessions) !== Boolean(youtubeSession)) throw new TypeError('central YouTube profile and session store required together');
  const quiesce = async handle => {
    handle.binding ??= await routes.bindingForExecution(nodeId, lease, slot);
    if (!handle.binding) return { active_managed_requests: 0 };
    await routes.requestStop(handle.binding.binding_id);
    const result = await routes.waitQuiesced(handle.binding.binding_id, { signal: AbortSignal.timeout(stopTimeoutMs) });
    if (youtubeSessions) handle.youtubeCheckpoint = await youtubeSessions.result(handle.binding.binding_id);
    if (youtubeCheckpointConsumer) await youtubeCheckpointConsumer.apply(handle.binding.binding_id);
    return result;
  };
  return {
    async acquire({ assignment, task, prepared, abortSignal }) {
      const fence = { slot_name: assignment.slot_name, worker_id: assignment.worker_id, worker_instance_id: assignment.worker_instance_id,
        lease_id: assignment.lease_id, route_generation: assignment.route_generation, task_id: task.task_id,
        business_run_id: prepared.businessRunId, job_execution_id: task.job_execution_id };
      const comparisons = { workload_scope: assignment.workload_scope,
        network_identity_key: assignment.network_identity_key, profile_epoch: assignment.profile_epoch,
        identity_policy_id: assignment.identity_policy_id, identity_policy_version: assignment.identity_policy_version,
        identity_policy_hash: assignment.identity_policy_hash, credential_generation: assignment.credential_generation,
        egress_country: assignment.egress_country ?? '' };
      // acquire must not publish a binding: its original failure path assumes
      // no requests started. Binding inside execute guarantees quiesce also
      // runs after a committed binding's response is lost.
      const handle = { binding: null, assignment, abortSignal,
        execute: async ({ job }, invoke) => {
          abortSignal?.throwIfAborted();
          const binding = youtubeSessions
            ? await youtubeSessions.bind({ nodeId, lease, slot, rotaFence: fence, expectedIdentity: comparisons, ...youtubeSession })
            : await routes.bind(nodeId, lease, slot, fence, comparisons);
          handle.binding = binding;
          abortSignal?.throwIfAborted();
          return withUploadsCountryExecution({ egressCountry: binding.identity.egress_country || null,
            recheck: job.data?.uploads_country_recheck }, invoke);
        } };
      return handle;
    },
    quiesce,
    // Node cleanup is durably acknowledged before either of these calls. The
    // central adapter must not claim to save the node's browser profile itself.
    checkpoint: quiesce,
    retire: quiesce,
  };
}
