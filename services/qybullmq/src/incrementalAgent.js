export async function enqueueIncrementalAgent({ plan, runId, agentBacklog }) {
  if (!agentBacklog || typeof agentBacklog.register !== "function") {
    throw new TypeError("agentBacklog.register is required");
  }
  const result = await agentBacklog.register({ plan, runId });
  if (result.skipped) {
    return {
      queued: false,
      skipped: true,
      reason: result.reason,
      plan_id: plan.plan_id,
      channel_id: plan.channel_id,
    };
  }
  return {
    queued: true,
    created: result.created,
    plan_id: plan.plan_id,
    channel_id: plan.channel_id,
  };
}
