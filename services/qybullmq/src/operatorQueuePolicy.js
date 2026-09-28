export const operatorQueueNames = Object.freeze(['youtube-channel-crawl', 'youtube-discover-page']);

// Opening the discovery consumer does not start Query scheduling. During a
// controlled migration it can only wait for work after natural completion.
export function operatorDiscoveryState({ enabled, completedIdle, pressure, paused, env = {} }) {
  const hold = reason => ({ paused: true, reason });
  if (!enabled || !completedIdle) return hold('controlled_migration');
  if (!Number.isFinite(pressure.discoverReady)) return hold('proxy_capacity_unavailable');
  if (pressure.discoverReady <= 0) return hold('proxy_capacity_low');
  for (const [metric, highKey, highDefault, lowKey, lowDefault, reason] of [
    ['channelBacklog', 'DISCOVER_PAUSE_CHANNEL_BACKLOG', 100, 'DISCOVER_RESUME_CHANNEL_BACKLOG', 40, 'channel_backlog_high'],
    ['detailBacklog', 'DISCOVER_PAUSE_DETAIL_BACKLOG', 80, 'DISCOVER_RESUME_DETAIL_BACKLOG', 30, 'content_detail_backlog_high'],
    ['dataApiBacklog', 'DATA_API_PAUSE_DISCOVER_BACKLOG', 20, 'DATA_API_RESUME_DISCOVER_BACKLOG', 8, 'youtube_api_backlog_high'],
    ['agentBacklog', 'AGENT_PAUSE_DISCOVER_BACKLOG', 10, 'AGENT_RESUME_DISCOVER_BACKLOG', 4, 'agent_backlog_high'],
  ]) {
    if (!Number.isFinite(pressure[metric])) return hold('queue_pressure_unavailable');
    const high = Number(env[highKey] ?? highDefault), low = Number(env[lowKey] ?? lowDefault);
    if (pressure[metric] >= high || (paused && pressure[metric] > low)) return hold(reason);
  }
  return { paused: false, reason: 'operator_resumed' };
}
