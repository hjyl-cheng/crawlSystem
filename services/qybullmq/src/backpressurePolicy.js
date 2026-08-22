export function discoveryPressureReason(metrics, limits) {
  if (metrics.channelBacklog >= limits.pauseChannelBacklog) return "channel_backlog_high";
  if (metrics.channelEtaSeconds != null && metrics.channelEtaSeconds >= limits.pauseChannelEtaSeconds) {
    return "channel_eta_high";
  }
  if (
    metrics.channelTerminalSamples >= limits.minimumFailureSamples
    && metrics.channelFailureRate >= limits.pauseChannelFailureRate
  ) return "channel_failure_rate_high";
  if (metrics.proxyCooldownRatio >= limits.pauseProxyCooldownRatio) return "proxy_cooldown_ratio_high";
  if (metrics.detailBacklog >= limits.pauseDetailBacklog) return "content_detail_backlog_high";
  if (metrics.dataApiBacklog >= limits.pauseDataApiBacklog) return "youtube_api_backlog_high";
  if (metrics.agentBacklog >= limits.pauseAgentBacklog) return "agent_backlog_high";
  return null;
}

export function discoveryPressureRecovered(metrics, limits) {
  const failureRecovered = metrics.channelTerminalSamples < limits.minimumFailureSamples
    || metrics.channelFailureRate <= limits.resumeChannelFailureRate;
  return metrics.channelBacklog <= limits.resumeChannelBacklog
    && (metrics.channelEtaSeconds == null || metrics.channelEtaSeconds <= limits.resumeChannelEtaSeconds)
    && failureRecovered
    && metrics.proxyCooldownRatio <= limits.resumeProxyCooldownRatio
    && metrics.detailBacklog <= limits.resumeDetailBacklog
    && metrics.dataApiBacklog <= limits.resumeDataApiBacklog
    && metrics.agentBacklog <= limits.resumeAgentBacklog;
}

export function discoveryPressureRecoveredForReason(reason, metrics, limits) {
  switch (reason) {
    case "channel_backlog_high":
      return metrics.channelBacklog <= limits.resumeChannelBacklog;
    case "channel_eta_high":
      return metrics.channelEtaSeconds == null
        || metrics.channelEtaSeconds <= limits.resumeChannelEtaSeconds;
    case "channel_failure_rate_high":
      return metrics.channelTerminalSamples < limits.minimumFailureSamples
        || metrics.channelFailureRate <= limits.resumeChannelFailureRate;
    case "proxy_cooldown_ratio_high":
      return metrics.proxyCooldownRatio <= limits.resumeProxyCooldownRatio;
    case "content_detail_backlog_high":
      return metrics.detailBacklog <= limits.resumeDetailBacklog;
    case "youtube_api_backlog_high":
      return metrics.dataApiBacklog <= limits.resumeDataApiBacklog;
    case "agent_backlog_high":
      return metrics.agentBacklog <= limits.resumeAgentBacklog;
    default:
      return discoveryPressureRecovered(metrics, limits);
  }
}

export function proxyUnavailableRatio(capacity = {}) {
  const active = Number(capacity.active);
  const cooldown = Number(capacity.cooldown);
  if (Number.isFinite(active) && Number.isFinite(cooldown) && active + cooldown > 0) {
    return Math.max(0, Math.min(1, cooldown / (active + cooldown)));
  }
  const total = Number(capacity.total);
  if (Number.isFinite(active) && Number.isFinite(total) && total > 0) {
    return Math.max(0, Math.min(1, (total - active) / total));
  }
  return 0;
}
