export const LOCAL_OFFLINE_AGENT_PROVIDER = "local-offline";

function provider(config) {
  return String(config?.provider ?? "").trim().toLocaleLowerCase("en");
}

export function isLocalOfflineAgentConfig(config) {
  return provider(config) === LOCAL_OFFLINE_AGENT_PROVIDER;
}

export function automaticLocalAgentConfigs(configs = []) {
  return configs.filter((config) => (
    config?.enabled !== false && isLocalOfflineAgentConfig(config)
  ));
}

export function explicitExternalAgentRequested(job) {
  return job?.data?.external_agent_opt_in === true;
}

export function selectAgentConfigForJob({
  job,
  requestedConfig = null,
  localConfig = null,
} = {}) {
  if (explicitExternalAgentRequested(job)) {
    if (!requestedConfig || isLocalOfflineAgentConfig(requestedConfig)) {
      throw new Error("explicit external Agent config is required");
    }
    return requestedConfig;
  }
  if (!localConfig || !isLocalOfflineAgentConfig(localConfig) || localConfig.enabled === false) {
    throw new Error("enabled local-offline Agent config is required for automatic execution");
  }
  return localConfig;
}
