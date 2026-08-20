function messageOf(error) {
  return String(error?.message ?? error ?? "").toLowerCase();
}

export function classifyAgentFailure(error) {
  const message = messageOf(error);
  const status = Number(error?.status) || Number(message.match(/agent http (\d{3})/)?.[1]) || null;
  if (status === 401 || status === 403) {
    return { kind: "authentication", status, retryable: false, splittable: false };
  }
  if (status === 429) {
    return { kind: "rate_limit", status, retryable: true, splittable: false };
  }
  if ([408, 409, 425].includes(status)) {
    return { kind: "transient_http", status, retryable: true, splittable: false };
  }
  if (status != null && status >= 500) {
    return { kind: "upstream", status, retryable: true, splittable: false };
  }
  if (/local profile snapshot is stale/.test(message)) {
    return { kind: "stale_snapshot", status, retryable: true, splittable: false };
  }
  if (/timeout|timed out|abort|fetch failed|network|socket|econnreset|eai_again/.test(message)) {
    return { kind: "transport", status, retryable: true, splittable: false };
  }
  if (
    status === 413
    || /context(?: window| length)?|maximum context|too many tokens|token limit|request too large|payload too large/.test(message)
  ) {
    return { kind: "context_limit", status, retryable: false, splittable: true };
  }
  if (
    /parseable json|json response must be an array|empty response|response incomplete|returned \d+\/\d+|truncated|unexpected end|unterminated string|(?:expected|unexpected token).+in json(?: at position|$)|in json at position \d+/.test(message)
  ) {
    return { kind: "invalid_response", status, retryable: true, splittable: true };
  }
  return { kind: "permanent", status, retryable: false, splittable: false };
}

export function agentRetryDelayMs(attempt, baseMs = 1000, random = Math.random) {
  const exponent = Math.max(0, Number(attempt || 1) - 1);
  const ceiling = Math.min(30000, Math.max(250, Number(baseMs) || 1000) * (2 ** exponent));
  return Math.round(ceiling * (0.5 + random()));
}
