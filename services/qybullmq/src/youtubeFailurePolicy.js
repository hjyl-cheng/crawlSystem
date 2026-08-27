const POSTGRES_CONTRACT_CODES = new Set([
  "22001",
  "22P02",
  "23502",
  "23503",
  "23505",
  "23514",
  "42804",
]);

const STRUCTURED_FAILURE_KINDS = new Set([
  "proxy_transport",
  "upstream_transient",
]);

const STRUCTURED_FAILURE_CODES = new Map([
  ["FINGERPRINT_PROXY_TRANSPORT", "proxy_transport"],
  ["FINGERPRINT_UPSTREAM_TRANSIENT", "upstream_transient"],
]);

function normalizedStatus(value, text) {
  const numeric = Number(value);
  if (Number.isInteger(numeric) && numeric >= 100 && numeric <= 599) return numeric;
  const match = String(text || "").match(/(?:http(?: error)?\s*)?\b([1-5]\d\d)\b/i);
  return match ? Number(match[1]) : null;
}

function boundedText(value, maxLength = 500) {
  const output = String(value ?? "");
  return output.length <= maxLength ? output : output.slice(0, maxLength);
}

function nonEmptyText(value) {
  const output = String(value ?? "").trim();
  return output || null;
}

function failureObjects(error) {
  const values = [];
  const pending = [error];
  const seen = new Set();
  while (pending.length > 0 && seen.size < 20) {
    const value = pending.shift();
    if (!value || (typeof value !== "object" && typeof value !== "function") || seen.has(value)) {
      continue;
    }
    seen.add(value);
    values.push(value);
    if (value.cause != null) pending.push(value.cause);
    if (Array.isArray(value.errors)) pending.push(...value.errors);
  }
  return values;
}

function structuredFailure(error) {
  const values = failureObjects(error);
  for (const value of values) {
    const failureKind = nonEmptyText(value.failureKind ?? value.failure_kind)?.toLowerCase() ?? null;
    const code = nonEmptyText(value.code)?.toUpperCase() ?? null;
    const kindFromField = STRUCTURED_FAILURE_KINDS.has(failureKind) ? failureKind : null;
    const kindFromCode = STRUCTURED_FAILURE_CODES.get(code) ?? null;
    if (!kindFromField && !kindFromCode) continue;
    if (kindFromField && kindFromCode && kindFromField !== kindFromCode) continue;
    const matchedKind = kindFromCode ?? kindFromField;
    const source = nonEmptyText(
      value?.youtube_failure_evidence?.source ?? value?.source,
    );
    return Object.freeze({
      failure_kind: matchedKind,
      code: kindFromCode ? code : null,
      source,
    });
  }
  return null;
}

export function youtubeFailureText(error) {
  const parts = [];
  const pending = [error];
  const seen = new Set();
  while (pending.length > 0 && seen.size < 20) {
    const value = pending.shift();
    if (value === null || value === undefined) continue;
    if (typeof value !== "object" && typeof value !== "function") {
      const normalized = String(value).trim();
      if (normalized) parts.push(normalized);
      continue;
    }
    if (seen.has(value)) continue;
    seen.add(value);
    const message = String(value.message ?? "").trim();
    const code = String(value.code ?? "").trim();
    if (message) parts.push(message);
    if (code) parts.push(code);
    if (value.cause != null) pending.push(value.cause);
    if (Array.isArray(value.errors)) pending.push(...value.errors);
  }
  return boundedText([...new Set(parts)].join(": "), 2000);
}

function decision(kind, {
  retryMode = "default",
  proxyAction = "none",
  clientAction = "none",
  terminal = false,
  status = null,
  evidence = null,
} = {}) {
  return Object.freeze({
    kind,
    retry_mode: retryMode,
    proxy_action: proxyAction,
    client_action: clientAction,
    terminal,
    status,
    ...(evidence ? { evidence: Object.freeze({ ...evidence }) } : {}),
  });
}

export function youtubeFailureEvidence(error, overrides = {}) {
  const values = failureObjects(error);
  const embeddedValues = values.map((value) => value?.youtube_failure_evidence ?? {});
  const embeddedStatus = embeddedValues.find((value) => value.status != null)?.status ?? null;
  const embeddedBody = embeddedValues.map((value) => nonEmptyText(value.body)).find(Boolean) ?? "";
  const embeddedSource = embeddedValues.map((value) => nonEmptyText(value.source)).find(Boolean)
    ?? values.map((value) => nonEmptyText(value.source)).find(Boolean)
    ?? "";
  const embeddedTargetUrl = embeddedValues
    .map((value) => nonEmptyText(value.target_url))
    .find(Boolean) ?? "";
  const embeddedClient = embeddedValues.map((value) => nonEmptyText(value.client)).find(Boolean) ?? "";
  return {
    error,
    status: overrides.status ?? embeddedStatus ?? error?.status ?? null,
    body: boundedText(nonEmptyText(overrides.body) ?? embeddedBody ?? error?.body ?? ""),
    source: nonEmptyText(overrides.source) ?? embeddedSource,
    target_url: nonEmptyText(overrides.targetUrl ?? overrides.target_url)
      ?? nonEmptyText(embeddedTargetUrl),
    client: nonEmptyText(overrides.client) ?? nonEmptyText(embeddedClient),
  };
}

export function annotateYoutubeFailure(error, evidence = {}) {
  const target = error instanceof Error ? error : new Error(String(error ?? "YouTube request failed"));
  const current = target.youtube_failure_evidence ?? {};
  target.youtube_failure_evidence = {
    ...current,
    status: evidence.status ?? current.status ?? null,
    body: boundedText(evidence.body ?? current.body ?? ""),
    source: String(evidence.source ?? current.source ?? ""),
    target_url: String(evidence.targetUrl ?? evidence.target_url ?? current.target_url ?? "") || null,
    client: String(evidence.client ?? current.client ?? "") || null,
  };
  return target;
}

export function decideYoutubeFailure({
  error = null,
  status = null,
  body = "",
  source = "",
} = {}) {
  const evidence = youtubeFailureEvidence(error, { status, body, source });
  const errorText = youtubeFailureText(error);
  const text = [errorText, evidence.body].filter(Boolean).join("\n");
  const lower = text.toLowerCase();
  const httpStatus = normalizedStatus(evidence.status, text);
  const errorName = String(error?.name || "");
  const errorCode = String(error?.code || error?.cause?.code || "").toUpperCase();
  const structured = structuredFailure(error);

  if (error?.youtube_collection_failure === true) {
    return decision("youtube_challenge", {
      retryMode: "new_identity",
      proxyAction: "cooldown_challenge",
      status: httpStatus,
    });
  }
  if (errorName === "ParserContractError" || errorName === "IncrementalPlanContractError") {
    return decision("parser_contract", { retryMode: "none", terminal: true, status: httpStatus });
  }
  if (POSTGRES_CONTRACT_CODES.has(errorCode)) {
    return decision("database_contract", { retryMode: "none", terminal: true, status: httpStatus });
  }
  if (structured?.failure_kind === "proxy_transport") {
    return decision("proxy_transport", {
      retryMode: "new_identity",
      proxyAction: "cooldown_network",
      status: httpStatus,
      evidence: structured,
    });
  }
  if (structured?.failure_kind === "upstream_transient") {
    return decision("upstream_transient", {
      retryMode: "same_identity",
      status: httpStatus,
      evidence: structured,
    });
  }
  if (
    httpStatus === 404
    || /(?:channel|video|playlist)[^\n]{0,100}(?:does not exist|not found)/i.test(text)
    || /private video|video is private|has been removed|members[- ]only|subscriber[- ]only|does not have a .* tab/i.test(text)
  ) {
    return decision("content_terminal", { retryMode: "none", terminal: true, status: httpStatus });
  }

  const proxyTransport = /proxyerror|proxy[_ ]unavailable|proxy connection|tunnel connection|socks(?:4|5)? connection|wrong[_ ]version[_ ]number|ssl routines|proxy authentication|\b407\b/i
    .test(text);
  if (proxyTransport) {
    return decision("proxy_transport", {
      retryMode: "new_identity",
      proxyAction: "cooldown_network",
      status: httpStatus,
    });
  }
  if (httpStatus === 429 || /too many requests|rate limit(?:ed)?/i.test(text)) {
    return decision("youtube_rate_limited", {
      retryMode: "new_identity",
      proxyAction: "cooldown_rate_limit",
      status: httpStatus,
    });
  }
  if (/sorry\/index|detected unusual traffic|automated quer|not a bot|bot challenge|captcha|verify you are human/i.test(text)) {
    return decision("youtube_challenge", {
      retryMode: "new_identity",
      proxyAction: "cooldown_challenge",
      status: httpStatus,
    });
  }
  if (
    httpStatus === 403
    || /po[_ -]?token|proof of origin|serviceintegritydimensions|login required|sign in|not available on this app|client.*(?:unsupported|not supported)/i.test(text)
  ) {
    return decision("token_or_client", {
      retryMode: "route_or_token",
      clientAction: "refresh_or_fallback",
      status: httpStatus,
    });
  }
  if (
    (httpStatus != null && httpStatus >= 500)
    || /\b(?:408|425)\b|timeout|timed out|abort|fetch failed|failed to extract any player response|no player response|network|socket|econn|eai_again|connection refused|connection reset|temporary failure in name resolution|name or service not known|dns|remote end closed/i.test(lower)
  ) {
    return decision("upstream_transient", { retryMode: "same_identity", status: httpStatus });
  }
  if (/ytinitialdata not found|parse|invalid json|unexpected token/i.test(lower)) {
    return decision("parser_runtime", { retryMode: "default", status: httpStatus });
  }
  if (
    /^[0-9A-Z]{5}$/.test(errorCode)
    || (/database|postgres|sql|constraint/i.test(lower) && evidence.source.includes("database"))
  ) {
    return decision("database_runtime", { retryMode: "same_identity", status: httpStatus });
  }
  return decision("unknown", { retryMode: "default", status: httpStatus });
}

export function shouldReportProxyFailure(value) {
  return decideYoutubeFailure(value).proxy_action !== "none";
}
