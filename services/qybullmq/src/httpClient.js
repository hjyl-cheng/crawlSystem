import { Agent, fetch as undiciFetch } from "undici";

function positiveInteger(value, fallback, minimum = 1) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  return Number.isFinite(parsed) ? Math.max(minimum, parsed) : fallback;
}

export function persistentHttpAgentOptions(env = process.env) {
  return {
    connections: positiveInteger(env.HTTP_CLIENT_CONNECTIONS, 8),
    pipelining: 1,
    keepAliveTimeout: positiveInteger(env.HTTP_KEEP_ALIVE_TIMEOUT_MS, 60000, 1000),
    keepAliveMaxTimeout: positiveInteger(env.HTTP_KEEP_ALIVE_MAX_TIMEOUT_MS, 300000, 1000),
    headersTimeout: positiveInteger(env.HTTP_HEADERS_TIMEOUT_MS, 300000, 1000),
    bodyTimeout: positiveInteger(env.HTTP_BODY_TIMEOUT_MS, 300000, 1000),
  };
}

const directAgent = new Agent(persistentHttpAgentOptions());

export function persistentFetch(url, init = {}) {
  return undiciFetch(url, {
    ...init,
    dispatcher: init.dispatcher ?? directAgent,
  });
}

export async function closePersistentHttpClient() {
  await directAgent.close();
}
