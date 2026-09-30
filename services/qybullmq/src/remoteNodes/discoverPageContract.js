import { DISCOVER_WORKLOAD } from './collectingWorkload.js';
import { RemoteProtocolError } from './protocol.js';

export const DISCOVER_PAGE_CAPABILITY = DISCOVER_WORKLOAD.capability;
export const DISCOVER_PAGE_OPERATION = 'collect_search_page';
// The result envelope is JSON-encoded before gzip; leave room below the
// protocol's 4 MiB JSON limit for escaping and the envelope itself.
export const DISCOVER_PAGE_MAX_RAW_BYTES = 3 * 1024 * 1024;

const invalid = (code = 'INVALID_DISCOVER_REQUEST') => { throw new RemoteProtocolError(code, 400); };
const text = (value, max) => typeof value === 'string' && value.length > 0 && value.length <= max;

// Same identifier Rota already charges for a local Discover page.
export const discoverBusinessRunId = pageId => `discover-page:${pageId}`;
export const discoverWorkKey = pageId => `discover-page:${pageId}`;
export const discoverAttemptId = rotaTaskId => `discover-attempt:${rotaTaskId}`;

// The exact input of youtube.js discoverSearchPageRequest. It is frozen by the
// center; the node never derives or rewrites any part of the request.
export function assertDiscoverRequest(request) {
  if (!request || typeof request !== 'object' || Array.isArray(request)) invalid();
  const allowed = ['kind', 'queryText', 'language', 'country', 'ytConfig', 'continuation'];
  if (Object.keys(request).some(key => !allowed.includes(key))) invalid();
  if (!text(request.queryText, 1000) || !text(request.language, 35) || !text(request.country, 8)) invalid();
  if (request.ytConfig != null && (typeof request.ytConfig !== 'object' || Array.isArray(request.ytConfig))) invalid();
  if (request.kind === 'initial') {
    // The initial request ignores ytConfig; only a continuation token is wrong.
    if (request.continuation != null) invalid();
  } else if (request.kind === 'continuation') {
    if (!text(request.continuation, 20000) || !request.ytConfig || typeof request.ytConfig !== 'object'
      || !text(request.ytConfig.apiKey, 200)) invalid();
  } else invalid();
  return request;
}

export function discoverTaskInput({ pageId, pageIntentHash, request }) {
  if (!text(pageId, 500) || !text(pageIntentHash, 200)) invalid();
  return { page_id: pageId, page_intent_hash: pageIntentHash, request: assertDiscoverRequest(request) };
}

export function discoverTaskFrom(task) {
  if (task?.capability !== DISCOVER_PAGE_CAPABILITY) throw new RemoteProtocolError('DISCOVER_TASK_REQUIRED');
  const input = task.input ?? {};
  discoverTaskInput({ pageId: input.page_id, pageIntentHash: input.page_intent_hash, request: input.request });
  return input;
}

// A node returns the raw page; the center alone parses it. Only the fields
// parseDiscoverSearchPage reads are accepted, for the exact request kind.
export function discoverPageFromWire(data, request) {
  const allowed = ['kind', 'url', 'status', 'rawText', 'rawContentType'];
  if (!data || typeof data !== 'object' || Object.keys(data).some(key => !allowed.includes(key))) {
    invalid('INVALID_DISCOVER_RESULT');
  }
  if (data.kind !== request.kind || !Number.isSafeInteger(data.status) || data.status < 100 || data.status > 599
    || typeof data.rawText !== 'string' || Buffer.byteLength(data.rawText) > DISCOVER_PAGE_MAX_RAW_BYTES
    || !text(data.rawContentType, 200)) invalid('INVALID_DISCOVER_RESULT');
  let url;
  try { url = new URL(data.url); } catch { invalid('INVALID_DISCOVER_RESULT'); }
  if (url.protocol !== 'https:' || !['www.youtube.com', 'm.youtube.com', 'youtube.com'].includes(url.hostname)) {
    invalid('INVALID_DISCOVER_RESULT');
  }
  return { kind: data.kind, url: data.url, status: data.status, rawText: data.rawText, rawContentType: data.rawContentType };
}
