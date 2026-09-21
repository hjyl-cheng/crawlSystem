import { normalizeFullCrawlFetchContract } from '../fullCrawlFetchContract.js';
import { FULL_CRAWL_WORKLOAD } from './collectingWorkload.js';
import { RemoteProtocolError, generation, hash, uuid } from './protocol.js';

export const FULL_CRAWL_PROTOCOL_VERSION = 1;
export const FULL_CRAWL_STAGES = Object.freeze(['admission', 'uploads', 'details', 'close_fetch']);
export const FULL_CRAWL_LIMITS = Object.freeze({
  inputBytes: 64 * 1024,
  partBytes: 512 * 1024,
  batchBytes: 8 * 1024 * 1024,
  detailTargets: 20,
});

const fail = code => { throw new RemoteProtocolError(code, 400); };
const object = value => value && typeof value === 'object' && !Array.isArray(value)
  && Object.getPrototypeOf(value) === Object.prototype;

function fields(value, names) {
  if (!object(value) || Object.keys(value).some(key => !names.includes(key))
    || names.some(key => !Object.hasOwn(value, key))) fail('FULL_CRAWL_INVALID_FIELDS');
}

function text(value) {
  if (typeof value !== 'string' || !value.trim() || value !== value.trim() || value.length > 500) {
    fail('FULL_CRAWL_INVALID_TEXT');
  }
  return value;
}

function canonicalId(value) {
  if (uuid(value) !== value) fail('NON_CANONICAL_ID');
}

function digest(value) {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) fail('FULL_CRAWL_INVALID_HASH');
}

function canonical(value, depth = 0) {
  if (depth > 32) fail('FULL_CRAWL_JSON_DEPTH');
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value;
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (Array.isArray(value)) {
    if (Object.keys(value).length !== value.length) fail('FULL_CRAWL_INVALID_JSON');
    return value.map(item => canonical(item, depth + 1));
  }
  if (!object(value)) fail('FULL_CRAWL_INVALID_JSON');
  return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonical(value[key], depth + 1)]));
}

export function fullCrawlInputHash(value) {
  const bytes = Buffer.from(JSON.stringify(canonical(value)));
  if (bytes.length > FULL_CRAWL_LIMITS.inputBytes) fail('FULL_CRAWL_INPUT_TOO_LARGE');
  return hash(bytes);
}

// This validates immutable input, not live business ownership. The future
// full-crawl processor must lock candidate/run/attempt before using this input.
export function validateFullCrawlExecution(value) {
  fields(value, ['version', 'queue_name', 'job_name', 'job_id', 'job_attempt',
    'candidate_id', 'channel_id', 'run_id', 'business_run_id', 'business_run_key',
    'intent_hash', 'dispatch_generation', 'execution_attempt_id', 'fetch_contract']);
  if (value.version !== FULL_CRAWL_PROTOCOL_VERSION || value.queue_name !== FULL_CRAWL_WORKLOAD.queue
    || value.job_name !== 'channel-snapshot') fail('FULL_CRAWL_UNSUPPORTED_JOB');
  for (const key of ['job_id', 'channel_id', 'run_id', 'business_run_id', 'business_run_key', 'execution_attempt_id']) text(value[key]);
  for (const key of ['job_attempt', 'candidate_id', 'dispatch_generation']) generation(value[key]);
  if (!/^sha256:[a-f0-9]{64}$/.test(value.intent_hash)) fail('FULL_CRAWL_INVALID_HASH');
  // Require the complete frozen contract and its verified hash; no defaults or
  // implicit legacy conversion are valid across a node transport.
  fields(value.fetch_contract, ['executor_id', 'executor_version', 'contract_hash']);
  const contract = normalizeFullCrawlFetchContract(value.fetch_contract, { missingAsLegacy: false });
  if (contract.executor_id !== 'youtubejs_full') fail('FULL_CRAWL_LOCAL_COMPATIBILITY_REQUIRED');
  if (Object.keys(contract).some(key => value.fetch_contract[key] !== contract[key])) fail('FULL_CRAWL_NON_CANONICAL_CONTRACT');
  fullCrawlInputHash(value);
  return value;
}

export function validateFullCrawlStage(value) {
  fields(value, ['version', 'task_id', 'generation', 'stage_id', 'stage', 'sequence',
    'execution_hash', 'input_hash', 'target_hash', 'input']);
  if (value.version !== FULL_CRAWL_PROTOCOL_VERSION || !FULL_CRAWL_STAGES.includes(value.stage)) fail('FULL_CRAWL_INVALID_STAGE');
  for (const key of ['task_id', 'stage_id']) canonicalId(value[key]);
  generation(value.generation); generation(value.sequence);
  digest(value.execution_hash); digest(value.input_hash);
  if (!object(value.input) || value.input_hash !== fullCrawlInputHash(value.input)) fail('FULL_CRAWL_INPUT_HASH_CONFLICT');
  if (value.stage === 'details' || value.stage === 'close_fetch') digest(value.target_hash);
  else if (value.target_hash !== null) fail('FULL_CRAWL_UNEXPECTED_TARGET_HASH');
  if (value.stage === 'details') {
    const targets = value.input.targets;
    if (!Array.isArray(targets) || !targets.length || targets.length > FULL_CRAWL_LIMITS.detailTargets) fail('FULL_CRAWL_INVALID_TARGETS');
    const ids = new Set(); const reservations = new Set(); let previousOrdinal = 0;
    for (const target of targets) {
      if (!object(target)) fail('FULL_CRAWL_INVALID_TARGETS');
      text(target.video_id); generation(target.ordinal); canonicalId(target.reservation_id);
      if (ids.has(target.video_id) || reservations.has(target.reservation_id)) fail('FULL_CRAWL_DUPLICATE_TARGET');
      if (target.ordinal <= previousOrdinal) fail('FULL_CRAWL_TARGET_ORDER');
      ids.add(target.video_id); reservations.add(target.reservation_id); previousOrdinal = target.ordinal;
    }
  }
  fullCrawlInputHash(value);
  return value;
}

// Durable batch identity is independent of transport ACKs. Hash applies to the
// complete uncompressed bytes; receiving/applying parts remains P2/P3 work.
export function validateFullCrawlBatch(value, stage) {
  validateFullCrawlStage(stage);
  fields(value, ['version', 'task_id', 'generation', 'stage_id', 'batch_id', 'sequence',
    'input_hash', 'target_hash', 'payload_hash', 'payload_bytes', 'part_count']);
  if (value.version !== FULL_CRAWL_PROTOCOL_VERSION) fail('FULL_CRAWL_INVALID_BATCH');
  for (const key of ['task_id', 'stage_id', 'batch_id']) canonicalId(value[key]);
  for (const key of ['generation', 'sequence', 'payload_bytes', 'part_count']) generation(value[key]);
  digest(value.payload_hash);
  for (const key of ['task_id', 'generation', 'stage_id', 'input_hash', 'target_hash']) {
    if (value[key] !== stage[key]) fail('FULL_CRAWL_BATCH_IDENTITY_CONFLICT');
  }
  if (value.payload_bytes > FULL_CRAWL_LIMITS.batchBytes
    || value.part_count !== Math.ceil(value.payload_bytes / FULL_CRAWL_LIMITS.partBytes)) fail('FULL_CRAWL_BATCH_SIZE');
  return value;
}
