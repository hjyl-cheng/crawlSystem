import { createHash } from 'node:crypto';
import { promisify } from 'node:util';
import { gzip, gunzip } from 'node:zlib';

export const MAX_JSON_BYTES = 4 * 1024 * 1024;
export const MAX_GZIP_BYTES = 1024 * 1024;
const compress = promisify(gzip);
const decompress = promisify(gunzip);

export class RemoteProtocolError extends Error {
  constructor(code, status = 409) {
    super(code);
    this.code = code;
    this.status = status;
  }
}

export function uuid(value) {
  if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) {
    throw new RemoteProtocolError('INVALID_ID', 400);
  }
  return value.toLowerCase();
}

export function generation(value) {
  if (!Number.isSafeInteger(value) || value < 1) throw new RemoteProtocolError('INVALID_GENERATION', 400);
  return value;
}

export function hash(value) {
  return createHash('sha256').update(value).digest('hex');
}

export async function encodeResult(value) {
  const bytes = Buffer.from(JSON.stringify(value));
  if (bytes.length > MAX_JSON_BYTES) throw new RemoteProtocolError('RESULT_TOO_LARGE', 413);
  const compressed = await compress(bytes);
  if (compressed.length > MAX_GZIP_BYTES) throw new RemoteProtocolError('RESULT_TOO_LARGE', 413);
  return compressed;
}

export async function decodeResult(compressed) {
  if (compressed.length > MAX_GZIP_BYTES) throw new RemoteProtocolError('RESULT_TOO_LARGE', 413);
  let bytes;
  let value;
  try {
    bytes = await decompress(compressed, { maxOutputLength: MAX_JSON_BYTES });
    value = JSON.parse(bytes.toString('utf8'));
  } catch {
    throw new RemoteProtocolError('INVALID_COMPRESSED_RESULT', 400);
  }
  if (value?.version !== 1 || !['success', 'failure'].includes(value?.outcome)
    || (value.outcome === 'success' && (!value.data || typeof value.data !== 'object'))
    || (value.outcome === 'failure' && typeof value.error?.code !== 'string')) {
    throw new RemoteProtocolError('INVALID_RESULT', 400);
  }
  // Canonical IDs make the receipt comparison stable even after a lost response.
  if (uuid(value.batch_id) !== value.batch_id) throw new RemoteProtocolError('NON_CANONICAL_ID', 400);
  generation(value.generation);
  return { value, sha256: hash(bytes), compressed };
}
