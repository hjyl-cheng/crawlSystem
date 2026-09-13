import { canonicalIncrementalJson } from '../incrementalPlan.js';
import { hash, RemoteProtocolError } from './protocol.js';

export const WHOLE_CHANNEL_VERSION = 1;
export const WHOLE_CHANNEL_CHUNK_BYTES = 512 * 1024;
export const WHOLE_CHANNEL_MAX_BYTES = 32 * 1024 * 1024;
const fail = code => { throw new RemoteProtocolError(code, 400); };

export function wholeChannelBytes(value) {
  const bytes = Buffer.from(canonicalIncrementalJson(value));
  if (!bytes.length || bytes.length > WHOLE_CHANNEL_MAX_BYTES) throw new RemoteProtocolError('WHOLE_CHANNEL_TOO_LARGE', 413);
  return bytes;
}

export function wholeChannelParts(value) {
  const bytes = wholeChannelBytes(value);
  const manifest = { version: WHOLE_CHANNEL_VERSION, sha256: hash(bytes), bytes: bytes.length,
    parts: Math.ceil(bytes.length / WHOLE_CHANNEL_CHUNK_BYTES) };
  const parts = [];
  for (let part = 0; part < manifest.parts; part++) {
    const payload = bytes.subarray(part * WHOLE_CHANNEL_CHUNK_BYTES, (part + 1) * WHOLE_CHANNEL_CHUNK_BYTES);
    parts.push({ part, sha256: hash(payload), data: payload.toString('base64') });
  }
  return { manifest, parts };
}

export function validateWholeChannelPart(manifest, chunk) {
  if (manifest?.version !== WHOLE_CHANNEL_VERSION || !/^[a-f0-9]{64}$/.test(manifest.sha256)
      || !Number.isSafeInteger(manifest.bytes) || manifest.bytes < 1 || manifest.bytes > WHOLE_CHANNEL_MAX_BYTES
      || manifest.parts !== Math.ceil(manifest.bytes / WHOLE_CHANNEL_CHUNK_BYTES)
      || !Number.isSafeInteger(chunk?.part) || chunk.part < 0 || chunk.part >= manifest.parts
      || typeof chunk.data !== 'string' || chunk.data.length > Math.ceil(WHOLE_CHANNEL_CHUNK_BYTES / 3) * 4) fail('INVALID_WHOLE_CHANNEL_PART');
  const payload = Buffer.from(chunk.data, 'base64');
  const size = chunk.part === manifest.parts - 1
    ? manifest.bytes - chunk.part * WHOLE_CHANNEL_CHUNK_BYTES : WHOLE_CHANNEL_CHUNK_BYTES;
  if (payload.length !== size || payload.toString('base64') !== chunk.data || hash(payload) !== chunk.sha256) fail('WHOLE_CHANNEL_PART_CORRUPT');
  return payload;
}

export function decodeWholeChannelParts(manifest, parts) {
  if (parts.length !== manifest.parts) fail('WHOLE_CHANNEL_INCOMPLETE');
  const sorted = [...parts].sort((a, b) => a.part - b.part);
  if (sorted.some((part, index) => part.part !== index)) fail('WHOLE_CHANNEL_INCOMPLETE');
  const bytes = Buffer.concat(sorted.map(part => validateWholeChannelPart(manifest, part)));
  if (bytes.length !== manifest.bytes || hash(bytes) !== manifest.sha256) fail('WHOLE_CHANNEL_RESULT_CORRUPT');
  try { return JSON.parse(bytes); } catch { fail('WHOLE_CHANNEL_INVALID_JSON'); }
}
