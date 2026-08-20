import { promisify } from "node:util";
import { gzip } from "node:zlib";

const gzipAsync = promisify(gzip);

export function isCompressibleRawContent(contentType) {
  return /(?:html|json)/i.test(String(contentType ?? ""));
}

export async function prepareRawPayload(body, contentType, {
  enabled = true,
  minBytes = 1024,
  level = 6,
} = {}) {
  const source = Buffer.isBuffer(body) ? body : Buffer.from(body ?? "");
  if (!enabled || source.length < minBytes || !isCompressibleRawContent(contentType)) {
    return {
      body: source,
      contentEncoding: null,
      originalSizeBytes: source.length,
      storedSizeBytes: source.length,
    };
  }
  const compressed = await gzipAsync(source, { level });
  if (compressed.length >= source.length) {
    return {
      body: source,
      contentEncoding: null,
      originalSizeBytes: source.length,
      storedSizeBytes: source.length,
    };
  }
  return {
    body: compressed,
    contentEncoding: "gzip",
    originalSizeBytes: source.length,
    storedSizeBytes: compressed.length,
  };
}
