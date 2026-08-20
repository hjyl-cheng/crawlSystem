import { createHash } from "node:crypto";
import http from "node:http";
import https from "node:https";
import { Client } from "minio";
import { query } from "./db.js";
import { prepareRawPayload } from "./rawCompression.js";

const rawStoreVideoWatchHtml = String(process.env.RAW_STORE_VIDEO_WATCH_HTML || "false").toLowerCase() === "true";
const rawStoreGzip = String(process.env.RAW_STORE_GZIP || "true").toLowerCase() !== "false";
const rawStoreGzipMinBytes = Math.max(0, Number(process.env.RAW_STORE_GZIP_MIN_BYTES || 1024));

let minioClient = null;
let minioTransportAgent = null;
let bucketReady = null;

function s3Config() {
  const endpointRaw = String(process.env.S3_ENDPOINT || "").trim();
  const bucket = String(process.env.S3_BUCKET || "crawler-raw").trim();
  const accessKey = String(process.env.S3_ACCESS_KEY || "").trim();
  const secretKey = String(process.env.S3_SECRET_KEY || "").trim();
  const region = String(process.env.S3_REGION || "local").trim();
  if (!endpointRaw || !bucket || !accessKey || !secretKey) return null;

  const url = endpointRaw.startsWith("http://") || endpointRaw.startsWith("https://")
    ? new URL(endpointRaw)
    : new URL(`http://${endpointRaw}`);
  return {
    endpoint: url.hostname,
    port: url.port ? Number(url.port) : (url.protocol === "https:" ? 443 : 80),
    useSSL: url.protocol === "https:",
    bucket,
    accessKey,
    secretKey,
    region,
  };
}

function client() {
  const cfg = s3Config();
  if (!cfg) return null;
  if (!minioClient) {
    const Agent = cfg.useSSL ? https.Agent : http.Agent;
    minioTransportAgent = new Agent({
      keepAlive: true,
      keepAliveMsecs: Math.max(1000, Number(process.env.S3_KEEP_ALIVE_MS || 30000)),
      maxSockets: Math.max(1, Number(process.env.S3_MAX_SOCKETS || 8)),
      maxFreeSockets: Math.max(1, Number(process.env.S3_MAX_FREE_SOCKETS || 2)),
    });
    minioClient = new Client({
      endPoint: cfg.endpoint,
      port: cfg.port,
      useSSL: cfg.useSSL,
      accessKey: cfg.accessKey,
      secretKey: cfg.secretKey,
      region: cfg.region,
      transportAgent: minioTransportAgent,
    });
  }
  return minioClient;
}

export function closeStorage() {
  minioTransportAgent?.destroy();
  minioTransportAgent = null;
  minioClient = null;
  bucketReady = null;
}

async function ensureBucket() {
  if (bucketReady) return bucketReady;
  bucketReady = (async () => {
    const cfg = s3Config();
    const cli = client();
    if (!cfg || !cli) return null;
    const exists = await cli.bucketExists(cfg.bucket).catch(() => false);
    if (!exists) await cli.makeBucket(cfg.bucket, cfg.region);
    return cfg.bucket;
  })().catch((error) => {
    bucketReady = null;
    throw error;
  });
  return bucketReady;
}

function serializePayload(payload, contentType) {
  if (Buffer.isBuffer(payload)) return payload;
  if (typeof payload === "string") return Buffer.from(payload);
  if (/json/i.test(contentType)) return Buffer.from(JSON.stringify(payload ?? {}, null, 2));
  return Buffer.from(String(payload ?? ""));
}

function safePathPart(value, fallback = "unknown") {
  const out = String(value ?? "")
    .trim()
    .replace(/[^a-zA-Z0-9_.@-]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return out || fallback;
}

function extensionFor(contentType) {
  if (/html/i.test(contentType)) return "html";
  if (/json/i.test(contentType)) return "json";
  if (/text/i.test(contentType)) return "txt";
  return "bin";
}

function objectKey({ objectType, entityType, entityId, contentType, hash }) {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = String(now.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(now.getUTCDate()).padStart(2, "0");
  const ts = now.toISOString().replace(/[:.]/g, "-");
  const ext = extensionFor(contentType);
  return [
    "youtube",
    safePathPart(entityType, "entity"),
    safePathPart(objectType, "raw"),
    String(yyyy),
    mm,
    dd,
    safePathPart(entityId),
    `${ts}-${hash.slice(0, 12)}.${ext}`,
  ].join("/");
}

export function shouldStoreVideoWatchHtml() {
  return rawStoreVideoWatchHtml;
}

export async function putRawObject({
  objectType,
  entityType,
  entityId,
  source,
  payload,
  contentType = "application/json; charset=utf-8",
  metadata = {},
}) {
  const cfg = s3Config();
  const cli = client();
  if (!cfg || !cli || payload == null) return null;

  await ensureBucket();
  const sourceBody = serializePayload(payload, contentType);
  const hash = createHash("sha256").update(sourceBody).digest("hex");
  const key = objectKey({ objectType, entityType, entityId, contentType, hash });
  const stored = await prepareRawPayload(sourceBody, contentType, {
    enabled: rawStoreGzip,
    minBytes: rawStoreGzipMinBytes,
  });
  const objectMetadata = {
    "Content-Type": contentType,
    "X-Amz-Meta-Content-Sha256": hash,
    "X-Amz-Meta-Original-Size-Bytes": String(stored.originalSizeBytes),
    "X-Amz-Meta-Stored-Size-Bytes": String(stored.storedSizeBytes),
  };
  if (stored.contentEncoding) objectMetadata["Content-Encoding"] = stored.contentEncoding;
  await cli.putObject(cfg.bucket, key, stored.body, stored.storedSizeBytes, objectMetadata);
  const objectPath = `${cfg.bucket}/${key}`;
  const row = await query(
    `INSERT INTO crawler.raw_objects (
       bucket, object_key, object_path, object_type, entity_type, entity_id,
       source, content_type, content_encoding, content_hash, size_bytes,
       original_size_bytes, stored_size_bytes, metadata_json
     )
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb)
     ON CONFLICT (object_path) DO UPDATE
       SET content_encoding=EXCLUDED.content_encoding,
           size_bytes=EXCLUDED.size_bytes,
           original_size_bytes=EXCLUDED.original_size_bytes,
           stored_size_bytes=EXCLUDED.stored_size_bytes,
           metadata_json=crawler.raw_objects.metadata_json || EXCLUDED.metadata_json
     RETURNING raw_object_id, object_path`,
    [
      cfg.bucket,
      key,
      objectPath,
      objectType,
      entityType,
      entityId == null ? null : String(entityId),
      source ?? null,
      contentType,
      stored.contentEncoding,
      hash,
      stored.originalSizeBytes,
      stored.originalSizeBytes,
      stored.storedSizeBytes,
      JSON.stringify({
        ...(metadata ?? {}),
        storage: {
          content_encoding: stored.contentEncoding,
          original_size_bytes: stored.originalSizeBytes,
          stored_size_bytes: stored.storedSizeBytes,
          compression_ratio: stored.originalSizeBytes > 0
            ? Math.round((stored.storedSizeBytes / stored.originalSizeBytes) * 10000) / 10000
            : 1,
        },
      }),
    ],
  );
  return row.rows[0] ?? { object_path: objectPath };
}

export async function storageHealth() {
  const cfg = s3Config();
  const cli = client();
  if (!cfg || !cli) return { ok: false, configured: false };
  try {
    await ensureBucket();
    return { ok: true, configured: true, bucket: cfg.bucket, endpoint: process.env.S3_ENDPOINT };
  } catch (error) {
    return { ok: false, configured: true, bucket: cfg.bucket, endpoint: process.env.S3_ENDPOINT, error: error?.message || String(error) };
  }
}
