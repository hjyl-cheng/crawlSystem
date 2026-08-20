import assert from "node:assert/strict";
import { gunzip } from "node:zlib";
import { promisify } from "node:util";
import test from "node:test";
import { prepareRawPayload } from "../src/rawCompression.js";

const gunzipAsync = promisify(gunzip);

test("raw HTML and JSON payloads use reversible gzip storage", async () => {
  const source = Buffer.from(`<html>${"channel-data".repeat(1000)}</html>`);
  const prepared = await prepareRawPayload(source, "text/html; charset=utf-8");
  assert.equal(prepared.contentEncoding, "gzip");
  assert.ok(prepared.storedSizeBytes < prepared.originalSizeBytes);
  assert.deepEqual(await gunzipAsync(prepared.body), source);
});

test("small and binary raw payloads remain unchanged", async () => {
  assert.equal((await prepareRawPayload(Buffer.from("{}"), "application/json")).contentEncoding, null);
  assert.equal((await prepareRawPayload(Buffer.alloc(2048), "application/octet-stream")).contentEncoding, null);
});
