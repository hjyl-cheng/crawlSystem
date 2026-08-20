import assert from "node:assert/strict";
import test from "node:test";
import { persistentHttpAgentOptions } from "../src/httpClient.js";

test("persistent HTTP client accepts one-hour header and body timeouts", () => {
  const options = persistentHttpAgentOptions({
    HTTP_CLIENT_CONNECTIONS: "1",
    HTTP_HEADERS_TIMEOUT_MS: "3600000",
    HTTP_BODY_TIMEOUT_MS: "3600000",
  });
  assert.equal(options.connections, 1);
  assert.equal(options.headersTimeout, 3600000);
  assert.equal(options.bodyTimeout, 3600000);
});

test("persistent HTTP client keeps bounded defaults for invalid settings", () => {
  const options = persistentHttpAgentOptions({
    HTTP_CLIENT_CONNECTIONS: "invalid",
    HTTP_HEADERS_TIMEOUT_MS: "invalid",
    HTTP_BODY_TIMEOUT_MS: "invalid",
  });
  assert.equal(options.connections, 8);
  assert.equal(options.headersTimeout, 300000);
  assert.equal(options.bodyTimeout, 300000);
});
