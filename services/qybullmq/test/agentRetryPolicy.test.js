import assert from "node:assert/strict";
import test from "node:test";
import { agentRetryDelayMs, classifyAgentFailure } from "../src/agentRetryPolicy.js";

test("agent transport and rate-limit failures never split a batch", () => {
  assert.deepEqual(classifyAgentFailure(new Error("agent HTTP 429: quota")), {
    kind: "rate_limit", status: 429, retryable: true, splittable: false,
  });
  assert.equal(classifyAgentFailure(new Error("request timed out")).splittable, false);
  assert.equal(classifyAgentFailure(new Error("agent HTTP 503: unavailable")).splittable, false);
});

test("agent response-shape and context failures may split a batch", () => {
  const incomplete = classifyAgentFailure(new Error("agent response incomplete: []"));
  assert.equal(incomplete.retryable, true);
  assert.equal(incomplete.splittable, true);
  assert.equal(classifyAgentFailure(new Error("agent HTTP 413: payload too large")).splittable, true);
  assert.equal(classifyAgentFailure(new Error("agent HTTP 401: invalid key")).splittable, false);
});

test("native JSON parse failures split only the unresolved agent batch", () => {
  assert.deepEqual(
    classifyAgentFailure(new SyntaxError("Expected ',' or '}' after property value in JSON at position 7288")),
    { kind: "invalid_response", status: null, retryable: true, splittable: true },
  );
  assert.equal(classifyAgentFailure(new SyntaxError("Unexpected end of JSON input")).splittable, true);
  assert.equal(classifyAgentFailure(new SyntaxError("Unterminated string in JSON at position 512")).splittable, true);
  assert.equal(classifyAgentFailure(new SyntaxError("Unexpected token } in JSON at position 90")).splittable, true);
});

test("agent retry delay uses exponential jitter with a bounded deterministic value", () => {
  assert.equal(agentRetryDelayMs(1, 1000, () => 0), 500);
  assert.equal(agentRetryDelayMs(3, 1000, () => 0.5), 4000);
});
