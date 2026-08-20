import assert from "node:assert/strict";
import test from "node:test";
import { RotaAdminClient } from "../src/rotaAdminClient.js";

function response(status, payload) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async text() { return JSON.stringify(payload); },
  };
}

test("Rota admin client refreshes one proxy user immediately", async () => {
  const calls = [];
  const client = new RotaAdminClient({
    baseUrl: "http://rota:8001",
    username: "admin",
    password: "secret",
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      if (url.endsWith("/api/v1/auth/login")) return response(200, { token: "token-1" });
      return response(200, { ok: true, username: "bullmq-channel-04" });
    },
  });

  const result = await client.refreshProxyUser("bullmq-channel-04");

  assert.equal(result.ok, true);
  assert.equal(calls.length, 2);
  assert.equal(calls[1].url, "http://rota:8001/api/v1/proxy-users/refresh");
  assert.equal(calls[1].init.headers.authorization, "Bearer token-1");
  assert.deepEqual(JSON.parse(calls[1].init.body), { username: "bullmq-channel-04" });
});

test("Rota admin client reauthenticates once after an expired token", async () => {
  const calls = [];
  let loginCount = 0;
  const client = new RotaAdminClient({
    baseUrl: "http://rota:8001",
    username: "admin",
    password: "secret",
    fetchImpl: async (url, init) => {
      calls.push({ url, init });
      if (url.endsWith("/api/v1/auth/login")) {
        loginCount += 1;
        return response(200, { token: `token-${loginCount}` });
      }
      if (init.headers.authorization === "Bearer token-1") return response(401, { error: "expired" });
      return response(200, { id: 235, status: "active" });
    },
  });

  const result = await client.testProxy(235);

  assert.equal(result.status, "active");
  assert.equal(loginCount, 2);
  assert.equal(calls.at(-1).init.headers.authorization, "Bearer token-2");
});
