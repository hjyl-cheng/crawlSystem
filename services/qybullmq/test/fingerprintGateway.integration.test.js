import assert from "node:assert/strict";
import http from "node:http";
import test from "node:test";
import { FingerprintGateway } from "../src/fingerprintGateway.js";

async function listen(server) {
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolve);
  });
  return server.address().port;
}

async function unusedLocalPort() {
  const server = http.createServer();
  const port = await listen(server);
  await new Promise((resolve) => server.close(resolve));
  return port;
}

test("real curl_cffi gateway keeps profile cookies while using the configured proxy", { timeout: 15000 }, async () => {
  const requests = [];
  const proxyServer = http.createServer((request, response) => {
    requests.push({ url: request.url, userAgent: request.headers["user-agent"] });
    response.writeHead(202, {
      "content-type": "text/plain",
      "set-cookie": "VISITOR_INFO1_LIVE=local-cookie; Path=/; HttpOnly",
      "x-local-proxy": "yes",
    });
    response.end("proxied-locally");
  });
  const proxyPort = await listen(proxyServer);
  const gateway = new FingerprintGateway({ port: await unusedLocalPort() });
  const chrome = {
    profile_id: "integration-chrome",
    engine: "youtubejs_chrome",
    impersonate_target: "chrome136",
    user_agent: "Integration Chrome UA",
    fingerprint_json: { max_connections: 2 },
    cookie_state: { cookies: [] },
  };

  try {
    await gateway.prepare({
      proxyUrl: `http://127.0.0.1:${proxyPort}`,
      profileGroup: {
        profile_group_id: "integration-group",
        profile_revision: 1,
        clients: { youtubejs_chrome: chrome },
      },
    });
    const response = await gateway.fetch(chrome, "http://fingerprint.invalid/channel", {
      headers: { accept: "text/plain" },
      timeoutMs: 5000,
    });
    const responseText = await response.text();
    const snapshot = await gateway.snapshot(chrome);

    assert.equal(response.status, 202);
    assert.equal(response.headers.get("x-local-proxy"), "yes");
    assert.equal(responseText, "proxied-locally");
    assert.equal(requests.length, 1);
    assert.equal(requests[0].url, "http://fingerprint.invalid/channel");
    assert.equal(requests[0].userAgent, "Integration Chrome UA");
    assert.equal(snapshot.cookies.find((cookie) => cookie.name === "VISITOR_INFO1_LIVE")?.value, "local-cookie");
  } finally {
    await gateway.close();
    await new Promise((resolve) => proxyServer.close(resolve));
  }
});
