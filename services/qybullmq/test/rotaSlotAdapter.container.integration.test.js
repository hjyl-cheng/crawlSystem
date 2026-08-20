import assert from "node:assert/strict";
import net from "node:net";
import test from "node:test";
import pg from "pg";
import { resolveWorkerIdentityPolicy } from "../src/identityPolicyCatalog.js";
import { ProxyControlClient } from "../src/proxyControlClient.js";
import { RotaSlotAdapter } from "../src/rotaSlotAdapter.js";

const enabled = process.env.ROTA_CONTAINER_INTEGRATION === "1";
const { Pool } = pg;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function requiredEnvironment(name) {
  const value = String(process.env[name] ?? "").trim();
  if (!value) throw new Error(`${name} is required for the container integration test`);
  return value;
}

async function waitFor(predicate, { timeoutMs = 20_000, intervalMs = 100 } = {}) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() <= deadline) {
    try {
      const value = await predicate();
      if (value) return value;
    } catch (error) {
      lastError = error;
    }
    await delay(intervalMs);
  }
  throw new Error("timed out waiting for the container integration condition", {
    cause: lastError,
  });
}

async function startMarkedConnectProxy({ marker, port }) {
  const sockets = new Set();
  const server = net.createServer((socket) => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    // Route retirement intentionally resets the old upstream tunnel.
    socket.on("error", () => {});
    let requestBuffer = Buffer.alloc(0);
    let tunnelReady = false;
    socket.on("data", (chunk) => {
      if (tunnelReady) {
        socket.write(Buffer.concat([Buffer.from(marker), chunk]));
        return;
      }
      requestBuffer = Buffer.concat([requestBuffer, chunk]);
      const headerEnd = requestBuffer.indexOf("\r\n\r\n");
      if (headerEnd < 0) return;
      const header = requestBuffer.subarray(0, headerEnd).toString("utf8");
      if (!header.startsWith("CONNECT ")) {
        socket.end("HTTP/1.1 405 Method Not Allowed\r\n\r\n");
        return;
      }
      tunnelReady = true;
      socket.write("HTTP/1.1 200 Connection Established\r\n\r\n");
      const remainder = requestBuffer.subarray(headerEnd + 4);
      requestBuffer = Buffer.alloc(0);
      if (remainder.length > 0) {
        socket.write(Buffer.concat([Buffer.from(marker), remainder]));
      }
    });
  });
  await new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(port, "0.0.0.0", resolve);
  });
  return async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  };
}

function readUntil(socket, predicate, timeoutMs = 3000) {
  return new Promise((resolve, reject) => {
    let buffer = Buffer.alloc(0);
    const timer = setTimeout(() => finish(new Error("timed out reading proxy response")), timeoutMs);
    const onData = (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);
      if (predicate(buffer)) finish(null, buffer);
    };
    const onError = (error) => finish(error);
    const onEnd = () => finish(new Error("proxy connection ended before the expected response"));
    const finish = (error, value) => {
      clearTimeout(timer);
      socket.off("data", onData);
      socket.off("error", onError);
      socket.off("end", onEnd);
      if (error) reject(error);
      else resolve(value);
    };
    socket.on("data", onData);
    socket.once("error", onError);
    socket.once("end", onEnd);
  });
}

async function connectThroughProxy(proxyURL, payload, expectedMarker) {
  const parsed = new URL(proxyURL);
  const username = decodeURIComponent(parsed.username);
  const password = decodeURIComponent(parsed.password);
  const authorization = Buffer.from(`${username}:${password}`).toString("base64");
  const socket = net.createConnection({
    host: parsed.hostname,
    port: Number(parsed.port || 80),
  });
  try {
    await new Promise((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("error", reject);
    });
    socket.write([
      "CONNECT integration.example:443 HTTP/1.1",
      "Host: integration.example:443",
      `Proxy-Authorization: Basic ${authorization}`,
      "Connection: keep-alive",
      "",
      "",
    ].join("\r\n"));
    const response = await readUntil(socket, (buffer) => buffer.includes("\r\n\r\n"));
    const statusLine = response.subarray(0, response.indexOf("\r\n")).toString("utf8");
    const status = Number(statusLine.split(" ")[1]);
    if (status !== 200) {
      const error = new Error(`proxy CONNECT returned HTTP ${status}`);
      error.status = status;
      throw error;
    }
    socket.write(payload);
    const expected = `${expectedMarker}${payload}`;
    const tunneled = await readUntil(socket, (buffer) => buffer.includes(expected));
    return tunneled.toString("utf8");
  } finally {
    socket.destroy();
  }
}

async function seedEligibleProxies(pool, proxyHost) {
  const values = [
    [`${proxyHost}:19081`, "container-e2e-node-a", "container-e2e-network-a"],
    [`${proxyHost}:19082`, "container-e2e-node-b", "container-e2e-network-b"],
  ];
  for (const [address, nodeIdentity, networkIdentity] of values) {
    await pool.query(`
      INSERT INTO proxies (
        address, protocol, status, tags, node_identity,
        base_health_status, youtube_health_status,
        country_code, country_verified_at,
        egress_identity_mode, identity_valid_until,
        network_identity_key, last_identity_verified_at
      ) VALUES (
        $1, 'http', 'active', ARRAY['role:channel'], $2,
        'passed', 'passed',
        'BR', NOW(),
        'static', NOW() + interval '1 day',
        $3, NOW()
      )
    `, [address, nodeIdentity, networkIdentity]);
  }
}

test("Rota API and Worker Adapter rotate a live CONNECT route end to end", {
  skip: !enabled,
  timeout: 45_000,
}, async () => {
  const controlURL = requiredEnvironment("ROTA_PROXY_CONTROL_URL");
  const proxyBaseURL = requiredEnvironment("ROTA_PROXY_BASE_URL");
  const controlToken = requiredEnvironment("ROTA_PROXY_CONTROL_TOKEN");
  const proxyPassword = requiredEnvironment("ROTA_BULLMQ_PROXY_PASSWORD");
  const databaseURL = requiredEnvironment("ROTA_TEST_DATABASE_URL");
  const proxyHost = requiredEnvironment("ROTA_TEST_PROXY_HOST");

  const closeProxyA = await startMarkedConnectProxy({ marker: "exit-a:", port: 19081 });
  const closeProxyB = await startMarkedConnectProxy({ marker: "exit-b:", port: 19082 });
  const pool = new Pool({ connectionString: databaseURL, max: 2 });
  const rawClient = new ProxyControlClient({
    controlUrl: controlURL,
    token: controlToken,
    timeoutMs: 3000,
    maxAttempts: 3,
  });
  const commands = [];
  const client = Object.fromEntries([
    "claim", "renew", "beginTask", "observe", "completeTask", "release",
  ].map((method) => [method, async (request) => {
    commands.push(method);
    return rawClient[method](request);
  }]));
  client.capacity = () => rawClient.capacity();

  let adapter = null;
  try {
    await seedEligibleProxies(pool, proxyHost);
    await waitFor(async () => {
      const capacity = await rawClient.capacity();
      return capacity.roles?.channel?.eligible === 2
        && capacity.roles.channel.ready === 1
        && capacity.roles.channel.reserve === 1;
    });

    const resolvedPolicy = resolveWorkerIdentityPolicy({
      role: "channel",
      policyId: "qy-br-channel-anonymous-v1",
      expectedWorkloadScope: "qy-production",
      environment: {},
    });
    const routeHistory = [];
    let activeProxyURL = null;
    const identityRuntime = {
      async acquire(context) {
        const handle = {
          assignment: context.assignment,
          proxyURL: context.proxyUrl,
          async execute(_execution, invoke) {
            activeProxyURL = context.proxyUrl;
            return invoke();
          },
        };
        routeHistory.push({
          slot: context.assignment.slot_name,
          lease: context.assignment.lease_id,
          generation: context.assignment.route_generation,
          proxyUser: context.assignment.proxy_user,
          networkIdentity: context.assignment.network_identity_key,
          proxyURL: context.proxyUrl,
        });
        return handle;
      },
      async quiesce() {
        return { active_managed_requests: 0 };
      },
      async checkpoint() {},
      async retire() {},
    };
    adapter = new RotaSlotAdapter({
      client,
      role: "channel",
      workerId: "container-e2e-channel-01",
      workerInstanceId: "container-e2e-instance-01",
      resolvedPolicy,
      proxyBaseUrl: proxyBaseURL,
      proxyPassword,
      identityRuntime,
      renewIntervalMs: 30_000,
      leaseSafetyMarginMs: 1000,
      routeReadyWaitMs: 10_000,
      maxRouteSwitchesPerExecution: 2,
    });

    await adapter.start();
    const result = await adapter.executeJob({
      id: "container-e2e-job-01",
      queueName: "youtube-channel-crawl",
      attemptsMade: 0,
      data: { channel_id: "UCcontainerE2E", run_id: "container-e2e-run-01" },
    }, {
      prepare: async () => ({
        kind: "ready",
        businessRunId: "container-e2e-run-01",
        workloadKind: "channel_full",
        identityPolicyId: resolvedPolicy.policy.id,
        identityPolicyVersion: resolvedPolicy.policy.version,
        identityPolicyHash: resolvedPolicy.policy.hash,
        initialResumeMode: "initial",
      }),
      executeAttempt: async (_prepared, attempt) => {
        if (attempt.number === 1) {
          const response = await connectThroughProxy(activeProxyURL, "attempt-1", "exit-a:");
          assert.match(response, /exit-a:attempt-1/);
          return {
            kind: "retryable_network_failure",
            observation: "youtube_challenge",
            source: "youtubejs_player",
            failedStage: "content_detail",
            checkpointPersisted: true,
          };
        }
        assert.equal(attempt.number, 2);
        const response = await connectThroughProxy(activeProxyURL, "attempt-2", "exit-b:");
        assert.match(response, /exit-b:attempt-2/);
        await assert.rejects(
          connectThroughProxy(routeHistory[0].proxyURL, "stale", "exit-a:"),
          (error) => error.status === 407,
        );
        return {
          kind: "managed_work_complete",
          businessState: "terminal",
          result: { route_markers: ["exit-a", "exit-b"] },
        };
      },
    });

    assert.deepEqual(result, { route_markers: ["exit-a", "exit-b"] });
    assert.equal(routeHistory.length, 2);
    assert.deepEqual(routeHistory.map((route) => route.generation), [1, 2]);
    assert.equal(new Set(routeHistory.map((route) => route.slot)).size, 1);
    assert.equal(new Set(routeHistory.map((route) => route.lease)).size, 1);
    assert.equal(new Set(routeHistory.map((route) => route.proxyUser)).size, 2);
    assert.equal(new Set(routeHistory.map((route) => route.networkIdentity)).size, 2);
    assert.deepEqual(commands, [
      "claim", "beginTask", "observe", "completeTask",
      "renew", "beginTask", "completeTask",
    ]);

    const releasedProxyURL = routeHistory[1].proxyURL;
    await adapter.close();
    adapter = null;
    assert.equal(commands.at(-1), "release");
    await assert.rejects(
      connectThroughProxy(releasedProxyURL, "released", "exit-b:"),
      (error) => error.status === 407,
    );

    const audit = await pool.query(`
      SELECT
        (SELECT COUNT(*)::int FROM proxy_control_tasks
         WHERE workload_scope='qy-production' AND business_run_id='container-e2e-run-01') AS tasks,
        (SELECT COUNT(*)::int FROM proxy_control_observations
         WHERE workload_scope='qy-production' AND business_run_id='container-e2e-run-01'
           AND kind='youtube_challenge') AS challenges,
        (SELECT COUNT(*)::int FROM proxy_control_leases
         WHERE workload_scope='qy-production' AND worker_id='container-e2e-channel-01'
           AND status='released') AS released_leases
    `);
    assert.deepEqual(audit.rows[0], {
      tasks: 2,
      challenges: 1,
      released_leases: 1,
    });
  } finally {
    if (adapter) await adapter.close().catch(() => {});
    await rawClient.close();
    await pool.end();
    await closeProxyA();
    await closeProxyB();
  }
});
