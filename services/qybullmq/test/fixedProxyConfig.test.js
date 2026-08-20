import assert from "node:assert/strict";
import test from "node:test";
import {
  dynamicRotaProxyConfig,
  fixedRotaProxyConfig,
} from "../src/fixedProxyConfig.js";

test("dynamic Rota proxy config is disabled only when no slot role is requested", () => {
  assert.equal(dynamicRotaProxyConfig({}), null);
  assert.equal(dynamicRotaProxyConfig({
    controlUrl: "http://rota:8001/api/v1/proxy-control",
    controlToken: "control-token-secret",
    proxyPassword: "worker-password-secret",
  }), null);
});

test("dynamic Rota proxy config requires a complete valid control plane", () => {
  const base = {
    slotRole: "channel",
    controlUrl: "http://rota:8001/api/v1/proxy-control",
    controlToken: "control-token-secret",
    proxyPassword: "worker-password-secret",
  };
  assert.deepEqual(dynamicRotaProxyConfig(base), {
    role: "channel",
    controlUrl: "http://rota:8001/api/v1/proxy-control",
  });
  assert.throws(
    () => dynamicRotaProxyConfig({ ...base, slotRole: "unknown" }),
    /unsupported PROXY_SLOT_ROLE/,
  );
  assert.throws(
    () => dynamicRotaProxyConfig({ ...base, controlUrl: "" }),
    /ROTA_PROXY_CONTROL_URL/,
  );
  assert.throws(
    () => dynamicRotaProxyConfig({ ...base, controlToken: "short" }),
    /ROTA_PROXY_CONTROL_TOKEN/,
  );
  assert.throws(
    () => dynamicRotaProxyConfig({ ...base, proxyPassword: "short" }),
    /ROTA_BULLMQ_PROXY_PASSWORD/,
  );
});

test("fixed Rota proxy config binds one channel worker to one managed proxy user", () => {
  const config = fixedRotaProxyConfig({
    proxyUser: "bullmq-channel-21",
    proxyPassword: "secret:with@symbols",
    baseUrl: "http://youtube-rota-qy-core:8000",
  });

  assert.equal(config.proxyUrl, "http://bullmq-channel-21:secret%3Awith%40symbols@youtube-rota-qy-core:8000/");
  assert.deepEqual(config.identity, {
    role: "channel",
    slot_name: "bullmq-channel-21",
    proxy_id: null,
  });
});

test("fixed proxy user and dynamic slot claiming are mutually exclusive", () => {
  assert.throws(() => fixedRotaProxyConfig({
    proxyUser: "bullmq-channel-21",
    proxyPassword: "secret",
    slotRole: "channel",
  }), /cannot be used together/);
});

test("empty fixed proxy user preserves the normal worker configuration", () => {
  assert.equal(fixedRotaProxyConfig({ proxyUser: "" }), null);
});
