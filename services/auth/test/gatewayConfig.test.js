import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const nginxPath = new URL("../../../deploy/nginx/qy.conf.template", import.meta.url);
const proxyParamsPath = new URL("../../../deploy/nginx/proxy_params", import.meta.url);
const composePath = new URL("../../../deploy/compose.yml", import.meta.url);

function occurrences(text, value) {
  return text.split(value).length - 1;
}

function serverBlock(config, hostVariable) {
  const marker = `server_name \${${hostVariable}};`;
  const markerIndex = config.indexOf(marker);
  assert.notEqual(markerIndex, -1, `missing server for ${hostVariable}`);
  const start = config.lastIndexOf("server {", markerIndex);
  const next = config.indexOf("\nserver {", markerIndex);
  return config.slice(start, next === -1 ? config.length : next);
}

function composeServiceBlock(config, service) {
  const marker = `\n  ${service}:\n`;
  const markerIndex = config.indexOf(marker);
  assert.notEqual(markerIndex, -1, `missing Compose service ${service}`);
  const start = markerIndex + 1;
  const next = config.slice(start + marker.length).search(/\n  [a-z0-9][a-z0-9-]*:\n/);
  return next === -1
    ? config.slice(start)
    : config.slice(start, start + marker.length + next);
}

test("portable QY hosts enforce one authentication boundary", async () => {
  const config = await readFile(nginxPath, "utf8");
  const proxyParams = await readFile(proxyParamsPath, "utf8");
  for (const host of [
    "QY_DASHBOARD_HOST",
    "QY_BULLMQ_HOST",
    "QY_ROTA_HOST",
    "QY_MINIO_HOST",
  ]) {
    const block = serverBlock(config, host);
    assert.match(block, /auth_request \/_auth;/);
    assert.match(block, /error_page 401 = @login;/);
    assert.match(block, /proxy_set_header X-Authenticated-User \$qy_auth_user;/);
  }
  assert.equal(occurrences(config, "listen 80;"), 4);
  assert.equal(occurrences(config, "location = /_auth"), 4);
  assert.match(config, /proxy_set_header X-Original-Method \$request_method;/);
  assert.match(config, /proxy_set_header X-Auth-Renew "1";/);
  assert.match(config, /auth_request_set \$qy_auth_cookie \$upstream_http_set_cookie;/);
  assert.equal(occurrences(proxyParams, "proxy_http_version 1.1;"), 1);
  assert.equal(occurrences(config, "proxy_http_version 1.1;"), 0);
});

test("auth service consumes generated files and is not exposed directly", async () => {
  const compose = await readFile(composePath, "utf8");
  const initializer = composeServiceBlock(compose, "auth-secret-init");
  assert.match(compose, /QY_AUTH_SESSION_TTL_SECONDS: 604800/);
  assert.match(compose, /QY_AUTH_RENEW_THRESHOLD_SECONDS: 259200/);
  assert.match(compose, /QY_AUTH_USERNAME_FILE: \/run\/qy-auth-secrets\/username/);
  assert.match(compose, /QY_AUTH_PASSWORD_HASH_FILE: \/run\/qy-auth-secrets\/password_hash/);
  assert.match(initializer, /qy_auth_initial_password/);
  assert.match(initializer, /qy_auth_password_hash/);
  assert.match(initializer, /qy_auth_username/);
  assert.doesNotMatch(initializer, /\/qy-auth-secrets\/initial_password/);
  assert.doesNotMatch(composeServiceBlock(compose, "auth"), /\n    ports:/);
  assert.match(composeServiceBlock(compose, "nginx"), /test: \["CMD", "nginx", "-t"\]/);
  assert.doesNotMatch(compose, /\d{1,3}(?:-\d{1,3}){3}\.nip\.io/);
});
