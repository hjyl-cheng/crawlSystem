import assert from "node:assert/strict";
import test from "node:test";
import {
  allowedReturnTo,
  clearedSessionCookie,
  mutationOriginAllowed,
  sessionCookie,
  sessionTokenFromCookie,
} from "../src/httpPolicy.js";

const policy = {
  dashboardOrigin: "https://qydashboard.example.test",
  allowedHosts: new Set([
    "qydashboard.example.test",
    "qybullmq.example.test",
  ]),
};

test("return destinations allow only QY HTTPS hosts", () => {
  assert.equal(
    allowedReturnTo("/migration-channels", policy),
    "https://qydashboard.example.test/migration-channels",
  );
  assert.equal(
    allowedReturnTo("https://qybullmq.example.test/queues", policy),
    "https://qybullmq.example.test/queues",
  );
  assert.equal(
    allowedReturnTo("https://attacker.example/", policy),
    "https://qydashboard.example.test/",
  );
  assert.equal(
    allowedReturnTo("//attacker.example/", policy),
    "https://qydashboard.example.test/",
  );
});

test("session cookie has the required browser protections", () => {
  const cookie = sessionCookie({
    name: "__Secure-qy_session",
    token: "token-value",
    domain: "example.test",
    maxAgeSeconds: 604800,
  });
  assert.match(cookie, /^__Secure-qy_session=token-value;/);
  assert.match(cookie, /Max-Age=604800/);
  assert.match(cookie, /Domain=example\.test/);
  assert.match(cookie, /HttpOnly/);
  assert.match(cookie, /Secure/);
  assert.match(cookie, /SameSite=Strict/);
  assert.match(clearedSessionCookie({
    name: "__Secure-qy_session",
    domain: "example.test",
  }), /Max-Age=0/);
});

test("duplicate session cookies are rejected", () => {
  assert.equal(
    sessionTokenFromCookie("a=1; __Secure-qy_session=good", "__Secure-qy_session"),
    "good",
  );
  assert.equal(
    sessionTokenFromCookie(
      "__Secure-qy_session=first; __Secure-qy_session=second",
      "__Secure-qy_session",
    ),
    null,
  );
});

test("mutating requests must come from the request host", () => {
  const expectedOrigin = "https://qydashboard.example.test";
  assert.equal(mutationOriginAllowed({ method: "GET", expectedOrigin }), true);
  assert.equal(mutationOriginAllowed({
    method: "POST",
    origin: expectedOrigin,
    expectedOrigin,
  }), true);
  assert.equal(mutationOriginAllowed({
    method: "POST",
    origin: "https://attacker.example",
    expectedOrigin,
  }), false);
  assert.equal(mutationOriginAllowed({
    method: "POST",
    referer: `${expectedOrigin}/migration-channels`,
    expectedOrigin,
  }), true);
});
