import assert from "node:assert/strict";
import test from "node:test";
import { credentialVersion, LoginLimiter, SessionStore } from "../src/sessionStore.js";

class FakeRedis {
  constructor() {
    this.values = new Map();
    this.ttls = new Map();
  }

  async set(key, value, _mode, ttl) {
    this.values.set(key, value);
    this.ttls.set(key, Number(ttl));
    return "OK";
  }

  async get(key) {
    return this.values.get(key) ?? null;
  }

  async del(key) {
    const existed = this.values.delete(key);
    this.ttls.delete(key);
    return existed ? 1 : 0;
  }

  async incr(key) {
    const value = Number(this.values.get(key) || 0) + 1;
    this.values.set(key, String(value));
    return value;
  }

  async expire(key, ttl) {
    this.ttls.set(key, ttl);
    return 1;
  }

  async ttl(key) {
    return this.ttls.get(key) ?? -2;
  }
}

test("sessions expire after seven inactive days and renew near expiry", async () => {
  const redis = new FakeRedis();
  let now = Date.UTC(2026, 6, 29, 0, 0, 0);
  const version = credentialVersion("qyadmin", "$2b$12$hash");
  const store = new SessionStore(redis, {
    sessionTtlSeconds: 7 * 86400,
    renewThresholdSeconds: 3 * 86400,
    credentialVersion: version,
    now: () => now,
  });

  const created = await store.create("qyadmin");
  now += 3 * 86400 * 1000;
  const early = await store.verify(created.token, { renew: true });
  assert.equal(early.valid, true);
  assert.equal(early.renewed, false);

  now += 2 * 86400 * 1000;
  const renewed = await store.verify(created.token, { renew: true });
  assert.equal(renewed.valid, true);
  assert.equal(renewed.renewed, true);
  assert.equal(renewed.session.expires_at, Math.floor(now / 1000) + 7 * 86400);

  now += 7 * 86400 * 1000;
  const expired = await store.verify(created.token);
  assert.deepEqual(expired, { valid: false, reason: "expired" });
});

test("background verification never extends the session", async () => {
  const redis = new FakeRedis();
  let now = 1_000_000_000_000;
  const store = new SessionStore(redis, {
    sessionTtlSeconds: 100,
    renewThresholdSeconds: 50,
    credentialVersion: "v1",
    now: () => now,
  });
  const created = await store.create("qyadmin");
  now += 75_000;
  const verified = await store.verify(created.token, { renew: false });
  assert.equal(verified.valid, true);
  assert.equal(verified.renewed, false);
  assert.equal(verified.session.expires_at, created.session.expires_at);
});

test("logout and password changes revoke sessions", async () => {
  const redis = new FakeRedis();
  const createdBy = new SessionStore(redis, {
    credentialVersion: "old",
    now: () => 1_000_000,
  });
  const created = await createdBy.create("qyadmin");
  const afterPasswordChange = new SessionStore(redis, {
    credentialVersion: "new",
    now: () => 1_000_000,
  });
  assert.equal((await afterPasswordChange.verify(created.token)).reason, "credentials_changed");

  const second = await afterPasswordChange.create("qyadmin");
  assert.equal(await afterPasswordChange.revoke(second.token), true);
  assert.equal((await afterPasswordChange.verify(second.token)).valid, false);
});

test("login limiter blocks the configured failure window", async () => {
  const redis = new FakeRedis();
  const limiter = new LoginLimiter(redis, { maxFailures: 2, windowSeconds: 600 });
  assert.equal((await limiter.status("127.0.0.1", "qyadmin")).blocked, false);
  await limiter.fail("127.0.0.1", "qyadmin");
  assert.equal((await limiter.fail("127.0.0.1", "qyadmin")).blocked, true);
  await limiter.clear("127.0.0.1", "qyadmin");
  assert.equal((await limiter.status("127.0.0.1", "qyadmin")).count, 0);
});
